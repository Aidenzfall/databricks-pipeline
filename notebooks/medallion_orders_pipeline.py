# Databricks notebook source
# MAGIC %md
# MAGIC # Orders pipeline with the medallion architecture
# MAGIC
# MAGIC Runnable starter: sample records flow through Bronze (raw), Silver (validated and deduplicated), and Gold (daily sales metrics). Generate the sample JSONL batches and upload them to a Unity Catalog Volume to run the pipeline.
# MAGIC
# MAGIC Assumptions: Unity Catalog is enabled and tables use Delta. Set the catalog and schema widgets before running.

# COMMAND ----------

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
dbutils.widgets.text("schema", "orders_demo", "Target schema")
dbutils.widgets.text("source_path", "/Volumes/main/orders_demo/landing/orders/", "JSONL landing directory")
dbutils.widgets.text("state_path", "/Volumes/main/orders_demo/landing/_checkpoints/orders/", "Auto Loader state directory")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
source_path = dbutils.widgets.get("source_path").strip()
state_path = dbutils.widgets.get("state_path").strip().rstrip("/")

if not catalog or not schema or not source_path or not state_path:
    raise ValueError("catalog, schema, source_path, and state_path widgets must be set")
if not source_path.startswith("/Volumes/") or not state_path.startswith("/Volumes/"):
    raise ValueError("source_path and state_path must be Unity Catalog Volume paths")
if state_path.startswith(source_path.rstrip("/") + "/"):
    raise ValueError("state_path must be outside the source_path directory")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
bronze_table = f"{catalog}.{schema}.bronze_orders"
silver_table = f"{catalog}.{schema}.silver_orders"
quarantine_table = f"{catalog}.{schema}.quarantine_orders"
gold_table = f"{catalog}.{schema}.gold_daily_sales"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source
# MAGIC Auto Loader incrementally reads JSONL batches from a Unity Catalog Volume. `state_path` stores its schema and checkpoint state, outside the input directory. Upload one batch, run the notebook, then upload the second batch and run again to see incremental ingestion.

# COMMAND ----------

source_schema = T.StructType([
    T.StructField("order_id", T.StringType(), True),
    T.StructField("customer_id", T.StringType(), True),
    T.StructField("order_ts", T.StringType(), True),
    T.StructField("quantity", T.StringType(), True),
    T.StructField("unit_price", T.StringType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("updated_at", T.StringType(), True),
])
schema_location = f"{state_path}/schema"
checkpoint_location = f"{state_path}/checkpoint"
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_location)
    .schema(source_schema)
    .load(source_path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .drop("_metadata")
)
bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .toTable(bronze_table)
)
bronze_query.awaitTermination()
print(f"Bronze rows: {spark.table(bronze_table).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: type, validate, and deduplicate
# MAGIC Rows with missing keys, invalid timestamps, nonpositive quantities, or negative prices go to quarantine with a reason. Valid records are deduplicated by `order_id`, keeping the latest `updated_at`.

# COMMAND ----------

typed = (
    spark.table(bronze_table)
    .withColumn("order_ts", F.expr("try_cast(order_ts AS TIMESTAMP)"))
    .withColumn("updated_at", F.expr("try_cast(updated_at AS TIMESTAMP)"))
    .withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
    .withColumn("unit_price", F.expr("try_cast(unit_price AS DECIMAL(12,2))"))
)
quality_issue = (
    F.when(F.col("order_id").isNull() | (F.trim("order_id") == ""), "missing_order_id")
     .when(F.col("customer_id").isNull() | (F.trim("customer_id") == ""), "missing_customer_id")
     .when(F.col("order_ts").isNull(), "invalid_order_ts")
     .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
     .when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), "invalid_unit_price")
     .otherwise(None)
)
classified = typed.withColumn("_quality_issue", quality_issue)
quarantine_df = classified.filter(F.col("_quality_issue").isNotNull())
(quarantine_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(quarantine_table))

valid = classified.filter(F.col("_quality_issue").isNull()).drop("_quality_issue")
latest = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("_ingested_at").desc())
silver_df = (
    valid
    .withColumn("_row_num", F.row_number().over(latest))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "_source_file")
    .withColumn("revenue", (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)"))
    .withColumn("_processed_at", F.current_timestamp())
)
(silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table))
print(f"Silver rows: {spark.table(silver_table).count()} | Quarantined rows: {spark.table(quarantine_table).count()}")
display(spark.table(silver_table))
display(spark.table(quarantine_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: daily sales metrics
# MAGIC Gold is an analytics-ready aggregate built only from cleaned Silver records.

# COMMAND ----------

gold_df = (
    spark.table(silver_table)
    .filter(F.upper(F.col("status")) == "COMPLETE")
    .groupBy(F.to_date("order_ts").alias("order_date"))
    .agg(
        F.countDistinct("order_id").alias("completed_orders"),
        F.countDistinct("customer_id").alias("customers"),
        F.sum("quantity").alias("units_sold"),
        F.sum("revenue").cast("decimal(16,2)").alias("gross_revenue"),
    )
    .withColumn("_refreshed_at", F.current_timestamp())
)
(gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table))
display(spark.table(gold_table).orderBy("order_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production next steps
# MAGIC - Bronze uses Auto Loader checkpoints and appends newly discovered files. Keep the input location append-only; do not overwrite previously ingested files under the same names.
# MAGIC - Silver and Gold are full refreshes in this starter. For large data, move Silver to an incremental `MERGE` keyed by `order_id` and refresh only affected Gold dates.
# MAGIC - Add expectations and alerting for quarantine volume, data freshness, and business metrics.
# MAGIC - Schedule this notebook as a Databricks Workflow job and grant its run identity only the required catalog, schema, Volume, and storage permissions.

