# Databricks notebook source
# MAGIC %md
# MAGIC # Orders pipeline with the medallion architecture
# MAGIC
# MAGIC Runnable starter: sample records flow through Bronze (raw), Silver (validated and deduplicated), and Gold (daily sales metrics). The inline source lets you try the pipeline without external files. Replace the source cell with a read from a landing location to connect real data.
# MAGIC
# MAGIC Assumptions: Unity Catalog is enabled and tables use Delta. Set the catalog and schema widgets before running.

# COMMAND ----------

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
dbutils.widgets.text("schema", "orders_demo", "Target schema")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()

if not catalog or not schema:
    raise ValueError("catalog and schema widgets must be set")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
bronze_table = f"{catalog}.{schema}.bronze_orders"
silver_table = f"{catalog}.{schema}.silver_orders"
quarantine_table = f"{catalog}.{schema}.quarantine_orders"
gold_table = f"{catalog}.{schema}.gold_daily_sales"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source
# MAGIC The sample includes a duplicate order and an invalid row to demonstrate deduplication and quarantine. Replace this DataFrame with a read from cloud storage, a Volume, or a source table.

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
sample_rows = [
    ("1001", "C01", "2026-11-10 09:15:00", "2", "19.99", "COMPLETE", "2026-11-10 09:20:00"),
    ("1002", "C02", "2026-11-10 10:00:00", "1", "8.50", "COMPLETE", "2026-11-10 10:02:00"),
    ("1001", "C01", "2026-11-10 09:15:00", "2", "19.99", "COMPLETE", "2026-11-10 09:25:00"),  # later duplicate
    ("1003", None, "not-a-timestamp", "0", "-4.00", "PENDING", "2026-11-11 08:00:00"),  # invalid
    ("1004", "C03", "2026-11-11 11:30:00", "3", "12.00", "COMPLETE", "2026-11-11 11:31:00"),
]
source_df = spark.createDataFrame(sample_rows, source_schema)
display(source_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: preserve source records
# MAGIC Bronze keeps the input columns and adds ingestion metadata. This demo overwrites its small sample table on each run. For real file ingestion, use Auto Loader with a checkpoint and append writes.

# COMMAND ----------

bronze_df = (
    source_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("inline_sample"))
)
(bronze_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table))
print(f"Bronze rows: {spark.table(bronze_table).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: type, validate, and deduplicate
# MAGIC Rows with missing keys, invalid timestamps, nonpositive quantities, or negative prices go to quarantine with a reason. Valid records are deduplicated by `order_id`, keeping the latest `updated_at`.

# COMMAND ----------

typed = (
    spark.table(bronze_table)
    .withColumn("order_ts", F.to_timestamp("order_ts"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
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
# MAGIC - Replace the inline sample with an Auto Loader stream from a Unity Catalog Volume or cloud storage location, using a durable checkpoint and append writes to Bronze.
# MAGIC - Choose an incremental strategy for Silver and Gold (for example, Delta `MERGE` keyed by `order_id`) and define late-arriving data handling.
# MAGIC - Add expectations and alerting for quarantine volume, data freshness, and business metrics.
# MAGIC - Schedule this notebook as a Databricks Workflow job and grant its run identity only the required catalog, schema, Volume, and storage permissions.

