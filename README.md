# Databricks medallion orders pipeline

An incremental orders pipeline starter using Auto Loader, Delta Lake, Unity Catalog, and a Databricks bundle job.

## Architecture

```text
JSONL files in a Unity Catalog Volume
  -> Auto Loader with a durable checkpoint
  -> Bronze Delta table (append-only source history)
  -> Silver Delta table (typed latest order state + quarantine)
  -> Gold Delta table (daily completed sales)
```

Bronze discovers only new files through its Auto Loader checkpoint. Silver and Gold are recomputed from Bronze on each job run in this starter, which keeps the example easy to follow. The next scale-up step is an incremental Silver `MERGE` and refreshing only affected Gold dates.

## Prerequisites

- Python 3.10 or newer.
- The [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/) installed and authenticated to your workspace.
- A Unity Catalog catalog and schema, plus a Volume named `landing` inside the schema. The job identity needs read access to the files and write access to the Volume state directory and target tables.
- Serverless jobs enabled in the workspace, or a project-specific compute configuration added to `databricks.yml`.

The notebook and bundle default to catalog `main` and schema `orders_demo`. Override these values in `databricks.yml` or with bundle variables for your workspace.

## Install the project CLI

From the repository root:

```bash
python -m pip install -e .
orders-pipeline --help
```

Generate deterministic batches and upload the first batch:

```bash
orders-pipeline data generate
orders-pipeline data upload --batch 1
```

Deploy and run the bundle job:

```bash
orders-pipeline validate --target dev
orders-pipeline deploy --target dev
orders-pipeline run --target dev
```

Then upload the second batch and run again:

```bash
orders-pipeline data upload --batch 2
orders-pipeline run --target dev
```

The commands accept `--profile PROFILE` to use a named Databricks CLI authentication profile. `data upload` also accepts `--volume-path` if your Volume differs from the default `/Volumes/main/orders_demo/landing/orders/`.

## Auto Loader state and safe replays

The notebook reads JSONL from `/Volumes/main/orders_demo/landing/orders/`. It writes schema and checkpoint state under `/Volumes/main/orders_demo/landing/_checkpoints/orders/`, outside the source directory. Keep the source directory append-only: publish each incoming file once with a new filename. Replacing an already ingested file under the same path does not make it a new Auto Loader input. To replay the complete sample, use a fresh state path and fresh target tables.

`orders-pipeline data upload --batch 1` and `--batch 2` are intended to demonstrate separate arrivals. `--batch all` uploads both files before the first run.

## Layers

- **Bronze:** source-shaped JSON records plus ingestion timestamp and source file path; append-only via Auto Loader.
- **Silver:** typed and validated current order state, with invalid rows in a quarantine table.
- **Gold:** daily completed order, customer, unit, and revenue metrics.

See the [orders source contract](docs/data-contract.md) for field definitions and sample cases.

