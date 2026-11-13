# Databricks medallion orders pipeline

Starter project for a Delta Lake orders pipeline using Bronze, Silver, and Gold layers in Unity Catalog.

## Current state

- `notebooks/medallion_orders_pipeline.py` is the Databricks notebook source.
- `scripts/generate_sample_data.py` creates deterministic JSONL input in two batches.
- `docs/data-contract.md` documents the source fields and sample scenarios.
- The notebook currently reads the landing directory as a batch. Incremental Auto Loader ingestion, repeat-safe writes, CI, and deployment config are planned next.

## Generate and upload the sample input

Run locally with Python 3.10 or newer:

```bash
python scripts/generate_sample_data.py
```

This creates `data/sample/orders/batch_001.jsonl` and `batch_002.jsonl`. Upload both files to a Unity Catalog Volume directory such as `/Volumes/main/orders_demo/landing/orders/` (create the catalog, schema, and Volume first). In the notebook, set `catalog=main`, `schema=orders_demo`, and `source_path=/Volumes/main/orders_demo/landing/orders/`.

## Layers

- **Bronze:** source-shaped rows plus ingestion metadata.
- **Silver:** typed and validated current order state, with invalid rows in a quarantine table.
- **Gold:** daily metrics for completed orders.

The sample includes two orders on the first day, a later update for one order, a duplicate replay, an older event arriving in the later batch, and one invalid record. See the [data contract](docs/data-contract.md).

