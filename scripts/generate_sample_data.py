"""Generate deterministic JSONL order events for the Databricks demo."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


EVENTS = {
    "batch_001.jsonl": [
        {
            "order_id": "1001",
            "customer_id": "C01",
            "order_ts": "2026-11-10 09:15:00",
            "quantity": "2",
            "unit_price": "19.99",
            "status": "COMPLETE",
            "updated_at": "2026-11-10 09:20:00",
        },
        {
            "order_id": "1002",
            "customer_id": "C02",
            "order_ts": "2026-11-10 10:00:00",
            "quantity": "1",
            "unit_price": "8.50",
            "status": "COMPLETE",
            "updated_at": "2026-11-10 10:02:00",
        },
        {
            "order_id": "1004",
            "customer_id": "C03",
            "order_ts": "2026-11-11 11:30:00",
            "quantity": "3",
            "unit_price": "12.00",
            "status": "COMPLETE",
            "updated_at": "2026-11-11 11:31:00",
        },
    ],
    "batch_002.jsonl": [
        # Exact replay of an event: ingestion should not change final metrics.
        {
            "order_id": "1001",
            "customer_id": "C01",
            "order_ts": "2026-11-10 09:15:00",
            "quantity": "2",
            "unit_price": "19.99",
            "status": "COMPLETE",
            "updated_at": "2026-11-10 09:20:00",
        },
        # A later update for the same order supersedes its earlier state.
        {
            "order_id": "1001",
            "customer_id": "C01",
            "order_ts": "2026-11-10 09:15:00",
            "quantity": "3",
            "unit_price": "19.99",
            "status": "COMPLETE",
            "updated_at": "2026-11-12 10:00:00",
        },
        # Arrives in a later batch even though the business order date is earlier.
        {
            "order_id": "1003",
            "customer_id": "C02",
            "order_ts": "2026-11-09 16:45:00",
            "quantity": "1",
            "unit_price": "42.00",
            "status": "COMPLETE",
            "updated_at": "2026-11-12 12:00:00",
        },
        # Intentionally invalid: should be routed to quarantine.
        {
            "order_id": "1005",
            "customer_id": None,
            "order_ts": "not-a-timestamp",
            "quantity": "0",
            "unit_price": "-4.00",
            "status": "PENDING",
            "updated_at": "2026-11-12 13:00:00",
        },
    ],
}


def generate(output_dir: Path) -> list[Path]:
    """Write the two deterministic batches and return their paths."""
    output_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for filename, rows in EVENTS.items():
        path = output_dir / filename
        with path.open("w", encoding="utf-8", newline="\n") as output:
            for row in rows:
                output.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False))
                output.write("\n")
        written.append(path)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "sample" / "orders",
        help="Directory to receive batch_001.jsonl and batch_002.jsonl",
    )
    args = parser.parse_args()
    for path in generate(args.output_dir):
        print(path)


if __name__ == "__main__":
    main()

