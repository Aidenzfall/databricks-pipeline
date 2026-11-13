"""Generate the committed sample batches using the shared project code."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from orders_pipeline.sample_data import generate  # noqa: E402


if __name__ == "__main__":
    for sample_path in generate():
        print(sample_path)

