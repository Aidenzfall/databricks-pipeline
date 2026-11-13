"""Command-line interface for generating, uploading, and running the pipeline."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from orders_pipeline.sample_data import generate


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = REPO_ROOT / "data" / "sample" / "orders"
DEFAULT_VOLUME_PATH = "/Volumes/main/orders_demo/landing/orders/"


def _databricks(args: list[str], profile: str | None = None) -> int:
    executable = shutil.which("databricks")
    if executable is None:
        print("Databricks CLI not found. Install it and configure workspace authentication first.", file=sys.stderr)
        return 127
    command = [executable, *args]
    if profile:
        command.extend(["--profile", profile])
    print("+ " + " ".join(command))
    return subprocess.run(command, check=False).returncode


def _add_profile(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", help="Databricks CLI authentication profile")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orders-pipeline", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    data = commands.add_parser("data", help="Generate or upload sample input")
    data_commands = data.add_subparsers(dest="data_command", required=True)
    generate_parser = data_commands.add_parser("generate", help="Generate the deterministic JSONL batches")
    generate_parser.add_argument("--output-dir", type=Path, default=DEFAULT_DATA_DIR)
    upload_parser = data_commands.add_parser("upload", help="Upload a sample batch to a Unity Catalog Volume")
    upload_parser.add_argument("--input-dir", type=Path, default=DEFAULT_DATA_DIR)
    upload_parser.add_argument("--volume-path", default=DEFAULT_VOLUME_PATH)
    upload_parser.add_argument("--batch", choices=("1", "2", "all"), default="all")
    _add_profile(upload_parser)

    for action, help_text in (
        ("validate", "Validate the Databricks bundle configuration"),
        ("deploy", "Deploy the job defined by the bundle"),
        ("run", "Run the deployed orders pipeline job"),
    ):
        command = commands.add_parser(action, help=help_text)
        command.add_argument("--target", default="dev", help="Bundle target (default: dev)")
        _add_profile(command)
        if action == "run":
            command.add_argument("--no-wait", action="store_true", help="Return before the job run completes")

    return parser


def main() -> int:
    args = _build_parser().parse_args()
    if args.command == "data" and args.data_command == "generate":
        for path in generate(args.output_dir):
            print(path)
        return 0

    if args.command == "data" and args.data_command == "upload":
        if not args.volume_path.startswith("/Volumes/"):
            print("--volume-path must start with /Volumes/", file=sys.stderr)
            return 2
        files = {
            "1": ["batch_001.jsonl"],
            "2": ["batch_002.jsonl"],
            "all": ["batch_001.jsonl", "batch_002.jsonl"],
        }[args.batch]
        source_files = [args.input_dir / name for name in files]
        missing = [path for path in source_files if not path.is_file()]
        if missing:
            print("Sample files missing. Run `orders-pipeline data generate` first.", file=sys.stderr)
            return 2
        remote_dir = "dbfs:" + args.volume_path.rstrip("/")
        status = _databricks(["fs", "mkdir", remote_dir], args.profile)
        if status:
            return status
        for source in source_files:
            destination = remote_dir + "/" + source.name
            status = _databricks(["fs", "cp", str(source.resolve()), destination, "--overwrite"], args.profile)
            if status:
                return status
        return 0

    action = args.command
    command = ["bundle", action, "--target", args.target]
    if action == "run":
        if args.no_wait:
            command.append("--no-wait")
        command.append("orders_pipeline_job")
    return _databricks(command, args.profile)


if __name__ == "__main__":
    raise SystemExit(main())

