from __future__ import annotations

import argparse
import logging
import os
import shlex
import sys
from pathlib import Path

from lane_geometry.config import JobConfig, load_app_config
from lane_geometry.job import run

LOGGER = logging.getLogger(__name__)
SOURCE_TYPE_ENV = "SOURCE_TYPE"
SOURCE_DATASET_ID_ENV = "SOURCE_DATASET_ID"


def get_optional_env(env_name: str) -> str | None:
    value = os.getenv(env_name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def log_progress(message: str) -> None:
    print(message, flush=True)


def expand_argv(argv: list[str]) -> list[str]:
    expanded: list[str] = []
    for arg in argv:
        if arg.startswith("--") and " " in arg:
            expanded.extend(shlex.split(arg))
        else:
            expanded.append(arg)
    return expanded


def build_parser() -> argparse.ArgumentParser:
    app_config = load_app_config()
    runtime_defaults = app_config["runtime"]

    parser = argparse.ArgumentParser(
        description=(
            "Compute road geometry from lane labels stored in BigQuery/GCS "
            "and merge the result into BigQuery."
        )
    )
    parser.add_argument(
        "--source-type",
        default=get_optional_env(SOURCE_TYPE_ENV),
        choices=["public"],
        help=f"Image source type to process. Required unless {SOURCE_TYPE_ENV} is set.",
    )
    parser.add_argument(
        "--source-dataset-id",
        default=get_optional_env(SOURCE_DATASET_ID_ENV),
        help=(
            "Source dataset to process, for example culane_v1. Required unless "
            f"{SOURCE_DATASET_ID_ENV} is set."
        ),
    )
    parser.add_argument(
        "--image-id",
        dest="image_ids",
        action="append",
        default=None,
        help="Specific image_id to process. Can be passed multiple times.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional cap for total images to process. If omitted, the job "
            "processes all pending images for the dataset."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=runtime_defaults["default_batch_size"],
        help="Number of images to process together inside the lane-geometry loop.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=runtime_defaults["default_workers"],
        help="Number of images to process in parallel inside the batch.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview results without writing them to BigQuery.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to dump lane-geometry results as JSON.",
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(expand_argv(sys.argv[1:]))
    log_progress(
        "Starting lane-geometry job "
        f"source_type={args.source_type or 'ENV'} "
        f"source_dataset_id={args.source_dataset_id or 'ENV'} "
        f"limit={args.limit} batch_size={args.batch_size} "
        f"workers={args.workers} dry_run={args.dry_run}"
    )
    run(
        JobConfig.from_values(
            source_type=args.source_type,
            source_dataset_id=args.source_dataset_id,
            batch_size=args.batch_size,
            image_ids=args.image_ids,
            limit=args.limit,
            workers=args.workers,
            dry_run=args.dry_run,
            output_json=args.output_json,
        )
    )


if __name__ == "__main__":
    main()
