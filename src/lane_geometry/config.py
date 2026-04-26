from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lane_geometry.datasets.public.registry import supported_public_dataset_ids

DEFAULT_PROJECT_ID = "vr-prj-prod-data-v1"
DEFAULT_METADATA_DATASET = "ds_raw_metadata"
DEFAULT_CLASSIFICATION_DATASET = "ds_classification"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_WORKERS = 8


def load_app_config() -> dict[str, Any]:
    return {
        "runtime": {
            "default_batch_size": DEFAULT_BATCH_SIZE,
            "default_workers": DEFAULT_WORKERS,
        }
    }


@dataclass(frozen=True)
class JobConfig:
    project_id: str
    metadata_dataset: str
    classification_dataset: str
    source_type: str | None
    source_dataset_id: str | None
    batch_size: int
    image_ids: list[str] | None = None
    limit: int | None = None
    workers: int = DEFAULT_WORKERS
    dry_run: bool = False
    output_json: Path | None = None

    @classmethod
    def from_env(cls) -> "JobConfig":
        config = cls(
            project_id=DEFAULT_PROJECT_ID,
            metadata_dataset=DEFAULT_METADATA_DATASET,
            classification_dataset=DEFAULT_CLASSIFICATION_DATASET,
            source_type=os.getenv("SOURCE_TYPE"),
            source_dataset_id=os.getenv("SOURCE_DATASET_ID"),
            batch_size=DEFAULT_BATCH_SIZE,
        )
        config.validate()
        return config

    @classmethod
    def from_values(
        cls,
        source_type: str | None = None,
        source_dataset_id: str | None = None,
        batch_size: int | None = None,
        image_ids: list[str] | None = None,
        limit: int | None = None,
        workers: int | None = None,
        dry_run: bool = False,
        output_json: Path | None = None,
    ) -> "JobConfig":
        config = cls(
            project_id=DEFAULT_PROJECT_ID,
            metadata_dataset=DEFAULT_METADATA_DATASET,
            classification_dataset=DEFAULT_CLASSIFICATION_DATASET,
            source_type=source_type or os.getenv("SOURCE_TYPE"),
            source_dataset_id=source_dataset_id or os.getenv("SOURCE_DATASET_ID"),
            batch_size=batch_size if batch_size is not None else DEFAULT_BATCH_SIZE,
            image_ids=image_ids,
            limit=limit,
            workers=workers if workers is not None else DEFAULT_WORKERS,
            dry_run=dry_run,
            output_json=output_json,
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.source_type:
            raise ValueError(
                "SOURCE_TYPE is required. Provide it as an environment variable "
                "or with --source-type."
            )
        if not self.source_dataset_id:
            raise ValueError(
                "SOURCE_DATASET_ID is required. Provide it as an environment "
                "variable or with --source-dataset-id."
            )
        if self.source_type != "public":
            raise ValueError("This job currently supports SOURCE_TYPE=public only.")
        if self.source_dataset_id not in supported_public_dataset_ids():
            supported = ", ".join(sorted(supported_public_dataset_ids()))
            raise ValueError(
                f"Unsupported SOURCE_DATASET_ID={self.source_dataset_id}. "
                f"Supported values: {supported}."
            )
        if self.batch_size <= 0:
            raise ValueError("BATCH_SIZE must be positive.")
        if self.limit is not None and self.limit <= 0:
            raise ValueError("limit must be positive when provided.")
        if self.workers <= 0:
            raise ValueError("workers must be positive.")
        if self.image_ids is not None and not self.image_ids:
            raise ValueError("image_ids must not be empty when provided.")
