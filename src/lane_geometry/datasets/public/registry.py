from __future__ import annotations

from lane_geometry.datasets.base import DatasetDefinition
from lane_geometry.datasets.public.culane import CULaneDataset
from lane_geometry.datasets.public.curvelanes import CurveLanesDataset

PUBLIC_DATASETS: tuple[DatasetDefinition, ...] = (
    CULaneDataset(),
    CurveLanesDataset(),
)


def supported_public_dataset_ids() -> set[str]:
    return {dataset.source_dataset_id for dataset in PUBLIC_DATASETS}


def public_dataset_definition(source_dataset_id: str) -> DatasetDefinition:
    for dataset in PUBLIC_DATASETS:
        if dataset.source_dataset_id == source_dataset_id:
            return dataset

    supported = ", ".join(sorted(supported_public_dataset_ids()))
    raise ValueError(
        f"Unsupported public SOURCE_DATASET_ID={source_dataset_id}. "
        f"Supported values: {supported}."
    )
