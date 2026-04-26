from __future__ import annotations

from lane_geometry.datasets.base import DatasetDefinition

USER_DATASETS: tuple[DatasetDefinition, ...] = ()


def supported_user_dataset_ids() -> set[str]:
    return {dataset.source_dataset_id for dataset in USER_DATASETS}
