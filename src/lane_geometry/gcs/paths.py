from __future__ import annotations

from lane_geometry.datasets.public.registry import public_dataset_definition
from lane_geometry.gcs.uris import split_gcs_uri


def build_label_uri_candidates(
    source_dataset_id: str,
    image_gcs_uri: str,
    label_gcs_uri: str | None = None,
) -> list[str]:
    dataset = public_dataset_definition(source_dataset_id)
    return dataset.label_uri_candidates(image_gcs_uri, label_gcs_uri)
