from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Point:
    x: float
    y: float


Lane = list[Point]


@dataclass(frozen=True)
class ImageToProcess:
    image_id: str
    source_dataset_id: str
    gcs_uri: str
    label_gcs_uri: str | None
    width_px: int | None
    height_px: int | None
    original_relative_path: str | None = None


@dataclass(frozen=True)
class GeometryResult:
    image_id: str
    road_geometry: str | None
    road_geometry_confidence: float | None
    status: str = "processed"
    error_message: str | None = None
