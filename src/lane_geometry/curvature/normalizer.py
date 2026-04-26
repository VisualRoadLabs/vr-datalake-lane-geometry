from __future__ import annotations

from lane_geometry.curvature.models import Lane, Point


def normalize_lanes(lanes: list[Lane], width_px: int, height_px: int) -> list[Lane]:
    if width_px <= 0 or height_px <= 0:
        raise ValueError("Image width and height must be positive.")

    return [
        [Point(x=point.x / width_px, y=point.y / height_px) for point in lane]
        for lane in lanes
    ]
