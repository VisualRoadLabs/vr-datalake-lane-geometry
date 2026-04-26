from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from lane_geometry.curvature.models import Lane
from lane_geometry.curvature.thresholds import load_curvature_thresholds


@dataclass(frozen=True)
class QuadraticLaneFit:
    lane_index: int
    a: float
    b: float
    c: float
    dy: float
    score: float


def fit_quadratic_lanes(lanes: list[Lane]) -> list[QuadraticLaneFit]:
    thresholds = load_curvature_thresholds()
    fits: list[QuadraticLaneFit] = []

    for lane_index, lane in enumerate(lanes):
        if len(lane) < thresholds.min_points_per_lane:
            continue

        y_values = np.array([point.y for point in lane], dtype=float)
        x_values = np.array([point.x for point in lane], dtype=float)

        if len(np.unique(y_values)) < thresholds.min_points_per_lane:
            continue

        a, b, c = np.polyfit(y_values, x_values, deg=2)
        dy = float(np.max(y_values) - np.min(y_values))
        score = abs(float(a)) * dy**2
        fits.append(
            QuadraticLaneFit(
                lane_index=lane_index,
                a=float(a),
                b=float(b),
                c=float(c),
                dy=dy,
                score=score,
            )
        )

    return fits


def median_curvature_score_from_fits(
    fits: list[QuadraticLaneFit],
) -> float | None:
    thresholds = load_curvature_thresholds()
    if len(fits) < thresholds.min_valid_lanes:
        return None

    return float(np.median([fit.score for fit in fits]))


def classify_curvature_score(score: float) -> str:
    thresholds = load_curvature_thresholds()
    if score < thresholds.straight_max_score:
        return "straight"
    if score < thresholds.slight_curve_max_score:
        return "slight_curve"
    return "curve"
