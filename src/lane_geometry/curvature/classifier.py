from lane_geometry.curvature.fitting import (
    classify_curvature_score,
    fit_quadratic_lanes,
    median_curvature_score_from_fits,
)
from lane_geometry.curvature.models import Lane


def median_curvature_score(lanes: list[Lane]) -> float | None:
    return median_curvature_score_from_fits(fit_quadratic_lanes(lanes))


def classify_road_geometry(lanes: list[Lane]) -> tuple[str | None, float | None]:
    if not lanes:
        return "no_lines", 0.0

    score = median_curvature_score(lanes)
    if score is None:
        return None, None

    return classify_curvature_score(score), score
