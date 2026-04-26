import pytest

from lane_geometry.curvature.fitting import (
    classify_curvature_score,
    fit_quadratic_lanes,
    median_curvature_score_from_fits,
)
from lane_geometry.curvature.models import Point


def test_fit_quadratic_lanes_returns_positive_score_for_negative_a() -> None:
    lanes = [
        [Point(x=-0.1 * y**2 + 0.5, y=y) for y in [0.0, 0.4, 0.8]],
        [Point(x=-0.1 * y**2 + 0.9, y=y) for y in [0.0, 0.4, 0.8]],
    ]

    fits = fit_quadratic_lanes(lanes)

    assert len(fits) == 2
    assert fits[0].a == pytest.approx(-0.1)
    assert fits[0].dy == pytest.approx(0.8)
    assert fits[0].score == pytest.approx(0.064)
    assert median_curvature_score_from_fits(fits) == pytest.approx(0.064)
    assert classify_curvature_score(0.064) == "slight_curve"
