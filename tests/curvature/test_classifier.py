import pytest

from lane_geometry.curvature.classifier import classify_road_geometry
from lane_geometry.curvature.models import Point


def quadratic_lane(a: float, y_values: list[float], x_offset: float) -> list[Point]:
    return [Point(x=a * y**2 + x_offset, y=y) for y in y_values]


def test_classifies_straight_lanes() -> None:
    lanes = [
        [Point(0.2, 0.1), Point(0.2, 0.5), Point(0.2, 0.9)],
        [Point(0.8, 0.1), Point(0.8, 0.5), Point(0.8, 0.9)],
    ]

    road_geometry, confidence = classify_road_geometry(lanes)

    assert road_geometry == "straight"
    assert confidence is not None
    assert abs(confidence) < 0.0005


def test_returns_null_when_not_enough_lanes() -> None:
    lanes = [[Point(0.2, 0.1), Point(0.3, 0.5), Point(0.4, 0.9)]]

    assert classify_road_geometry(lanes) == (None, None)


def test_returns_no_lines_when_no_lanes_are_detected() -> None:
    assert classify_road_geometry([]) == ("no_lines", 0.0)


def test_short_sharp_segment_is_not_overweighted() -> None:
    y_values = [0.0, 0.05, 0.1]
    lanes = [
        quadratic_lane(a=0.4, y_values=y_values, x_offset=0.2),
        quadratic_lane(a=0.4, y_values=y_values, x_offset=0.6),
    ]

    road_geometry, confidence = classify_road_geometry(lanes)

    assert road_geometry == "straight"
    assert confidence == pytest.approx(0.004)


def test_long_smooth_curve_is_weighted_as_slight_curve() -> None:
    y_values = [0.0, 0.4, 0.8]
    lanes = [
        quadratic_lane(a=0.1, y_values=y_values, x_offset=0.2),
        quadratic_lane(a=0.1, y_values=y_values, x_offset=0.6),
    ]

    road_geometry, confidence = classify_road_geometry(lanes)

    assert road_geometry == "slight_curve"
    assert confidence == pytest.approx(0.064)


def test_stronger_long_curve_is_classified_as_curve() -> None:
    y_values = [0.0, 0.4, 0.8]
    lanes = [
        quadratic_lane(a=0.4, y_values=y_values, x_offset=0.2),
        quadratic_lane(a=0.4, y_values=y_values, x_offset=0.6),
    ]

    road_geometry, confidence = classify_road_geometry(lanes)

    assert road_geometry == "curve"
    assert confidence == pytest.approx(0.256)


def test_negative_quadratic_coefficient_still_produces_positive_score() -> None:
    y_values = [0.0, 0.4, 0.8]
    lanes = [
        quadratic_lane(a=-0.1, y_values=y_values, x_offset=0.5),
        quadratic_lane(a=-0.1, y_values=y_values, x_offset=0.9),
    ]

    road_geometry, confidence = classify_road_geometry(lanes)

    assert road_geometry == "slight_curve"
    assert confidence == pytest.approx(0.064)
