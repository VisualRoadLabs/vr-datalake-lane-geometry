from lane_geometry.curvature.thresholds import CurvatureThresholds, load_curvature_thresholds


def test_loads_curvature_thresholds_from_yaml() -> None:
    load_curvature_thresholds.cache_clear()

    thresholds = load_curvature_thresholds()

    assert isinstance(thresholds, CurvatureThresholds)
    assert thresholds.straight_max_score > 0
    assert thresholds.slight_curve_max_score > thresholds.straight_max_score
    assert thresholds.min_valid_lanes > 0
    assert thresholds.min_points_per_lane >= 3
