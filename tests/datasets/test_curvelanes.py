import json

from lane_geometry.datasets.public.curvelanes import CurveLanesParser


def test_parses_curvelanes_lines_points_shape() -> None:
    payload = {
        "Lines": [
            {"points": [{"x": 1, "y": 2}, {"x": 3, "y": 4}]},
            {"points": [[5, 6], [7, 8]]},
        ]
    }

    lanes = CurveLanesParser().parse(json.dumps(payload))

    assert len(lanes) == 2
    assert [(point.x, point.y) for point in lanes[0]] == [(1, 2), (3, 4)]
    assert [(point.x, point.y) for point in lanes[1]] == [(5, 6), (7, 8)]
