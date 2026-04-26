from lane_geometry.datasets.public.culane import CULaneParser


def test_parses_culane_lines() -> None:
    lanes = CULaneParser().parse("1 2 3 4 5 6\n-1 2 7 8 9 10\n")

    assert len(lanes) == 2
    assert [(point.x, point.y) for point in lanes[0]] == [(1, 2), (3, 4), (5, 6)]
    assert [(point.x, point.y) for point in lanes[1]] == [(7, 8), (9, 10)]
