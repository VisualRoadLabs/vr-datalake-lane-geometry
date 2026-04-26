from __future__ import annotations

from lane_geometry.curvature.models import Lane, Point
from lane_geometry.parsers.base import LaneParser


class CULaneParser(LaneParser):
    source_dataset_id = "culane_v1"

    def parse(self, label_text: str) -> list[Lane]:
        lanes: list[Lane] = []

        for raw_line in label_text.splitlines():
            values = raw_line.strip().split()
            if len(values) < 6:
                continue
            if len(values) % 2 != 0:
                values = values[:-1]

            points: Lane = []
            for index in range(0, len(values), 2):
                try:
                    x = float(values[index])
                    y = float(values[index + 1])
                except ValueError:
                    continue
                if x >= 0 and y >= 0:
                    points.append(Point(x=x, y=y))

            if points:
                lanes.append(points)

        return lanes
