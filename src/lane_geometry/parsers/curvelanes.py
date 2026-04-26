from __future__ import annotations

import json
from typing import Any

from lane_geometry.curvature.models import Lane, Point
from lane_geometry.parsers.base import LaneParser


class CurveLanesParser(LaneParser):
    source_dataset_id = "curvelanes_v1"

    def parse(self, label_text: str) -> list[Lane]:
        payload = json.loads(label_text)
        lane_items = self._lane_items(payload)
        lanes: list[Lane] = []

        for item in lane_items:
            points = self._points_from_item(item)
            if points:
                lanes.append(points)

        return lanes

    def _lane_items(self, payload: Any) -> list[Any]:
        if isinstance(payload, dict):
            for key in ("lanes", "Lines", "lines", "annotations"):
                value = payload.get(key)
                if isinstance(value, list):
                    return value
        if isinstance(payload, list):
            return payload
        return []

    def _points_from_item(self, item: Any) -> Lane:
        if isinstance(item, dict):
            for key in ("points", "Points", "polyline"):
                if isinstance(item.get(key), list):
                    return self._points_from_sequence(item[key])
            if isinstance(item.get("x"), list) and isinstance(item.get("y"), list):
                return [
                    Point(x=float(x), y=float(y))
                    for x, y in zip(item["x"], item["y"], strict=False)
                    if x is not None and y is not None
                ]
        return self._points_from_sequence(item)

    def _points_from_sequence(self, sequence: Any) -> Lane:
        points: Lane = []
        if not isinstance(sequence, list):
            return points

        for item in sequence:
            point = self._point_from_value(item)
            if point is not None and point.x >= 0 and point.y >= 0:
                points.append(point)

        return points

    def _point_from_value(self, value: Any) -> Point | None:
        if isinstance(value, dict):
            x = value.get("x", value.get("X"))
            y = value.get("y", value.get("Y"))
            if x is None or y is None:
                return None
            return Point(x=float(x), y=float(y))

        if isinstance(value, (list, tuple)) and len(value) >= 2:
            return Point(x=float(value[0]), y=float(value[1]))

        return None
