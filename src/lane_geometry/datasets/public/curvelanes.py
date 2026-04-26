from __future__ import annotations

import json
from pathlib import PurePosixPath
from typing import Any

from lane_geometry.curvature.models import Lane, Point
from lane_geometry.datasets.base import DatasetDefinition, LaneParser, dedupe_candidates
from lane_geometry.gcs.uris import split_gcs_uri


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


class CurveLanesDataset(DatasetDefinition):
    source_type = "public"
    source_dataset_id = "curvelanes_v1"
    parser_class = CurveLanesParser

    def label_uri_candidates(
        self,
        image_gcs_uri: str,
        label_gcs_uri: str | None = None,
    ) -> list[str]:
        candidates: list[str] = []
        if label_gcs_uri:
            candidates.append(label_gcs_uri)
        candidates.extend(self._native_label_candidates(image_gcs_uri))

        return dedupe_candidates(candidates)

    def _native_label_candidates(self, image_gcs_uri: str) -> list[str]:
        bucket, blob = split_gcs_uri(image_gcs_uri)
        path = PurePosixPath(blob)
        suffixes = [".json", ".lines.json"]

        blobs: list[str] = []
        for suffix in suffixes:
            blobs.append(str(path.with_suffix(suffix)))

        parts = path.parts
        for image_dir in ("images", "image"):
            if image_dir in parts:
                index = parts.index(image_dir)
                for label_dir in ("labels", "label", "annotations"):
                    label_parts = (*parts[:index], label_dir, *parts[index + 1 :])
                    label_path = PurePosixPath(*label_parts)
                    for suffix in suffixes:
                        blobs.append(str(label_path.with_suffix(suffix)))

        return [f"gs://{bucket}/{candidate}" for candidate in blobs]
