from __future__ import annotations

from pathlib import PurePosixPath

from lane_geometry.curvature.models import Lane, Point
from lane_geometry.datasets.base import DatasetDefinition, LaneParser, dedupe_candidates
from lane_geometry.gcs.uris import split_gcs_uri


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


class CULaneDataset(DatasetDefinition):
    source_type = "public"
    source_dataset_id = "culane_v1"
    parser_class = CULaneParser

    def label_uri_candidates(
        self,
        image_gcs_uri: str,
        label_gcs_uri: str | None = None,
    ) -> list[str]:
        candidates: list[str] = []
        if label_gcs_uri:
            candidates.append(label_gcs_uri)

        bucket, blob = split_gcs_uri(image_gcs_uri)
        path = PurePosixPath(blob)
        candidates.append(f"gs://{bucket}/{path.with_suffix('.lines.txt')}")

        return dedupe_candidates(candidates)
