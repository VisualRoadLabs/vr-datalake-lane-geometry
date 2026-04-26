from __future__ import annotations

from abc import ABC, abstractmethod

from lane_geometry.curvature.models import Lane


class LaneParser(ABC):
    source_dataset_id: str

    @abstractmethod
    def parse(self, label_text: str) -> list[Lane]:
        raise NotImplementedError


class DatasetDefinition(ABC):
    source_type: str
    source_dataset_id: str
    parser_class: type[LaneParser]

    def parser(self) -> LaneParser:
        return self.parser_class()

    @abstractmethod
    def label_uri_candidates(
        self,
        image_gcs_uri: str,
        label_gcs_uri: str | None = None,
    ) -> list[str]:
        raise NotImplementedError


def dedupe_candidates(candidates: list[str]) -> list[str]:
    return list(dict.fromkeys(candidates))
