from __future__ import annotations

from abc import ABC, abstractmethod

from lane_geometry.curvature.models import Lane


class LaneParser(ABC):
    source_dataset_id: str

    @abstractmethod
    def parse(self, label_text: str) -> list[Lane]:
        raise NotImplementedError
