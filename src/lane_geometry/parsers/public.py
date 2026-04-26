from __future__ import annotations

from lane_geometry.parsers.base import LaneParser
from lane_geometry.parsers.culane import CULaneParser
from lane_geometry.parsers.curvelanes import CurveLanesParser

PUBLIC_PARSER_CLASSES: tuple[type[LaneParser], ...] = (
    CULaneParser,
    CurveLanesParser,
)


def supported_public_dataset_ids() -> set[str]:
    return {parser_class.source_dataset_id for parser_class in PUBLIC_PARSER_CLASSES}


def parser_for_public_dataset(source_dataset_id: str) -> LaneParser:
    for parser_class in PUBLIC_PARSER_CLASSES:
        if parser_class.source_dataset_id == source_dataset_id:
            return parser_class()

    supported = ", ".join(sorted(supported_public_dataset_ids()))
    raise ValueError(
        f"Unsupported public SOURCE_DATASET_ID={source_dataset_id}. "
        f"Supported values: {supported}."
    )
