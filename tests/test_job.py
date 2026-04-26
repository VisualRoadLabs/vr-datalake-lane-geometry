from lane_geometry.curvature.models import ImageToProcess
from lane_geometry.datasets.base import LaneParser
from lane_geometry.job import process_image


class MissingLabelReader:
    def read_first_existing_text(self, gcs_uris: list[str]) -> tuple[str, str]:
        raise FileNotFoundError("missing")


class InvalidLabelReader:
    def read_first_existing_text(self, gcs_uris: list[str]) -> tuple[str, str]:
        return gcs_uris[0], "bad label"


class InvalidParser(LaneParser):
    source_dataset_id = "culane_v1"

    def parse(self, label_text: str):
        raise ValueError("invalid label")


class EmptyParser(LaneParser):
    source_dataset_id = "culane_v1"

    def parse(self, label_text: str):
        return []


def image_to_process() -> ImageToProcess:
    return ImageToProcess(
        image_id="image-1",
        source_dataset_id="culane_v1",
        gcs_uri="gs://bucket/culane/v1.0/frame.jpg",
        label_gcs_uri=None,
        width_px=1640,
        height_px=590,
    )


def test_process_image_writes_no_labels_when_label_file_is_missing() -> None:
    result = process_image(image_to_process(), MissingLabelReader(), EmptyParser())

    assert result.status == "processed"
    assert result.road_geometry == "no_labels"
    assert result.road_geometry_confidence == 0.0


def test_process_image_writes_no_labels_when_label_parse_fails() -> None:
    result = process_image(image_to_process(), InvalidLabelReader(), InvalidParser())

    assert result.status == "processed"
    assert result.road_geometry == "no_labels"
    assert result.road_geometry_confidence == 0.0
