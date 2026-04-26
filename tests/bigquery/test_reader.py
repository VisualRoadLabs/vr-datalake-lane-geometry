from lane_geometry.bigquery.reader import BigQueryReader
from lane_geometry.curvature.models import ImageToProcess


class FakeScalarQueryParameter:
    def __init__(self, name, field_type, value):
        self.name = name
        self.field_type = field_type
        self.value = value


class FakeArrayQueryParameter:
    def __init__(self, name, field_type, values):
        self.name = name
        self.field_type = field_type
        self.values = values


class FakeQueryJobConfig:
    def __init__(self, query_parameters):
        self.query_parameters = query_parameters


class FakeBigQueryModule:
    QueryJobConfig = FakeQueryJobConfig
    ScalarQueryParameter = FakeScalarQueryParameter
    ArrayQueryParameter = FakeArrayQueryParameter


class FakeRows:
    def result(self):
        return [
            ImageToProcess(
                image_id="image-2",
                source_dataset_id="culane_v1",
                gcs_uri="gs://bucket/image.jpg",
                label_gcs_uri=None,
                width_px=1640,
                height_px=590,
            )
        ]


class FakeClient:
    def __init__(self):
        self.query_text = None
        self.job_config = None

    def query(self, query, job_config=None):
        self.query_text = query
        self.job_config = job_config
        return FakeRows()


def test_pending_geometry_excludes_already_seen_images() -> None:
    client = FakeClient()
    reader = BigQueryReader(
        project_id="project",
        metadata_dataset="metadata",
        classification_dataset="classification",
        client=client,
    )
    reader.bigquery = FakeBigQueryModule

    rows = reader.public_images_pending_geometry(
        source_dataset_id="culane_v1",
        limit=100,
        exclude_image_ids={"image-1"},
    )

    assert rows[0].image_id == "image-2"
    assert "NOT IN UNNEST(@exclude_image_ids)" in client.query_text
    params = {param.name: param for param in client.job_config.query_parameters}
    assert params["exclude_image_ids"].values == ["image-1"]
    assert params["exclude_image_count"].value == 1
