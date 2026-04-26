from lane_geometry.bigquery.writer import BigQueryWriter
from lane_geometry.curvature.models import GeometryResult


class FakeScalarQueryParameter:
    def __init__(self, name, field_type, value):
        self.name = name
        self.field_type = field_type
        self.value = value


class FakeQueryJobConfig:
    def __init__(self, query_parameters):
        self.query_parameters = query_parameters


class FakeArrayQueryParameter:
    def __init__(self, name, field_type, values):
        self.name = name
        self.field_type = field_type
        self.values = values


class FakeBigQueryModule:
    QueryJobConfig = FakeQueryJobConfig
    ScalarQueryParameter = FakeScalarQueryParameter
    ArrayQueryParameter = FakeArrayQueryParameter


class FakeQueryJob:
    num_dml_affected_rows = 1

    def result(self):
        return None


class FakeBigQueryClient:
    def __init__(self):
        self.queries = []
        self.job_configs = []

    def query(self, query, job_config=None):
        self.queries.append(query)
        self.job_configs.append(job_config)
        return FakeQueryJob()


def test_update_geometry_results_uses_direct_update_queries() -> None:
    client = FakeBigQueryClient()
    writer = BigQueryWriter(
        project_id="project",
        classification_dataset="dataset",
        client=client,
    )
    writer.bigquery = FakeBigQueryModule

    updated_count = writer.update_geometry_results(
        [
            GeometryResult(
                image_id="image-1",
                road_geometry="straight",
                road_geometry_confidence=0.01,
            )
        ]
    )

    assert updated_count == 1
    assert len(client.queries) == 1
    assert "UPDATE `project.dataset.tbl_clip_classifications`" in client.queries[0]
    assert "FROM UNNEST(@results_json)" in client.queries[0]
    assert client.job_configs[0].query_parameters[0].name == "results_json"
