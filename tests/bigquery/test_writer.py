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


class FakeStreamingBufferErrorJob:
    num_dml_affected_rows = 0

    def result(self):
        raise RuntimeError(
            "UPDATE or DELETE statement over table project.dataset.table would "
            "affect rows in the streaming buffer, which is not supported"
        )


class FakeBigQueryClient:
    def __init__(self):
        self.queries = []
        self.job_configs = []

    def query(self, query, job_config=None):
        self.queries.append(query)
        self.job_configs.append(job_config)
        return FakeQueryJob()


class FakeRetryBigQueryClient(FakeBigQueryClient):
    def query(self, query, job_config=None):
        self.queries.append(query)
        self.job_configs.append(job_config)
        if len(self.queries) == 1:
            return FakeStreamingBufferErrorJob()
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


def test_update_geometry_results_retries_streaming_buffer_errors() -> None:
    client = FakeRetryBigQueryClient()
    sleeps = []
    writer = BigQueryWriter(
        project_id="project",
        classification_dataset="dataset",
        client=client,
        streaming_buffer_retries=1,
        streaming_buffer_retry_seconds=0.25,
        sleep=sleeps.append,
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
    assert len(client.queries) == 2
    assert sleeps == [0.25]
