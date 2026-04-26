from __future__ import annotations

import json

from lane_geometry.bigquery.queries import update_geometry_result_query
from lane_geometry.curvature.models import GeometryResult


class BigQueryWriter:
    def __init__(
        self,
        project_id: str,
        classification_dataset: str,
        client=None,
    ) -> None:
        self.project_id = project_id
        self.classification_dataset = classification_dataset
        self.classifications_table = (
            f"{project_id}.{classification_dataset}.tbl_clip_classifications"
        )
        if client is None:
            from google.cloud import bigquery

            self.bigquery = bigquery
            self.client = bigquery.Client(project=project_id)
        else:
            self.bigquery = None
            self.client = client

    def update_geometry_results(self, results: list[GeometryResult]) -> int:
        rows = [
            {
                "image_id": result.image_id,
                "road_geometry": result.road_geometry,
                "road_geometry_confidence": result.road_geometry_confidence,
            }
            for result in results
            if result.status == "processed"
        ]
        if not rows:
            return 0

        query = update_geometry_result_query(self.classifications_table)
        results_json = [json.dumps(row) for row in rows]

        bigquery = self._bigquery_module()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("results_json", "STRING", results_json),
            ]
        )
        query_job = self.client.query(query, job_config=job_config)
        query_job.result()
        return int(query_job.num_dml_affected_rows or 0)

    def _bigquery_module(self):
        if self.bigquery is not None:
            return self.bigquery

        from google.cloud import bigquery

        self.bigquery = bigquery
        return self.bigquery
