from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable

from lane_geometry.bigquery.queries import update_geometry_result_query
from lane_geometry.curvature.models import GeometryResult

LOGGER = logging.getLogger(__name__)
DEFAULT_STREAMING_BUFFER_RETRIES = 15
DEFAULT_STREAMING_BUFFER_RETRY_SECONDS = 120.0
STREAMING_BUFFER_ERROR_TEXT = "would affect rows in the streaming buffer"


class BigQueryWriter:
    def __init__(
        self,
        project_id: str,
        classification_dataset: str,
        client=None,
        streaming_buffer_retries: int = DEFAULT_STREAMING_BUFFER_RETRIES,
        streaming_buffer_retry_seconds: float = DEFAULT_STREAMING_BUFFER_RETRY_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.project_id = project_id
        self.classification_dataset = classification_dataset
        self.classifications_table = (
            f"{project_id}.{classification_dataset}.tbl_clip_classifications"
        )
        self.streaming_buffer_retries = streaming_buffer_retries
        self.streaming_buffer_retry_seconds = streaming_buffer_retry_seconds
        self.sleep = sleep
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

        for attempt in range(self.streaming_buffer_retries + 1):
            query_job = self.client.query(query, job_config=job_config)
            try:
                query_job.result()
                return int(query_job.num_dml_affected_rows or 0)
            except Exception as exc:
                if not _is_streaming_buffer_error(exc):
                    raise
                if attempt >= self.streaming_buffer_retries:
                    raise RuntimeError(
                        "BigQuery update is still blocked by streaming-buffer rows "
                        f"after {self.streaming_buffer_retries} retries. "
                        "Run the lane-geometry job later or add a delay after the "
                        "CLIP streaming insert finishes."
                    ) from exc

                wait_seconds = self.streaming_buffer_retry_seconds
                LOGGER.warning(
                    "BigQuery update blocked by streaming buffer. "
                    "Retrying in %.0f seconds (attempt %s/%s).",
                    wait_seconds,
                    attempt + 1,
                    self.streaming_buffer_retries,
                )
                self.sleep(wait_seconds)

        return 0

    def _bigquery_module(self):
        if self.bigquery is not None:
            return self.bigquery

        from google.cloud import bigquery

        self.bigquery = bigquery
        return self.bigquery


def _is_streaming_buffer_error(exc: Exception) -> bool:
    return STREAMING_BUFFER_ERROR_TEXT in str(exc).lower()
