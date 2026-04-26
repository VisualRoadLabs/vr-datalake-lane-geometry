from __future__ import annotations

from lane_geometry.bigquery.queries import (
    public_images_by_ids_query,
    public_images_pending_geometry_query,
)
from lane_geometry.curvature.models import ImageToProcess


class BigQueryReader:
    def __init__(
        self,
        project_id: str,
        metadata_dataset: str,
        classification_dataset: str,
        client=None,
    ) -> None:
        self.project_id = project_id
        self.metadata_table = f"{project_id}.{metadata_dataset}.tbl_images"
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

    def public_images_pending_geometry(
        self,
        source_dataset_id: str,
        limit: int,
        exclude_image_ids: set[str] | None = None,
    ) -> list[ImageToProcess]:
        excluded_ids = sorted(exclude_image_ids or set())
        bigquery = self._bigquery_module()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "source_dataset_id", "STRING", source_dataset_id
                ),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
                bigquery.ArrayQueryParameter(
                    "exclude_image_ids", "STRING", excluded_ids
                ),
                bigquery.ScalarQueryParameter(
                    "exclude_image_count", "INT64", len(excluded_ids)
                ),
            ]
        )
        rows = self.client.query(
            public_images_pending_geometry_query(
                self.metadata_table,
                self.classifications_table,
            ),
            job_config=job_config,
        ).result()

        return self._rows_to_images(rows)

    def public_images_by_ids(
        self,
        source_dataset_id: str,
        image_ids: list[str],
        limit: int,
    ) -> list[ImageToProcess]:
        bigquery = self._bigquery_module()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "source_dataset_id", "STRING", source_dataset_id
                ),
                bigquery.ArrayQueryParameter("image_ids", "STRING", image_ids),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ]
        )
        rows = self.client.query(
            public_images_by_ids_query(self.metadata_table),
            job_config=job_config,
        ).result()

        return self._rows_to_images(rows)

    def _rows_to_images(self, rows) -> list[ImageToProcess]:
        return [
            ImageToProcess(
                image_id=row.image_id,
                source_dataset_id=row.source_dataset_id,
                gcs_uri=row.gcs_uri,
                label_gcs_uri=row.label_gcs_uri,
                width_px=row.width_px,
                height_px=row.height_px,
                original_relative_path=row.original_relative_path,
            )
            for row in rows
        ]

    def _bigquery_module(self):
        if self.bigquery is not None:
            return self.bigquery

        from google.cloud import bigquery

        self.bigquery = bigquery
        return self.bigquery
