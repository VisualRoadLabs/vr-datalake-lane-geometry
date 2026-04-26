from __future__ import annotations

from google.api_core.exceptions import NotFound
from google.cloud import storage

from lane_geometry.gcs.uris import split_gcs_uri


class GCSReader:
    def __init__(self, client: storage.Client | None = None) -> None:
        self.client = client or storage.Client()

    def read_first_existing_text(self, gcs_uris: list[str]) -> tuple[str, str]:
        missing: list[str] = []
        for gcs_uri in gcs_uris:
            bucket_name, blob_name = split_gcs_uri(gcs_uri)
            blob = self.client.bucket(bucket_name).blob(blob_name)
            try:
                return gcs_uri, blob.download_as_text(encoding="utf-8")
            except NotFound:
                missing.append(gcs_uri)

        raise FileNotFoundError(f"No label file found. Tried: {', '.join(missing)}")

    def read_bytes(self, gcs_uri: str) -> bytes:
        bucket_name, blob_name = split_gcs_uri(gcs_uri)
        return self.client.bucket(bucket_name).blob(blob_name).download_as_bytes()
