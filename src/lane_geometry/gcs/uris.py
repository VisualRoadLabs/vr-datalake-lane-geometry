from __future__ import annotations


def split_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")
    bucket_and_blob = gcs_uri[5:]
    bucket, _, blob = bucket_and_blob.partition("/")
    if not bucket or not blob:
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")
    return bucket, blob
