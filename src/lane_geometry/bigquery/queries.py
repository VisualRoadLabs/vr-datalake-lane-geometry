from __future__ import annotations


def public_images_pending_geometry_query(
    metadata_table: str,
    classifications_table: str,
) -> str:
    return f"""
    SELECT
      i.image_id,
      i.source_dataset_id,
      i.gcs_uri,
      i.label_gcs_uri,
      i.width_px,
      i.height_px,
      i.original_relative_path
    FROM `{metadata_table}` AS i
    INNER JOIN `{classifications_table}` AS c
      ON c.image_id = i.image_id
    WHERE i.source_type = 'public'
      AND i.source_dataset_id = @source_dataset_id
      AND LOWER(CAST(c.is_current AS STRING)) = 'true'
      AND c.road_geometry IS NULL
      AND (i.gcs_status IS NULL OR i.gcs_status = 'available')
      AND (
        @exclude_image_count = 0
        OR i.image_id NOT IN UNNEST(@exclude_image_ids)
      )
    ORDER BY i.image_id
    LIMIT @limit
    """


def public_images_by_ids_query(metadata_table: str) -> str:
    return f"""
    SELECT
      i.image_id,
      i.source_dataset_id,
      i.gcs_uri,
      i.label_gcs_uri,
      i.width_px,
      i.height_px,
      i.original_relative_path
    FROM `{metadata_table}` AS i
    WHERE i.source_type = 'public'
      AND i.source_dataset_id = @source_dataset_id
      AND i.image_id IN UNNEST(@image_ids)
      AND (i.gcs_status IS NULL OR i.gcs_status = 'available')
    ORDER BY i.image_id
    LIMIT @limit
    """


def update_geometry_result_query(
    classifications_table: str,
) -> str:
    return f"""
    UPDATE `{classifications_table}` AS target
    SET
      road_geometry = JSON_VALUE(result_json, '$.road_geometry'),
      road_geometry_confidence = SAFE_CAST(
        JSON_VALUE(result_json, '$.road_geometry_confidence') AS FLOAT64
      )
    FROM UNNEST(@results_json) AS result_json
    WHERE target.image_id = JSON_VALUE(result_json, '$.image_id')
      AND LOWER(CAST(target.is_current AS STRING)) = 'true'
      AND target.road_geometry IS NULL
    """
