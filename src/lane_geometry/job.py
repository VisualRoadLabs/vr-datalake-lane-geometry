from __future__ import annotations

import json
import logging
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict

from lane_geometry.config import JobConfig
from lane_geometry.curvature.classifier import classify_road_geometry
from lane_geometry.curvature.models import GeometryResult, ImageToProcess
from lane_geometry.curvature.normalizer import normalize_lanes
from lane_geometry.datasets.base import LaneParser
from lane_geometry.datasets.public.registry import public_dataset_definition
from lane_geometry.gcs.paths import build_label_uri_candidates
from lane_geometry.gcs.reader import GCSReader

LOGGER = logging.getLogger(__name__)


class LabelUnavailableError(Exception):
    pass


def log_progress(message: str) -> None:
    print(message, flush=True)


def parser_for_dataset(source_dataset_id: str) -> LaneParser:
    return public_dataset_definition(source_dataset_id).parser()


def process_image(
    image: ImageToProcess,
    gcs_reader: GCSReader,
    parser: LaneParser,
) -> GeometryResult:
    if image.width_px is None or image.height_px is None:
        return GeometryResult(
            image_id=image.image_id,
            road_geometry=None,
            road_geometry_confidence=None,
            status="skipped",
            error_message="Missing image dimensions.",
        )

    try:
        label_candidates = build_label_uri_candidates(
            image.source_dataset_id,
            image.gcs_uri,
            image.label_gcs_uri,
        )
        try:
            label_uri, label_text = gcs_reader.read_first_existing_text(label_candidates)
            lanes = parser.parse(label_text)
        except (FileNotFoundError, UnicodeDecodeError, ValueError) as exc:
            raise LabelUnavailableError(str(exc)) from exc

        normalized_lanes = normalize_lanes(lanes, image.width_px, image.height_px)
        road_geometry, confidence = classify_road_geometry(normalized_lanes)
        LOGGER.debug(
            "Processed %s with label %s: %s",
            image.image_id,
            label_uri,
            road_geometry,
        )
        return GeometryResult(
            image_id=image.image_id,
            road_geometry=road_geometry,
            road_geometry_confidence=confidence,
        )
    except LabelUnavailableError as exc:
        LOGGER.debug("No labels available for %s: %s", image.image_id, exc)
        return GeometryResult(
            image_id=image.image_id,
            road_geometry="no_labels",
            road_geometry_confidence=0.0,
        )
    except Exception as exc:
        LOGGER.warning("Skipping %s: %s", image.image_id, exc)
        return GeometryResult(
            image_id=image.image_id,
            road_geometry=None,
            road_geometry_confidence=None,
            status="skipped",
            error_message=str(exc),
        )


def run(config: JobConfig) -> int:
    from lane_geometry.bigquery.reader import BigQueryReader

    reader = BigQueryReader(
        project_id=config.project_id,
        metadata_dataset=config.metadata_dataset,
        classification_dataset=config.classification_dataset,
    )
    gcs_reader = GCSReader()
    parser = parser_for_dataset(config.source_dataset_id)

    writer = None
    if not config.dry_run:
        from lane_geometry.bigquery.writer import BigQueryWriter

        writer = BigQueryWriter(
            project_id=config.project_id,
            classification_dataset=config.classification_dataset,
        )

    all_results = []
    total_updated = 0
    total_processed = 0
    total_skipped = 0
    remaining_limit = config.limit
    batch_number = 0
    seen_image_ids: set[str] = set()

    while True:
        read_limit = min(config.batch_size, remaining_limit) if remaining_limit else config.batch_size
        images = _read_images(reader, config, read_limit, seen_image_ids)
        if not images:
            log_progress("No more public images pending geometry.")
            break

        batch_number += 1
        log_progress(
            f"Processing batch {batch_number} with {len(images)} images."
        )

        with ThreadPoolExecutor(max_workers=config.workers) as executor:
            results = list(
                executor.map(
                    lambda image: process_image(image, gcs_reader, parser),
                    images,
                )
            )

        all_results.extend(results)
        seen_image_ids.update(image.image_id for image in images)
        processed_count = len(
            [result for result in results if result.status == "processed"]
        )
        skipped_count = len([result for result in results if result.status == "skipped"])
        geometry_counts = Counter(
            result.road_geometry if result.road_geometry is not None else "null"
            for result in results
            if result.status == "processed"
        )
        total_processed += processed_count
        total_skipped += skipped_count

        if config.dry_run:
            log_progress(
                "Dry run enabled. "
                f"Batch {batch_number} summary: processed={processed_count} "
                f"skipped={skipped_count} "
                f"geometry_counts={dict(sorted(geometry_counts.items()))}. "
                "BigQuery not updated."
            )
        else:
            assert writer is not None
            updated_count = writer.update_geometry_results(results)
            total_updated += updated_count
            log_progress(
                f"Batch {batch_number} summary: processed={processed_count} "
                f"updated={updated_count} skipped={skipped_count} "
                f"geometry_counts={dict(sorted(geometry_counts.items()))}."
            )

        if remaining_limit is not None:
            remaining_limit -= len(images)
            if remaining_limit <= 0:
                log_progress("Reached requested limit.")
                break

        if config.image_ids:
            break

    if config.output_json:
        config.output_json.parent.mkdir(parents=True, exist_ok=True)
        config.output_json.write_text(
            json.dumps([asdict(result) for result in all_results], indent=2),
            encoding="utf-8",
        )
        LOGGER.info("Wrote results JSON to %s.", config.output_json)

    log_progress(
        f"Completed. Processed {total_processed} rows. "
        f"Updated {total_updated} rows. Skipped {total_skipped} rows."
    )
    return total_updated


def _read_images(
    reader,
    config: JobConfig,
    limit: int,
    seen_image_ids: set[str],
):
    if config.image_ids:
        return reader.public_images_by_ids(
            source_dataset_id=config.source_dataset_id,
            image_ids=config.image_ids,
            limit=limit,
        )

    return reader.public_images_pending_geometry(
        source_dataset_id=config.source_dataset_id,
        limit=limit,
        exclude_image_ids=seen_image_ids,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(JobConfig.from_env())


if __name__ == "__main__":
    main()
