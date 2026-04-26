from lane_geometry.gcs.paths import build_label_uri_candidates


def test_builds_culane_label_candidate() -> None:
    candidates = build_label_uri_candidates(
        "culane_v1",
        "gs://bucket/culane/v1.0/driver/frame.jpg",
    )

    assert candidates == ["gs://bucket/culane/v1.0/driver/frame.lines.txt"]


def test_builds_curvelanes_label_candidates() -> None:
    candidates = build_label_uri_candidates(
        "curvelanes_v1",
        "gs://bucket/curvelanes/v1.0/images/train/frame.jpg",
    )

    assert "gs://bucket/curvelanes/v1.0/images/train/frame.json" in candidates
    assert "gs://bucket/curvelanes/v1.0/labels/train/frame.json" in candidates
