from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - fallback for minimal local environments.
    yaml = None


DEFAULT_THRESHOLDS_CONFIG_PACKAGE = "lane_geometry.configs"
DEFAULT_THRESHOLDS_CONFIG_FILE = "curvature_thresholds.yaml"
THRESHOLDS_CONFIG_ENV_VAR = "CURVATURE_THRESHOLDS_CONFIG"


@dataclass(frozen=True)
class CurvatureThresholds:
    straight_max_score: float
    slight_curve_max_score: float
    min_valid_lanes: int
    min_points_per_lane: int


@lru_cache(maxsize=1)
def load_curvature_thresholds() -> CurvatureThresholds:
    config_path_override = os.getenv(THRESHOLDS_CONFIG_ENV_VAR)
    if config_path_override:
        payload = _load_yaml_from_path(Path(config_path_override))
    else:
        payload = _load_packaged_yaml()
    curvature_config = payload.get("curvature", {})

    thresholds = CurvatureThresholds(
        straight_max_score=float(curvature_config["straight_max_score"]),
        slight_curve_max_score=float(curvature_config["slight_curve_max_score"]),
        min_valid_lanes=int(curvature_config["min_valid_lanes"]),
        min_points_per_lane=int(curvature_config["min_points_per_lane"]),
    )
    _validate_thresholds(thresholds)
    return thresholds


def _load_yaml_from_path(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Curvature thresholds config not found: {config_path}")

    text = config_path.read_text(encoding="utf-8")
    return _load_yaml_from_text(text)


def _load_packaged_yaml() -> dict[str, Any]:
    resource = files(DEFAULT_THRESHOLDS_CONFIG_PACKAGE).joinpath(
        DEFAULT_THRESHOLDS_CONFIG_FILE
    )
    return _load_yaml_from_text(resource.read_text(encoding="utf-8"))


def _load_yaml_from_text(text: str) -> dict[str, Any]:
    if yaml is not None:
        payload = yaml.safe_load(text)
        if not isinstance(payload, dict):
            raise ValueError("Curvature thresholds config must be a YAML mapping.")
        return payload

    return _load_simple_yaml(text)


def _load_simple_yaml(text: str) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    current_section: dict[str, Any] | None = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue

        if not raw_line.startswith(" "):
            key = line.rstrip(":")
            payload[key] = {}
            current_section = payload[key]
            continue

        if current_section is None or ":" not in line:
            raise ValueError("Unsupported YAML structure in thresholds config.")

        key, value = line.strip().split(":", maxsplit=1)
        current_section[key] = value.strip()

    return payload


def _validate_thresholds(thresholds: CurvatureThresholds) -> None:
    if thresholds.straight_max_score <= 0:
        raise ValueError("straight_max_score must be positive.")
    if thresholds.slight_curve_max_score <= thresholds.straight_max_score:
        raise ValueError(
            "slight_curve_max_score must be greater than straight_max_score."
        )
    if thresholds.min_valid_lanes <= 0:
        raise ValueError("min_valid_lanes must be positive.")
    if thresholds.min_points_per_lane < 3:
        raise ValueError("min_points_per_lane must be at least 3 for polyfit degree 2.")
