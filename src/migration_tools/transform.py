from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

_KIND_TO_ASPECT = {
    "BACKGROUND": "card-background",
    "FRAME": "card-background",
    "CHARACTER": "card-background",
    "MASK": "card-background",
    "LABEL": "card-background",
    "PASSNAME": "sega-passname",
}


class UnknownAspectError(ValueError):
    """Raised when a legacy kind cannot be mapped to a known aspect."""


def derive_aspect_id(kind: str) -> str:
    """Map the legacy `kind` enum to the correct aspect identifier."""
    key = (kind or "").upper()
    if key not in _KIND_TO_ASPECT:
        raise UnknownAspectError(f"未识别的图片类型: {kind!r}")
    return _KIND_TO_ASPECT[key]


def build_image_labels(kind: str, category: Optional[str]) -> List[str]:
    """Compose the labels array to be written into `tbl_image.labels`."""
    labels: List[str] = []
    if kind:
        labels.append(kind.lower())
    if category:
        normalized = category.strip()
        if normalized:
            labels.append(normalized.lower())
    return labels or ["unclassified"]


def build_image_name(label: Optional[str], uuid: str, kind: str) -> str:
    """Generate the `name` field for a migrated image."""
    if label and label.strip():
        return label.strip()
    fallback_kind = kind.title() if kind else "Image"
    return f"{fallback_kind}-{uuid}"


def build_image_description(
    label: Optional[str], category: Optional[str], kind: str
) -> str:
    """Generate the `description` field for a migrated image."""
    for candidate in (label, category, kind.title() if kind else None):
        if candidate and candidate.strip():
            return candidate.strip()
    return "Legacy image imported via migration-tools"


@dataclass(slots=True)
class MergeSectionResult:
    """Per-section migration counters."""

    processed: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0


__all__ = [
    "MergeSectionResult",
    "UnknownAspectError",
    "build_image_description",
    "build_image_labels",
    "build_image_name",
    "derive_aspect_id",
]
