from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

_KIND_TO_ASPECT = {
    "BACKGROUND": "id-1-ff",
    "FRAME": "id-1-ff",
    "CHARACTER": "id-1-ff",
    "MASK": "id-1-ff",
    "LABEL": "id-1-ff",
}


class UnknownAspectError(ValueError):
    """Raised when a legacy kind cannot be mapped to a known aspect."""


def derive_aspect_id(kind: str) -> str:
    """Map the legacy `kind` enum to the correct aspect identifier."""
    key = (kind or "").upper()
    if key not in _KIND_TO_ASPECT:
        raise UnknownAspectError(f"未识别的图片类型: {kind!r}")
    return _KIND_TO_ASPECT[key]


def build_image_labels(kind: str, category: Optional[str], workshop: bool) -> List[str]:
    """Compose the labels array to be written into `tbl_image.labels`."""
    labels: List[str] = []
    if kind:
        labels.append(kind.lower())
    if category:
        normalized = category.strip()
        if normalized:
            labels.append(normalized.lower())
    if workshop:
        labels.append("workshop")
    return labels


def build_image_name(label: Optional[str], trace_id: Optional[str]) -> str:
    """Generate the `name` field for a migrated image."""
    name = ""
    if label and label.strip():
        name = label.strip()
    elif trace_id and trace_id.strip():
        name = trace_id.strip()
    return name


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
    "build_image_labels",
    "build_image_name",
    "derive_aspect_id",
]
