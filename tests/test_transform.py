from __future__ import annotations

import pytest

from migration_tools.transform import (
    UnknownAspectError,
    build_image_description,
    build_image_labels,
    build_image_name,
    derive_aspect_id,
)


@pytest.mark.parametrize(
    "kind,expected",
    [
        ("BACKGROUND", "id-1-ff"),
        ("frame", "id-1-ff"),
    ],
)
def test_derive_aspect_id(kind: str, expected: str) -> None:
    assert derive_aspect_id(kind) == expected


def test_derive_aspect_id_unknown() -> None:
    with pytest.raises(UnknownAspectError):
        derive_aspect_id("unknown")


@pytest.mark.parametrize(
    "kind,category,expected",
    [
        ("BACKGROUND", None, ["background"]),
        ("FRAME", "Event", ["frame", "event"]),
        ("", " ", ["unclassified"]),
    ],
)
def test_build_image_labels(
    kind: str, category: str | None, expected: list[str]
) -> None:
    assert build_image_labels(kind, category) == expected


@pytest.mark.parametrize(
    "label,uuid,kind,expected",
    [
        ("My Label", "abc", "FRAME", "My Label"),
        (None, "xyz", "background", "Background-xyz"),
        (" ", "uuid", "", "Image-uuid"),
    ],
)
def test_build_image_name(
    label: str | None, uuid: str, kind: str, expected: str
) -> None:
    assert build_image_name(label, uuid, kind) == expected


def test_build_image_description_prefers_label_then_category_then_kind() -> None:
    assert build_image_description("L", "C", "K") == "L"
    assert build_image_description(None, "C", "K") == "C"
    assert build_image_description(None, None, "kind") == "Kind"
    assert (
        build_image_description(None, None, "")
        == "Legacy image imported via migration-tools"
    )
