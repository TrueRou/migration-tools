from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from migration_tools import merge


def test_adapt_image_row_assigns_user_and_visibility() -> None:
    row = SimpleNamespace(
        uuid="uuid-1",
        kind="BACKGROUND",
        label=None,
        file_name="legacy.png",
        uploaded_at=datetime(2024, 1, 2, 3, 4, 5),
        category=None,
        trace_id="trace-123",
    )

    entry = merge._adapt_image_row(row, "id-1-ff", 42, 1)  # type: ignore[arg-type]

    assert entry["uuid"] == "uuid-1"
    assert entry["user_id"] == 42
    assert entry["visibility"] == 1
    assert entry["aspect_id"] == "id-1-ff"
    assert entry["metadata_id"] == "trace-123"
    assert entry["file_name"] == "legacy.png"
    assert entry["labels"] == ["background"]
