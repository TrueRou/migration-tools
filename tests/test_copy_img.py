from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, text

from migration_tools import copy_img


def _init_db(db_path: Path, image_ids: list[str]) -> None:
    engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE tbl_image (id TEXT PRIMARY KEY)"))
        for image_id in image_ids:
            conn.execute(
                text("INSERT INTO tbl_image (id) VALUES (:id)"),
                {"id": image_id},
            )
    engine.dispose()


def test_copy_images_copies_existing_files(tmp_path) -> None:
    source_dir = tmp_path / "src"
    target_dir = tmp_path / "dst"
    db_path = tmp_path / "db.sqlite"

    source_dir.mkdir()
    target_dir.mkdir()

    existing_id = str(uuid4())
    missing_id = str(uuid4())

    (source_dir / f"{existing_id}.webp").write_bytes(b"image-data")

    _init_db(db_path, [existing_id, missing_id])

    engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)
    with engine.connect() as conn:
        stats = copy_img._copy_images(
            conn=conn,
            source_dir=source_dir,
            target_dir=target_dir,
            overwrite=False,
        )

    assert stats.processed == 2
    assert stats.copied == 1
    assert stats.skipped_missing == 1
    assert stats.skipped_existing == 0
    assert (target_dir / f"{existing_id}.webp").read_bytes() == b"image-data"


def test_copy_images_skips_when_target_exists(tmp_path) -> None:
    source_dir = tmp_path / "src"
    target_dir = tmp_path / "dst"
    db_path = tmp_path / "db.sqlite"

    source_dir.mkdir()
    target_dir.mkdir()

    image_id = str(uuid4())
    src_path = source_dir / f"{image_id}.webp"
    dst_path = target_dir / f"{image_id}.webp"

    src_path.write_bytes(b"new-data")
    dst_path.write_bytes(b"old-data")

    _init_db(db_path, [image_id])

    engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)
    with engine.connect() as conn:
        stats = copy_img._copy_images(
            conn=conn,
            source_dir=source_dir,
            target_dir=target_dir,
            overwrite=False,
        )

    assert stats.processed == 1
    assert stats.copied == 0
    assert stats.skipped_missing == 0
    assert stats.skipped_existing == 1
    assert dst_path.read_bytes() == b"old-data"
