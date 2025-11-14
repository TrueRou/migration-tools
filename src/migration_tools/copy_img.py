from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CopyImageConfig:
    """Runtime configuration for copy-img operations."""

    source_dir: Path
    target_dir: Path
    leporid_url: str
    overwrite: bool = False


@dataclass(slots=True)
class CopyImageStats:
    """Execution statistics produced by copy-img."""

    processed: int = 0
    copied: int = 0
    skipped_missing: int = 0
    skipped_existing: int = 0


def run_copy_img(config: CopyImageConfig) -> CopyImageStats:
    """Connect to leporid and copy matching image files."""

    source_dir = config.source_dir.resolve()
    target_dir = config.target_dir.resolve()

    if not source_dir.is_dir():
        raise ValueError(f"source 目录不存在或不可访问: {source_dir}")

    target_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "开始复制图片，source_dir=%s target_dir=%s overwrite=%s",
        source_dir,
        target_dir,
        config.overwrite,
    )

    engine = create_engine(config.leporid_url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            return _copy_images(
                conn=conn,
                source_dir=source_dir,
                target_dir=target_dir,
                overwrite=config.overwrite,
            )
    finally:
        engine.dispose()


def _copy_images(
    *,
    conn: Connection,
    source_dir: Path,
    target_dir: Path,
    overwrite: bool,
) -> CopyImageStats:
    stats = CopyImageStats()

    stmt = text("SELECT id FROM tbl_image ORDER BY id").execution_options(
        stream_results=True
    )
    rows = conn.execute(stmt)

    for row in rows:
        stats.processed += 1
        image_id = str(row.id)
        filename = f"{image_id}.webp"
        src_path = source_dir / filename
        if not src_path.is_file():
            stats.skipped_missing += 1
            logger.debug("跳过 %s：源文件缺失", filename)
            continue

        dst_path = target_dir / filename
        if dst_path.exists() and not overwrite:
            stats.skipped_existing += 1
            logger.debug("跳过 %s：目标已存在", filename)
            continue

        if overwrite and dst_path.exists():
            dst_path.unlink()

        shutil.copy2(src_path, dst_path)
        stats.copied += 1

    logger.info(
        "复制完成：total=%s copied=%s missing=%s existing=%s",
        stats.processed,
        stats.copied,
        stats.skipped_missing,
        stats.skipped_existing,
    )

    return stats


__all__ = [
    "CopyImageConfig",
    "CopyImageStats",
    "run_copy_img",
]
