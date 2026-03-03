from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from .transform import MergeSectionResult, convert_maimai_score

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MigrateScoresConfig:
    """uc-scores 命令运行时配置。"""

    source_url: str
    """来源 PostgreSQL 连接串（UsagiCard 数据库）。"""

    target_url: str
    """目标 PostgreSQL 连接串（otoge-service 数据库）。"""

    source_table: str = "maimai_scores"
    """源库成绩表名，默认 maimai_scores。"""

    batch_size: int = 500
    dry_run: bool = False


@dataclass(slots=True)
class MigrateScoresResult:
    """迁移执行汇总。"""

    scores: MergeSectionResult


def run_migrate_scores(config: MigrateScoresConfig) -> MigrateScoresResult:
    """入口：将 UsagiCard maimai 成绩迁移到 otoge-service tbl_maimai_scores。"""
    source_engine = _create_engine(config.source_url, name="source")
    target_engine = _create_engine(config.target_url, name="target")

    try:
        with (
            source_engine.connect() as source_conn,
            target_engine.connect() as target_conn,
        ):
            source_tx = source_conn.begin()
            target_tx = target_conn.begin()
            try:
                result = _execute_migrate_scores(
                    source_conn=source_conn,
                    target_conn=target_conn,
                    config=config,
                )
            except Exception:
                logger.exception("迁移过程中发生错误，正在回滚")
                target_tx.rollback()
                source_tx.rollback()
                raise
            else:
                if config.dry_run:
                    logger.info("dry-run 模式启用，所有更改已回滚")
                    target_tx.rollback()
                    source_tx.rollback()
                else:
                    target_tx.commit()
                    source_tx.commit()
                    logger.info("迁移完成，所有更改已提交")
    finally:
        source_engine.dispose()
        target_engine.dispose()

    return result


# ---------------------------------------------------------------------------
# 核心流程
# ---------------------------------------------------------------------------


def _execute_migrate_scores(
    *,
    source_conn: Connection,
    target_conn: Connection,
    config: MigrateScoresConfig,
) -> MigrateScoresResult:
    rows = _load_scores(source_conn, config.source_table)
    scores_result = _migrate_scores(
        rows=rows,
        target_conn=target_conn,
        config=config,
    )
    return MigrateScoresResult(scores=scores_result)


# ---------------------------------------------------------------------------
# 数据加载
# ---------------------------------------------------------------------------


def _load_scores(conn: Connection, source_table: str) -> List[Dict[str, Any]]:
    """从源库全量读取 maimai 成绩，按 id 升序返回。"""
    rows = conn.execute(
        text(
            f"SELECT song_id, level_index, achievements, dx_score, dx_rating, "  # noqa: S608
            f"play_count, fc, fs, rate, type, uuid, created_at, updated_at "
            f"FROM {source_table} ORDER BY id"
        )
    )
    return [dict(row._mapping) for row in rows]


# ---------------------------------------------------------------------------
# 成绩写入
# ---------------------------------------------------------------------------


def _migrate_scores(
    *,
    rows: List[Dict[str, Any]],
    target_conn: Connection,
    config: MigrateScoresConfig,
) -> MergeSectionResult:
    summary = MergeSectionResult()
    payload: List[dict] = []

    for row in rows:
        summary.processed += 1
        try:
            record = convert_maimai_score(row)
        except Exception:
            logger.warning("跳过成绩行（转换失败）: %s", row, exc_info=True)
            summary.skipped += 1
            continue

        summary.inserted += 1
        payload.append(record)

        if len(payload) >= config.batch_size:
            _insert_scores(target_conn, payload)
            payload.clear()

    if payload:
        _insert_scores(target_conn, payload)

    logger.info(
        "成绩迁移完成：共处理 %d 条，新增 %d 条，跳过 %d 条",
        summary.processed,
        summary.inserted,
        summary.skipped,
    )
    return summary


def _insert_scores(conn: Connection, payload: Sequence[dict]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO tbl_maimai_scores (
                song_id, level_index, achievements, dx_score, dx_rating,
                play_count, fc, fs, rate, type, uuid, created_at, updated_at
            )
            VALUES (
                :song_id,
                CAST(:level_index AS levelindex),
                :achievements,
                :dx_score,
                :dx_rating,
                :play_count,
                CAST(:fc AS fctype),
                CAST(:fs AS fstype),
                CAST(:rate AS ratetype),
                CAST(:type AS songtype),
                :uuid,
                :created_at,
                :updated_at
            )
            """
        ),
        payload,
    )


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------


def _create_engine(url: str, *, name: str) -> Engine:
    engine = create_engine(url, pool_pre_ping=True, future=True)
    logger.debug("已创建 %s 数据库引擎: %s", name, url)
    return engine


__all__ = [
    "MigrateScoresConfig",
    "MigrateScoresResult",
    "run_migrate_scores",
]
