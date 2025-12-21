from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine, Row

from .transform import (
    MergeSectionResult,
    UnknownAspectError,
    build_image_labels,
    build_image_name,
    derive_aspect_id,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MergeConfig:
    """Runtime配置。"""

    source_url: str
    target_url: str
    batch_size: int = 500
    dry_run: bool = False
    admin_user_id: str | None = None


@dataclass(slots=True)
class MergeResult:
    """汇总两类迁移的执行情况。"""

    users: MergeSectionResult
    images: MergeSectionResult


def run_merge(config: MergeConfig) -> MergeResult:
    """入口：建立连接并执行用户、图片的迁移。"""
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
                logger.info("开始迁移 users 与 images 表数据")
                users_summary = merge_users(
                    source_conn=source_conn,
                    target_conn=target_conn,
                    batch_size=config.batch_size,
                )
                images_summary = merge_images(
                    source_conn=source_conn,
                    target_conn=target_conn,
                    batch_size=config.batch_size,
                    admin_user_id=config.admin_user_id,
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

    return MergeResult(users=users_summary, images=images_summary)


# ---------------------------------------------------------------------------
# users
# ---------------------------------------------------------------------------


def merge_users(
    *, source_conn: Connection, target_conn: Connection, batch_size: int
) -> MergeSectionResult:
    summary = MergeSectionResult()
    existing_ids = _collect_existing_ids(target_conn, "tbl_user")

    stmt = text(
        "SELECT id, username, hashed_password, email, created_at FROM users ORDER BY id"
    )
    rows = source_conn.execute(stmt)

    payload: List[dict] = []
    for row in rows:
        summary.processed += 1
        entry = _adapt_user_row(row)
        if row.id in existing_ids:
            summary.updated += 1
        else:
            summary.inserted += 1
            existing_ids.add(row.id)
        payload.append(entry)

        if len(payload) >= batch_size:
            _upsert_users(target_conn, payload)
            payload.clear()

    if payload:
        _upsert_users(target_conn, payload)

    logger.info(
        "用户迁移完成：共处理 %s 条，新增 %s 条，更新 %s 条，跳过 %s 条",
        summary.processed,
        summary.inserted,
        summary.updated,
        summary.skipped,
    )
    return summary


def _adapt_user_row(row: Row) -> dict:
    created_at = _ensure_datetime(row.created_at)
    now = datetime.utcnow()
    return {
        "id": uuid.uuid4(),
        "username": row.username,
        "hashed_password": row.hashed_password,
        "email": row.email,
        "created_at": created_at,
        "updated_at": now,
    }


def _upsert_users(conn: Connection, payload: Sequence[dict]) -> None:
    if not payload:
        return
    if not payload:
        return

    # Filter out rows where the username is already taken by a different id.
    existing = {
        str(row.username): str(row.id)
        for row in conn.execute(text("SELECT username, id FROM tbl_user"))
    }

    filtered: List[dict] = []
    for entry in payload:
        uname = entry.get("username")
        if uname is None:
            filtered.append(entry)
            continue
        existing_id = existing.get(str(uname))
        if existing_id and existing_id != str(entry["id"]):
            logger.warning(
                "跳过用户 %s：用户名已被不同用户 %s 占用", uname, existing_id
            )
            continue
        filtered.append(entry)

    if not filtered:
        return

    conn.execute(
        text(
            """
            INSERT INTO tbl_user (id, username, hashed_password, email, permissions, created_at, updated_at)
            VALUES (:id, :username, :hashed_password, :email, '{}', :created_at, :updated_at)
            ON CONFLICT (id) DO UPDATE SET
                username = EXCLUDED.username,
                hashed_password = EXCLUDED.hashed_password,
                email = EXCLUDED.email,
                updated_at = EXCLUDED.updated_at
            """
        ),
        filtered,
    )


# ---------------------------------------------------------------------------
# images
# ---------------------------------------------------------------------------


def merge_images(
    *,
    source_conn: Connection,
    target_conn: Connection,
    batch_size: int,
    admin_user_id: str | None = None,
) -> MergeSectionResult:
    summary = MergeSectionResult()
    ensure_required_aspects(target_conn)

    # 构造用于将源库的用户 id 映射到目标库的用户 id 的映射。
    # 源库 users 表保留了 username；目标库 tbl_user 使用 username 唯一匹配并可能有新的 id。
    source_id_to_username = {
        str(row.id): row.username
        for row in source_conn.execute(text("SELECT id, username FROM users"))
    }

    target_username_to_id = {
        str(row.username): str(row.id)
        for row in target_conn.execute(text("SELECT username, id FROM tbl_user"))
    }

    existing_uuids = _collect_existing_ids(target_conn, "tbl_image", column="id")
    known_user_ids = set(target_username_to_id.values())

    if admin_user_id is not None and admin_user_id not in known_user_ids:
        raise ValueError(f"admin-user-id {admin_user_id} 不存在于目标库的 tbl_user 中")

    stmt = text(
        """
        SELECT uuid, kind, label, file_name, uploaded_by, uploaded_at, category, trace_id
        FROM images
        ORDER BY uploaded_at, uuid
        """
    )
    rows = source_conn.execute(stmt)

    payload: List[dict] = []
    for row in rows:
        summary.processed += 1

        if row.uploaded_by is None:
            if admin_user_id is None:
                summary.skipped += 1
                logger.warning(
                    "图片 %s 无上传用户且未提供 admin-user-id，已跳过", row.uuid
                )
                continue
            user_id = admin_user_id
            visibility = 1
        else:
            # uploaded_by 在源库是源用户 id；目标库的用户 id 可能已改变。
            # 通过源库的 id->username 映射，再用 username 在目标库查找对应 id。
            src_uid = str(row.uploaded_by)
            username = source_id_to_username.get(src_uid)
            if username is None:
                summary.skipped += 1
                logger.warning(
                    "图片 %s 的上传者 %s 在源库 users 表中不存在，已跳过",
                    row.uuid,
                    row.uploaded_by,
                )
                continue

            tgt_user_id = target_username_to_id.get(username)
            if tgt_user_id is None:
                summary.skipped += 1
                logger.warning(
                    "图片 %s 的上传者用户名 %s 在目标库 tbl_user 中不存在，已跳过",
                    row.uuid,
                    username,
                )
                continue

            user_id = tgt_user_id
            visibility = 0

        try:
            aspect_id = derive_aspect_id(row.kind)
        except UnknownAspectError as exc:
            summary.skipped += 1
            logger.warning("图片 %s 跳过：%s", row.uuid, exc)
            continue

        entry = _adapt_image_row(
            row, aspect_id, user_id, visibility, row.uploaded_by is not None
        )

        if row.uuid in existing_uuids:
            summary.updated += 1
        else:
            summary.inserted += 1
            existing_uuids.add(row.uuid)

        payload.append(entry)
        if len(payload) >= batch_size:
            _upsert_images(target_conn, payload)
            payload.clear()

    if payload:
        _upsert_images(target_conn, payload)

    logger.info(
        "图片迁移完成：共处理 %s 条，新增 %s 条，更新 %s 条，跳过 %s 条",
        summary.processed,
        summary.inserted,
        summary.updated,
        summary.skipped,
    )
    return summary


def ensure_required_aspects(target_conn: Connection) -> None:
    required = {
        "id-1-ff": {
            "id": "id-1-ff",
            "name": "ISO 7810 ID-1 FF",
            "description": "ISO 7810 ID-1 全画幅平铺",
            "ratio_width_unit": 768,
            "ratio_height_unit": 1220,
        }
    }

    existing = {
        row.id
        for row in target_conn.execute(
            text("SELECT id FROM tbl_image_aspect WHERE id IN (:bg)"),
            {"bg": "id-1-ff"},
        )
    }

    missing = [data for key, data in required.items() if key not in existing]
    if not missing:
        return

    target_conn.execute(
        text(
            """
            INSERT INTO tbl_image_aspect (id, name, description, ratio_width_unit, ratio_height_unit)
            VALUES (:id, :name, :description, :ratio_width_unit, :ratio_height_unit)
            ON CONFLICT (id) DO NOTHING
            """
        ),
        missing,
    )
    logger.info(
        "已补充缺失的图片比例配置：%s", ", ".join(item["id"] for item in missing)
    )


def _adapt_image_row(
    row: Row, aspect_id: str, user_id: str, visibility: int, workshop: bool
) -> dict:
    uploaded_at = _ensure_datetime(row.uploaded_at)
    labels = build_image_labels(row.kind, row.category, workshop)

    return {
        "id": row.uuid,
        "uuid": row.uuid,
        "user_id": user_id,
        "aspect_id": aspect_id,
        "name": build_image_name(row.label, row.trace_id),
        "description": "",
        "visibility": visibility,
        "labels": labels,
        "original_name": row.file_name,
        "original_id": row.trace_id,
        "created_at": uploaded_at,
        "updated_at": datetime.utcnow(),
    }


def _upsert_images(conn: Connection, payload: Sequence[dict]) -> None:
    if not payload:
        return
    conn.execute(
        text(
            """
            INSERT INTO tbl_image (
                id,
                user_id,
                aspect_id,
                name,
                description,
                visibility,
                labels,
                original_name,
                original_id,
                created_at,
                updated_at
            )
            VALUES (
                :id,
                :user_id,
                :aspect_id,
                :name,
                :description,
                :visibility,
                :labels,
                :original_name,
                :original_id,
                :created_at,
                :updated_at
            )
            ON CONFLICT (id) DO UPDATE SET
                id = EXCLUDED.id,
                user_id = EXCLUDED.user_id,
                aspect_id = EXCLUDED.aspect_id,
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                visibility = EXCLUDED.visibility,
                labels = EXCLUDED.labels,
                original_name = EXCLUDED.original_name,
                original_id = EXCLUDED.original_id,
                updated_at = EXCLUDED.updated_at
            """
        ),
        payload,
    )


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _create_engine(url: str, *, name: str) -> Engine:
    engine = create_engine(url, pool_pre_ping=True, future=True)
    logger.debug("已创建 %s 数据库引擎: %s", name, url)
    return engine


def _collect_existing_ids(conn: Connection, table: str, column: str = "id") -> set:
    result = conn.execute(text(f"SELECT {column} FROM {table}"))
    return {str(row[0]) for row in result}


def _ensure_datetime(value: datetime | None) -> datetime:
    if value is None:
        return datetime.utcnow()
    if value.tzinfo:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


__all__ = [
    "MergeConfig",
    "MergeResult",
    "ensure_required_aspects",
    "merge_images",
    "merge_users",
    "run_merge",
]
