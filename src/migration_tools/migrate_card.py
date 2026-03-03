from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Sequence, Set

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from .transform import (
    MergeSectionResult,
    build_usagicard_storage,
    convert_dxpass_design,
    convert_wars_design,
)

logger = logging.getLogger(__name__)

# ── 占位符 UUID，运行前请填写正确的值 ──────────────────────────────────────
MATERIAL_ID_DX: str = "3b3f301c-d7c4-474e-a677-93c46eefb375"    # DX 卡片使用的 tbl_product_material.id
MATERIAL_ID_WARS: str = "3b3f301c-d7c4-474e-a677-93c46eefb375"  # Wars 卡片使用的 tbl_product_material.id
TYPE_ID_DX: str = "bbe19682-4c2d-4109-ac64-64c7f51e7b77"        # DX 卡片使用的 tbl_product_type.id（design_type=0）
TYPE_ID_WARS: str = "bbe19682-4c2d-4109-ac64-64c7f51e7b77"      # Wars 卡片使用的 tbl_product_type.id（design_type=1）
ADMIN_USER_ID: str = "07923542-ff85-442d-9fc9-247e0815e2e1"  # 用于关联工单的管理员用户 ID（tbl_user.id）
# ───────────────────────────────────────────────────────────────────────────

# UsagiCard CardStatus（存储为字符串枚举）→ leporidae ArtifactStatus（整数）
_CARD_STATUS_TO_ARTIFACT_STATUS: Dict[str, int] = {
    "DRAFTED": 0,    # PENDING
    "LOCKED": 1,     # IN_PRODUCTION
    "DELIVERED": 2,  # COMPLETED
    "ACTIVATED": 3,  # ACTIVATED
}

# UsagiCard CardPattern 整数值
_PATTERN_DXPASS = 1
_PATTERN_WARS = 2


@dataclass(slots=True)
class MigrateCardConfig:
    """uc-card 命令运行时配置。"""

    source_url: str
    target_url: str
    uuid_mapping_path: str | None = None
    material_id_dx: str = MATERIAL_ID_DX
    material_id_wars: str = MATERIAL_ID_WARS
    type_id_dx: str = TYPE_ID_DX
    type_id_wars: str = TYPE_ID_WARS
    batch_size: int = 500
    dry_run: bool = False


@dataclass(slots=True)
class MigrateCardResult:
    """迁移执行汇总。"""

    products: MergeSectionResult
    artifacts: MergeSectionResult


def run_migrate_card(config: MigrateCardConfig) -> MigrateCardResult:
    """入口：将 UsagiCard cards 迁移到 leporidae tbl_product + tbl_artifact。"""
    source_engine = _create_engine(config.source_url, name="source")
    target_engine = _create_engine(config.target_url, name="target")

    uuid_map: Dict[str, str] = {}
    if config.uuid_mapping_path:
        with open(config.uuid_mapping_path) as f:
            uuid_map = json.load(f)
        logger.info("已加载图片 UUID 映射，共 %d 条", len(uuid_map))

    try:
        with (
            source_engine.connect() as source_conn,
            target_engine.connect() as target_conn,
        ):
            source_tx = source_conn.begin()
            target_tx = target_conn.begin()
            try:
                result = _execute_migrate_card(
                    source_conn=source_conn,
                    target_conn=target_conn,
                    config=config,
                    uuid_map=uuid_map,
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


def _execute_migrate_card(
    *,
    source_conn: Connection,
    target_conn: Connection,
    config: MigrateCardConfig,
    uuid_map: Mapping[str, str],
) -> MigrateCardResult:
    domain_to_user = _build_domain_user_mapping(source_conn, target_conn)
    associated_card_ids = _load_associated_card_ids(source_conn)
    cards = _load_cards(source_conn)

    # 为幂等性：通过 name（=card.uuid）查找已存在的 product，复用其 ID
    existing_by_name: Dict[str, str] = {
        str(row.name): str(row.id)
        for row in target_conn.execute(text("SELECT id, name FROM tbl_product"))
    }
    card_to_product_id: Dict[int, str] = {
        card["id"]: existing_by_name.get(card["uuid"]) or str(uuid.uuid4())
        for card in cards
    }

    # 旧 card.uuid → 新 product UUID，用于解析 derived_from
    old_uuid_to_product_id: Dict[str, str] = {
        card["uuid"]: card_to_product_id[card["id"]] for card in cards
    }

    products_result, migrated_card_ids = _migrate_products(
        source_cards=cards,
        target_conn=target_conn,
        domain_to_user=domain_to_user,
        associated_card_ids=associated_card_ids,
        card_to_product_id=card_to_product_id,
        uuid_map=uuid_map,
        config=config,
    )

    migrated_cards = [c for c in cards if c["id"] in migrated_card_ids]
    artifacts_result = _migrate_artifacts(
        source_cards=migrated_cards,
        target_conn=target_conn,
        card_to_product_id=card_to_product_id,
        old_uuid_to_product_id=old_uuid_to_product_id,
        config=config,
    )

    return MigrateCardResult(products=products_result, artifacts=artifacts_result)


# ---------------------------------------------------------------------------
# 辅助数据加载
# ---------------------------------------------------------------------------


def _build_domain_user_mapping(
    source_conn: Connection, target_conn: Connection
) -> Dict[int, str]:
    """通过 dominations 表将 domain_id 映射到 leporidae tbl_user.id。"""
    # 每个 domain 取第一个 user
    domain_to_src_uid: Dict[int, int] = {}
    for row in source_conn.execute(
        text("SELECT domain_id, user_id FROM dominations ORDER BY domain_id, user_id")
    ):
        if row.domain_id not in domain_to_src_uid:
            domain_to_src_uid[row.domain_id] = row.user_id

    source_users: Dict[int, str] = {
        row.id: row.username
        for row in source_conn.execute(text("SELECT id, username FROM users"))
    }
    target_users: Dict[str, str] = {
        str(row.username): str(row.id)
        for row in target_conn.execute(text("SELECT id, username FROM tbl_user"))
    }

    result: Dict[int, str] = {}
    for domain_id, src_uid in domain_to_src_uid.items():
        username = source_users.get(src_uid)
        if not username:
            logger.warning(
                "domain %d 的用户 id=%d 在源库中不存在，将跳过该 domain 的所有卡片",
                domain_id,
                src_uid,
            )
            continue
        tgt_uid = target_users.get(username)
        if not tgt_uid:
            logger.warning(
                "domain %d 的用户 %s 在目标库 tbl_user 中不存在，将跳过该 domain 的所有卡片",
                domain_id,
                username,
            )
            continue
        result[domain_id] = tgt_uid
    return result


def _load_associated_card_ids(conn: Connection) -> Set[int]:
    """加载已关联订单的 card_id 集合（card_orders.order_id IS NOT NULL）。"""
    rows = conn.execute(
        text(
            "SELECT card_id FROM card_orders "
            "WHERE order_id IS NOT NULL AND card_id IS NOT NULL"
        )
    )
    return {row.card_id for row in rows}


def _load_cards(conn: Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        text(
            "SELECT id, uuid, domain_id, status, preference, properties, "
            "created_at, updated_at FROM cards ORDER BY id"
        )
    )
    result = []
    for row in rows:
        result.append(
            {
                "id": row.id,
                "uuid": row.uuid,
                "domain_id": row.domain_id,
                "status": row.status,
                "preference": _parse_json(row.preference),
                "properties": _parse_json(row.properties),
                "created_at": _ensure_datetime(row.created_at),
                "updated_at": _ensure_datetime(row.updated_at),
            }
        )
    return result


# ---------------------------------------------------------------------------
# products
# ---------------------------------------------------------------------------


def _migrate_products(
    *,
    source_cards: Sequence[Dict[str, Any]],
    target_conn: Connection,
    domain_to_user: Mapping[int, str],
    associated_card_ids: Set[int],
    card_to_product_id: Mapping[int, str],
    uuid_map: Mapping[str, str],
    config: MigrateCardConfig,
) -> tuple[MergeSectionResult, Set[int]]:
    summary = MergeSectionResult()
    migrated_card_ids: Set[int] = set()
    existing_ids: Set[str] = {
        str(row.id) for row in target_conn.execute(text("SELECT id FROM tbl_product"))
    }

    payload: List[dict] = []

    for card in source_cards:
        summary.processed += 1

        user_id = domain_to_user.get(card["domain_id"])
        if not user_id:
            summary.skipped += 1
            logger.warning(
                "跳过卡片 %s：domain %d 无法映射到目标库用户",
                card["uuid"],
                card["domain_id"],
            )
            continue

        pref = card["preference"]
        pattern = pref.get("pattern")

        if pattern == _PATTERN_DXPASS:
            material_id = config.material_id_dx
            type_id = config.type_id_dx
            design = convert_dxpass_design(pref, uuid_map, card["id"])
        elif pattern == _PATTERN_WARS:
            material_id = config.material_id_wars
            type_id = config.type_id_wars
            design = convert_wars_design(pref, uuid_map)
        else:
            summary.skipped += 1
            logger.warning(
                "跳过卡片 %s：不支持的 pattern %s（仅支持 DXPASS=1, WARS=2）",
                card["uuid"],
                pattern,
            )
            continue

        product_id = card_to_product_id[card["id"]]
        if product_id in existing_ids:
            summary.updated += 1
        else:
            summary.inserted += 1
            existing_ids.add(product_id)

        migrated_card_ids.add(card["id"])
        payload.append(
            {
                "id": product_id,
                "name": "",
                "description": "",
                "price": "0.00",
                "design": json.dumps(design, ensure_ascii=False),
                "is_associated": card["id"] in associated_card_ids,
                "is_modify_allowed": False,
                "user_id": ADMIN_USER_ID,
                "material_id": material_id,
                "type_id": type_id,
                "created_at": card["created_at"],
                "updated_at": card["updated_at"],
            }
        )

        if len(payload) >= config.batch_size:
            _upsert_products(target_conn, payload)
            payload.clear()

    if payload:
        _upsert_products(target_conn, payload)

    logger.info(
        "商品迁移完成：共处理 %d 条，新增 %d 条，更新 %d 条，跳过 %d 条",
        summary.processed,
        summary.inserted,
        summary.updated,
        summary.skipped,
    )
    return summary, migrated_card_ids


def _upsert_products(conn: Connection, payload: Sequence[dict]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO tbl_product (
                id, name, description, price, design,
                is_associated, is_modify_allowed,
                user_id, material_id, type_id,
                created_at, updated_at
            )
            VALUES (
                :id, :name, :description, :price, :design,
                :is_associated, :is_modify_allowed,
                :user_id, :material_id, :type_id,
                :created_at, :updated_at
            )
            ON CONFLICT (id) DO UPDATE SET
                name              = EXCLUDED.name,
                description       = EXCLUDED.description,
                price             = EXCLUDED.price,
                design            = EXCLUDED.design,
                is_associated     = EXCLUDED.is_associated,
                is_modify_allowed = EXCLUDED.is_modify_allowed,
                user_id           = EXCLUDED.user_id,
                material_id       = EXCLUDED.material_id,
                type_id           = EXCLUDED.type_id,
                updated_at        = EXCLUDED.updated_at
            """
        ),
        payload,
    )


# ---------------------------------------------------------------------------
# artifacts
# ---------------------------------------------------------------------------


def _migrate_artifacts(
    *,
    source_cards: Sequence[Dict[str, Any]],
    target_conn: Connection,
    card_to_product_id: Mapping[int, str],
    old_uuid_to_product_id: Mapping[str, str],
    config: MigrateCardConfig,
) -> MergeSectionResult:
    summary = MergeSectionResult()

    # 用 product_id 检测已有工件，避免重复插入（tbl_artifact 无 unique(product_id) 约束）
    existing_product_ids: Set[str] = {
        str(row.product_id)
        for row in target_conn.execute(text("SELECT product_id FROM tbl_artifact"))
    }

    payload: List[dict] = []

    for card in source_cards:
        summary.processed += 1

        product_id = card_to_product_id[card["id"]]
        if product_id in existing_product_ids:
            summary.skipped += 1
            logger.debug("跳过工件（商品 %s 已有工件，不重复插入）", product_id)
            continue

        artifact_status = _CARD_STATUS_TO_ARTIFACT_STATUS.get(str(card["status"]), 0)

        # 解析 derived_from：旧 card.uuid → 新 product UUID
        derived_from_old: str | None = card["properties"].get("derived_from")
        derived_from_new: str | None = None
        if derived_from_old:
            derived_from_new = old_uuid_to_product_id.get(derived_from_old)
            if not derived_from_new:
                logger.warning(
                    "卡片 %s 的 derived_from=%s 找不到对应商品，将忽略该字段",
                    card["uuid"],
                    derived_from_old,
                )

        storage = build_usagicard_storage(derived_from=derived_from_new)

        summary.inserted += 1
        existing_product_ids.add(product_id)

        payload.append(
            {
                "id": card["uuid"],
                "status": artifact_status,
                "storage": json.dumps(storage, ensure_ascii=False),
                "batch_id": None,
                "product_id": product_id,
                "created_at": card["created_at"],
                "updated_at": card["updated_at"],
            }
        )

        if len(payload) >= config.batch_size:
            _insert_artifacts(target_conn, payload)
            payload.clear()

    if payload:
        _insert_artifacts(target_conn, payload)

    logger.info(
        "工件迁移完成：共处理 %d 条，新增 %d 条，更新 %d 条，跳过 %d 条",
        summary.processed,
        summary.inserted,
        summary.updated,
        summary.skipped,
    )
    return summary


def _insert_artifacts(conn: Connection, payload: Sequence[dict]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO tbl_artifact (id, status, storage, batch_id, product_id, created_at, updated_at)
            VALUES (:id, :status, :storage, :batch_id, :product_id, :created_at, :updated_at)
            """
        ),
        payload,
    )


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _parse_json(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _create_engine(url: str, *, name: str) -> Engine:
    engine = create_engine(url, pool_pre_ping=True, future=True)
    logger.debug("已创建 %s 数据库引擎: %s", name, url)
    return engine


def _ensure_datetime(value: datetime | None) -> datetime:
    if value is None:
        return datetime.utcnow()
    if value.tzinfo:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


__all__ = [
    "MATERIAL_ID_DX",
    "MATERIAL_ID_WARS",
    "TYPE_ID_DX",
    "TYPE_ID_WARS",
    "MigrateCardConfig",
    "MigrateCardResult",
    "run_migrate_card",
]
