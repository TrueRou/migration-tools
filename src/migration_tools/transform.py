from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, List, Mapping, Optional

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


# ---------------------------------------------------------------------------
# UsagiCard card design converters
# ---------------------------------------------------------------------------


def _resolve_image_id(old_uuid: Optional[str], uuid_map: Mapping[str, str]) -> str:
    """将旧系统图片 UUID 解析为新系统 UUID。

    merge-uc 命令迁移图片时保留了原始 UUID（images.uuid → tbl_image.id），
    因此大多数情况下直接原样返回即可；uuid_map 仅用于覆盖已知需要重映射的情况。
    """
    if not old_uuid:
        return ""
    return uuid_map.get(old_uuid, old_uuid)


def convert_dxpass_design(preference: dict, uuid_map: Mapping[str, str], card_id: int) -> dict:
    """将 UsagiCard DXPass CardPreference 转换为 leporidae UsagiCardDXDesign。

    - CardElement 数组（1=QRCODE_FRONT, 2=QRCODE_BACK）→ enable_* bool 字段
    - leporidae 新增字段（card_number / card_number_label / mask_id）填充空字符串默认值
    - enable_chara_info / enable_landscape / enable_mask 使用保守默认值
    """
    elements: list = preference.get("elements") or []
    return {
        "game_version": preference.get("game_version") or "",
        "simplified_code": preference.get("simplified_code") or "",
        "character_name": preference.get("character_name") or "",
        "friend_code": preference.get("friend_code") or "",
        "display_name": preference.get("display_name") or "",
        "dx_rating": preference.get("dx_rating") or "",
        "card_number": str(card_id),
        "card_number_label": "",
        "override_qrcode": preference.get("override_qrcode") or "",
        "player_info_color": preference.get("player_info_color") or "#ffffff",
        "chara_info_color": preference.get("chara_info_color") or "#fee37c",
        "enable_qrcode_front": 1 in elements,
        "enable_qrcode_back": 2 in elements,
        "enable_chara_info": True,
        "enable_landscape": False,
        "enable_mask": False,
        "character_id": _resolve_image_id(preference.get("character_id"), uuid_map),
        "mask_id": "",
        "background_id": _resolve_image_id(preference.get("background_id"), uuid_map),
        "cardback_id": _resolve_image_id(preference.get("cardback_id"), uuid_map),
        "frame_id": _resolve_image_id(preference.get("frame_id"), uuid_map),
        "passname_id": _resolve_image_id(preference.get("passname_id"), uuid_map),
    }


def convert_wars_design(preference: dict, uuid_map: Mapping[str, str]) -> dict:
    """将 UsagiCard Wars CardPreference 转换为 leporidae UsagiCardWarsDesign。"""
    return {
        "character_name": preference.get("character_name") or "",
        "skill_name": preference.get("skill_name") or "",
        "skill_description": preference.get("skill_description") or "",
        "miss": int(preference.get("miss") or 0),
        "combo": int(preference.get("combo") or 0),
        "chain": int(preference.get("chain") or 0),
        "character_id": _resolve_image_id(preference.get("character_id"), uuid_map),
        "background_id": _resolve_image_id(preference.get("background_id"), uuid_map),
        "cardback_id": _resolve_image_id(preference.get("cardback_id"), uuid_map),
        "frame_id": _resolve_image_id(preference.get("frame_id"), uuid_map),
        "label_id": _resolve_image_id(preference.get("label_id"), uuid_map),
    }


def build_usagicard_storage(*, derived_from: Optional[str] = None) -> dict:
    """构建 UsagiCardStorage 字典，使用迁移默认值。

    - 若卡片为派生卡（derived_from 不为 None），derived_behavior 设为 "redirect"
    - 其余字段均使用 UsagiCardStorage 的零值默认值
    """
    return {
        "card_title": None,
        "card_avatar": None,
        "card_profile": None,
        "skip_tour": False,
        "default_function_tab": None,
        "default_qbutton_tab": None,
        "secondary_auth_policy": "public_read",
        "secondary_auth_enabled": False,
        "secondary_auth_users": [],
        "derived_from": derived_from,
        "derived_behavior": "redirect" if derived_from else "none",
    }


# ---------------------------------------------------------------------------
# Maimai score converters (otoge-service)
# ---------------------------------------------------------------------------

# PostgreSQL ENUM 合法值（来自 otoge-service DDL）
_MAIMAI_FC_VALUES: frozenset[str] = frozenset({"APP", "AP", "FCP", "FC"})
_MAIMAI_FS_VALUES: frozenset[str] = frozenset({"SYNC", "FS", "FSP", "FSD", "FSDP"})
_MAIMAI_RATE_VALUES: frozenset[str] = frozenset({
    "SSSP", "SSS", "SSP", "SS", "SP", "S",
    "AAA", "AA", "A", "BBB", "BB", "B", "C", "D",
})
_MAIMAI_SONG_TYPE_VALUES: frozenset[str] = frozenset({"STANDARD", "DX", "UTAGE"})

# level_index 规范化表：处理 ReMASTER 大小写及常见变体
_LEVEL_INDEX_NORMALIZE: dict[str, str] = {
    "BASIC": "BASIC",
    "ADVANCED": "ADVANCED",
    "EXPERT": "EXPERT",
    "MASTER": "MASTER",
    "REMASTER": "ReMASTER",
    "RE_MASTER": "ReMASTER",
    "REMASTER": "ReMASTER",
}


def _ensure_score_datetime(value: Any) -> datetime:
    """将任意值规范化为无时区 UTC datetime。"""
    if value is None:
        return datetime.utcnow()
    if isinstance(value, datetime):
        if value.tzinfo:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    return datetime.utcnow()


def convert_maimai_score(row: dict) -> dict:
    """将源库 maimai 成绩行转换为 otoge-service tbl_maimai_scores 的插入字典。

    源库预期字段（列名）：
        song_id, level_index, achievements, dx_score, dx_rating,
        play_count, fc (nullable), fs (nullable), rate, type, uuid,
        created_at, updated_at

    转换规则：
    - fc / fs 为空字符串或 NULL 时转为 None；值不在枚举集合中时也转为 None
    - level_index 统一大写后查表规范化，处理 ReMASTER 大小写
    - rate / type 统一大写
    - achievements 保留原始精度（Decimal / float），由 psycopg 写入 numeric(7,4)
    - created_at / updated_at 规范化为无时区 UTC datetime
    """
    # fc / fs：可为 NULL，超出枚举范围的值降级为 NULL
    fc_raw = row.get("fc")
    fc: Optional[str] = fc_raw.upper() if isinstance(fc_raw, str) and fc_raw else None
    if fc is not None and fc not in _MAIMAI_FC_VALUES:
        fc = None

    fs_raw = row.get("fs")
    fs: Optional[str] = fs_raw.upper() if isinstance(fs_raw, str) and fs_raw else None
    if fs is not None and fs not in _MAIMAI_FS_VALUES:
        fs = None

    # level_index：处理 ReMASTER 混合大小写
    level_index_key = str(row.get("level_index") or "").upper().replace("-", "_")
    level_index = _LEVEL_INDEX_NORMALIZE.get(level_index_key, level_index_key)

    rate = str(row.get("rate") or "").upper()
    song_type = str(row.get("type") or "").upper()

    return {
        "song_id": int(row["song_id"]),
        "level_index": level_index,
        "achievements": row["achievements"],
        "dx_score": int(row["dx_score"]),
        "dx_rating": float(row["dx_rating"]),
        "play_count": int(row["play_count"]),
        "fc": fc,
        "fs": fs,
        "rate": rate,
        "type": song_type,
        "uuid": str(row["uuid"]),
        "created_at": _ensure_score_datetime(row.get("created_at")),
        "updated_at": _ensure_score_datetime(row.get("updated_at")),
    }


__all__ = [
    "MergeSectionResult",
    "UnknownAspectError",
    "build_image_labels",
    "build_image_name",
    "build_usagicard_storage",
    "convert_dxpass_design",
    "convert_maimai_score",
    "convert_wars_design",
    "derive_aspect_id",
]
