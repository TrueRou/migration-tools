from __future__ import annotations

import json
import logging
import secrets
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Mapping, Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from .merge import ensure_required_aspects
from .transform import (
    MergeSectionResult,
    UnknownAspectError,
    build_image_description,
    build_image_labels,
    build_image_name,
    derive_aspect_id,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MergeUpConfig:
    """Runtime configuration for merge-up migrations."""

    source_url: str
    leporid_url: str
    usagipass_url: str
    batch_size: int = 500
    dry_run: bool = False


@dataclass(slots=True)
class MergeUpResult:
    """Grouped execution results for merge-up."""

    users: MergeSectionResult
    third_parties: MergeSectionResult
    accounts: MergeSectionResult
    ratings: MergeSectionResult
    preferences: MergeSectionResult
    images: MergeSectionResult


@dataclass(slots=True)
class SourceUser:
    username: str
    prefer_server: str
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class SourceAccount:
    username: str
    account_name: str
    account_server: str
    account_password: str
    nickname: str
    bind_qq: str
    player_rating: int
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class SourcePreference:
    username: str
    maimai_version: str
    simplified_code: str
    character_name: str
    friend_code: str
    display_name: str
    dx_rating: str
    qr_size: int
    mask_type: int
    character_id: str
    background_id: str
    frame_id: str
    passname_id: str
    chara_info_color: str
    show_date: bool


@dataclass(slots=True)
class MigratedUser:
    source: SourceUser
    new_user_id: str
    new_username: str
    prefer_server: str
    primary_account: SourceAccount
    accounts: Sequence[SourceAccount]


SERVER_RULES: Dict[str, dict] = {
    "DIVING_FISH": {"prefix": "dvfh_", "strategy": 1, "identifier": "DIVING_FISH"},
    "LXNS": {"prefix": "lxns_", "strategy": 2, "identifier": "LXNS"},
}


def run_merge_up(config: MergeUpConfig) -> MergeUpResult:
    """Entry point: coordinate merge-up migrations across three databases."""

    source_engine = _create_engine(config.source_url, name="source")
    leporid_engine = _create_engine(config.leporid_url, name="leporid")
    usagipass_engine = _create_engine(config.usagipass_url, name="usagipass")

    try:
        with (
            source_engine.connect() as source_conn,
            leporid_engine.connect() as leporid_conn,
            usagipass_engine.connect() as usagipass_conn,
        ):
            source_tx = source_conn.begin()
            leporid_tx = leporid_conn.begin()
            usagipass_tx = usagipass_conn.begin()
            try:
                result = _execute_merge_up(
                    source_conn=source_conn,
                    leporid_conn=leporid_conn,
                    usagipass_conn=usagipass_conn,
                    batch_size=config.batch_size,
                )
            except Exception:
                logger.exception("迁移过程中发生错误，正在回滚")
                source_tx.rollback()
                leporid_tx.rollback()
                usagipass_tx.rollback()
                raise
            else:
                if config.dry_run:
                    logger.info("dry-run 模式启用，所有更改已回滚")
                    source_tx.rollback()
                    leporid_tx.rollback()
                    usagipass_tx.rollback()
                else:
                    source_tx.commit()
                    leporid_tx.commit()
                    usagipass_tx.commit()
                    logger.info("迁移完成，所有更改已提交")
    finally:
        source_engine.dispose()
        leporid_engine.dispose()
        usagipass_engine.dispose()

    return result


def _execute_merge_up(
    *,
    source_conn: Connection,
    leporid_conn: Connection,
    usagipass_conn: Connection,
    batch_size: int,
) -> MergeUpResult:
    users = _load_source_users(source_conn)
    accounts = _load_source_accounts(source_conn)
    preferences = _load_source_preferences(source_conn)
    images = _load_source_images(source_conn)

    accounts_by_user: Dict[str, List[SourceAccount]] = defaultdict(list)
    for account in accounts:
        accounts_by_user[account.username].append(account)

    preferences_by_user = {pref.username: pref for pref in preferences}

    existing_users = _collect_existing_usernames(leporid_conn)
    server_ids = _load_server_ids(usagipass_conn)

    users_result, migrated_users, leporid_payload = _migrate_users(
        users=users,
        accounts_by_user=accounts_by_user,
        existing_users=existing_users,
    )

    _upsert_leporid_users(leporid_conn, leporid_payload, batch_size)

    third_result = _migrate_third_parties(
        leporid_conn=leporid_conn,
        migrated_users=migrated_users,
        batch_size=batch_size,
    )

    account_result = _migrate_accounts(
        usagipass_conn=usagipass_conn,
        migrated_users=migrated_users,
        server_ids=server_ids,
        batch_size=batch_size,
    )

    rating_result = _migrate_ratings(
        usagipass_conn=usagipass_conn,
        migrated_users=migrated_users,
        batch_size=batch_size,
    )

    preference_result = _migrate_preferences(
        usagipass_conn=usagipass_conn,
        leporid_conn=leporid_conn,
        migrated_users=migrated_users,
        preferences_by_user=preferences_by_user,
        batch_size=batch_size,
    )

    ensure_required_aspects(leporid_conn)
    image_result = _migrate_images(
        source_images=images,
        leporid_conn=leporid_conn,
        migrated_users={user.source.username: user for user in migrated_users},
        batch_size=batch_size,
    )

    return MergeUpResult(
        users=users_result,
        third_parties=third_result,
        accounts=account_result,
        ratings=rating_result,
        preferences=preference_result,
        images=image_result,
    )


def _load_source_users(conn: Connection) -> List[SourceUser]:
    rows = conn.execute(
        text(
            """
            SELECT username, prefer_server, created_at, updated_at
            FROM users
            ORDER BY username
            """
        )
    )
    result: List[SourceUser] = []
    for row in rows:
        result.append(
            SourceUser(
                username=row.username,
                prefer_server=(row.prefer_server or "").upper(),
                created_at=_ensure_datetime(row.created_at),
                updated_at=_ensure_datetime(row.updated_at),
            )
        )
    return result


def _load_source_accounts(conn: Connection) -> List[SourceAccount]:
    rows = conn.execute(
        text(
            """
            SELECT account_name, account_server, account_password, nickname, bind_qq, player_rating, username, created_at, updated_at
            FROM user_accounts
            ORDER BY username, account_server
            """
        )
    )
    result: List[SourceAccount] = []
    for row in rows:
        result.append(
            SourceAccount(
                username=row.username,
                account_name=row.account_name,
                account_server=(row.account_server or "").upper(),
                account_password=row.account_password,
                nickname=row.nickname,
                bind_qq=row.bind_qq,
                player_rating=row.player_rating or 0,
                created_at=_ensure_datetime(row.created_at),
                updated_at=_ensure_datetime(row.updated_at),
            )
        )
    return result


def _load_source_preferences(conn: Connection) -> List[SourcePreference]:
    rows = conn.execute(
        text(
            """
            SELECT username, maimai_version, simplified_code, character_name, friend_code, display_name, dx_rating,
                   qr_size, mask_type, character_id, background_id, frame_id, passname_id, chara_info_color, show_date
            FROM user_preferences
            """
        )
    )
    result: List[SourcePreference] = []
    for row in rows:
        result.append(
            SourcePreference(
                username=row.username,
                maimai_version=_null_to_empty(row.maimai_version),
                simplified_code=_null_to_empty(row.simplified_code),
                character_name=_null_to_empty(row.character_name),
                friend_code=_null_to_empty(row.friend_code),
                display_name=_null_to_empty(row.display_name),
                dx_rating=_null_to_empty(row.dx_rating),
                qr_size=row.qr_size or 15,
                mask_type=row.mask_type or 0,
                character_id=_null_to_empty(row.character_id),
                background_id=_null_to_empty(row.background_id),
                frame_id=_null_to_empty(row.frame_id),
                passname_id=_null_to_empty(row.passname_id),
                chara_info_color=_null_to_empty(row.chara_info_color) or "#fee37c",
                show_date=_coerce_bool(row.show_date, default=True),
            )
        )
    return result


def _load_source_images(conn: Connection):
    return list(
        conn.execute(
            text(
                """
                SELECT id, name, kind, sega_name, uploaded_by, uploaded_at
                FROM images
                WHERE uploaded_by IS NOT NULL
                ORDER BY uploaded_at, id
                """
            )
        )
    )


def _collect_existing_usernames(conn: Connection) -> Dict[str, str]:
    rows = conn.execute(text("SELECT username, id FROM tbl_user"))
    return {str(row.username): str(row.id) for row in rows}


def _load_server_ids(conn: Connection) -> Dict[str, int]:
    rows = conn.execute(text("SELECT id, identifier FROM tbl_server"))
    mapping: Dict[str, int] = {}
    for row in rows:
        identifier = (row.identifier or "").upper()
        mapping[identifier] = int(row.id)
    required = {"DIVING_FISH", "LXNS"}
    missing = required - mapping.keys()
    if missing:
        raise RuntimeError(
            f"Usagipass 数据库缺少 server 标识: {', '.join(sorted(missing))}"
        )
    return mapping


def _migrate_users(
    *,
    users: Sequence[SourceUser],
    accounts_by_user: Mapping[str, Sequence[SourceAccount]],
    existing_users: Dict[str, str],
) -> tuple[MergeSectionResult, List[MigratedUser], List[dict]]:
    summary = MergeSectionResult()
    migrated: List[MigratedUser] = []
    payload: List[dict] = []

    for user in users:
        summary.processed += 1
        server_rule = SERVER_RULES.get(user.prefer_server)
        if server_rule is None:
            summary.skipped += 1
            logger.warning(
                "跳过用户 %s：无法识别的 prefer_server=%s",
                user.username,
                user.prefer_server,
            )
            continue

        accounts = list(accounts_by_user.get(user.username, ()))
        primary = _select_primary_account(user.prefer_server, accounts)
        if primary is None:
            summary.skipped += 1
            logger.warning(
                "跳过用户 %s：未找到首选服务器 %s 对应的账号",
                user.username,
                user.prefer_server,
            )
            continue

        new_username = f"{server_rule['prefix']}{primary.account_name}"
        if new_username in existing_users:
            new_user_id = existing_users[new_username]
            summary.updated += 1
        else:
            new_user_id = str(uuid.uuid4())
            existing_users[new_username] = new_user_id
            summary.inserted += 1

        payload.append(
            {
                "id": new_user_id,
                "username": new_username,
                "password": _generate_password_hash(),
                "email": "",
                "permissions": "{}",
                "created_at": user.created_at,
                "updated_at": user.updated_at,
            }
        )

        migrated.append(
            MigratedUser(
                source=user,
                new_user_id=new_user_id,
                new_username=new_username,
                prefer_server=user.prefer_server,
                primary_account=primary,
                accounts=accounts,
            )
        )

    return summary, migrated, payload


def _upsert_leporid_users(
    conn: Connection, payload: Sequence[dict], batch_size: int
) -> None:
    for chunk in _chunked(payload, batch_size):
        if not chunk:
            continue
        conn.execute(
            text(
                """
                INSERT INTO tbl_user (id, username, password, email, permissions, created_at, updated_at)
                VALUES (:id, :username, :password, :email, :permissions, :created_at, :updated_at)
                ON CONFLICT (id) DO UPDATE SET
                    username = EXCLUDED.username,
                    password = EXCLUDED.password,
                    email = EXCLUDED.email,
                    permissions = EXCLUDED.permissions,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            chunk,
        )


def _migrate_third_parties(
    *,
    leporid_conn: Connection,
    migrated_users: Sequence[MigratedUser],
    batch_size: int,
) -> MergeSectionResult:
    summary = MergeSectionResult()
    existing = {
        (str(row.username), int(row.strategy)): str(row.id)
        for row in leporid_conn.execute(
            text("SELECT id, username, strategy FROM tbl_user_third_party")
        )
    }

    payload: List[dict] = []
    for user in migrated_users:
        for account in user.accounts:
            rule = SERVER_RULES.get(account.account_server)
            if rule is None:
                continue
            summary.processed += 1
            key = (account.account_name, rule["strategy"])
            if key in existing:
                entry_id = existing[key]
                summary.updated += 1
            else:
                entry_id = str(uuid.uuid4())
                existing[key] = entry_id
                summary.inserted += 1

            payload.append(
                {
                    "id": entry_id,
                    "user_id": user.new_user_id,
                    "username": account.account_name,
                    "strategy": rule["strategy"],
                    "created_at": account.created_at,
                    "updated_at": account.updated_at,
                }
            )

            if len(payload) >= batch_size:
                _upsert_third_party(leporid_conn, payload)
                payload.clear()

    if payload:
        _upsert_third_party(leporid_conn, payload)

    return summary


def _upsert_third_party(conn: Connection, payload: Sequence[dict]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO tbl_user_third_party (id, user_id, username, strategy, created_at, updated_at)
            VALUES (:id, :user_id, :username, :strategy, :created_at, :updated_at)
            ON CONFLICT (id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                username = EXCLUDED.username,
                strategy = EXCLUDED.strategy,
                updated_at = EXCLUDED.updated_at
            """
        ),
        payload,
    )


def _migrate_accounts(
    *,
    usagipass_conn: Connection,
    migrated_users: Sequence[MigratedUser],
    server_ids: Mapping[str, int],
    batch_size: int,
) -> MergeSectionResult:
    summary = MergeSectionResult()
    existing = {
        (str(row.user_id), int(row.server_id)): str(row.id)
        for row in usagipass_conn.execute(
            text("SELECT id, user_id, server_id FROM tbl_account")
        )
    }

    payload: List[dict] = []
    for user in migrated_users:
        for account in user.accounts:
            rule = SERVER_RULES.get(account.account_server)
            if rule is None:
                continue
            identifier = rule["identifier"]
            server_id = server_ids.get(identifier)
            if server_id is None:
                logger.warning(
                    "跳过账号 %s：未在 usagipass 找到 server %s",
                    account.account_name,
                    identifier,
                )
                continue
            summary.processed += 1
            key = (user.new_user_id, server_id)
            if key in existing:
                entry_id = existing[key]
                summary.updated += 1
            else:
                entry_id = str(uuid.uuid4())
                existing[key] = entry_id
                summary.inserted += 1

            credentials = json.dumps(
                {
                    "accountName": account.account_name,
                    "accountPassword": account.account_password,
                },
                ensure_ascii=False,
            )

            payload.append(
                {
                    "id": entry_id,
                    "user_id": user.new_user_id,
                    "server_id": server_id,
                    "credentials": credentials,
                    "enabled": True,
                    "created_at": account.created_at,
                    "updated_at": account.updated_at,
                }
            )

            if len(payload) >= batch_size:
                _upsert_accounts(usagipass_conn, payload)
                payload.clear()

    if payload:
        _upsert_accounts(usagipass_conn, payload)

    return summary


def _upsert_accounts(conn: Connection, payload: Sequence[dict]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO tbl_account (id, user_id, server_id, credentials, enabled, created_at, updated_at)
            VALUES (:id, :user_id, :server_id, :credentials, :enabled, :created_at, :updated_at)
            ON CONFLICT (id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                server_id = EXCLUDED.server_id,
                credentials = EXCLUDED.credentials,
                enabled = EXCLUDED.enabled,
                updated_at = EXCLUDED.updated_at
            """
        ),
        payload,
    )


def _migrate_ratings(
    *,
    usagipass_conn: Connection,
    migrated_users: Sequence[MigratedUser],
    batch_size: int,
) -> MergeSectionResult:
    summary = MergeSectionResult()
    existing_users = {
        str(row.user_id)
        for row in usagipass_conn.execute(text("SELECT user_id FROM tbl_rating"))
    }
    payload: List[dict] = []
    for user in migrated_users:
        summary.processed += 1
        payload.append(
            {
                "user_id": user.new_user_id,
                "name": "",
                "rating": user.primary_account.player_rating,
                "friend_code": "",
                "updated_at": user.primary_account.updated_at,
            }
        )
        if user.new_user_id in existing_users:
            summary.updated += 1
        else:
            summary.inserted += 1
            existing_users.add(user.new_user_id)

        if len(payload) >= batch_size:
            _upsert_ratings(usagipass_conn, payload)
            payload.clear()

    if payload:
        _upsert_ratings(usagipass_conn, payload)

    return summary


def _upsert_ratings(conn: Connection, payload: Sequence[dict]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO tbl_rating (user_id, name, rating, friend_code, updated_at)
            VALUES (:user_id, :name, :rating, :friend_code, :updated_at)
            ON CONFLICT (user_id) DO UPDATE SET
                name = EXCLUDED.name,
                rating = EXCLUDED.rating,
                friend_code = EXCLUDED.friend_code,
                updated_at = EXCLUDED.updated_at
            """
        ),
        payload,
    )


def _migrate_preferences(
    *,
    usagipass_conn: Connection,
    leporid_conn: Connection,
    migrated_users: Sequence[MigratedUser],
    preferences_by_user: Mapping[str, SourcePreference],
    batch_size: int,
) -> MergeSectionResult:
    summary = MergeSectionResult()
    existing_users = {
        str(row.user_id)
        for row in usagipass_conn.execute(text("SELECT user_id FROM tbl_preference"))
    }
    image_uuid_lookup = _build_image_uuid_lookup(usagipass_conn, leporid_conn)

    payload: List[dict] = []
    for user in migrated_users:
        summary.processed += 1
        pref = preferences_by_user.get(user.source.username)
        payload.append(_build_preference_row(user, pref, image_uuid_lookup))
        if user.new_user_id in existing_users:
            summary.updated += 1
        else:
            summary.inserted += 1
            existing_users.add(user.new_user_id)

        if len(payload) >= batch_size:
            _upsert_preferences(usagipass_conn, payload)
            payload.clear()

    if payload:
        _upsert_preferences(usagipass_conn, payload)

    return summary


def _build_preference_row(
    user: MigratedUser,
    pref: SourcePreference | None,
    image_uuid_lookup: Mapping[str, str],
) -> dict:
    pref = pref or SourcePreference(
        username=user.source.username,
        maimai_version="",
        simplified_code="",
        character_name="",
        friend_code="",
        display_name="",
        dx_rating="",
        qr_size=15,
        mask_type=0,
        character_id="",
        background_id="",
        frame_id="",
        passname_id="",
        chara_info_color="#fee37c",
        show_date=True,
    )

    character_id = _resolve_image_reference(pref.character_id, image_uuid_lookup)
    background_id = _resolve_image_reference(pref.background_id, image_uuid_lookup)
    frame_id = _resolve_image_reference(pref.frame_id, image_uuid_lookup)
    passname_id = _resolve_image_reference(pref.passname_id, image_uuid_lookup)

    return {
        "user_id": user.new_user_id,
        "maimai_version": pref.maimai_version,
        "simplified_code": pref.simplified_code,
        "character_name": pref.character_name,
        "friend_code": pref.friend_code,
        "display_name": pref.display_name,
        "dx_rating": pref.dx_rating,
        "qr_size": pref.qr_size,
        "mask_type": pref.mask_type,
        "player_info_color": "#ffffff",
        "chara_info_color": pref.chara_info_color or "#fee37c",
        "show_dx_rating": True,
        "show_display_name": True,
        "show_friend_code": True,
        "show_date": pref.show_date,
        "character_id": character_id,
        "mask_id": "",
        "background_id": background_id,
        "frame_id": frame_id,
        "passname_id": passname_id,
    }


def _build_image_uuid_lookup(
    usagipass_conn: Connection, leporid_conn: Connection
) -> Dict[str, str]:
    sega_name_by_id = {
        str(row.id): row.sega_name
        for row in usagipass_conn.execute(text("SELECT id, sega_name FROM images"))
        if row.sega_name
    }
    if not sega_name_by_id:
        return {}

    image_id_by_file_name = {
        row.file_name: str(row.id)
        for row in leporid_conn.execute(
            text(
                """
                SELECT id, file_name
                FROM tbl_image
                WHERE file_name IS NOT NULL AND file_name <> ''
                """
            )
        )
        if row.file_name
    }

    mapping: Dict[str, str] = {}
    for source_id, sega_name in sega_name_by_id.items():
        target_id = image_id_by_file_name.get(sega_name)
        if target_id:
            mapping[source_id] = target_id
    return mapping


def _resolve_image_reference(value: str, lookup: Mapping[str, str]) -> str:
    if not value:
        return value
    return lookup.get(value, value)


def _upsert_preferences(conn: Connection, payload: Sequence[dict]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO tbl_preference (
                user_id,
                maimai_version,
                simplified_code,
                character_name,
                friend_code,
                display_name,
                dx_rating,
                qr_size,
                mask_type,
                player_info_color,
                chara_info_color,
                show_dx_rating,
                show_display_name,
                show_friend_code,
                show_date,
                character_id,
                mask_id,
                background_id,
                frame_id,
                passname_id
            )
            VALUES (
                :user_id,
                :maimai_version,
                :simplified_code,
                :character_name,
                :friend_code,
                :display_name,
                :dx_rating,
                :qr_size,
                :mask_type,
                :player_info_color,
                :chara_info_color,
                :show_dx_rating,
                :show_display_name,
                :show_friend_code,
                :show_date,
                :character_id,
                :mask_id,
                :background_id,
                :frame_id,
                :passname_id
            )
            ON CONFLICT (user_id) DO UPDATE SET
                maimai_version = EXCLUDED.maimai_version,
                simplified_code = EXCLUDED.simplified_code,
                character_name = EXCLUDED.character_name,
                friend_code = EXCLUDED.friend_code,
                display_name = EXCLUDED.display_name,
                dx_rating = EXCLUDED.dx_rating,
                qr_size = EXCLUDED.qr_size,
                mask_type = EXCLUDED.mask_type,
                player_info_color = EXCLUDED.player_info_color,
                chara_info_color = EXCLUDED.chara_info_color,
                show_dx_rating = EXCLUDED.show_dx_rating,
                show_display_name = EXCLUDED.show_display_name,
                show_friend_code = EXCLUDED.show_friend_code,
                show_date = EXCLUDED.show_date,
                character_id = EXCLUDED.character_id,
                mask_id = EXCLUDED.mask_id,
                background_id = EXCLUDED.background_id,
                frame_id = EXCLUDED.frame_id,
                passname_id = EXCLUDED.passname_id
            """
        ),
        payload,
    )


def _migrate_images(
    *,
    source_images: Sequence,
    leporid_conn: Connection,
    migrated_users: Mapping[str, MigratedUser],
    batch_size: int,
) -> MergeSectionResult:
    summary = MergeSectionResult()
    existing_ids = {
        str(row.id) for row in leporid_conn.execute(text("SELECT id FROM tbl_image"))
    }

    payload: List[dict] = []
    for row in source_images:
        summary.processed += 1
        username = str(row.uploaded_by)
        user = migrated_users.get(username)
        if user is None:
            summary.skipped += 1
            logger.warning("跳过图片 %s：上传者 %s 未迁移", row.id, username)
            continue

        try:
            aspect_id = derive_aspect_id(row.kind)
        except UnknownAspectError as exc:
            summary.skipped += 1
            logger.warning("图片 %s 跳过：%s", row.id, exc)
            continue

        entry = _adapt_image_row_for_up(row, aspect_id, user.new_user_id)
        payload.append(entry)
        if str(row.id) in existing_ids:
            summary.updated += 1
        else:
            summary.inserted += 1
            existing_ids.add(str(row.id))

        if len(payload) >= batch_size:
            _upsert_images(leporid_conn, payload)
            payload.clear()

    if payload:
        _upsert_images(leporid_conn, payload)

    return summary


def _adapt_image_row_for_up(row, aspect_id: str, user_id: str) -> dict:
    uploaded_at = _ensure_datetime(row.uploaded_at)
    labels = build_image_labels(row.kind, row.sega_name)
    return {
        "id": row.id,
        "user_id": user_id,
        "aspect_id": aspect_id,
        "name": build_image_name(row.name, row.id, row.kind),
        "description": build_image_description(row.name, row.sega_name, row.kind),
        "visibility": 0,
        "labels": labels,
        "file_name": None,
        "metadata_id": None,
        "created_at": uploaded_at,
        "updated_at": datetime.utcnow(),
    }


def _upsert_images(conn: Connection, payload: Sequence[dict]) -> None:
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
                file_name,
                metadata_id,
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
                :file_name,
                :metadata_id,
                :created_at,
                :updated_at
            )
            ON CONFLICT (id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                aspect_id = EXCLUDED.aspect_id,
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                visibility = EXCLUDED.visibility,
                labels = EXCLUDED.labels,
                file_name = EXCLUDED.file_name,
                metadata_id = EXCLUDED.metadata_id,
                updated_at = EXCLUDED.updated_at
            """
        ),
        payload,
    )


def _select_primary_account(
    prefer_server: str, accounts: Sequence[SourceAccount]
) -> SourceAccount | None:
    for account in accounts:
        if account.account_server == prefer_server:
            return account
    return None


def _null_to_empty(value: str | None) -> str:
    return value or ""


def _coerce_bool(value, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return bool(int(value))


def _chunked(sequence: Sequence[dict], size: int) -> Iterator[Sequence[dict]]:
    chunk: List[dict] = []
    for item in sequence:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _create_engine(url: str, *, name: str) -> Engine:
    engine = create_engine(url, pool_pre_ping=True, future=True)
    logger.debug("已创建 %s 数据库引擎: %s", name, url)
    return engine


def _generate_password_hash() -> str:
    try:
        import importlib  # Delayed import keeps module importable without argon2 installed.

        argon2_low_level = importlib.import_module("argon2.low_level")
        hash_secret = argon2_low_level.hash_secret
        Type = argon2_low_level.Type
    except ImportError as exc:  # pragma: no cover - 环境缺失依赖时提示
        raise RuntimeError(
            "argon2-cffi 未安装，请先执行 `uv sync` 或手动安装依赖后再运行 merge-up"
        ) from exc

    secret = secrets.token_bytes(32)
    salt = secrets.token_bytes(16)
    hashed = hash_secret(
        secret,
        salt,
        time_cost=3,
        memory_cost=65536,
        parallelism=4,
        hash_len=32,
        type=Type.I,
    )
    return hashed.decode("utf-8")


def _ensure_datetime(value: datetime | None) -> datetime:
    if value is None:
        return datetime.utcnow()
    if value.tzinfo:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


__all__ = [
    "MergeUpConfig",
    "MergeUpResult",
    "run_merge_up",
]
