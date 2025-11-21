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
    build_image_labels,
    build_image_name,
    derive_aspect_id,
)

logger = logging.getLogger(__name__)

# wtf i have no idea to map these properly so hardcode for now
uuid_mapping = {
    "1051f05f-d605-4b73-93b8-66c6e32c4979": "bce7437d-0243-4b03-8ee4-90cf069fb403",
    "1a0bbc52-a927-45ef-b81d-a14c0442b563": "dce699fc-6878-43d6-b7fc-ce2d2d5afb40",
    "1c30bd9d-a0be-434f-96a3-31a34291ebc6": "deff9fde-4542-4dad-aa60-15b8848b2513",
    "1ed53623-0107-4b20-bd6b-699b27ad5b09": "84adfa36-69b7-491e-8faf-ecfeca08ba2c",
    "268a5b81-1e53-42aa-aaf0-4d17936d7116": "039f9d39-4bc1-4569-b4c1-ae5b7c6483c1",
    "2e0265e4-89b5-4977-86d4-96f961960561": "fcab9c9c-0be7-4d14-9172-72f10ee4282e",
    "3f9e16ad-cd47-4910-a4e5-d8c222976791": "cad4cc4f-0073-42e8-8596-bd22a7f5193d",
    "4091f5b5-f08e-45f3-ab0b-53249f3609e2": "f56964ac-6940-40b0-bb31-bd60afa064d2",
    "596a0a0e-721a-4fc4-a49b-b54f612d75e5": "1d4628b8-c1e8-4c8c-9d75-c6cb08cead0c",
    "77e8169b-e48f-43f0-b4ed-e9fd3c87805b": "23164b47-1cd4-4c39-b895-488eba89b8ab",
    "7d25b953-39b3-41a3-aec3-3cfa5e2d7adb": "b916c283-7003-4bec-b1d1-950ecb5b79ff",
    "888794e2-8d8a-4838-9d0f-d3e8d06d9b30": "3b57ce3f-e2d4-4165-844c-0d0d39fbbab2",
    "8eb3efbf-b199-48b3-aa08-fd427ac91cc9": "7e30dae1-d1f8-47bd-a6ff-56ac5983e6e2",
    "944d9a96-2774-4f4a-a9f9-422fb5f5f1a6": "ce79feda-4f5e-47fc-95bc-23f1e293570b",
    "adb6b3e9-b7f5-4724-946a-ef35be51603e": "6a742fd3-f9e2-4edf-ab65-9208fae30d36",
    "b0456170-441f-4dc1-81ce-fe549d442e90": "f7d4c8a8-9028-4375-9908-1ee59aadc18b",
    "b1c34d34-d086-4a08-9c96-df9b6dfe701f": "380e61f5-78a7-49c4-ba2e-013277f2741a",
    "b4152ca1-4722-414a-8b43-7483154bf217": "a23b0140-42e6-43df-b303-34596cb389be",
    "dd5043d8-02ef-4f94-877e-de236c048a02": "d9fd855e-d44b-48d0-aa0e-bf9c966e7ed9",
    "f0a98df7-7b0a-4e60-bfe9-5ce419713d34": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "7a54e27d-0908-45be-a4f2-5a32021e26f9": "f363a632-4b9f-41de-824b-43bd010b9975",
    "48de8539-d700-4323-8e18-218c1751fd7b": "2aa7c158-875e-4cb1-96b1-65446917e4dc",
    "da6224bd-b7a3-4d29-8ba4-337347cba745": "036f4e1b-75f9-428c-9bab-180547bfaa3c",
    "d94e8c0d-15ac-47f6-92d1-d0fe6ffb99f8": "421943e9-2221-45f1-8f76-5a1ca012028e",
    "6c990fb7-beca-4ce5-93a7-f19f5e591a4d": "b86508d5-1aba-45b0-b66a-c91f2efcd319",
    "de4ba22d-37b4-43e9-8eaa-845f3e1b02b1": "fa52e0c6-656a-4812-818b-fb19455b1043",
    "76675abf-c8de-4486-b673-587e3b3e7f8f": "421943e9-2221-45f1-8f76-5a1ca012028e",
    "10843d4c-5522-4fb5-943c-9e4da8966d5a": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "25776251-7a69-4265-a076-a3f8a526aee6": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "2c4bc603-322a-4e7b-a5be-083ef3143583": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "2d3fa0aa-5bc0-48a1-bcb6-ae05c94dfdb5": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "2e104068-2394-4337-b87b-9c03b39ec552": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "2f49c787-0e8a-40ca-adf2-0c665a93e942": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "40a158db-0e11-466b-919d-ac4e4307ecea": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "418737b1-8e60-4221-87cd-eb984ee47f10": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "4f54a8d1-4c60-414a-8846-d60ecf37b50b": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "50fa5f8e-5fcb-4407-aeb9-953c24fe192d": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "6764ee42-09e3-4123-9e06-476b50fc1d8e": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "a9aa77c8-8a75-4446-a4f9-7b42e5e84014": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "c809fa61-8a3a-40c5-a25c-4fa7353b28d5": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "e3976992-97b7-4e3a-8a8f-b21e531a5b77": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "f0ce2fde-99a7-4619-8059-3ed902d6d301": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
    "f973d4f4-e1d4-468c-a38b-ea5bcd6232f3": "d46688e2-ace7-40ce-b76a-f89e11a016ff",
}


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
    "DIVING_FISH": {"prefix": "dvfh_", "strategy": 1, "identifier": "divingfish"},
    "LXNS": {"prefix": "lxns_", "strategy": 2, "identifier": "lxns"},
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
        source_conn=source_conn,
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
        identifier = (row.identifier or "").strip().lower()
        if not identifier:
            continue
        mapping[identifier] = int(row.id)
    required = {"divingfish", "lxns"}
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

        accounts = list(accounts_by_user.get(user.username, ()))
        if not accounts:
            summary.skipped += 1
            logger.warning(
                "跳过用户 %s：未找到任何账号",
                user.username,
            )
            continue

        primary = _select_primary_account(user.prefer_server, accounts)
        if primary is None or primary.account_server not in SERVER_RULES:
            primary = next(
                (acct for acct in accounts if acct.account_server in SERVER_RULES),
                None,
            )
            if primary is None:
                summary.skipped += 1
                logger.warning(
                    "跳过用户 %s：所有账号的服务器均不受支持",
                    user.username,
                )
                continue
            logger.info(
                "用户 %s 的首选服务器 %s 不可用，改用账号 %s 的服务器 %s",
                user.username,
                user.prefer_server or "",
                primary.account_name,
                primary.account_server,
            )

        server_rule = SERVER_RULES.get(primary.account_server)
        if server_rule is None:
            summary.skipped += 1
            logger.warning(
                "跳过用户 %s：无法识别的服务器 %s",
                user.username,
                primary.account_server,
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
                prefer_server=primary.account_server,
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

            if account.account_server == "DIVING_FISH":
                continue

            credentials = account.account_password

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
    source_conn: Connection,
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
    image_uuid_lookup = _build_image_uuid_lookup(source_conn, leporid_conn)

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
        "enable_mask": False,
        "show_date": pref.show_date,
        "character_id": character_id,
        "mask_id": "",
        "background_id": background_id,
        "frame_id": frame_id,
        "passname_id": passname_id,
    }


def _build_image_uuid_lookup(
    source_conn: Connection, leporid_conn: Connection
) -> Dict[str, str]:
    sega_name_by_id = {
        str(row.id): row.sega_name
        for row in source_conn.execute(text("SELECT id, sega_name FROM images"))
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
    if value in lookup:
        return lookup[value]
    if value in uuid_mapping:
        return uuid_mapping[value]
    return value


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

        entry = _adapt_image_row_for_up(
            row, aspect_id, user.new_user_id, row.uploaded_by is not None
        )
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


def _adapt_image_row_for_up(row, aspect_id: str, user_id: str, workshop: bool) -> dict:
    uploaded_at = _ensure_datetime(row.uploaded_at)
    labels = build_image_labels(row.kind, row.sega_name, workshop)
    return {
        "id": row.id,
        "user_id": user_id,
        "aspect_id": aspect_id,
        "name": build_image_name(row.name, ""),
        "description": "",
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
