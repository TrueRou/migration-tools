from __future__ import annotations

from datetime import datetime

from migration_tools import merge_up


def _make_account(
    username: str = "alice", server: str = "DIVING_FISH"
) -> merge_up.SourceAccount:
    now = datetime.utcnow()
    return merge_up.SourceAccount(
        username=username,
        account_name=f"{username}-acc",
        account_server=server,
        account_password="secret",
        nickname="nick",
        bind_qq="qq",
        player_rating=123,
        created_at=now,
        updated_at=now,
    )


def _make_user(
    username: str = "alice", prefer_server: str = "DIVING_FISH"
) -> merge_up.SourceUser:
    now = datetime.utcnow()
    return merge_up.SourceUser(
        username=username,
        prefer_server=prefer_server,
        created_at=now,
        updated_at=now,
    )


def test_select_primary_account_matches_preference() -> None:
    accounts = [_make_account(server="LXNS"), _make_account(server="DIVING_FISH")]
    result = merge_up._select_primary_account("DIVING_FISH", accounts)
    assert result is not None
    assert result.account_server == "DIVING_FISH"


def test_build_preference_row_defaults_when_missing() -> None:
    account = _make_account()
    user = _make_user()
    migrated = merge_up.MigratedUser(
        source=user,
        new_user_id="uuid-1",
        new_username="dvfh_alice",
        prefer_server=user.prefer_server,
        primary_account=account,
        accounts=[account],
    )

    row = merge_up._build_preference_row(migrated, None, {})

    assert row["player_info_color"] == "#ffffff"
    assert row["chara_info_color"] == "#fee37c"
    assert row["mask_id"] == ""
    assert row["show_dx_rating"] is True
    assert row["show_date"] is True


def test_build_preference_row_respects_existing_values() -> None:
    account = _make_account()
    user = _make_user()
    migrated = merge_up.MigratedUser(
        source=user,
        new_user_id="uuid-2",
        new_username="dvfh_alice",
        prefer_server=user.prefer_server,
        primary_account=account,
        accounts=[account],
    )

    pref = merge_up.SourcePreference(
        username=user.username,
        maimai_version="FESTiVAL",
        simplified_code="ABC",
        character_name="Char",
        friend_code="1234",
        display_name="Display",
        dx_rating="15.00",
        qr_size=20,
        mask_type=3,
        character_id="char-1",
        background_id="bg-1",
        frame_id="frame-1",
        passname_id="pass-1",
        chara_info_color="",
        show_date=False,
    )

    row = merge_up._build_preference_row(migrated, pref, {})

    assert row["maimai_version"] == "FESTiVAL"
    assert row["qr_size"] == 20
    assert row["mask_type"] == 3
    assert row["background_id"] == "bg-1"
    assert row["chara_info_color"] == "#fee37c"
    assert row["show_date"] is False
