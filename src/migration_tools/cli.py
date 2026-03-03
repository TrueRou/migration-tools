from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .copy_img import CopyImageConfig, run_copy_img
from .logging import configure_logging
from .merge import MergeConfig, run_merge
from .merge_up import MergeUpConfig, MergeUpResult, run_merge_up
from .migrate_card import MigrateCardConfig, run_migrate_card
from .migrate_scores import MigrateScoresConfig, run_migrate_scores
from .transform import MergeSectionResult

console = Console()
app = typer.Typer(help="数据库迁移命令行工具")


def _render_section(title: str, stats: MergeSectionResult) -> None:
    table = Table(title=title, expand=True)
    table.add_column("指标", justify="left")
    table.add_column("数量", justify="right")
    table.add_row("处理总数", str(stats.processed))
    table.add_row("新增", str(stats.inserted))
    table.add_row("更新", str(stats.updated))
    table.add_row("跳过", str(stats.skipped))
    console.print(table)


@app.command("merge-uc")
def merge_uc(
    source: str = typer.Option(
        ...,
        "--source",
        help="MariaDB 连接串，例如 mysql+pymysql://user:pass@host:port/db",
    ),
    target: str = typer.Option(
        ...,
        "--target",
        help="PostgreSQL 连接串，例如 postgresql+psycopg://user:pass@host:port/db",
    ),
    batch_size: int = typer.Option(500, min=1, help="每批批处理的数据量"),
    dry_run: bool = typer.Option(False, help="仅演练，不提交任何更改"),
    admin_user_id: Optional[str] = typer.Option(
        None, help="当图片缺失上传用户时，补充的管理员用户ID"
    ),
    log_level: Optional[str] = typer.Option("INFO", help="日志级别"),
) -> None:
    """将旧数据库中的用户与图片迁移到新架构。"""

    configure_logging(log_level)
    logging.getLogger(__name__).debug(
        "执行 merge-uc，source=%s target=%s batch_size=%s dry_run=%s admin_user_id=%s",
        source,
        target,
        batch_size,
        dry_run,
        admin_user_id,
    )

    config = MergeConfig(
        source_url=source,
        target_url=target,
        batch_size=batch_size,
        dry_run=dry_run,
        admin_user_id=admin_user_id,
    )

    try:
        result = run_merge(config)
    except Exception as exc:
        logging.getLogger("migration_tools").exception("迁移失败")
        typer.secho(f"迁移失败: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    console.rule("迁移结果")
    _render_section("Users", result.users)
    _render_section("Images", result.images)
    console.print("[green]迁移执行完毕[/green]")


def _render_merge_up(result: MergeUpResult) -> None:
    console.rule("迁移结果")
    _render_section("Users", result.users)
    _render_section("Third Parties", result.third_parties)
    _render_section("Accounts", result.accounts)
    _render_section("Ratings", result.ratings)
    _render_section("Preferences", result.preferences)
    _render_section("Images", result.images)


@app.command("copy-img")
def copy_img(
    source_dir: Path = typer.Option(
        ...,
        "--source-dir",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
        help="图片源目录，需包含 {id}.webp 文件",
    ),
    target_dir: Path = typer.Option(
        ...,
        "--target-dir",
        file_okay=False,
        dir_okay=True,
        writable=True,
        resolve_path=True,
        help="复制图片的目标目录",
    ),
    leporid: str = typer.Option(
        ...,
        "--leporid",
        help="Leporid PostgreSQL 连接串，例如 postgresql+psycopg://user:pass@host:port/leporid",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite/--no-overwrite",
        help="目标已存在时是否覆盖",
    ),
    log_level: Optional[str] = typer.Option("INFO", help="日志级别"),
) -> None:
    """从 leporid 查询图片列表并复制本地文件。"""

    configure_logging(log_level)

    source_dir = source_dir.resolve()
    target_dir = target_dir.resolve()

    logging.getLogger(__name__).debug(
        "执行 copy-img，source_dir=%s target_dir=%s leporid=%s overwrite=%s",
        source_dir,
        target_dir,
        leporid,
        overwrite,
    )

    config = CopyImageConfig(
        source_dir=source_dir,
        target_dir=target_dir,
        leporid_url=leporid,
        overwrite=overwrite,
    )

    try:
        stats = run_copy_img(config)
    except Exception as exc:
        logging.getLogger("migration_tools").exception("复制失败")
        typer.secho(f"复制失败: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    console.rule("复制结果")
    table = Table(expand=True)
    table.add_column("指标", justify="left")
    table.add_column("数量", justify="right")
    table.add_row("处理总数", str(stats.processed))
    table.add_row("已复制", str(stats.copied))
    table.add_row("源文件缺失", str(stats.skipped_missing))
    table.add_row("目标已存在", str(stats.skipped_existing))
    console.print(table)
    console.print("[green]图片复制完成[/green]")


@app.command("merge-up")
def merge_up(
    source: str = typer.Option(
        ...,
        "--source",
        help="MariaDB 连接串，例如 mysql+pymysql://user:pass@host:port/usagipass",
    ),
    leporid: str = typer.Option(
        ...,
        "--leporid",
        help="Leporid PostgreSQL 连接串，例如 postgresql+psycopg://user:pass@host:port/leporid",
    ),
    usagipass: str = typer.Option(
        ...,
        "--usagipass",
        help="Usagipass PostgreSQL 连接串，例如 postgresql+psycopg://user:pass@host:port/usagipass",
    ),
    batch_size: int = typer.Option(500, min=1, help="每批批处理的数据量"),
    dry_run: bool = typer.Option(False, help="仅演练，不提交任何更改"),
    log_level: Optional[str] = typer.Option("INFO", help="日志级别"),
) -> None:
    """迁移 usagipass MariaDB 数据到 leporid/usagipass PostgreSQL 架构。"""

    configure_logging(log_level)
    logging.getLogger(__name__).debug(
        "执行 merge-up，source=%s leporid=%s usagipass=%s batch_size=%s dry_run=%s",
        source,
        leporid,
        usagipass,
        batch_size,
        dry_run,
    )

    config = MergeUpConfig(
        source_url=source,
        leporid_url=leporid,
        usagipass_url=usagipass,
        batch_size=batch_size,
        dry_run=dry_run,
    )

    try:
        result = run_merge_up(config)
    except Exception as exc:
        logging.getLogger("migration_tools").exception("迁移失败")
        typer.secho(f"迁移失败: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    _render_merge_up(result)
    console.print("[green]迁移执行完毕[/green]")


@app.command("uc-card")
def uc_card(
    source: str = typer.Option(
        ...,
        "--source",
        help="MariaDB 连接串，例如 mysql+pymysql://user:pass@host:port/usagi_card",
    ),
    target: str = typer.Option(
        ...,
        "--target",
        help="PostgreSQL 连接串，例如 postgresql+psycopg://user:pass@host:port/leporidae",
    ),
    uuid_mapping: Optional[Path] = typer.Option(
        None,
        "--uuid-mapping",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="图片 UUID 映射 JSON 文件路径（由 generate_uuid_mapping.py 生成）",
    ),
    batch_size: int = typer.Option(500, min=1, help="每批处理的数据量"),
    dry_run: bool = typer.Option(False, help="仅演练，不提交任何更改"),
    log_level: Optional[str] = typer.Option("INFO", help="日志级别"),
) -> None:
    """将 UsagiCard 中的卡片迁移到 leporidae 的 tbl_product + tbl_artifact。

    每张 card 对应生成一条 tbl_product（设计数据）和一条 tbl_artifact（物理卡实体）。
    迁移前请先运行 merge-uc 完成用户与图片的迁移，并在 migrate_card.py 中
    填写 MATERIAL_ID_DX / MATERIAL_ID_WARS / TYPE_ID_DX / TYPE_ID_WARS。
    """
    configure_logging(log_level)
    logging.getLogger(__name__).debug(
        "执行 uc-card，source=%s target=%s uuid_mapping=%s batch_size=%s dry_run=%s",
        source,
        target,
        uuid_mapping,
        batch_size,
        dry_run,
    )

    config = MigrateCardConfig(
        source_url=source,
        target_url=target,
        uuid_mapping_path=str(uuid_mapping) if uuid_mapping else None,
        batch_size=batch_size,
        dry_run=dry_run,
    )

    try:
        result = run_migrate_card(config)
    except Exception as exc:
        logging.getLogger("migration_tools").exception("迁移失败")
        typer.secho(f"迁移失败: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    console.rule("迁移结果")
    _render_section("Products (tbl_product)", result.products)
    _render_section("Artifacts (tbl_artifact)", result.artifacts)
    console.print("[green]迁移执行完毕[/green]")


@app.command("uc-scores")
def uc_scores(
    source: str = typer.Option(
        ...,
        "--source",
        help="来源 PostgreSQL 连接串，例如 postgresql+psycopg://user:pass@host:port/usagicard",
    ),
    target: str = typer.Option(
        ...,
        "--target",
        help="目标 PostgreSQL 连接串，例如 postgresql+psycopg://user:pass@host:port/otoge-service",
    ),
    source_table: str = typer.Option(
        "maimai_scores",
        "--source-table",
        help="来源库中的成绩表名，默认 maimai_scores",
    ),
    batch_size: int = typer.Option(500, min=1, help="每批处理的数据量"),
    dry_run: bool = typer.Option(False, help="仅演练，不提交任何更改"),
    log_level: Optional[str] = typer.Option("INFO", help="日志级别"),
) -> None:
    """将 UsagiCard 的 maimai 成绩迁移到 otoge-service 的 tbl_maimai_scores。

    来源库需提供 PostgreSQL 连接串，目标库为 otoge-service 的 PostgreSQL 实例。
    迁移假设目标库成绩表为空，所有源库记录均直接 INSERT，不做去重处理。
    """
    configure_logging(log_level)
    logging.getLogger(__name__).debug(
        "执行 uc-scores，source=%s target=%s source_table=%s batch_size=%s dry_run=%s",
        source,
        target,
        source_table,
        batch_size,
        dry_run,
    )

    config = MigrateScoresConfig(
        source_url=source,
        target_url=target,
        source_table=source_table,
        batch_size=batch_size,
        dry_run=dry_run,
    )

    try:
        result = run_migrate_scores(config)
    except Exception as exc:
        logging.getLogger("migration_tools").exception("迁移失败")
        typer.secho(f"迁移失败: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    console.rule("迁移结果")
    _render_section("Scores (tbl_maimai_scores)", result.scores)
    console.print("[green]迁移执行完毕[/green]")


if __name__ == "__main__":
    app()
