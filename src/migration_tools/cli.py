from __future__ import annotations

import logging
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .logging import configure_logging
from .merge import MergeConfig, run_merge
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


@app.command()
def merge(
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
        "执行 merge，source=%s target=%s batch_size=%s dry_run=%s admin_user_id=%s",
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


if __name__ == "__main__":
    app()
