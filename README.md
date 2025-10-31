# migration-tools

用于将旧版 MariaDB 数据库的数据迁移到新版 PostgreSQL 结构的命令行工具。工具基于 [Typer](https://typer.tiangolo.com/) 构建，并使用 `uv` 管理依赖。

## 环境准备

1. 安装 [uv](https://docs.astral.sh/uv/)。
2. 在项目根目录安装依赖：

```powershell
uv sync
```

## 使用方法

执行 `merge` 命令，将源 MariaDB 与目标 PostgreSQL 之间的 `users`、`images` 数据完成迁移：

```powershell
uv run migration-tools merge --source "mysql+pymysql://user:pass@localhost:3307/usagi_card" --target "postgresql+psycopg://user:pass@localhost:5432/leporid" --admin-user-id 1
```

命令会：

- 合并用户，并统一将目标库的权限设置为 `["NORMAL"]`。
- 对缺失上传者的图片，可通过 `--admin-user-id` 指定管理员账号接管，并保持其 `visibility=1`；原始带上传用户的图片会写入 `visibility=0`。
- 确保所需的 `id-1-ff` 图片比例配置存在，不存在则自动创建。

## 开发辅助

运行单元测试：

```powershell
uv run pytest
```

如需查看更多命令帮助：

```powershell
uv run migration-tools --help
```
