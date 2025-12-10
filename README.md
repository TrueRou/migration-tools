# migration-tools

用于将旧版 MariaDB 数据库的数据迁移到新版 PostgreSQL 结构的命令行工具。工具基于 [Typer](https://typer.tiangolo.com/) 构建，并使用 `uv` 管理依赖。

## 环境准备

1. 安装 [uv](https://docs.astral.sh/uv/)。
2. 在项目根目录安装依赖：

```powershell
uv sync
```

## 使用方法

执行 `merge-uc` 命令，将旧 UC 数据库中的 `users`、`images` 表迁移到 Leporid PostgreSQL：

```powershell
uv run migration-tools merge-uc --source "mysql+pymysql://user:pass@localhost:3307/usagi_card" --target "postgresql+psycopg://user:pass@localhost:5432/leporid" --admin-user-id 1
```

命令会：

- 对缺失上传者的图片，可通过 `--admin-user-id` 指定管理员账号接管，并保持其 `visibility=1`；原始带上传用户的图片会写入 `visibility=0`。
- 确保所需的 `id-1-ff` 图片比例配置存在，不存在则自动创建。

执行 `merge-up` 命令，可将 usagipass MariaDB 迁移到新的 Leporid / Usagipass PostgreSQL 架构：

```powershell
uv run migration-tools merge-up `
	--source "mysql+pymysql://user:pass@localhost:3307/usagipass" `
	--leporid "postgresql+psycopg://user:pass@localhost:5432/leporid" `
	--usagipass "postgresql+psycopg://user:pass@localhost:5432/usagipass"
```

该命令会：

- 为每位用户根据 `prefer_server` 生成新的 Leporid 账号，并补充第三方账号绑定、评分、偏好设置。
- 写入 Usagipass 的账号信息、评级与偏好，缺失字段会自动套用默认值。
- 只迁移由用户上传的图片（带 `uploaded_by` 的记录），公共图片不会重复上传。

## 开发辅助

运行单元测试：

```powershell
uv run pytest
```

如需查看更多命令帮助：

```powershell
uv run migration-tools --help
```
