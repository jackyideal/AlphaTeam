# AlphaFinTeam (GitHub Staging)

此仓库用于发布可复现的 `AlphaTeam` 模块：
- 保留团队协作、3D 场景、工具调用与投资组合流程。
- 不包含本地运行数据库、历史会话、运行轨迹等私有数据。

## 1. 快速启动

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

设置环境变量（参考 `.env.example`），至少需要：
- `TUSHARE_TOKEN`
- `QWEN_API_KEY`
- `MOONSHOT_API_KEY`（联网搜索链路建议配置）
- `SECRET_KEY`

启动：

```bash
python3 AlphaFin/app.py
```

访问：
- `http://127.0.0.1:5002/team`

## 2. 数据库构建（快速优先）

默认数据库目录：
- `data/db`（可通过 `ALPHAFIN_DB_ROOT` 覆盖）

快速模式（推荐，最近 1 年）：

```bash
python3 -m AlphaFin.scripts.build_db --mode quick
```

全量模式：

```bash
python3 -m AlphaFin.scripts.build_db --mode full
```

附加财务指标全量构建（慢）：

```bash
python3 -m AlphaFin.scripts.build_db --mode full --include-fina
```

## 3. 3D 可视化复现说明

`/team` 页面 3D 场景使用：
- `AlphaFin/ai_team/static/js/three_scene.js`
- Three.js ES Module（通过 `importmap` 从 jsDelivr 加载）

因此在默认配置下，3D 复现需要网络可访问 jsDelivr CDN。页面中的 ECharts/Marked 也默认走 CDN。

## 4. 可选策略模型依赖

以下两个策略指标依赖外部项目目录，不属于本仓库主体：
- `ind_21_agent_strategy` 依赖 `STOCKAGENT_DIR`（默认 `third_party/stockagent`）
- `ind_22_fin_evolver` 依赖 `FIN_EVOLVER_DIR`（默认 `third_party/Evolve_Fin/script`）

如果不提供这些目录，系统主功能（含 AlphaTeam 与 3D 页面）仍可运行，但对应策略指标不可用。

## 5. 已做安全处理

- 移除仓库中的明文 API key / token。
- 移除并忽略本地数据库与运行态文件（`*.db`, traces, reports）。
- 保留数据库构建脚本，支持用户自行构建本地库。

## 6. 推送前检查

在推送到 GitHub 前，建议再执行一次：

```bash
rg -n "sk-[A-Za-z0-9]{16,}|TUSHARE_TOKEN\\s*=\\s*'|API_KEY\\s*=\\s*'" AlphaFin
```

若出现真实密钥，请先替换为环境变量并轮换旧密钥。
