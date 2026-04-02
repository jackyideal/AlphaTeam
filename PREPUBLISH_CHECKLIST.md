# Prepublish Checklist (2026-04-02)

目标：在推送到 GitHub 前，确认仓库满足「可公开、可复现、可运行」。

## 1. 安全检查

- 结果：通过
- 明文密钥扫描：未发现 `sk-` 长 token / 硬编码 `API_KEY` / 硬编码 `TUSHARE_TOKEN`
- 本机绝对路径扫描：未发现 `/Users/<name>/...` 路径残留

## 2. 数据泄露检查

- 结果：通过
- 数据库文件：未发现 `*.db` / `*.db-shm` / `*.db-wal`
- 本地环境文件：未发现 `.env`
- 大文件检查：未发现单文件 > 20MB
- 备注：`/api/team/status` 在运行时可能自动创建 `ai_team/data/agent_memory.db`，推送前需再次确认该文件未出现。

## 3. 结构与忽略规则

- 结果：通过
- `.gitignore` 已覆盖运行态产物：
- `AlphaFin/ai_team/data/*.db*`
- `AlphaFin/static/charts/*`（保留 `.gitkeep`）
- `AlphaFin/data/*`（保留 `.gitkeep`）
- `data/db/*`（保留 `.gitkeep`）
- Python 缓存目录与 `.DS_Store`

## 4. 运行链路验证

- 结果：通过（基于 Flask test client）
- 页面路由：
- `/` -> 200
- `/team` -> 200
- `/ai` -> 200
- `/alpha_worker` -> 404（已移除，纯 AlphaTeam 版本）
- `/api/team/status` -> 200
- `/team` 页面包含 `three_scene.js` 引用（3D 前端链路存在）
- `/api/update`（POST）-> 200（数据库更新入口可触发任务）
- `python3 -m AlphaFin.scripts.build_db --help` 可执行
- 全量 Python 语法编译检查通过（`py_compile`）

## 5. 可复现资产检查

- 结果：通过
- 已包含：
- `requirements.txt`
- `.env.example`
- `README.md`
- `AlphaFin/scripts/build_db.py`
- `AlphaFin/scripts/update_funcs.py`
- 配置文件已切换为环境变量驱动：
- `AlphaFin/config.py`
- `AlphaFin/ai_team/config.py`

## 6. 已知剩余风险（可接受）

- 3D/图表前端默认依赖 CDN（Three.js/ECharts/Marked）；离线环境会受影响。
- 两个策略指标需要外部目录：
- `ind_21_agent_strategy` 依赖 `STOCKAGENT_DIR`
- `ind_22_fin_evolver` 依赖 `FIN_EVOLVER_DIR`
- 不影响 AlphaTeam 主流程与 3D 页面运行，但会影响上述策略指标功能。

## 7. 结论

- 当前仓库可进入推送阶段。
- 建议推送前最后再执行一次：

```bash
rg -n --hidden "sk-[A-Za-z0-9]{8,}|/Users/[A-Za-z0-9_\\-]+|PycharmProjects/stock" .
```
