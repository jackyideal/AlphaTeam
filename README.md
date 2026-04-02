# AlphaTeam

> A multi-agent open architecture for stock research and decision support.

AlphaTeam 是一个面向股票投研场景的开源系统架构，强调“可组合智能体 + 可追踪流程编排 + 可控工具调用”，用于支持研究、讨论、风控、复核与组合管理等协作流程。

## Abstract

传统投研系统通常在“单模型问答”与“固定规则引擎”之间二选一。AlphaTeam 的目标不是替代研究员判断，而是提供一个可扩展的协作式架构：

- 通过 `orchestrator` 将复杂任务分解为可执行流程。
- 通过角色化智能体将研究、风控、审计等职责解耦。
- 通过统一工具注册与审计机制约束模型的外部动作。
- 通过会话状态、记忆系统与活动流实现全过程可观测。

该仓库当前聚焦 `A-share first` 的应用环境，优先保证中国市场数据链路、时钟语义与策略语境的一致性。

## Research Positioning

AlphaTeam 是一个开源股票投研架构（Open Research Architecture），不是单一策略模型仓库，也不是纯 UI 项目。项目核心交付物是系统方法学与可运行实现，包括：

- 协作编排机制（任务分配、讨论、汇总、风控复核）。
- 智能体运行时机制（角色边界、模型路由、停止控制）。
- 工具治理机制（可见目录、源码透明、风险审查）。
- 组合运行机制（初始化、周期调度、盯盘流程）。

## Architecture Spec

### 1) Design Principles

- Role Decomposition: 将“研究、判断、风控、复盘”拆分为独立职责。
- Orchestration First: 先定义流程，再调用模型，避免无结构对话。
- Observable by Default: 所有关键动作进入统一活动流与可追踪状态。
- Tool Governance: 工具默认显式注册、审计可见、权限边界清晰。
- A-share Constraint Awareness: 交易时段、T+1 与本地数据链路优先。

### 2) Layered System View

- Interface Layer
  - Web App 与 Team Workspace 页面。
  - 3D 协作场景、活动时间线、配置与诊断面板。
- Orchestration Layer
  - `orchestrator` 负责研究流程调度与会话编排。
  - `portfolio_scheduler` 负责投资周期与盘中盯盘调度。
- Agent Runtime Layer
  - 角色化智能体执行具体任务并协作交互。
  - `agent_registry` 统一管理模型配置、状态与停止控制。
- Tool and Data Layer
  - `tool_registry` 管理工具注册、参数模式与执行入口。
  - 数据更新与构建脚本负责本地数据库可复现初始化。
- Memory and Governance Layer
  - 记忆系统、活动总线、会话控制、报告归档与工具审计接口。

### 3) Request Lifecycle (User Ask)

1. 用户在 `/team` 提交研究问题。
2. 编排器根据工作流决定直连回答或多智能体协作。
3. 智能体调用工具与上下文，逐步生成阶段性结论。
4. 风控/审计角色执行校验与补充说明。
5. 总监角色生成汇总回复，并写入活动流与可追踪状态。
6. 用户可继续追问、评分反馈或终止当前流程。

## Core Modules

- App Entry: `AlphaFin/app.py`
- Team API and Routes: `AlphaFin/ai_team/routes.py`
- Orchestrator: `AlphaFin/ai_team/core/orchestrator.py`
- Portfolio Scheduler: `AlphaFin/ai_team/core/portfolio_scheduler.py`
- Agent Registry: `AlphaFin/ai_team/core/agent_registry.py`
- Tool Registry: `AlphaFin/ai_team/core/tool_registry.py`
- Memory System: `AlphaFin/ai_team/core/memory.py`
- Team Frontend: `AlphaFin/ai_team/templates/ai_team.html`
- Team 3D Scene: `AlphaFin/ai_team/static/js/three_scene.js`
- DB Build Script: `AlphaFin/scripts/build_db.py`

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

配置环境变量（参考 `.env.example`），至少设置：

- `SECRET_KEY`
- `TUSHARE_TOKEN`
- `QWEN_API_KEY`
- `MOONSHOT_API_KEY`（建议，用于联网搜索链路）

启动服务：

```bash
python3 AlphaFin/app.py
```

访问入口：

- Team Workspace: `http://127.0.0.1:5002/team`

## Data Bootstrap

默认数据库目录为 `data/db`（可通过 `ALPHAFIN_DB_ROOT` 覆盖）。

```bash
# 快速模式（推荐，近 1 年）
python3 -m AlphaFin.scripts.build_db --mode quick

# 全量模式
python3 -m AlphaFin.scripts.build_db --mode full

# 全量 + 财务指标
python3 -m AlphaFin.scripts.build_db --mode full --include-fina
```

## A-share First Constraints

该项目当前优先面向 A 股投研流程，默认强调以下约束：

- 中国市场时钟语义与交易时段判断。
- 本地数据构建与更新链路可控。
- 投研场景中的风控与复核角色优先。

项目并不排斥扩展到其他市场，但相关数据适配与规则语义需由社区补充。

## 3D Workspace Reproduction

`/team` 页面的 3D 协作场景依赖：

- `AlphaFin/ai_team/static/js/three_scene.js`
- Three.js ES Module（通过 `importmap` 从 jsDelivr 加载）

因此默认配置下需可访问 CDN。ECharts/Marked 也使用 CDN 分发。

## Security and Data Governance

- 仓库不包含本地运行数据库、历史会话、运行轨迹等私有数据。
- 通过 `.gitignore` 屏蔽 `*.db`、trace、报告等运行态产物。
- API key 与 token 统一由环境变量注入，不在代码中明文提交。

推送前建议执行：

```bash
rg -n "sk-[A-Za-z0-9]{16,}|TUSHARE_TOKEN\\s*=\\s*'|API_KEY\\s*=\\s*'" AlphaFin
```

## Optional External Dependencies

以下指标依赖外部目录，不属于本仓库主体：

- `ind_21_agent_strategy` 依赖 `STOCKAGENT_DIR`（默认 `third_party/stockagent`）
- `ind_22_fin_evolver` 依赖 `FIN_EVOLVER_DIR`（默认 `third_party/Evolve_Fin/script`）

缺少上述目录不会阻塞 AlphaTeam 主流程，但对应指标不可用。

## Non-goals

- 本项目不保证投资收益或胜率。
- 本项目不是自动实盘交易系统。
- 本项目输出内容不构成投资建议。

## Citation

若你在研究或工程工作中使用 AlphaTeam，建议按软件仓库方式引用：

```bibtex
@software{alphateam2026,
  title   = {AlphaTeam: A Multi-Agent Open Architecture for Stock Research and Decision Support},
  author  = {AlphaTeam Contributors},
  year    = {2026},
  url     = {https://github.com/jackyideal/AlphaTeam}
}
```
