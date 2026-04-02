<p align="center">
  <img src="docs/assets/logo.png" alt="AlphaTeam Logo" width="200" />
</p>

<h1 align="center">AlphaTeam</h1>

<p align="center">
  <a href="./README.md"><strong>中文</strong></a> | <a href="./README_EN.md">English</a>
</p>

> **构建资本市场可信、可控、可回溯的量化投研开放架构**
> *A Trustworthy, Controllable, and Traceable Framework for Financial Intelligence.*

---

## 📖 Background & Lineage (背景与渊源)

AlphaTeam 是基于我们前期学术开源项目 **[AlphaFin](https://github.com/AlphaFin-proj/AlphaFin)** 的工业级进阶版本。

* **[AlphaFin](https://github.com/AlphaFin-proj/AlphaFin) (前身):** 侧重于金融高质量数据集的构建、大模型微调、检索增强以及初步的单体逻辑链推理探索，旨在解决金融数据时效性与计算复杂性的基础问题。
  
* **AlphaTeam :** 在 AlphaFin 积累的金融直觉与数据底座之上，全面升级为**多智能体协作** 与**系统化工程治理**架构。完成了从“启发式的单体问答” 向 “具备可信，可控，可进化的量化投研框架”的跨越。
---

## 💡 核心理念 

在充满噪声的资本市场，我们不追求单一模型的 **“猜测”**，而追求投研过程的 **“确定性”**。

AlphaTeam 旨在解决传统 AI 投研中“流程黑盒、责任不清、不可审计”的痛点，将投研逻辑从“只看结果”转向 **“过程可解释、动作可约束、状态可追踪”**。

![AlphaTeam Architecture and Core Philosophy](docs/assets/alphateam-architecture.png)

---

## 🚀 核心特性 

| 特性 | 描述 |
| :--- | :--- |
| **🛡️ 可信 (Reliable)** | 独创“因果假设链”推理模式，强制要求“证据-结论”一一对应，从底层逻辑拒绝 AI 幻觉。 |
| **🎮 可控 (Controllable)** | 引入**反方审计** 与风险复核机制，确保每一项投资建议都经过严苛的逻辑挑战。 |
| **🔍 可回溯 (Traceable)** | 全链路流程与记忆中心，每一秒的决策依据、工具调用和原始数据均可审计还原。 |
| **🧬 可进化 (Evolving)** | 深度适配 **A-share first** 约束场景，支持多智能体在复杂实战反馈中实现自我进化与迭代。 |

---

## 🎬 可视化展示 

以下为 AlphaTeam 的实际运行界面示意（含可视化协作与分析工作台）：

![AlphaTeam Demo](docs/assets/demo.png)

---

## 🛠️ 系统架构 

AlphaTeam 并非简单的问答机器人，而是一套**面向投研的工业级底座**：

### 1. 多智能体协作层 (Multi-Agent Collaboration)
研究、情报、风控、审计职责分离，深度模拟现实金融机构的投研流水线，实现真正的专业化分工。

### 2. 工具治理中心 (Tool Governance)
对 API 调用与数据检索进行严格审计，确保数据源合规、调用链透明，且研究结果具备 100% 可重现性。

### 3. 可视化工作流 (Visual Workflow)
通过直观的决策链路展示，将量化分析的逻辑完全“白盒化”，让每一个策略细节都清晰可见。

---

## 🆚 AlphaTeam vs 传统金融框架

| 维度 | 传统金融/传统 AI 投研框架 | AlphaTeam |
| :--- | :--- | :--- |
| 执行范式 | 单体模型或固定规则链，流程弹性弱 | 多智能体协作 + 编排器驱动 |
| 可控性（流程与策略治理） | 控制面分散，难统一时限、审批、中断策略 | 支持工作流配置、超时治理、全局中止与角色边界约束 |
| 可回溯性 | 常见“结果可见、过程不可见” | Activity Stream + Trace + 报告归档全链路回溯 |
| 合规与审计证据 | 缺乏统一审计入口，证据链割裂 | 工具目录、源码审查、审计接口统一沉淀证据链 |
| 记忆透明度 | 上下文记忆常不可见 | 提供 `memory_center` 可视化记忆中心 |
| 可视化程度 | 多为文本与静态图表 | 3D 工作流场景 + 实时状态面板 |
| 架构演进能力 | 扩展成本高，角色复用差 | 角色、工具、技能模块化，可持续迭代 |

---

> **Mission:** AlphaTeam 不替代人类的投资判断，我们致力于为全球投研团队提供一套 **“可扩展、可验证、可推理、可回溯、具备金融直觉”** 的数字基础设施。

## Role Taxonomy 

| Runtime ID | Runtime Name | Academic Role |
|---|---|---|
| `director` | 决策总监 | Coordinator |
| `analyst` | 投资分析师 | Fundamental Researcher |
| `intel` | 市场情报员 | Market Intelligence Researcher |
| `quant` | 量化策略师 | Quantitative Researcher |
| `risk` | 风控官 | Risk Reviewer |
| `auditor` | 反思审计员 | Audit Reviewer |
| `restructuring` | 资产重组专家 | Special Situations Researcher |

## Architecture Spec

完整规格文档见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。以下为摘要：

- Orchestration Layer: `orchestrator` 与 `portfolio_scheduler` 驱动研究与组合周期。
- Agent Runtime Layer: `agent_registry` 管理角色模型路由、状态与停止控制。
- Tool and Data Layer: `tool_registry` 约束工具调用入口与参数模式。
- Memory and Governance Layer: 记忆系统、活动总线、Trace、工具审计共同构成治理闭环。
- Interface Layer: `/team` 统一交互与可视化入口，支持 3D 协作场景。

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

## 📊 指标与策略生态（简述）

- 当前开源版本包含 `25` 个可运行指标/策略模块，按四大分组组织：`资金面指标(7)`、`宏观与估值指标(7)`、`市场结构指标(7)`、`策略模型(4)`。
- `策略模型` 当前覆盖：`理想行业轮动策略`、`优质股票筛选`、`智能体协同策略`、`大师选股策略`。
- 全部指标采用统一注册与加载机制：`indicator_registry` 自动发现，前端通过 `/indicator/<indicator_id>` 统一访问与渲染。
- 设计目标是让每个指标既可独立使用，也可在 AlphaTeam 协作链路中被审计、被复核、被复现。

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

当前版本优先服务 A 股投研语境：

- 中国市场时钟语义与交易时段约束。
- 本地数据链路可控、可重建。
- 风控与复核环节默认前置。

项目可扩展至其他市场，但需要新增数据适配、时钟规则与交易语义层。

## Observability and Governance APIs

| 能力 | API |
|---|---|
| 活动流（SSE） | `/api/team/activity` |
| 运行链路 Trace | `/api/team/trace/runs`, `/api/team/trace/<run_id>` |
| 记忆中心 | `/api/team/memory_center` |
| 工具目录与源码审查 | `/api/team/tools_audit/catalog`, `/api/team/tools_audit/source`, `/api/team/tools_audit/review` |
| 生命周期控制 | `/api/team/module/start`, `/api/team/module/stop`, `/api/team/stop_all_work` |
| 超时治理 | `/api/team/session/<session_id>/overtime` |



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
