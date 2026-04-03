<p align="center">
  <img src="docs/assets/logo.png" alt="AlphaTeam Logo" width="160" />
</p>

<h1 align="center">AlphaTeam</h1>

<p align="center">
  <a href="./README.md">中文</a> | <a href="./README_EN.md"><strong>English</strong></a>
</p>

> **Building a trustworthy, controllable, and traceable open architecture for quantitative investment research in capital markets**
> *A Trustworthy, Controllable, and Traceable Framework for Financial Intelligence.*

## 📖 Background & Lineage

AlphaTeam is the industrial-grade evolution of our previous academic open-source project, **[AlphaFin](https://github.com/AlphaFin-proj/AlphaFin)**.

* **[AlphaFin](https://github.com/AlphaFin-proj/AlphaFin) (Predecessor):** Focused on the construction of high-quality financial datasets, LLM fine-tuning, retrieval augmentation, and preliminary explorations into single-agent chain-of-logic reasoning. It was designed to address the foundational challenges of financial data timeliness and computational complexity.

* **AlphaTeam:** Building upon the financial intuition and data foundation established by AlphaFin, this project comprehensively upgrades to an architecture of **Multi-Agent Collaboration** and **Systematic Engineering Governance**. It marks the definitive leap from "heuristic single-agent Q&A" to a "trustworthy, controllable, and evolvable quantitative research framework."

---

## 💡 Core Philosophy

In noisy capital markets, we do not pursue a single-model “oracle.”
We pursue **deterministic research processes**.

AlphaTeam addresses key pain points of traditional AI research systems: black-box workflows, unclear accountability, and weak auditability. We shift the focus from “result-only outputs” to **interpretable processes, constrained actions, and traceable states**.

![AlphaTeam Architecture and Core Philosophy](docs/assets/alphateam-architecture.png)

---

## 🚀 Key Features

| Feature | Description |
| :------- | :--- |
| **🛡️ Reliable** | A causal hypothesis-chain reasoning pattern enforces strict evidence-to-conclusion mapping to reduce hallucinations at the logic level. |
| **🎮 Controllable** | Built-in **adversarial audit** and risk re-check mechanisms challenge every investment conclusion under rigorous logic constraints. |
| **🔍 Traceable** | Full-chain **Activity Stream** plus memory center allows audit reconstruction of decision basis, tool calls, and source data. |
| **🧬 Evolving** | Deeply adapted to **A-share first** constraints; supports multi-agent evolution through real-world feedback loops. |

---

## 🎬 Demo Preview

The following screenshot shows AlphaTeam in action, including visual collaboration and research workspace:

![AlphaTeam Demo](docs/assets/demo.png)

---

## 🛠️ Architecture

AlphaTeam is not a simple chat assistant. It is an **industrial-grade substrate for investment research**:

### 1. Multi-Agent Collaboration Layer
Research, intelligence, risk, and audit responsibilities are separated to simulate real financial research pipelines with professional specialization.

### 2. Tool Governance Center
API calls and data retrieval are governed with strict auditing to ensure compliant sources, transparent call chains, and reproducible research outputs.

### 3. Visual Workflow
Decision chains are rendered transparently so quantitative logic is white-box and each strategy detail is visible.

---

## 🆚 AlphaTeam vs Traditional Financial Frameworks

| Dimension | Traditional Finance / Traditional AI Research Frameworks | AlphaTeam |
| :--- | :--- | :--- |
| Execution Paradigm | Monolithic model or fixed rule chain, limited flexibility | Multi-agent collaboration + orchestrator-driven workflow |
| Controllability (process & policy governance) | Fragmented control planes; hard to unify timeout, approval, and interruption policies | Workflow-level configuration, timeout governance, global stop, and role-boundary constraints |
| Traceability | Results may be visible but processes are often opaque | End-to-end traceability via Activity Stream + Trace + report archival |
| Compliance & Audit Evidence | No unified audit entry; fragmented evidence chain | Unified evidence chain via tool catalog, source review, and audit APIs |
| Memory Transparency | Context memory is often hidden | Visualized memory center via `memory_center` |
| Visualization Capability | Mostly text and static charts | 3D workflow scene + real-time status panel |
| Evolution Capacity | High extension cost, weak role reuse | Modular roles/tools/skills for continuous evolution |

---

> **Mission:** AlphaTeam does not replace human investment judgment. We provide a **scalable, verifiable, and financially grounded** digital infrastructure for global research teams.

## Role Taxonomy (Academic Naming)

| Runtime ID | Runtime Name | Academic Role |
|---|---|---|
| `director` | Decision Director | Coordinator |
| `analyst` | Investment Analyst | Fundamental Researcher |
| `intel` | Market Intelligence Officer | Market Intelligence Researcher |
| `quant` | Quant Strategist | Quantitative Researcher |
| `risk` | Risk Officer | Risk Reviewer |
| `auditor` | Reflective Auditor | Audit Reviewer |
| `restructuring` | Asset Restructuring Specialist | Special Situations Researcher |

## Architecture Spec

Full specification: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Summary:

- Orchestration Layer: `orchestrator` and `portfolio_scheduler` drive research and portfolio cycles.
- Agent Runtime Layer: `agent_registry` manages model routing, runtime states, and stop control.
- Tool and Data Layer: `tool_registry` constrains tool entry points and parameter schemas.
- Memory and Governance Layer: memory system, activity bus, traces, and tool audits form a governance loop.
- Interface Layer: `/team` provides unified interaction and visualization entry, including 3D collaboration scenes.

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


## 📊 Indicator and Strategy Coverage (Brief)

- The current open-source release includes `25` runnable indicator/strategy modules across four groups: `Capital Flow (7)`, `Macro and Valuation (7)`, `Market Structure (7)`, and `Strategy Models (4)`.
- Current `Strategy Models` include: `Industry Rotation`, `Quality Stock Screening`, `Agent Collaborative Strategy`, and `Master Stock-Picking Strategies`.
- Indicators follow a unified registry and loading path: auto-discovered by `indicator_registry`, and served/rendered through `/indicator/<indicator_id>`.
- The design goal is that each indicator is independently usable while remaining auditable, reviewable, and reproducible in AlphaTeam workflows.

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Configure environment variables (see `.env.example`), at least:

- `SECRET_KEY`
- `TUSHARE_TOKEN`
- `QWEN_API_KEY`
- `MOONSHOT_API_KEY` 

Start service:

```bash
python3 AlphaFin/app.py
```

Access:

- Team Workspace: `http://127.0.0.1:5002/team`

## Data Bootstrap

Default database directory is `data/db` (override with `ALPHAFIN_DB_ROOT`).

```bash
# Quick mode (recommended, recent 1 year)
python3 -m AlphaFin.scripts.build_db --mode quick

# Full mode
python3 -m AlphaFin.scripts.build_db --mode full

# Full mode + financial indicators
python3 -m AlphaFin.scripts.build_db --mode full --include-fina
```

## A-share First Constraints

Current release prioritizes A-share research context:

- China market clock semantics and trading-session constraints.
- Controllable, rebuildable local data pipelines.
- Risk review and audit are default pre-steps.

The architecture can be extended to other markets by adding market-specific data adapters, clock rules, and trading semantics.

## Observability and Governance APIs

| Capability | API |
|---|---|
| Activity Stream (SSE) | `/api/team/activity` |
| Runtime Trace | `/api/team/trace/runs`, `/api/team/trace/<run_id>` |
| Memory Center | `/api/team/memory_center` |
| Tool Catalog and Source Review | `/api/team/tools_audit/catalog`, `/api/team/tools_audit/source`, `/api/team/tools_audit/review` |
| Lifecycle Controls | `/api/team/module/start`, `/api/team/module/stop`, `/api/team/stop_all_work` |
| Timeout Governance | `/api/team/session/<session_id>/overtime` |

## Non-goals

- This project does not guarantee investment returns or win rates.
- This project is not an automated live-trading system.
- Outputs from this project do not constitute investment advice.

## Citation

If you use AlphaTeam in research or engineering, please cite as software:

```bibtex
@software{alphateam2026,
  title   = {AlphaTeam: Trustworthy, Controllable, and Traceable Quantitative Trading PlatForm},
  author  = {AlphaTeam Contributors},
  year    = {2026},
  url     = {https://github.com/jackyideal/AlphaTeam}
}
```
