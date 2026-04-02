# AlphaTeam Architecture Specification

## 1. Scope and Positioning

AlphaTeam is an open architecture for stock research and decision support. It is designed for teams that need:

- process transparency over black-box answers,
- controllability over autonomous execution,
- traceability over point-in-time outputs,
- extensibility over monolithic pipelines.

This specification documents the system boundary implemented in this repository (`A-share first`), with concrete module mappings and control surfaces.

## 2. Problem Setting

Financial research workflows operate in an environment characterized by:

- high observation noise,
- non-stationary market regimes,
- heterogeneous data and tool interfaces,
- strict requirements on governance and accountability.

A single-prompt architecture can provide fast responses, but it often under-specifies responsibility boundaries and post-hoc auditability. AlphaTeam addresses this with role decomposition and orchestration-first execution.

## 3. Architectural Objectives

- Credible: every major step should leave inspectable artifacts.
- Controllable: users can start, stop, and interrupt workflows safely.
- Traceable: users can inspect activity streams and run traces.
- Evolvable: roles, tools, skills, and workflows should be modular.
- Market-aware: preserve A-share constraints in time and data semantics.

## 4. System Layers

### 4.1 Interface Layer

- Team workspace page: `/team`
- Team static assets: `/team_static/*`
- 3D workspace visualization: `AlphaFin/ai_team/static/js/three_scene.js`

Responsibilities:

- collect user requests and context,
- render workflow states and agent states,
- expose control actions and report views.

### 4.2 Orchestration Layer

- `AlphaFin/ai_team/core/orchestrator.py`
- `AlphaFin/ai_team/core/portfolio_scheduler.py`

Responsibilities:

- route user tasks into workflow modes,
- coordinate research and portfolio cycles,
- manage session timing and timeout behavior.

### 4.3 Agent Runtime Layer

- `AlphaFin/ai_team/core/agent.py`
- `AlphaFin/ai_team/core/agent_registry.py`
- `AlphaFin/ai_team/agents/*`

Responsibilities:

- isolate agent responsibilities by role,
- manage per-agent model settings and runtime status,
- support interruption and safe termination.

### 4.4 Tool and Data Layer

- `AlphaFin/ai_team/core/tool_registry.py`
- `AlphaFin/scripts/build_db.py`
- `AlphaFin/scripts/update_funcs.py`
- `data/db/*` (runtime build target)

Responsibilities:

- standardize tool registration and execution entrypoints,
- provide reproducible local database bootstrap,
- support deterministic data-update workflows.

### 4.5 Memory and Governance Layer

- `AlphaFin/ai_team/core/memory.py`
- `AlphaFin/ai_team/core/message_bus.py`
- `AlphaFin/ai_team/core/session_control.py`
- `AlphaFin/ai_team/services/report_service.py`

Responsibilities:

- maintain memory and reflection artifacts,
- broadcast workflow activity in real time,
- enforce timeout and overtime decisions,
- archive and retrieve generated reports.

## 5. Runtime Role Taxonomy

| Runtime ID | Runtime Name | Academic Role |
|---|---|---|
| `director` | 决策总监 | Coordinator |
| `analyst` | 投资分析师 | Fundamental Researcher |
| `intel` | 市场情报员 | Market Intelligence Researcher |
| `quant` | 量化策略师 | Quantitative Researcher |
| `risk` | 风控官 | Risk Reviewer |
| `auditor` | 反思审计员 | Audit Reviewer |
| `restructuring` | 资产重组专家 | Special Situations Researcher |

## 6. Workflow Lifecycle (User Ask)

Canonical request path:

1. User submits request in `/team`.
2. Orchestrator chooses workflow route.
3. Coordinator allocates subtasks to role agents.
4. Agents execute tool-backed analysis and update activity stream.
5. Risk and audit roles review intermediate conclusions.
6. Coordinator synthesizes final response.
7. Runtime state, trace records, and report artifacts remain queryable.

## 7. Control and Governance Surfaces

| Capability | Endpoint |
|---|---|
| Module start | `/api/team/module/start` |
| Module stop | `/api/team/module/stop` |
| Global stop | `/api/team/stop_all_work` |
| Real-time activity stream | `/api/team/activity` |
| Trace listing/detail | `/api/team/trace/runs`, `/api/team/trace/<run_id>` |
| Memory center inspection | `/api/team/memory_center` |
| Tool catalog and source | `/api/team/tools_audit/catalog`, `/api/team/tools_audit/source` |
| Tool review | `/api/team/tools_audit/review` |
| Overtime decision | `/api/team/session/<session_id>/overtime` |

These APIs are first-class architecture features, not auxiliary debug helpers.

## 8. Traditional vs AlphaTeam

| Aspect | Traditional Single-Agent Stack | AlphaTeam |
|---|---|---|
| Responsibility boundary | Implicit | Explicit role decomposition |
| Process observability | Limited | Activity stream + trace endpoints |
| Runtime control | Weak interruption semantics | Start/stop/global-stop surfaces |
| Memory transparency | Often opaque | Memory center API |
| Tool governance | Fragmented wrappers | Registry + source review + audit flow |
| Visual workflow | Mostly text | 3D workspace + structured status panels |

## 9. Causal Hypothesis Orientation

AlphaTeam does not claim formal causal identification in the econometric sense. Instead, it enforces a practical causal-hypothesis discipline:

- hypotheses should articulate drivers and expected market impact,
- evidence should be linked to intermediate reasoning states,
- risk/audit roles are expected to challenge weak causal narratives.

This design aims to reduce purely pattern-matching style responses in non-stationary market contexts.

## 10. A-share First Constraints

This repository prioritizes A-share semantics:

- market clock and trading-session awareness,
- local data bootstrap/update reliability,
- risk-review-first workflow posture.

Cross-market support is feasible but out of current repository scope.

## 11. Non-goals

- no guarantee of investment returns,
- no direct brokerage execution,
- no investment advice commitment.

## 12. Reproducibility Notes

- environment variables are required for keys and runtime settings (`.env.example`),
- runtime artifacts (`*.db`, traces, reports) are ignored from version control,
- database bootstrap scripts are included for local reconstruction.
