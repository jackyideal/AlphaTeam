"""
工具注册表 - 将现有 AlphaFin 能力包装为 Qwen function calling 格式
"""
import json
import math
import os
import traceback
import uuid
import time

# 指标工具开关：开启后可调用指标系统（受分组与关键词限制）
RUN_INDICATOR_ENABLED = True

# 智能团队可用指标策略：
# - 全开，但禁用“策略模型”分组
# - 禁用“市场顶底指标”相关主题（顶底/逃顶/铜油比）
DISABLED_INDICATOR_GROUPS = {'策略模型'}
DISABLED_INDICATOR_NAME_KEYWORDS = {'顶底', '逃顶', '牛市逃顶', '铜油比'}
DISABLED_INDICATOR_ALIASES = {'market_hub', 'bull-top', 'copper-oil-ratio'}

# ──────────────── 工具 JSON Schema 定义 ────────────────

TOOLS_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "get_kline",
            "description": "获取个股K线数据（日/周/月线），包含OHLCV和每日基础指标（PE/PB/换手率/市值等）",
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {"type": "string", "description": "股票代码，如 600519.SH, 000001.SZ"},
                    "freq": {"type": "string", "enum": ["D", "W", "M"], "description": "频率：D日线/W周线/M月线，默认D"},
                    "start_date": {"type": "string", "description": "起始日期 YYYYMMDD，默认 20200101"},
                },
                "required": ["ts_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_kline_technical",
            "description": "获取与“个股通用分析”同源的K线数据并做多周期技术面解读（默认日/周/月），输出MACD/KDJ与趋势判断",
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {"type": "string", "description": "股票或指数代码，如 600425.SH, 000001.SH"},
                    "asset_type": {"type": "string", "enum": ["auto", "stock", "index"], "description": "资产类型，默认auto自动识别"},
                    "start_date": {"type": "string", "description": "起始日期 YYYYMMDD，默认 20200101"},
                    "periods": {
                        "type": "string",
                        "description": "周期列表，逗号分隔，默认 D,W,M（可选值：D,W,M）"
                    },
                },
                "required": ["ts_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_financials",
            "description": "获取个股财务指标（最近20期），包含EPS、BPS、ROE、净利率、营收增速等",
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {"type": "string", "description": "股票代码"},
                },
                "required": ["ts_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_chip_distribution",
            "description": "获取个股筹码分布数据（成本分位和胜率）",
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {"type": "string", "description": "股票代码"},
                    "start_date": {"type": "string", "description": "起始日期 YYYYMMDD，默认 20180101"},
                },
                "required": ["ts_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock_news",
            "description": "获取个股相关新闻（行业政策、公司公告、公司新闻、全球事件）",
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {"type": "string", "description": "股票代码"},
                },
                "required": ["ts_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_sector_report",
            "description": "获取每日板块热点分析报告（TOP10板块热度、龙头股、投资建议）",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
        {
            "type": "function",
            "function": {
            "name": "web_search",
            "description": "通过 Kimi `$web_search` 联网搜索最新信息（股票新闻、宏观政策、行业动态等），返回 Kimi 原始回答与结构化来源",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "搜索查询内容"},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_current_time",
            "description": "获取当前北京时间、交易日期和市场时段状态（盘前/连续竞价/午休/收盘后）",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_intraday_index",
            "description": "获取指数实时分钟行情快照（默认上证/沪深300/深证成指/创业板）。"
                           "数据源：TuShare rt_idx_min。",
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_codes": {"type": "string", "description": "指数代码，逗号分隔，如 000001.SH,000300.SH"},
                    "freq": {"type": "string", "enum": ["1MIN", "5MIN", "15MIN", "30MIN", "60MIN"],
                             "description": "分钟频率，默认1MIN"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_intraday_sector_heat",
            "description": "获取申万行业实时热度（涨跌幅TOP）。数据源：TuShare rt_sw_k。",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "返回行业数量，默认10，最大30"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_intraday_hotrank",
            "description": "获取同花顺热榜（热股/ETF/可转债）。数据源：TuShare ths_hot。",
            "parameters": {
                "type": "object",
                "properties": {
                    "market": {"type": "string", "enum": ["热股", "ETF", "可转债"], "description": "榜单类型"},
                    "limit": {"type": "integer", "description": "返回条数，默认20，最大50"},
                    "latest_only": {"type": "boolean", "description": "是否仅最新快照，默认true"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_intraday_news",
            "description": "获取最近N小时财经快讯。数据源：TuShare news。",
            "parameters": {
                "type": "object",
                "properties": {
                    "hours": {"type": "integer", "description": "回看小时数，默认2小时，范围1-24"},
                    "src": {"type": "string", "description": "新闻源，默认cls"},
                    "limit": {"type": "integer", "description": "返回条数，默认20，最大100"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_intraday_stock_quote",
            "description": "获取个股最新可用报价（优先分时/实时；失败时回退最新收盘），"
                           "返回价格、时间、涨跌幅与数据来源。",
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {"type": "string", "description": "股票代码，如 600425.SH 或 600425"},
                    "freq": {"type": "string", "enum": ["1MIN", "5MIN", "15MIN", "30MIN", "60MIN"],
                             "description": "分钟频率，默认1MIN"},
                },
                "required": ["ts_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_indicator",
            "description": "运行 AlphaFin 指标并产出“图片+文字”摘要，返回中会明确标注 indicator_id/名称/分组。"
                           "限制：禁用“策略模型”分组与“市场顶底指标”相关主题。",
            "parameters": {
                "type": "object",
                "properties": {
                    "indicator_id": {"type": "string", "description": "指标模块ID"},
                    "ts_code": {"type": "string", "description": "股票代码（部分指标需要）"},
                    "image_readable": {"type": "boolean", "description": "是否启用图像阅读摘要，默认 true"},
                    "max_struct_figures": {"type": "integer", "description": "结构摘要最大图数，默认4"},
                    "max_vision_figures": {"type": "integer", "description": "视觉摘要最大图数，默认2"},
                },
                "required": ["indicator_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_database",
            "description": "执行只读SQL查询（SELECT），可查询的数据库: daily_kline(日K线), dailybasic(日线基础指标), "
                           "daily_adj(复权因子), fina_indicator(财务指标), financial_data(利润表/资产负债表/现金流)",
            "parameters": {
                "type": "object",
                "properties": {
                    "db_name": {"type": "string", "enum": ["daily_kline", "dailybasic", "daily_adj",
                                                            "fina_indicator", "financial_data"],
                                "description": "数据库名称"},
                    "sql": {"type": "string", "description": "SELECT SQL 查询语句（仅允许 SELECT）"},
                    "limit": {"type": "integer", "description": "返回行数限制，默认100，最大500"},
                },
                "required": ["db_name", "sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "send_message_to_agent",
            "description": "向团队中其他智能体发送消息或提问",
            "parameters": {
                "type": "object",
                "properties": {
                    "to_agent": {"type": "string", "enum": ["director", "analyst", "risk", "intel", "quant", "auditor", "restructuring"],
                                 "description": "目标智能体ID"},
                    "message": {"type": "string", "description": "消息内容"},
                },
                "required": ["to_agent", "message"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "save_knowledge",
            "description": "将重要结论保存到长期记忆，支持 HOT/WARM/COLD 分层、项目域标签、模式验证。",
            "parameters": {
                "type": "object",
                "properties": {
                    "category": {"type": "string", "description": "分类: stock_analysis/market_view/strategy/risk_flag/macro"},
                    "subject": {"type": "string", "description": "主题，如股票代码或分析对象"},
                    "content": {"type": "string", "description": "知识内容"},
                    "confidence": {"type": "number", "description": "置信度 0.0-1.0，默认0.8"},
                    "tier": {"type": "string", "enum": ["hot", "warm", "cold"],
                             "description": "记忆层级：hot核心规则/warm常用经验/cold历史归档"},
                    "project": {"type": "string", "description": "项目名（可选，如 ai_team）"},
                    "domain": {"type": "string", "description": "领域名（可选，如 a_share）"},
                    "tags": {"type": "array", "items": {"type": "string"}, "description": "标签列表（可选）"},
                    "pattern_key": {"type": "string", "description": "可复用模式ID（可选，用于统计验证次数）"},
                    "outcome": {"type": "string", "enum": ["success", "failure", "observation"],
                                "description": "本次模式结果（可选）"},
                    "rule_text": {"type": "string", "description": "模式规则描述（可选）"},
                },
                "required": ["category", "subject", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_skill",
            "description": "创建一个新的自定义分析技能（Python函数）。代码在安全沙箱中运行，"
                           "只允许使用 pandas/numpy/math/datetime/json/statistics。"
                           "代码中可通过 input_data 字典获取输入数据，将结果赋值给 result 变量。"
                           "分类为 data_analysis/visualization/statistics 的技能自动部署；"
                           "trading_strategy/risk_rule/portfolio 类需人工审核。",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "技能名称（英文，如 pe_screener, momentum_rank）"},
                    "code": {"type": "string", "description": "Python 代码字符串，结果赋值给 result 变量"},
                    "description": {"type": "string", "description": "技能功能描述"},
                    "category": {
                        "type": "string",
                        "enum": ["data_analysis", "visualization", "statistics",
                                 "trading_strategy", "risk_rule", "portfolio"],
                        "description": "技能分类",
                    },
                },
                "required": ["name", "code", "description", "category"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "execute_skill",
            "description": "执行一个已创建并批准的自定义技能。可通过 list_skills 查看可用技能。",
            "parameters": {
                "type": "object",
                "properties": {
                    "skill_id": {"type": "string", "description": "技能ID（如 skill_pe_screener_1709876543）"},
                    "input_data": {
                        "type": "object",
                        "description": "传入技能的输入数据（可选），技能代码中通过 input_data 访问",
                    },
                },
                "required": ["skill_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_skills",
            "description": "列出所有已创建的自定义技能，查看可用的技能ID和描述",
            "parameters": {
                "type": "object",
                "properties": {
                    "approved_only": {"type": "boolean", "description": "是否只列出已批准的技能，默认true"},
                },
            },
        },
    },
    # ──── 投资组合工具 ────
    {
        "type": "function",
        "function": {
            "name": "submit_trade_signal",
            "description": "提交买卖交易信号。信号将进入审批队列：风控官审核→决策总监批准→系统用次日开盘价执行。"
                           "A股T+1制度：买入次日才可卖出。单只股票仓位不超过总资产30%，最多持有8只。"
                           "同一标的同一交易日禁止重复/反向频繁提交信号。"
                           "并受每日信号限额约束（单智能体与团队总量）。",
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {"type": "string", "description": "股票代码（如 000001.SZ, 600519.SH）"},
                    "direction": {"type": "string", "enum": ["buy", "sell"], "description": "买入buy或卖出sell"},
                    "target_ratio": {"type": "number", "description": "目标仓位占总资产比例（0-0.3），与quantity二选一"},
                    "quantity": {"type": "integer", "description": "交易数量（股，100的整数倍），与target_ratio二选一"},
                    "reason": {"type": "string", "description": "交易理由（必填，需有数据支撑）"},
                },
                "required": ["ts_code", "direction", "reason"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "review_trade_signal",
            "description": "审核交易信号。风控官/审计员审核风险面，决策总监最终批准或否决。",
            "parameters": {
                "type": "object",
                "properties": {
                    "signal_id": {"type": "integer", "description": "信号ID"},
                    "approved": {"type": "boolean", "description": "是否通过"},
                    "review_text": {"type": "string", "description": "审核意见"},
                },
                "required": ["signal_id", "approved", "review_text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_portfolio_status",
            "description": "查看投资组合当前状态：现金、持仓明细、总资产、净值、累计收益率、最大回撤。",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_trade_signals",
            "description": "查询交易信号及审批状态（待风控/待总监/已批准/已执行/已拒绝）。"
                           "用于回答“是否已批准、是否仍在审批队列”等问题。",
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {"type": "string", "description": "股票代码，可填600425或600425.SH（可选）"},
                    "status": {
                        "type": "string",
                        "enum": ["pending_risk", "pending_director", "approved", "rejected", "executed", "expired"],
                        "description": "按状态过滤（可选）",
                    },
                    "limit": {"type": "integer", "description": "返回条数，默认20，最大100"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "flag_risk_warning",
            "description": "标记风险预警。如后续5个交易日内标的下跌超3%，预警成功可获500元奖励。",
            "parameters": {
                "type": "object",
                "properties": {
                    "ts_code": {"type": "string", "description": "相关股票代码"},
                    "risk_type": {
                        "type": "string",
                        "enum": ["market_crash", "stock_plunge", "policy_risk", "overvaluation", "concentration"],
                        "description": "风险类型",
                    },
                    "severity": {"type": "string", "enum": ["high", "medium", "low"], "description": "严重程度"},
                    "description": {"type": "string", "description": "风险描述和依据"},
                },
                "required": ["ts_code", "risk_type", "description"],
            },
        },
    },
]


# ──────────────── 工具执行函数 ────────────────


def _infer_tool_status_from_text(text):
    t = str(text or '').strip()
    if not t:
        return True, 'ok'
    low = t.lower()
    if t.startswith('未知工具'):
        return False, 'not_found'
    if '已拦截' in t or '受限' in t or '已停用' in t:
        return False, 'blocked'
    if (
        '失败' in t or '异常' in t or '错误' in t or
        'traceback' in low or 'not found' in low
    ):
        return False, 'error'
    return True, 'ok'


def _build_tool_result_v2(tool_name, arguments, raw_result,
                          ok=None, status=None, error='',
                          duration_ms=0):
    text = str(raw_result or '')
    if ok is None or not status:
        infer_ok, infer_status = _infer_tool_status_from_text(text)
        if ok is None:
            ok = infer_ok
        if not status:
            status = infer_status
    return {
        'schema_version': 'tool_result.v2',
        'tool': str(tool_name or ''),
        'args': arguments if isinstance(arguments, dict) else {},
        'ok': bool(ok),
        'status': str(status or ('ok' if ok else 'error')),
        'text': text,
        'error': str(error or ''),
        'duration_ms': int(max(0, duration_ms)),
        'timestamp': int(time.time()),
    }


def _tool_result_to_legacy_text(result_v2):
    if isinstance(result_v2, dict):
        return str(result_v2.get('text') or '')
    return str(result_v2 or '')


def execute_tool(tool_name, arguments, agent_id=None, message_bus=None, return_protocol='v1'):
    """
    执行工具调用，兼容返回两种协议：
    - v1: 纯文本（历史逻辑）
    - v2: 结构化结果（供智能体端到端 trace / 数据协议统一）
    """
    started_at = time.time()
    result_v2 = None
    try:
        extra_fields = {}
        if tool_name == 'get_kline':
            raw = _exec_get_kline(arguments)
        elif tool_name == 'get_kline_technical':
            raw = _exec_get_kline_technical(arguments)
        elif tool_name == 'get_financials':
            raw = _exec_get_financials(arguments)
        elif tool_name == 'get_chip_distribution':
            raw = _exec_get_chip_distribution(arguments)
        elif tool_name == 'get_stock_news':
            raw = _exec_get_stock_news(arguments)
        elif tool_name == 'get_sector_report':
            raw = _exec_get_sector_report(arguments)
        elif tool_name == 'web_search':
            raw = _exec_web_search(arguments, agent_id=agent_id)
        elif tool_name == 'get_current_time':
            raw = _exec_get_current_time(arguments)
        elif tool_name == 'get_intraday_index':
            raw = _exec_get_intraday_index(arguments)
        elif tool_name == 'get_intraday_sector_heat':
            raw = _exec_get_intraday_sector_heat(arguments)
        elif tool_name == 'get_intraday_hotrank':
            raw = _exec_get_intraday_hotrank(arguments)
        elif tool_name == 'get_intraday_news':
            raw = _exec_get_intraday_news(arguments)
        elif tool_name == 'get_intraday_stock_quote':
            raw = _exec_get_intraday_stock_quote(arguments)
        elif tool_name == 'run_indicator':
            if not RUN_INDICATOR_ENABLED:
                raw = '工具已停用: run_indicator（为提升响应速度与稳定性，当前会话暂不执行指标图生成）'
            else:
                raw = _exec_run_indicator(arguments)
        elif tool_name == 'query_database':
            raw = _exec_query_database(arguments)
        elif tool_name == 'send_message_to_agent':
            raw = _exec_send_message(arguments, agent_id, message_bus)
        elif tool_name == 'save_knowledge':
            raw = _exec_save_knowledge(arguments, agent_id)
        elif tool_name == 'create_skill':
            raw = _exec_create_skill(arguments, agent_id)
        elif tool_name == 'execute_skill':
            raw = _exec_execute_skill(arguments)
        elif tool_name == 'list_skills':
            raw = _exec_list_skills(arguments)
        elif tool_name == 'submit_trade_signal':
            raw = _exec_submit_trade_signal(arguments, agent_id)
        elif tool_name == 'review_trade_signal':
            raw = _exec_review_trade_signal(arguments, agent_id)
        elif tool_name == 'get_portfolio_status':
            raw = _exec_get_portfolio_status(arguments)
        elif tool_name == 'get_trade_signals':
            raw = _exec_get_trade_signals(arguments)
        elif tool_name == 'flag_risk_warning':
            raw = _exec_flag_risk_warning(arguments, agent_id)
        else:
            raw = '未知工具: ' + str(tool_name or '')

        raw_text = raw
        if isinstance(raw, dict) and raw.get('_tool_payload') is True:
            raw_text = str(raw.get('text') or '')
            for key in ('links', 'query', 'search_model', 'items'):
                if key in raw:
                    extra_fields[key] = raw.get(key)

        result_v2 = _build_tool_result_v2(
            tool_name=tool_name,
            arguments=arguments,
            raw_result=raw_text,
            duration_ms=int((time.time() - started_at) * 1000),
        )
        if extra_fields:
            result_v2.update(extra_fields)
    except Exception as e:
        result_v2 = _build_tool_result_v2(
            tool_name=tool_name,
            arguments=arguments,
            raw_result='工具执行失败 [%s]: %s\n%s' % (
                tool_name, str(e), traceback.format_exc()[:500]
            ),
            ok=False,
            status='error',
            error=str(e),
            duration_ms=int((time.time() - started_at) * 1000),
        )

    if str(return_protocol or 'v1').lower() == 'v2':
        return result_v2
    return _tool_result_to_legacy_text(result_v2)


def _exec_get_kline(args):
    from AlphaFin.services.stock_service import get_daily_data, get_weekly_monthly_data
    ts_code = args.get('ts_code', '600519.SH')
    freq = args.get('freq', 'D')
    start_date = args.get('start_date', '20200101')
    if freq == 'D':
        result = get_daily_data(ts_code, start_date)
    else:
        result = get_weekly_monthly_data(ts_code, start_date, freq)
    if 'error' in result:
        return '获取K线失败: ' + result['error']
    # 兼容 stock_service 的返回协议: {dates, ohlc, volumes, basic}
    dates = result.get('dates') or []
    ohlc = result.get('ohlc') or []
    volumes = result.get('volumes') or []
    if not dates or not ohlc:
        return '无K线数据'

    size = min(len(dates), len(ohlc))
    rows = []
    for i in range(size):
        k = ohlc[i]
        if not isinstance(k, (list, tuple)) or len(k) < 4:
            continue
        try:
            row = {
                'trade_date': dates[i],
                'open': float(k[0]),
                'high': float(k[1]),
                'low': float(k[2]),
                'close': float(k[3]),
                'vol': float(volumes[i]) if i < len(volumes) else 0.0,
            }
        except (TypeError, ValueError):
            continue
        rows.append(row)

    if not rows:
        return '无K线数据'

    recent = rows[-5:] if len(rows) > 5 else rows
    summary = '最近%d条K线数据（共%d条）:\n' % (len(recent), len(rows))
    for row in recent:
        summary += '  %s: O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f' % (
            row['trade_date'], row['open'], row['high'], row['low'], row['close'], row['vol']
        )
        summary += '\n'

    basic = result.get('basic') or {}
    if basic.get('pe') is not None:
        summary += '最新估值: PE=%.2f' % float(basic['pe'])
    elif basic.get('pe_ttm') is not None:
        summary += '最新估值: PE(TTM)=%.2f' % float(basic['pe_ttm'])

    # 添加基础统计
    if len(rows) > 20:
        closes = [r['close'] for r in rows]
        if closes:
            summary += ('\n' if summary and not summary.endswith('\n') else '') + '统计: 最新=%.2f, 20日高=%.2f, 20日低=%.2f' % (
                closes[-1], max(closes[-20:]), min(closes[-20:])
            )
    return summary


def _normalize_asset_type_for_kline(ts_code, asset_type='auto'):
    t = str(asset_type or 'auto').strip().lower()
    if t in ('stock', 'index'):
        return t
    code = str(ts_code or '').strip().upper()
    if '.' not in code:
        if len(code) == 6:
            code = code + ('.SH' if code.startswith(('6', '9')) else '.SZ')
    head = code.split('.', 1)[0]
    if (code.endswith('.SH') and head.startswith('000')) or (code.endswith('.SZ') and head.startswith('399')):
        return 'index'
    return 'stock'


def _parse_kline_periods(periods_text):
    raw = str(periods_text or 'D,W,M').upper().replace(' ', '')
    parts = [x for x in raw.split(',') if x]
    out = []
    for p in parts:
        if p in ('D', 'W', 'M') and p not in out:
            out.append(p)
    return out or ['D', 'W', 'M']


def _calc_sma(values, window):
    arr = [float(x) for x in (values or []) if x is not None]
    if not arr:
        return None
    n = int(max(1, window))
    if len(arr) < n:
        return sum(arr) / float(len(arr))
    return sum(arr[-n:]) / float(n)


def _safe_fmt(v, nd=2):
    if v is None:
        return 'N/A'
    try:
        return ('%.' + str(int(nd)) + 'f') % float(v)
    except Exception:
        return 'N/A'


def _relation_name(a, b):
    try:
        return '金叉上方' if float(a) > float(b) else ('死叉下方' if float(a) < float(b) else '临界粘合')
    except Exception:
        return '未知'


def _trend_name(close_v, ma20_v):
    try:
        c = float(close_v)
        m = float(ma20_v)
    except Exception:
        return '未知'
    if c > m:
        return '偏强'
    if c < m:
        return '偏弱'
    return '中性'


def _exec_get_kline_technical(args):
    from AlphaFin.services.stock_service import _fetch_daily_week_month, _calc_macd, _calc_kdj

    ts_code = str(args.get('ts_code', '600425.SH')).strip().upper()
    start_date = str(args.get('start_date', '20200101')).replace('-', '')
    periods = _parse_kline_periods(args.get('periods', 'D,W,M'))
    asset_type = _normalize_asset_type_for_kline(ts_code, args.get('asset_type', 'auto'))
    period_label = {'D': '日线', 'W': '周线', 'M': '月线'}

    lines = [
        'K线技术面解读（与“个股通用分析”同源OHLC数据）',
        '标的: %s | 类型: %s | 周期: %s' % (ts_code, asset_type, ','.join(periods)),
    ]

    any_ok = False
    for p in periods:
        df = _fetch_daily_week_month(asset_type, ts_code, freq=p, start_date=start_date)
        if df is None or df.empty:
            lines.append('[%s] 无有效K线数据' % period_label.get(p, p))
            continue

        any_ok = True
        df = df.dropna(subset=['close']).reset_index(drop=True)
        if df.empty:
            lines.append('[%s] 无有效收盘价' % period_label.get(p, p))
            continue

        close = [float(x) for x in df['close'].tolist()]
        high = [float(x) for x in df['high'].tolist()]
        low = [float(x) for x in df['low'].tolist()]
        latest_close = close[-1]
        latest_time = str(df['trade_time'].iloc[-1])
        ma5 = _calc_sma(close, 5)
        ma20 = _calc_sma(close, 20)
        trend = _trend_name(latest_close, ma20)

        macd = _calc_macd(close)
        kdj = _calc_kdj(high, low, close)
        macd_dif = float(macd['dif'].iloc[-1]) if len(macd) else None
        macd_dea = float(macd['dea'].iloc[-1]) if len(macd) else None
        macd_hist = float(macd['hist'].iloc[-1]) if len(macd) else None
        k_val = float(kdj['k'].iloc[-1]) if len(kdj) else None
        d_val = float(kdj['d'].iloc[-1]) if len(kdj) else None
        j_val = float(kdj['j'].iloc[-1]) if len(kdj) else None

        change_10 = None
        if len(close) >= 11 and close[-11] != 0:
            change_10 = (latest_close - close[-11]) / close[-11] * 100.0

        lines.append(
            '[%s] 截至%s | C=%s | MA5=%s MA20=%s | 趋势=%s | 近10周期涨跌=%s%%' % (
                period_label.get(p, p),
                latest_time,
                _safe_fmt(latest_close, 3),
                _safe_fmt(ma5, 3),
                _safe_fmt(ma20, 3),
                trend,
                _safe_fmt(change_10, 2),
            )
        )
        lines.append(
            '  MACD: DIF=%s DEA=%s HIST=%s -> %s' % (
                _safe_fmt(macd_dif, 4),
                _safe_fmt(macd_dea, 4),
                _safe_fmt(macd_hist, 4),
                _relation_name(macd_dif, macd_dea),
            )
        )
        lines.append(
            '  KDJ: K=%s D=%s J=%s -> %s' % (
                _safe_fmt(k_val, 2),
                _safe_fmt(d_val, 2),
                _safe_fmt(j_val, 2),
                _relation_name(k_val, d_val),
            )
        )

    if not any_ok:
        return '获取K线技术面失败: %s 未获取到有效%s数据' % (ts_code, asset_type)

    lines.append(
        '图表一致性说明: 以上结论基于与“个股通用分析”同源的K线数据口径（/api/stock/kline 日/周/月）。'
    )
    lines.append('使用建议: 快速模式优先看日线方向；深度/团队模式同时校验日-周-月一致性，避免短线与长线冲突。')
    return '\n'.join(lines)


def _exec_get_financials(args):
    from AlphaFin.services.stock_service import get_fina_indicator
    ts_code = args.get('ts_code', '600519.SH')
    result = get_fina_indicator(ts_code)
    if 'error' in result:
        return '获取财务数据失败: ' + result['error']
    data = result.get('data', [])
    if not data:
        return '无财务数据'
    recent = data[:4]  # 最近4期
    summary = '%s 最近%d期财务指标:\n' % (ts_code, len(recent))
    for row in recent:
        summary += '  %s: EPS=%.3f ROE=%.2f%% 净利率=%.2f%% 营收增速=%.2f%%\n' % (
            row.get('end_date', ''), row.get('eps', 0), row.get('roe', 0),
            row.get('netprofit_margin', 0), row.get('or_yoy', 0)
        )
    return summary


def _exec_get_chip_distribution(args):
    from AlphaFin.services.stock_service import get_cyq_perf
    ts_code = args.get('ts_code', '600519.SH')
    start_date = args.get('start_date', '20180101')
    result = get_cyq_perf(ts_code, start_date)
    if 'error' in result:
        return '获取筹码数据失败: ' + result['error']
    # 兼容 stock_service 的返回协议: {dates, cost_*, weight_avg, winner_rate, current_price}
    dates = result.get('dates') or []
    if not dates:
        return '无筹码数据'

    idx = len(dates) - 1

    def _pick(key):
        arr = result.get(key) or []
        if idx < len(arr):
            return arr[idx]
        return None

    def _fnum(v, ndigits=2):
        if v is None:
            return 'N/A'
        try:
            return ('%.' + str(ndigits) + 'f') % float(v)
        except (TypeError, ValueError):
            return 'N/A'

    winner_rate = _pick('winner_rate')
    try:
        winner_rate = float(winner_rate) if winner_rate is not None else None
        # 兼容 0~1 / 0~100 两种口径
        if winner_rate is not None and 0 <= winner_rate <= 1:
            winner_rate *= 100
    except (TypeError, ValueError):
        winner_rate = None

    summary = (
        '%s 最新筹码分布(%s): 5%%成本=%s, 50%%成本=%s, 95%%成本=%s, '
        '加权成本=%s, 胜率=%s%%'
    ) % (
        ts_code,
        dates[idx],
        _fnum(_pick('cost_5pct')),
        _fnum(_pick('cost_50pct')),
        _fnum(_pick('cost_95pct')),
        _fnum(_pick('weight_avg')),
        _fnum(winner_rate, 1),
    )

    current_price = result.get('current_price')
    if current_price is not None:
        summary += ', 最新价=%s' % _fnum(current_price)

    return summary


def _exec_get_stock_news(args):
    from AlphaFin.services.news_service import fetch_stock_news
    ts_code = args.get('ts_code', '600519.SH')
    result = fetch_stock_news(ts_code)
    if 'error' in result:
        return '获取新闻失败: ' + result['error']
    summary = '%s 相关新闻:\n' % ts_code
    for category in ['industry_policy', 'company_announcements', 'company_news', 'world_events']:
        items = result.get(category, [])
        if items:
            summary += '\n[%s] (%d条):\n' % (category, len(items))
            for item in items[:3]:  # 每类最多3条摘要
                title = item.get('title', item) if isinstance(item, dict) else str(item)
                summary += '  - %s\n' % str(title)[:100]
    return summary


def _exec_get_sector_report(args):
    from AlphaFin.services.sector_news_service import fetch_sector_report
    result = fetch_sector_report()
    if 'error' in result:
        return '获取板块报告失败: ' + result['error']
    content = result.get('content', result.get('report', ''))
    # 截取摘要
    if len(content) > 2000:
        content = content[:2000] + '\n...(报告已截取前2000字)'
    return content


def _exec_web_search(args, agent_id=''):
    from AlphaFin.services.ai_chat_service import (
        _collect_web_search_snapshot,
        _format_web_search_packet_for_prompt,
    )
    from AlphaFin.services.model_config_service import (
        get_team_agent_model,
        get_module_model,
        normalize_model_name,
    )
    query = args.get('query', '')
    if not query:
        return '搜索查询不能为空'

    preferred_model = normalize_model_name(
        args.get('model')
        or (get_team_agent_model(agent_id) if agent_id else '')
        or get_module_model('ai_team')
    )

    search_links = []
    summary_lines = []
    snapshot = _collect_web_search_snapshot(query, model_name=preferred_model)
    items = (snapshot or {}).get('items') or []
    raw_summary = str((snapshot or {}).get('raw') or '').strip()
    packet_text = _format_web_search_packet_for_prompt(raw_summary, items, limit=6, raw_limit=3200)
    if items:
        for row in items[:8]:
            if not isinstance(row, dict):
                continue
            title = str(row.get('title') or '').strip()
            url = str(row.get('url') or '').strip()
            source = str(row.get('source') or '').strip()
            published_at = str(row.get('published_at') or '').strip()
            summary = str(row.get('summary') or row.get('snippet') or '').strip()
            if url:
                search_links.append({
                    'title': title,
                    'url': url,
                    'source': source,
                    'published_at': published_at,
                    'summary': summary,
                })

        if packet_text:
            summary_lines.append(packet_text)
    else:
        err = str((snapshot or {}).get('error') or '').strip()
        if err:
            summary_lines.append('联网检索结构化结果为空: %s' % err)
        if raw_summary:
            summary_lines.append(raw_summary[:1400])

    if not summary_lines:
        summary_lines.append('联网检索暂无可用结果')
    text = '\n'.join(summary_lines).strip()
    return {
        '_tool_payload': True,
        'text': text,
        'links': search_links,
        'items': items[:8] if isinstance(items, list) else [],
        'raw': raw_summary[:4000],
        'query': str(query or ''),
        'search_model': str((snapshot or {}).get('model') or ''),
    }


def _exec_get_current_time(args):
    from AlphaFin.ai_team.services.tushare_watch_service import get_market_clock
    info = get_market_clock()
    phase_text = {
        'pre_open': '盘前',
        'call_auction': '集合竞价',
        'morning_session': '上午连续竞价',
        'lunch_break': '午间休市',
        'afternoon_session': '下午连续竞价',
        'after_close': '收盘后',
        'closed_weekend': '周末休市',
    }.get(info.get('phase'), info.get('phase'))
    return '当前北京时间: %s | 交易日: %s | 市场状态: %s' % (
        info.get('datetime', '-'),
        info.get('trade_date', '-'),
        phase_text,
    )


def _exec_get_intraday_index(args):
    from AlphaFin.ai_team.services.tushare_watch_service import fetch_intraday_index
    codes = args.get('ts_codes', '')
    freq = args.get('freq', '1MIN')
    try:
        data = fetch_intraday_index(ts_codes=codes, freq=freq)
    except Exception as e:
        return '获取指数实时分钟数据失败: %s' % str(e)
    if not data:
        return '暂无指数实时分钟数据（可能是非交易时段或接口权限不足）'
    lines = ['指数实时快照 (%s):' % (freq or '1MIN')]
    for row in data:
        source = row.get('source', '')
        source_text = ' [%s]' % source if source and source != 'rt_idx_min' else ''
        lines.append(
            '  %s @%s%s | 最新=%.2f | 日内波动=%.2f%% | H/L=%.2f/%.2f | 成交额=%.2f亿' % (
                row.get('ts_code', '-'),
                row.get('time', '-'),
                source_text,
                row.get('close') or 0,
                row.get('intraday_pct') or 0,
                row.get('high') or 0,
                row.get('low') or 0,
                (row.get('amount') or 0) / 1e8,
            )
        )
    return '\n'.join(lines)


def _exec_get_intraday_sector_heat(args):
    from AlphaFin.ai_team.services.tushare_watch_service import fetch_intraday_sector_heat
    try:
        limit = int(args.get('limit', 10))
    except (TypeError, ValueError):
        limit = 10
    limit = max(1, min(limit, 30))
    try:
        rows = fetch_intraday_sector_heat(limit=limit)
    except Exception as e:
        return '获取行业实时热度失败: %s' % str(e)
    if not rows:
        return '暂无行业实时热度数据（可能是非交易时段或接口权限不足）'
    lines = ['行业实时热度 TOP%d:' % limit]
    for i, r in enumerate(rows, 1):
        source = r.get('source', '')
        source_text = ' [%s]' % source if source and source != 'rt_sw_k' else ''
        lines.append(
            '  %d) %s %s%s | 涨跌=%.2f%% | 最新=%.2f | 时间=%s' % (
                i,
                r.get('name', ''),
                r.get('ts_code', ''),
                source_text,
                r.get('pct_change') or 0,
                r.get('close') or 0,
                r.get('trade_time', '-'),
            )
        )
    return '\n'.join(lines)


def _exec_get_intraday_hotrank(args):
    from AlphaFin.ai_team.services.tushare_watch_service import fetch_intraday_hotrank
    market = args.get('market', '热股')
    latest_only = args.get('latest_only', True)
    try:
        limit = int(args.get('limit', 20))
    except (TypeError, ValueError):
        limit = 20
    limit = max(1, min(limit, 50))
    try:
        rows = fetch_intraday_hotrank(market=market, limit=limit, latest_only=latest_only)
    except Exception as e:
        return '获取同花顺热榜失败: %s' % str(e)
    if not rows:
        return '暂无热榜数据（可能是非交易时段或接口权限不足）'
    lines = ['同花顺热榜 (%s) TOP%d:' % (market, limit)]
    for i, r in enumerate(rows, 1):
        lines.append(
            '  %d) #%s %s %s | 热度=%s | 涨跌=%s%% | 时间=%s' % (
                i,
                r.get('rank', '-'),
                r.get('name', ''),
                r.get('ts_code', ''),
                r.get('hot', '-'),
                r.get('pct_change', '-'),
                r.get('rank_time', '-') or '-',
            )
        )
    return '\n'.join(lines)


def _exec_get_intraday_news(args):
    from AlphaFin.ai_team.services.tushare_watch_service import fetch_intraday_news
    try:
        hours = int(args.get('hours', 2))
    except (TypeError, ValueError):
        hours = 2
    src = args.get('src', 'cls')
    try:
        limit = int(args.get('limit', 20))
    except (TypeError, ValueError):
        limit = 20
    try:
        rows = fetch_intraday_news(hours=hours, src=src, limit=limit)
    except Exception as e:
        return '获取盘中快讯失败: %s' % str(e)
    if not rows:
        return '最近%d小时暂无快讯（或接口权限不足）' % max(1, min(hours, 24))
    lines = ['最近%d小时快讯 (%s) TOP%d:' % (max(1, min(hours, 24)), src, min(limit, 100))]
    for i, r in enumerate(rows, 1):
        title = (r.get('title') or '').replace('\n', ' ').strip()
        content = (r.get('content') or '').replace('\n', ' ').strip()
        if len(content) > 80:
            content = content[:80] + '...'
        lines.append('  %d) %s | %s | %s' % (i, r.get('datetime', '-'), title[:60], content))
    return '\n'.join(lines)


def _exec_get_intraday_stock_quote(args):
    from AlphaFin.ai_team.services.tushare_watch_service import fetch_intraday_stock_quote
    from AlphaFin.services.ai_chat_service import _get_local_latest_close, _normalize_ts_code

    code = _normalize_ts_code(args.get('ts_code', ''))
    if not code:
        return '参数错误: ts_code 无效'

    freq = str(args.get('freq', '1MIN') or '1MIN').upper()
    if freq not in ('1MIN', '5MIN', '15MIN', '30MIN', '60MIN'):
        freq = '1MIN'

    try:
        quote = fetch_intraday_stock_quote(code, freq=freq)
    except Exception as e:
        quote = None
        quote_error = str(e)
    else:
        quote_error = ''

    if quote and quote.get('price') not in (None, ''):
        price = quote.get('price')
        prev_close = quote.get('prev_close')
        pct_text = ''
        try:
            if prev_close not in (None, 0, 0.0):
                pct = (float(price) - float(prev_close)) / float(prev_close) * 100.0
                pct_text = ' | 涨跌幅=%+.2f%%' % pct
        except Exception:
            pct_text = ''
        return '%s 最新报价: %.3f | 时间=%s | 来源=%s%s' % (
            code,
            float(price),
            str(quote.get('time') or '-'),
            str(quote.get('source') or 'intraday_quote'),
            pct_text,
        )

    local = _get_local_latest_close(code)
    if local and local.get('close') is not None:
        return (
            '%s 实时报价暂不可用，已回退本地最新收盘: %.3f | 交易日=%s | 来源=%s'
            % (
                code,
                float(local.get('close')),
                str(local.get('trade_date') or '-'),
                str(local.get('source') or 'local_latest_close'),
            )
        )

    if quote_error:
        return '获取个股报价失败: %s' % quote_error
    return '暂无可用个股报价数据（可能是非交易时段或接口权限不足）'


def _coerce_bool(value, default=True):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    s = str(value).strip().lower()
    if s in ('1', 'true', 'yes', 'y', 'on', '开启', '开'):
        return True
    if s in ('0', 'false', 'no', 'n', 'off', '关闭', '关'):
        return False
    return default


def _safe_float(v):
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(n):
        return None
    return n


def _indicator_policy_check(indicator_id, meta):
    iid = str(indicator_id or '').strip()
    if iid in DISABLED_INDICATOR_ALIASES:
        return False, '该主题属于“市场顶底指标”，当前已禁用。'
    if not meta:
        return False, '指标不存在: %s' % iid

    group = str(meta.get('group') or '').strip()
    if group in DISABLED_INDICATOR_GROUPS:
        return False, '指标已禁用：分组“%s”不允许调用。' % group

    name = str(meta.get('name') or '').strip()
    text = (iid + ' ' + name + ' ' + group).lower()
    for kw in DISABLED_INDICATOR_NAME_KEYWORDS:
        if str(kw).strip().lower() in text:
            return False, '指标已禁用：命中限制关键词“%s”。' % kw
    return True, ''


def _extract_figure_pairs(figures):
    pairs = []
    for i, item in enumerate(figures or [], 1):
        fig = None
        title = '图表%d' % i
        if isinstance(item, (list, tuple)):
            if len(item) >= 1:
                fig = item[0]
            if len(item) >= 2:
                title = str(item[1] or title)
        else:
            fig = item
        if fig is None:
            continue
        pairs.append((fig, title))
    return pairs


def _summarize_figure_structure(fig, title):
    axes = list(getattr(fig, 'axes', []) or [])
    if not axes:
        return '%s: 图像对象可用，但未检测到坐标轴。' % title

    line_count = 0
    bar_count = 0
    scatter_count = 0
    axis_parts = []
    for ax in axes[:3]:
        lines = list(ax.get_lines() or [])
        line_count += len(lines)
        for c in list(getattr(ax, 'containers', []) or []):
            bar_count += len(getattr(c, 'patches', []) or [])
        scatter_count += len(list(getattr(ax, 'collections', []) or []))

        trend_parts = []
        for line in lines[:2]:
            raw_y = line.get_ydata()
            if raw_y is None:
                y = []
            else:
                try:
                    y = list(raw_y)
                except Exception:
                    y = []
            nums = []
            for v in y:
                f = _safe_float(v)
                if f is not None:
                    nums.append(f)
            if len(nums) < 2:
                continue
            start_v = nums[0]
            end_v = nums[-1]
            delta = end_v - start_v
            if abs(delta) < 1e-9:
                trend = '平'
            elif delta > 0:
                trend = '上行'
            else:
                trend = '下行'
            label = (line.get_label() or '').strip()
            if label.startswith('_'):
                label = ''
            name = label or '序列'
            trend_parts.append('%s %s(%.4f→%.4f)' % (name, trend, start_v, end_v))

        ax_title = (ax.get_title() or '').strip()
        if trend_parts:
            axis_parts.append('%s%s' % ((ax_title + ': ') if ax_title else '', '；'.join(trend_parts)))
        elif ax_title:
            axis_parts.append(ax_title)

    summary = '%s: 子图%d, 曲线%d, 柱形%d, 散点%d' % (
        title, len(axes), line_count, bar_count, scatter_count
    )
    if axis_parts:
        summary += ' | ' + ' | '.join(axis_parts[:2])
    return summary


def _close_figure_objects(pairs):
    try:
        import matplotlib.pyplot as _plt
    except Exception:
        return
    for fig, _ in pairs:
        try:
            _plt.close(fig)
        except Exception:
            pass


def _vision_describe_figures(pairs, indicator_id, max_figures=2):
    try:
        from AlphaFin.config import CHART_DIR
        from AlphaFin.services.claude_service import analyze_charts
    except Exception as e:
        return '', '视觉模块不可用: %s' % str(e)

    use_pairs = pairs[:max(1, min(int(max_figures), 4))]
    if not use_pairs:
        return '', '无可用图表'

    os.makedirs(CHART_DIR, exist_ok=True)
    chart_paths = []
    abs_paths = []
    token = uuid.uuid4().hex[:10]
    for i, (fig, _) in enumerate(use_pairs, 1):
        fname = 'team_%s_%s_%02d.png' % (indicator_id, token, i)
        abs_path = os.path.join(CHART_DIR, fname)
        try:
            fig.savefig(abs_path, dpi=130, bbox_inches='tight', facecolor='white')
            chart_paths.append('/static/charts/' + fname)
            abs_paths.append(abs_path)
        except Exception:
            continue

    if not chart_paths:
        return '', '图像导出失败'

    titles = [t for _, t in use_pairs[:len(chart_paths)]]
    prompt = (
        '请阅读这些量化图表，并按“图名-关键信号-趋势-风险”给出简明结论。'
        '每张图不超过2行，禁止编造看不到的数据。'
        '图表顺序标题: ' + ' | '.join(titles)
    )
    try:
        text = analyze_charts(chart_paths, prompt, history=None)
        if not text:
            return '', '视觉模型返回为空'
        if '调用失败' in text or '请求超时' in text or '请在 config.py 中设置' in text:
            return '', text
        return str(text).strip()[:2200], ''
    except Exception as e:
        return '', str(e)
    finally:
        for p in abs_paths:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass


def _exec_run_indicator(args):
    from AlphaFin.indicators.indicator_registry import REGISTRY, get_indicator_module
    indicator_id = args.get('indicator_id', '')
    ts_code = args.get('ts_code')
    image_readable = _coerce_bool(args.get('image_readable', True), default=True)
    try:
        max_struct = int(args.get('max_struct_figures', 4))
    except (TypeError, ValueError):
        max_struct = 4
    max_struct = max(1, min(max_struct, 8))
    try:
        max_vision = int(args.get('max_vision_figures', 2))
    except (TypeError, ValueError):
        max_vision = 2
    max_vision = max(1, min(max_vision, 4))

    meta = REGISTRY.get(indicator_id)
    allowed, reason = _indicator_policy_check(indicator_id, meta)
    if not allowed:
        return 'run_indicator 已拦截: %s' % reason
    try:
        mod = get_indicator_module(indicator_id)
    except Exception as e:
        return '指标模块加载失败 [%s]: %s' % (indicator_id, str(e))
    if not mod or not hasattr(mod, 'generate'):
        return '指标模块无法调用: ' + indicator_id
    pairs = []
    try:
        kwargs = {}
        if ts_code and meta.get('input_type') == 'stock':
            kwargs['ts_code'] = ts_code
        figures = mod.generate(**kwargs)
        if not figures:
            return '指标 %s 未生成结果' % indicator_id

        pairs = _extract_figure_pairs(figures)
        if not pairs:
            return '指标 %s 已运行，但图表对象为空' % indicator_id

        titles = [t for _, t in pairs]
        indicator_name = str(meta.get('name') or indicator_id)
        indicator_group = str(meta.get('group') or '未分组')
        used_params = []
        if ts_code:
            used_params.append('ts_code=%s' % ts_code)
        lines = [
            '已调用指标:',
            '- indicator_id: %s' % indicator_id,
            '- indicator_name: %s' % indicator_name,
            '- indicator_group: %s' % indicator_group,
            '- params: %s' % (', '.join(used_params) if used_params else '(默认参数)'),
            '',
            '指标 %s 已生成 %d 张图表。' % (indicator_id, len(titles)),
            '图表标题: ' + ', '.join(titles[:12]),
            '图像结构摘要:',
        ]
        for i, (fig, title) in enumerate(pairs[:max_struct], 1):
            lines.append('  %d) %s' % (i, _summarize_figure_structure(fig, title)))

        if image_readable:
            vision_text, vision_err = _vision_describe_figures(
                pairs, indicator_id=indicator_id, max_figures=max_vision
            )
            if vision_text:
                lines.extend([
                    '',
                    '视觉解读摘要:',
                    vision_text,
                ])
            elif vision_err:
                lines.append('')
                lines.append('视觉解读不可用，已使用结构摘要。原因: %s' % str(vision_err)[:240])
        else:
            lines.append('')
            lines.append('视觉解读已关闭（image_readable=false）。')

        return '\n'.join(lines)[:3800]
    except Exception as e:
        return '指标运行失败 [%s]: %s' % (indicator_id, str(e))
    finally:
        _close_figure_objects(pairs)


def _exec_query_database(args):
    import sqlite3 as _sqlite3
    from AlphaFin.config import DB_ROOT
    import os

    db_name = args.get('db_name', '')
    sql = args.get('sql', '').strip()
    limit = min(args.get('limit', 100), 500)

    # 安全检查：仅允许 SELECT
    sql_upper = sql.upper().lstrip()
    if not sql_upper.startswith('SELECT'):
        return '安全限制：仅允许 SELECT 查询'
    for forbidden in ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'ATTACH', 'DETACH']:
        if forbidden in sql_upper:
            return '安全限制：禁止使用 %s 语句' % forbidden

    db_path = os.path.join(DB_ROOT, db_name + '.db')
    if not os.path.exists(db_path):
        return '数据库不存在: ' + db_name

    conn = _sqlite3.connect(db_path)
    conn.row_factory = _sqlite3.Row
    try:
        # 强制添加 LIMIT
        if 'LIMIT' not in sql_upper:
            sql = sql.rstrip(';') + ' LIMIT %d' % limit
        rows = conn.execute(sql).fetchall()
        if not rows:
            return '查询无结果'
        # 格式化输出
        columns = rows[0].keys()
        result = '查询结果 (%d行):\n' % len(rows)
        result += ' | '.join(columns) + '\n'
        for row in rows[:20]:  # 最多显示20行
            result += ' | '.join(str(row[c]) for c in columns) + '\n'
        if len(rows) > 20:
            result += '... (共 %d 行，仅显示前20行)\n' % len(rows)
        return result
    finally:
        conn.close()


def _exec_send_message(args, agent_id, message_bus):
    if not message_bus:
        return '消息总线不可用'
    to_agent = args.get('to_agent', '')
    message = args.get('message', '')
    if not to_agent or not message:
        return '需要指定目标智能体和消息内容'
    if agent_id and to_agent == agent_id:
        return '不能给自己发送消息'

    message_bus.send(agent_id, to_agent, 'question', message)

    # 自动触发目标智能体处理收到的消息，避免只入队不消费
    try:
        from AlphaFin.ai_team.core.agent_registry import get_agent
        target_agent = get_agent(to_agent)
    except Exception:
        target_agent = None

    if not target_agent:
        return '消息已发送给 %s（目标智能体不可用）' % to_agent

    import threading
    import time as _time
    session_id = 'msg_%s_%d' % (agent_id or 'unknown', int(_time.time()))

    def _dispatch():
        try:
            reply = target_agent.process_incoming_messages(session_id=session_id)
            if reply and agent_id:
                # 将回复回传给发起者，便于后续继续协作
                message_bus.send(to_agent, agent_id, 'report', reply,
                                 metadata={'auto_reply': True, 'session_id': session_id})
        except Exception as e:
            message_bus.post_activity(
                to_agent, 'error',
                '处理来自%s的消息失败: %s' % (agent_id or 'unknown', str(e))
            )

    t = threading.Thread(target=_dispatch, daemon=True)
    t.start()
    return '消息已发送给 %s，已触发其处理' % to_agent


def _exec_save_knowledge(args, agent_id):
    if not agent_id:
        return '无法确定智能体ID'
    from AlphaFin.ai_team.core.memory import AgentMemory
    mem = AgentMemory(agent_id)
    category = args.get('category', 'general')
    subject = args.get('subject', '')
    content = args.get('content', '')
    confidence = args.get('confidence', 0.8)
    tier = args.get('tier', 'warm')
    project = args.get('project', '')
    domain = args.get('domain', '')
    tags = args.get('tags', [])
    pattern_key = args.get('pattern_key', '')
    outcome = args.get('outcome')
    rule_text = args.get('rule_text', '')
    result = mem.save_knowledge(
        category=category,
        subject=subject,
        content=content,
        confidence=confidence,
        tier=tier,
        project=project,
        domain=domain,
        tags=tags,
        pattern_key=pattern_key,
        source_type='tool_call',
        outcome=outcome,
        rule_text=rule_text,
    )
    extra = ''
    pattern = (result or {}).get('pattern') or {}
    if pattern.get('promoted_now'):
        extra = '；模式已升级HOT规则: %s' % (pattern.get('hot_subject') or pattern.get('pattern_key', ''))
    return '知识已保存: [%s/%s], tier=%s, v%d%s' % (
        category, subject, result.get('tier', 'warm'), result.get('version', 1), extra
    )


def _exec_create_skill(args, agent_id):
    from AlphaFin.ai_team.core.skill_sandbox import sandbox
    name = args.get('name', '')
    code = args.get('code', '')
    description = args.get('description', '')
    category = args.get('category', 'data_analysis')
    if not name or not code:
        return '需要提供技能名称和代码'
    result = sandbox.create_skill(
        name=name,
        code_string=code,
        description=description,
        category=category,
        creator=agent_id or 'unknown',
    )
    if result['success']:
        return '技能创建成功: %s (ID: %s, 状态: %s)' % (
            name, result['skill_id'],
            '已自动部署' if result['approved'] else '等待人工审核'
        )
    return '技能创建失败: %s' % result['message']


def _exec_execute_skill(args):
    from AlphaFin.ai_team.core.skill_sandbox import sandbox
    skill_id = args.get('skill_id', '')
    input_data = args.get('input_data', {})
    if not skill_id:
        return '需要提供技能ID'
    return sandbox.execute_skill(skill_id, input_data)


def _exec_list_skills(args):
    from AlphaFin.ai_team.core.skill_sandbox import sandbox
    approved_only = args.get('approved_only', True)
    skills = sandbox.list_skills(approved_only=approved_only)
    if not skills:
        return '暂无%s技能' % ('已批准的' if approved_only else '')
    lines = ['共 %d 个技能:' % len(skills)]
    for s in skills:
        status = '已批准' if s.get('approved') else '待审核'
        lines.append('  - %s [%s] (%s): %s' % (
            s['id'], status, s.get('category', ''), s.get('description', '')
        ))
    return '\n'.join(lines)


# ──────────────── 投资组合工具执行 ────────────────

def _exec_submit_trade_signal(args, agent_id):
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    ts_code = args.get('ts_code', '')
    direction = args.get('direction', '')
    reason = args.get('reason', '')
    target_ratio = args.get('target_ratio')
    quantity = args.get('quantity')
    if not ts_code or not direction or not reason:
        return '需要提供股票代码、方向和理由'
    result = pm.submit_signal(
        ts_code=ts_code, direction=direction, reason=reason,
        proposed_by=agent_id or 'unknown',
        target_ratio=target_ratio, quantity=quantity,
    )
    if result['success']:
        return '交易信号已提交: %s %s (ID: %d), 等待风控审核' % (
            direction, ts_code, result['signal_id'])
    return '信号提交失败: %s' % result['message']


def _exec_review_trade_signal(args, agent_id):
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    signal_id = args.get('signal_id')
    approved = args.get('approved', False)
    review_text = args.get('review_text', '')
    if signal_id is None:
        return '需要提供信号ID'
    # 根据调用者角色决定是风控审核还是总监审批
    if agent_id == 'director':
        result = pm.review_signal_director(signal_id, approved, review_text, agent_id)
    elif agent_id in ('risk', 'auditor'):
        result = pm.review_signal_risk(signal_id, approved, review_text, agent_id)
    else:
        return '你没有审核权限（仅风控组和总监可审核）'
    return result['message'] if result.get('success') else '审核失败: %s' % result['message']


def _exec_get_portfolio_status(args):
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    status = pm.get_portfolio_status()
    if not status.get('initialized'):
        return '投资组合尚未初始化'
    lines = [
        '=== 投资组合状态 ===',
        '模式: %s' % ('自由选股' if status['mode'] == 'free' else '指定标的: %s' % status['target_code']),
        '总资产: %.2f 元' % status['total_assets'],
        '现金: %.2f 元' % status['current_cash'],
        '持仓市值: %.2f 元' % status['market_value'],
        '净值: %.4f' % status['nav'],
        '累计收益: %.2f%%' % status['cumulative_return'],
        '今日收益: %.2f%%' % status['daily_return'],
        '',
        '持仓明细 (%d只):' % status['holdings_count'],
    ]
    for h in status['holdings']:
        lines.append('  %s(%s): %d股, 成本%.2f, 现价%.2f, 浮盈%.2f%%, 占比%.1f%%' % (
            h['ts_code'], h['name'], h['quantity'], h['avg_cost'],
            h['latest_price'] or 0, h['pnl_pct'], h['weight']))
    if not status['holdings']:
        lines.append('  (空仓)')

    # 补充信号概览，避免“只看审批队列”导致口径偏差
    summary = pm.get_signal_status_summary()
    if summary:
        lines.extend([
            '',
            '交易信号统计: 待风控%d / 待总监%d / 已批准%d / 已执行%d / 已拒绝%d / 已过期%d' % (
                summary.get('pending_risk', 0),
                summary.get('pending_director', 0),
                summary.get('approved', 0),
                summary.get('executed', 0),
                summary.get('rejected', 0),
                summary.get('expired', 0),
            ),
        ])

        recent = pm.query_trade_signals(limit=3)
        if recent:
            status_map = {
                'pending_risk': '待风控',
                'pending_director': '待总监',
                'approved': '已批准',
                'executed': '已执行',
                'rejected': '已拒绝',
                'expired': '已过期',
            }
            direction_map = {'buy': '买入', 'sell': '卖出'}
            lines.append('最近3条信号:')
            for s in recent:
                lines.append('  #%d %s %s %s (%s→%s)' % (
                    s['id'],
                    direction_map.get(s.get('direction'), s.get('direction') or ''),
                    s.get('ts_code', ''),
                    status_map.get(s.get('status'), s.get('status') or ''),
                    s.get('signal_date', '-') or '-',
                    s.get('execute_date', '-') or '-',
                ))
    return '\n'.join(lines)


def _exec_get_trade_signals(args):
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm

    raw_code = (args.get('ts_code') or '').strip()
    status = (args.get('status') or '').strip()
    try:
        limit = int(args.get('limit', 20))
    except (TypeError, ValueError):
        limit = 20
    limit = max(1, min(limit, 100))

    ts_code = raw_code.upper()
    if ts_code and '.' not in ts_code and len(ts_code) == 6 and ts_code.isdigit():
        ts_code = ts_code + ('.SH' if ts_code.startswith(('6', '9')) else '.SZ')

    signals = pm.query_trade_signals(
        ts_code=ts_code or None,
        status=status or None,
        limit=limit,
    )

    if not signals:
        return '未找到匹配的交易信号'

    status_map = {
        'pending_risk': '待风控',
        'pending_director': '待总监',
        'approved': '已批准',
        'executed': '已执行',
        'rejected': '已拒绝',
        'expired': '已过期',
    }
    direction_map = {'buy': '买入', 'sell': '卖出'}

    filters = []
    if raw_code:
        filters.append('股票=%s' % (ts_code or raw_code))
    if status:
        filters.append('状态=%s' % status_map.get(status, status))

    title = '交易信号查询结果: %d 条' % len(signals)
    if filters:
        title += '（' + '，'.join(filters) + '）'

    def _short(text, size=80):
        text = (text or '').replace('\n', ' ').strip()
        if len(text) <= size:
            return text
        return text[:size] + '...'

    lines = [title]
    for s in signals:
        lines.append(
            '#%d %s %s | %s | 提交:%s | 计划执行:%s' % (
                s['id'],
                direction_map.get(s.get('direction'), s.get('direction') or ''),
                s.get('ts_code', ''),
                status_map.get(s.get('status'), s.get('status') or ''),
                s.get('signal_date', '-') or '-',
                s.get('execute_date', '-') or '-',
            )
        )
        if s.get('risk_review'):
            lines.append('  风控意见: %s' % _short(s.get('risk_review')))
        if s.get('director_review'):
            lines.append('  总监意见: %s' % _short(s.get('director_review')))
        if s.get('reason'):
            lines.append('  理由: %s' % _short(s.get('reason')))
    return '\n'.join(lines)


def _exec_flag_risk_warning(args, agent_id):
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    ts_code = args.get('ts_code', '')
    risk_type = args.get('risk_type', '')
    severity = args.get('severity', 'medium')
    description = args.get('description', '')
    if not ts_code or not risk_type or not description:
        return '需要提供股票代码、风险类型和描述'
    result = pm.submit_risk_warning(
        ts_code=ts_code, risk_type=risk_type, severity=severity,
        description=description, warned_by=agent_id or 'unknown',
    )
    if result['success']:
        return '风险预警已记录 (ID: %d)。如5个交易日内下跌>3%%将获奖励。' % result['warning_id']
    return '预警提交失败: %s' % result['message']
