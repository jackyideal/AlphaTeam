"""
AlphaFin 智能分析团队 - 配置文件
"""
import os

# Qwen API 基础配置
QWEN_BASE_URL = os.getenv('QWEN_BASE_URL', 'https://dashscope.aliyuncs.com/compatible-mode/v1')
QWEN_MODEL = os.getenv('ALPHAFIN_TEAM_QWEN_MODEL', 'qwen3.5-plus')
QWEN_FALLBACK_MODEL = os.getenv('ALPHAFIN_TEAM_QWEN_FALLBACK_MODEL', QWEN_MODEL)
QWEN_MAX_PARALLEL_REQUESTS = max(1, int(os.getenv('ALPHAFIN_TEAM_QWEN_MAX_PARALLEL_REQUESTS', '2')))
QWEN_REQUEST_RETRIES = max(0, int(os.getenv('ALPHAFIN_TEAM_QWEN_REQUEST_RETRIES', '2')))
QWEN_RETRY_BACKOFF = max(0.1, float(os.getenv('ALPHAFIN_TEAM_QWEN_RETRY_BACKOFF', '1.0')))

# 工具协议版本（agent 内部优先使用 v2，其他链路保持兼容）
TOOL_PROTOCOL_DEFAULT = 'v2'

# Token 预算控制（防止多智能体长会议导致资源失控）
TOKEN_BUDGET_ENABLED = True
# 额度规则：<=0 视为不限额
# 按需求关闭“当日上限”和“会话上限”
TOKEN_BUDGET_DAILY_LIMIT = 0
TOKEN_BUDGET_SESSION_LIMIT = 0
TOKEN_BUDGET_WARN_RATIO = float(os.getenv('ALPHAFIN_TEAM_TOKEN_WARN_RATIO', '0.80'))
TOKEN_BUDGET_HARD_RATIO = float(os.getenv('ALPHAFIN_TEAM_TOKEN_HARD_RATIO', '0.95'))

# 7个智能体的 API Key 配置
_DEFAULT_TEAM_API_KEY = str(os.getenv('QWEN_API_KEY', '') or '').strip()
AGENT_API_KEYS = {
    'director': str(os.getenv('ALPHAFIN_TEAM_DIRECTOR_API_KEY', _DEFAULT_TEAM_API_KEY) or '').strip(),
    'analyst': str(os.getenv('ALPHAFIN_TEAM_ANALYST_API_KEY', _DEFAULT_TEAM_API_KEY) or '').strip(),
    'risk': str(os.getenv('ALPHAFIN_TEAM_RISK_API_KEY', _DEFAULT_TEAM_API_KEY) or '').strip(),
    'intel': str(os.getenv('ALPHAFIN_TEAM_INTEL_API_KEY', _DEFAULT_TEAM_API_KEY) or '').strip(),
    'quant': str(os.getenv('ALPHAFIN_TEAM_QUANT_API_KEY', _DEFAULT_TEAM_API_KEY) or '').strip(),
    'auditor': str(os.getenv('ALPHAFIN_TEAM_AUDITOR_API_KEY', _DEFAULT_TEAM_API_KEY) or '').strip(),
    'restructuring': str(os.getenv('ALPHAFIN_TEAM_RESTRUCTURING_API_KEY', _DEFAULT_TEAM_API_KEY) or '').strip(),
}

# 智能体元信息
AGENT_META = {
    'director': {
        'name': '决策总监',
        'color': '#d97706',
        'icon': 'crown',
        'description': '团队领导者，负责分配任务、综合各方意见、生成最终研究报告',
    },
    'analyst': {
        'name': '投资分析师',
        'color': '#1a6ddb',
        'icon': 'chart',
        'description': '专注个股和行业深度分析、估值研究、基本面剖析',
    },
    'risk': {
        'name': '风控官',
        'color': '#dc2626',
        'icon': 'shield',
        'description': '负责风险评估、仓位建议、回撤预警、组合风险管理',
    },
    'intel': {
        'name': '市场情报员',
        'color': '#0891b2',
        'icon': 'globe',
        'description': '负责新闻监控、宏观经济分析、市场情绪研判、政策解读',
    },
    'quant': {
        'name': '量化策略师',
        'color': '#059669',
        'icon': 'function',
        'description': '负责策略回测、因子筛选、信号生成、量化模型优化',
    },
    'auditor': {
        'name': '反思审计员',
        'color': '#7c3aed',
        'icon': 'magnifier',
        'description': '负责复盘历史决策、交叉验证结论、唱反调、确保分析质量',
    },
    'restructuring': {
        'name': '资产重组专家',
        'color': '#0ea5a4',
        'icon': 'spark',
        'description': '负责识别重组预期、资产注入预期、资本运作催化与兑现风险',
    },
}

# 调度配置
DEFAULT_CYCLE_INTERVAL = 4 * 3600  # 默认每4小时一次研究周期（秒）
MAX_CONVERSATION_CONTEXT = 20       # 每个智能体保留的最近对话条数
KNOWLEDGE_RETENTION_DAYS = 90       # 知识库条目保留天数
REPORT_TOP_N_STOCKS = 5             # 每次研究周期深度分析的股票数量
WORKFLOW_DEADLINE_SOFT_RATIO = 0.20  # 剩余20%时间时进入收敛模式
MANUAL_ANALYZE_DEFAULT_TIMEOUT = 8 * 60
TEAM_WORKFLOW_DEFAULT_TIMEOUT = 8 * 60
PORTFOLIO_MANUAL_DEFAULT_TIMEOUT = 10 * 60
PORTFOLIO_WATCH_MANUAL_DEFAULT_TIMEOUT = 6 * 60
# 智能分析模块默认不自启动，由首页手动控制启动/停止
TEAM_MODULE_AUTO_START = False

# 闲时自学习（Idle Cycle）配置
IDLE_LEARNING_ENABLED = True
DEFAULT_IDLE_INTERVAL = 2 * 3600  # 默认每2小时一次闲时学习
IDLE_LEARNING_TOPICS = [
    '市场情绪监控与风格切换信号',
    '融资融券与杠杆资金风险偏好',
    '估值分位与市场波动率共振',
    '行业轮动持续性与反转风险',
    '交易复盘与误差归因改进',
]
# 第一版：仅允许只读学习工具，禁止交易/审批/技能创建
IDLE_ALLOWED_TOOLS = [
    'get_current_time',
    'get_intraday_index', 'get_intraday_sector_heat',
    'get_intraday_hotrank', 'get_intraday_news', 'get_intraday_stock_quote',
    'web_search', 'get_stock_news', 'get_sector_report',
    'run_indicator', 'query_database',
    'get_kline', 'get_kline_technical', 'get_financials', 'get_chip_distribution',
    'get_portfolio_status', 'get_trade_signals',
    'list_skills', 'execute_skill',
    'save_knowledge',
]
IDLE_BLOCKED_TOOLS = [
    'submit_trade_signal', 'review_trade_signal',
    'create_skill', 'flag_risk_warning',
    'send_message_to_agent',
]

# 同事闲聊（Office Chat）配置：低频、低风险、模拟真实办公沟通
OFFICE_CHAT_ENABLED = True
DEFAULT_OFFICE_CHAT_INTERVAL = 3 * 3600  # 默认每3小时一次
OFFICE_CHAT_TOPICS = [
    '今天最想聊的一条市场八卦或叙事',
    '最近一个让你意外的行业变化',
    '宏观政策或国际政治里，哪个变量最可能传导到A股',
    '你见过最典型的一次“主力做局”案例',
    '未来半年你最看好的一个方向和最担心的一个坑',
    '如果你是基金经理，这周会先做哪件事',
    '最近一条行业新闻背后的利益链',
    '从生活经验看，哪些消费趋势正在变',
    '今天盘面最值得一笑的现象',
    '一个被市场忽视的风险或机会',
]

# 技能沙箱配置
ALLOWED_SKILL_IMPORTS = ['pandas', 'numpy', 'math', 'datetime', 'json', 'statistics']
SKILL_EXECUTION_TIMEOUT = 30  # 秒
AUTO_APPROVE_CATEGORIES = ['data_analysis', 'visualization', 'statistics']
MANUAL_REVIEW_CATEGORIES = ['trading_strategy', 'risk_rule', 'portfolio']

# 投资组合配置
PORTFOLIO_INITIAL_CAPITAL = 10000000      # 初始本金（元）
PORTFOLIO_DAILY_SALARY = 1000             # 每人每日工资（元）
PORTFOLIO_STRATEGY_BONUS_RATE = 0.20      # 策略组盈利提成比例
PORTFOLIO_MAX_POSITION_RATIO = 0.30       # 单只股票最大仓位比例
PORTFOLIO_MAX_HOLDINGS = 8               # 最大持仓数量
PORTFOLIO_DAILY_SIGNAL_AGENT_LIMIT = 3   # 单智能体每日最多提交信号数
PORTFOLIO_DAILY_SIGNAL_TOTAL_LIMIT = 10  # 团队每日最多提交信号数
PORTFOLIO_COMMISSION_RATE = 0.00025       # 佣金费率（万2.5）
PORTFOLIO_STAMP_TAX_RATE = 0.001          # 印花税费率（千1，仅卖出）
PORTFOLIO_STOP_LOSS = 0.05               # 单股止损线 5%
PORTFOLIO_SEVERE_LOSS = 0.10             # 单股重大亏损线 10%
PORTFOLIO_DRAWDOWN_PENALTY_THRESHOLD = 0.03  # 组合回撤惩罚阈值 3%
PORTFOLIO_RISK_WARNING_REWARD = 500       # 成功预警奖励（元）
PORTFOLIO_RISK_WARNING_VERIFY_DAYS = 5    # 预警验证窗口（交易日）
PORTFOLIO_RISK_WARNING_VERIFY_DROP = 0.03 # 预警验证下跌阈值 3%
PORTFOLIO_BENCHMARK_CODE = '000300.SH'    # 默认基准（沪深300）
PORTFOLIO_AUTO_RUN_TIME = '15:30'         # 自动运行时间（收盘后）

# 交易日盘中盯盘配置（中国时间）
PORTFOLIO_WATCH_ENABLED = True
PORTFOLIO_WATCH_START = '09:00'
PORTFOLIO_WATCH_END = '15:00'
PORTFOLIO_WATCH_INTERVAL = 30 * 60  # 每30分钟一轮盯盘
# 当日行情尚未写入 daily_kline 时，盘中盯盘可按工作日兜底运行（周一至周五）
PORTFOLIO_WATCH_WEEKDAY_FALLBACK = True

# 每日数据库自动更新（北京时间）
PORTFOLIO_DB_AUTO_UPDATE_ENABLED = True
PORTFOLIO_DB_AUTO_UPDATE_TIME = '06:00'

# 数据目录
AI_TEAM_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(AI_TEAM_DIR, 'data')
MEMORY_DB_PATH = os.path.join(DATA_DIR, 'agent_memory.db')
REPORTS_DIR = os.path.join(DATA_DIR, 'research_reports')
SKILLS_DIR = os.path.join(AI_TEAM_DIR, 'skills')
PORTFOLIO_DB_PATH = os.path.join(DATA_DIR, 'portfolio.db')
ORCHESTRATOR_STATE_PATH = os.path.join(DATA_DIR, 'orchestrator_state.json')
