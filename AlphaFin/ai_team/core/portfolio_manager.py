"""
投资组合管理器 - 持仓、交易、净值、奖惩的核心引擎
"""
import json
import os
import sqlite3
import time
import threading
import datetime

from AlphaFin.ai_team.config import (
    PORTFOLIO_DB_PATH, DATA_DIR,
    PORTFOLIO_INITIAL_CAPITAL, PORTFOLIO_DAILY_SALARY,
    PORTFOLIO_STRATEGY_BONUS_RATE, PORTFOLIO_MAX_POSITION_RATIO,
    PORTFOLIO_MAX_HOLDINGS, PORTFOLIO_DAILY_SIGNAL_AGENT_LIMIT,
    PORTFOLIO_DAILY_SIGNAL_TOTAL_LIMIT, PORTFOLIO_COMMISSION_RATE,
    PORTFOLIO_STAMP_TAX_RATE, PORTFOLIO_STOP_LOSS,
    PORTFOLIO_SEVERE_LOSS, PORTFOLIO_DRAWDOWN_PENALTY_THRESHOLD,
    PORTFOLIO_RISK_WARNING_REWARD, PORTFOLIO_RISK_WARNING_VERIFY_DAYS,
    PORTFOLIO_RISK_WARNING_VERIFY_DROP, PORTFOLIO_BENCHMARK_CODE,
)

# 智能体角色分组
STRATEGY_GROUP = ('intel', 'analyst', 'quant', 'restructuring')
RISK_GROUP = ('risk', 'auditor')
ALL_AGENTS = ('director', 'analyst', 'risk', 'intel', 'quant', 'auditor', 'restructuring')


class PortfolioManager:
    """投资组合管理器"""

    def __init__(self):
        self.db_path = PORTFOLIO_DB_PATH
        self._lock = threading.Lock()
        self._init_db()

    # ━━━━━━━━━━━━━━━━ 数据库初始化 ━━━━━━━━━━━━━━━━

    def _init_db(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        with self._connect() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS portfolio_config (
                    id INTEGER PRIMARY KEY,
                    mode TEXT DEFAULT 'free',
                    target_code TEXT DEFAULT '',
                    initial_capital REAL DEFAULT 10000000,
                    current_cash REAL DEFAULT 10000000,
                    start_date TEXT,
                    auto_run INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'idle',
                    created_at REAL,
                    updated_at REAL
                );

                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code TEXT NOT NULL,
                    name TEXT DEFAULT '',
                    quantity INTEGER NOT NULL,
                    cost_price REAL NOT NULL,
                    buy_date TEXT NOT NULL,
                    available_date TEXT NOT NULL,
                    status TEXT DEFAULT 'holding',
                    created_at REAL
                );

                CREATE TABLE IF NOT EXISTS trade_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code TEXT NOT NULL,
                    name TEXT DEFAULT '',
                    direction TEXT NOT NULL,
                    price REAL NOT NULL,
                    quantity INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    commission REAL DEFAULT 0,
                    stamp_tax REAL DEFAULT 0,
                    reason TEXT DEFAULT '',
                    proposed_by TEXT DEFAULT '',
                    approved_by TEXT DEFAULT '',
                    trade_date TEXT NOT NULL,
                    status TEXT DEFAULT 'executed',
                    created_at REAL
                );

                CREATE TABLE IF NOT EXISTS daily_nav (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date TEXT NOT NULL UNIQUE,
                    total_assets REAL NOT NULL,
                    cash REAL NOT NULL,
                    market_value REAL NOT NULL,
                    nav REAL NOT NULL,
                    benchmark_nav REAL DEFAULT 1.0,
                    daily_return REAL DEFAULT 0,
                    benchmark_return REAL DEFAULT 0,
                    max_drawdown REAL DEFAULT 0,
                    positions_snapshot TEXT DEFAULT '[]',
                    created_at REAL
                );

                CREATE TABLE IF NOT EXISTS agent_compensation (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    agent_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    type TEXT NOT NULL,
                    amount REAL NOT NULL,
                    reason TEXT DEFAULT '',
                    related_trade_id INTEGER,
                    created_at REAL
                );

                CREATE TABLE IF NOT EXISTS trade_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code TEXT NOT NULL,
                    name TEXT DEFAULT '',
                    direction TEXT NOT NULL,
                    target_ratio REAL,
                    quantity INTEGER,
                    reason TEXT DEFAULT '',
                    proposed_by TEXT NOT NULL,
                    risk_review TEXT DEFAULT '',
                    risk_approved INTEGER DEFAULT 0,
                    director_review TEXT DEFAULT '',
                    director_approved INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending_risk',
                    signal_date TEXT NOT NULL,
                    execute_date TEXT,
                    created_at REAL
                );

                CREATE TABLE IF NOT EXISTS risk_warnings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code TEXT NOT NULL,
                    risk_type TEXT NOT NULL,
                    severity TEXT DEFAULT 'medium',
                    description TEXT DEFAULT '',
                    warned_by TEXT NOT NULL,
                    warn_date TEXT NOT NULL,
                    verified INTEGER DEFAULT 0,
                    verified_date TEXT,
                    reward_paid INTEGER DEFAULT 0,
                    created_at REAL
                );
            ''')
            self._migrate_schema(conn)

    def _migrate_schema(self, conn):
        """轻量迁移：为旧库补齐新增字段。"""
        self._ensure_column(conn, 'trade_signals', 'execute_price', 'REAL')
        self._ensure_column(conn, 'trade_signals', 'execute_qty', 'INTEGER')
        self._ensure_column(conn, 'trade_signals', 'execute_amount', 'REAL')
        self._ensure_column(conn, 'trade_signals', 'execute_commission', 'REAL DEFAULT 0')
        self._ensure_column(conn, 'trade_signals', 'execute_stamp_tax', 'REAL DEFAULT 0')
        self._ensure_column(conn, 'trade_signals', 'execute_cost', 'REAL DEFAULT 0')
        self._ensure_column(conn, 'trade_signals', 'execute_price_source', 'TEXT DEFAULT ""')
        self._ensure_column(conn, 'trade_signals', 'executed_trade_date', 'TEXT')
        self._ensure_column(conn, 'trade_signals', 'executed_at', 'REAL')
        self._ensure_column(conn, 'trade_signals', 'execute_message', 'TEXT DEFAULT ""')

    @staticmethod
    def _ensure_column(conn, table_name, column_name, column_def):
        rows = conn.execute('PRAGMA table_info(%s)' % table_name).fetchall()
        columns = set(r['name'] for r in rows)
        if column_name in columns:
            return
        conn.execute(
            'ALTER TABLE %s ADD COLUMN %s %s' % (table_name, column_name, column_def)
        )

    def _connect(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _dict_row(self, row):
        if row is None:
            return None
        return dict(row)

    def _dict_rows(self, rows):
        return [dict(r) for r in rows]

    # ━━━━━━━━━━━━━━━━ 配置管理 ━━━━━━━━━━━━━━━━

    def get_config(self):
        with self._connect() as conn:
            row = conn.execute('SELECT * FROM portfolio_config WHERE id=1').fetchone()
            return self._dict_row(row)

    def init_portfolio(self, mode='free', target_code='', initial_capital=None):
        """初始化投资组合（首次或重置）"""
        capital = initial_capital or PORTFOLIO_INITIAL_CAPITAL
        now = time.time()
        import datetime
        today = datetime.datetime.now().strftime('%Y%m%d')

        with self._lock:
            with self._connect() as conn:
                existing = conn.execute('SELECT id FROM portfolio_config WHERE id=1').fetchone()
                if existing:
                    conn.execute('''UPDATE portfolio_config SET
                        mode=?, target_code=?, initial_capital=?, current_cash=?,
                        start_date=?, status='idle', updated_at=?
                        WHERE id=1''',
                        (mode, target_code, capital, capital, today, now))
                else:
                    conn.execute('''INSERT INTO portfolio_config
                        (id, mode, target_code, initial_capital, current_cash,
                         start_date, auto_run, status, created_at, updated_at)
                        VALUES (1, ?, ?, ?, ?, ?, 0, 'idle', ?, ?)''',
                        (mode, target_code, capital, capital, today, now, now))
                # 清空旧持仓和信号
                conn.execute('DELETE FROM positions')
                conn.execute('DELETE FROM trade_signals WHERE status IN ("pending_risk","pending_director")')

        return {'success': True, 'message': '投资组合已初始化',
                'mode': mode, 'capital': capital, 'start_date': today}

    def update_config(self, **kwargs):
        with self._lock:
            with self._connect() as conn:
                allowed = ('mode', 'target_code', 'auto_run', 'status')
                sets = []
                vals = []
                for k, v in kwargs.items():
                    if k in allowed:
                        sets.append('%s=?' % k)
                        vals.append(v)
                if sets:
                    vals.append(time.time())
                    conn.execute('UPDATE portfolio_config SET %s, updated_at=? WHERE id=1'
                                 % ', '.join(sets), vals)
        return self.get_config()

    # ━━━━━━━━━━━━━━━━ 交易信号 ━━━━━━━━━━━━━━━━

    def submit_signal(self, ts_code, direction, reason, proposed_by,
                      target_ratio=None, quantity=None, name=''):
        """策略组提交交易信号"""
        ts_code = (ts_code or '').strip().upper()
        if '.' not in ts_code and len(ts_code) == 6 and ts_code.isdigit():
            ts_code = ts_code + ('.SH' if ts_code.startswith(('6', '9')) else '.SZ')

        config = self.get_config()
        if not config:
            return {'success': False, 'message': '投资组合未初始化'}

        if proposed_by not in STRATEGY_GROUP:
            return {'success': False, 'message': '只有策略组成员可提交交易信号'}

        if direction not in ('buy', 'sell'):
            return {'success': False, 'message': '方向必须为 buy 或 sell'}

        if direction == 'buy':
            # 检查持仓数量上限
            positions = self.get_positions()
            held_codes = set(p['ts_code'] for p in positions)
            if ts_code not in held_codes and len(held_codes) >= PORTFOLIO_MAX_HOLDINGS:
                return {'success': False, 'message': '持仓数量已达上限 %d' % PORTFOLIO_MAX_HOLDINGS}

            # 检查单只仓位上限
            if target_ratio and target_ratio > PORTFOLIO_MAX_POSITION_RATIO:
                return {'success': False,
                        'message': '单只仓位不能超过 %.0f%%' % (PORTFOLIO_MAX_POSITION_RATIO * 100)}

        import datetime
        today = datetime.datetime.now().strftime('%Y%m%d')
        next_trade = self._get_next_trade_date(today)

        # 每日限额：控制信号总量，避免“刷信号”
        with self._connect() as conn:
            agent_cnt_row = conn.execute(
                'SELECT COUNT(*) FROM trade_signals WHERE signal_date=? AND proposed_by=?',
                (today, proposed_by)
            ).fetchone()
            total_cnt_row = conn.execute(
                'SELECT COUNT(*) FROM trade_signals WHERE signal_date=?',
                (today,)
            ).fetchone()
        agent_cnt = int(agent_cnt_row[0]) if agent_cnt_row else 0
        total_cnt = int(total_cnt_row[0]) if total_cnt_row else 0
        if agent_cnt >= PORTFOLIO_DAILY_SIGNAL_AGENT_LIMIT:
            return {
                'success': False,
                'message': '%s 今日信号数已达上限(%d)，请等待下一交易日或复用已有结论' % (
                    proposed_by, PORTFOLIO_DAILY_SIGNAL_AGENT_LIMIT),
            }
        if total_cnt >= PORTFOLIO_DAILY_SIGNAL_TOTAL_LIMIT:
            return {
                'success': False,
                'message': '团队今日信号总数已达上限(%d)，请先消化已有审批与执行结果' % (
                    PORTFOLIO_DAILY_SIGNAL_TOTAL_LIMIT),
            }

        # 同一标的当日限频：禁止重复/反向反复提交，降低噪声信号
        with self._connect() as conn:
            today_signals = conn.execute(
                'SELECT id, direction, status FROM trade_signals WHERE ts_code=? AND signal_date=? ORDER BY id DESC',
                (ts_code, today)
            ).fetchall()
        if today_signals:
            latest = dict(today_signals[0])
            if latest.get('direction') == direction:
                return {
                    'success': False,
                    'message': '%s 当日已提交过同向信号（ID:%s），请避免重复下单建议' % (ts_code, latest.get('id')),
                }
            return {
                'success': False,
                'message': '%s 当日已存在反向信号（ID:%s），禁止同日来回交易建议' % (ts_code, latest.get('id')),
            }

        now = time.time()
        with self._lock:
            with self._connect() as conn:
                conn.execute('''INSERT INTO trade_signals
                    (ts_code, name, direction, target_ratio, quantity, reason,
                     proposed_by, status, signal_date, execute_date, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_risk', ?, ?, ?)''',
                    (ts_code, name, direction, target_ratio, quantity, reason,
                     proposed_by, today, next_trade, now))
                signal_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]

        return {'success': True, 'signal_id': signal_id,
                'message': '信号已提交，等待风控审核 (ID: %d)' % signal_id}

    def review_signal_risk(self, signal_id, approved, review_text, reviewer):
        """风控组审核信号"""
        if reviewer not in RISK_GROUP:
            return {'success': False, 'message': '只有风控组成员可审核'}

        with self._lock:
            with self._connect() as conn:
                signal = conn.execute('SELECT * FROM trade_signals WHERE id=?',
                                      (signal_id,)).fetchone()
                if not signal:
                    return {'success': False, 'message': '信号不存在'}
                if signal['status'] != 'pending_risk':
                    return {'success': False, 'message': '信号状态不正确: %s' % signal['status']}

                # 风控不再“直接否决”：无论同意或反对，都进入总监裁决阶段
                new_status = 'pending_director'
                conn.execute('''UPDATE trade_signals SET
                    risk_review=?, risk_approved=?, status=?
                    WHERE id=?''',
                    (review_text, 1 if approved else 0, new_status, signal_id))

        if approved:
            return {'success': True, 'message': '风控审核通过，已提交总监最终审批: %s' % review_text}
        return {'success': True, 'message': '风控提出反对意见，已提交总监裁决: %s' % review_text}

    def review_signal_director(self, signal_id, approved, review_text, reviewer):
        """决策总监最终审批"""
        if reviewer != 'director':
            return {'success': False, 'message': '只有决策总监可最终审批'}

        with self._lock:
            with self._connect() as conn:
                signal = conn.execute('SELECT * FROM trade_signals WHERE id=?',
                                      (signal_id,)).fetchone()
                if not signal:
                    return {'success': False, 'message': '信号不存在'}
                if signal['status'] != 'pending_director':
                    return {'success': False, 'message': '信号状态不正确: %s' % signal['status']}

                new_status = 'approved' if approved else 'rejected'
                conn.execute('''UPDATE trade_signals SET
                    director_review=?, director_approved=?, status=?
                    WHERE id=?''',
                    (review_text, 1 if approved else 0, new_status, signal_id))

        action = '批准' if approved else '否决'
        return {'success': True, 'message': '总监%s: %s' % (action, review_text)}

    def get_pending_signals(self, status=None):
        with self._connect() as conn:
            if status:
                rows = conn.execute('SELECT * FROM trade_signals WHERE status=? ORDER BY id',
                                    (status,)).fetchall()
            else:
                rows = conn.execute(
                    'SELECT * FROM trade_signals WHERE status IN '
                    '("pending_risk","pending_director","approved") ORDER BY id'
                ).fetchall()
            return self._dict_rows(rows)

    def get_all_signals(self, limit=50):
        with self._connect() as conn:
            rows = conn.execute(
                'SELECT * FROM trade_signals ORDER BY signal_date DESC, created_at DESC, id DESC LIMIT ?',
                (limit,)
            ).fetchall()
            return self._dict_rows(rows)

    def query_trade_signals(self, ts_code=None, status=None, limit=50):
        """按条件查询交易信号（给智能体工具使用）"""
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, 100))

        sql = 'SELECT * FROM trade_signals'
        conds = []
        vals = []

        if status:
            conds.append('status=?')
            vals.append(status)

        if ts_code:
            code = ts_code.strip().upper()
            # 支持仅输入 6 位代码（如 600425），匹配 600425.SH / 600425.SZ
            if '.' in code:
                conds.append('ts_code=?')
                vals.append(code)
            else:
                conds.append('ts_code LIKE ?')
                vals.append(code + '.%')

        if conds:
            sql += ' WHERE ' + ' AND '.join(conds)
        sql += ' ORDER BY signal_date DESC, created_at DESC, id DESC LIMIT ?'
        vals.append(limit)

        with self._connect() as conn:
            rows = conn.execute(sql, tuple(vals)).fetchall()
            return self._dict_rows(rows)

    def get_signal_status_summary(self):
        """返回各状态信号数量统计"""
        with self._connect() as conn:
            rows = conn.execute(
                'SELECT status, COUNT(*) AS cnt FROM trade_signals GROUP BY status'
            ).fetchall()
            return {r['status']: r['cnt'] for r in rows}

    # ━━━━━━━━━━━━━━━━ 交易执行 ━━━━━━━━━━━━━━━━

    def execute_approved_signals(self, trade_date):
        """执行所有已批准的信号（用当日开盘价）"""
        results = []
        with self._connect() as conn:
            signals = conn.execute(
                'SELECT * FROM trade_signals WHERE status="approved" AND execute_date<=?',
                (trade_date,)
            ).fetchall()

        for signal in signals:
            signal = dict(signal)
            try:
                result = self._execute_single_trade(signal, trade_date)
                results.append(result)
            except Exception as e:
                results.append({'signal_id': signal['id'], 'success': False,
                                'message': '执行失败: %s' % str(e)})
        return results

    def _execute_single_trade(self, signal, trade_date):
        """执行单笔交易"""
        config = self.get_config()
        ts_code = signal['ts_code']
        direction = signal['direction']

        # 获取成交参考价（优先开盘价，缺失时按实时分钟价/最近收盘价兜底）
        open_price, price_source = self._get_open_price(ts_code, trade_date, with_source=True)
        if not open_price:
            return {'signal_id': signal['id'], 'success': False,
                    'message': '无法获取 %s 在 %s 的成交参考价' % (ts_code, trade_date)}

        # 计算交易数量
        if direction == 'buy':
            if signal['target_ratio']:
                total_assets = self._calc_total_assets(trade_date)
                target_amount = total_assets * signal['target_ratio']
                quantity = int(target_amount / open_price / 100) * 100  # 整手（100股）
            else:
                quantity = signal['quantity'] or 0
                quantity = int(quantity / 100) * 100

            if quantity <= 0:
                return {'signal_id': signal['id'], 'success': False,
                        'message': '计算交易数量为0'}

            amount = quantity * open_price
            commission = max(amount * PORTFOLIO_COMMISSION_RATE, 5)  # 最低5元
            total_cost = amount + commission

            if total_cost > config['current_cash']:
                return {'signal_id': signal['id'], 'success': False,
                        'message': '资金不足: 需要%.2f, 可用%.2f' % (total_cost, config['current_cash'])}

            # T+1: 次日可卖
            available_date = self._get_next_trade_date(trade_date)

            with self._lock:
                with self._connect() as conn:
                    # 写入持仓
                    conn.execute('''INSERT INTO positions
                        (ts_code, name, quantity, cost_price, buy_date, available_date, status, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, 'holding', ?)''',
                        (ts_code, signal['name'], quantity, open_price,
                         trade_date, available_date, time.time()))
                    # 写入交易记录
                    conn.execute('''INSERT INTO trade_history
                        (ts_code, name, direction, price, quantity, amount,
                         commission, stamp_tax, reason, proposed_by, approved_by,
                         trade_date, status, created_at)
                        VALUES (?, ?, 'buy', ?, ?, ?, ?, 0, ?, ?, 'director', ?, 'executed', ?)''',
                        (ts_code, signal['name'], open_price, quantity, amount,
                         commission, signal['reason'], signal['proposed_by'],
                         trade_date, time.time()))
                    # 扣除现金
                    conn.execute('UPDATE portfolio_config SET current_cash=current_cash-?, updated_at=? WHERE id=1',
                                 (total_cost, time.time()))
                    # 更新信号状态
                    conn.execute('''UPDATE trade_signals SET
                        status="executed",
                        execute_price=?,
                        execute_qty=?,
                        execute_amount=?,
                        execute_commission=?,
                        execute_stamp_tax=0,
                        execute_cost=?,
                        execute_price_source=?,
                        executed_trade_date=?,
                        executed_at=?,
                        execute_message=?
                        WHERE id=?''',
                        (open_price, quantity, amount, commission, commission,
                         price_source, trade_date, time.time(),
                         '买入 %s %d股 @ %.2f (价格来源: %s)' % (
                             ts_code, quantity, open_price, price_source),
                         signal['id']))

            return {'signal_id': signal['id'], 'success': True,
                    'message': '买入 %s %d股 @ %.2f, 费用%.2f, 价格来源:%s' % (
                        ts_code, quantity, open_price, commission, price_source)}

        else:  # sell
            # 查找可卖出的持仓（仅执行A股T+1规则：买入次日可卖）
            with self._connect() as conn:
                eligible_positions = self._dict_rows(conn.execute(
                    'SELECT * FROM positions WHERE ts_code=? AND status="holding" AND available_date<=?',
                    (ts_code, trade_date)
                ).fetchall())

            if not eligible_positions:
                return {'signal_id': signal['id'], 'success': False,
                        'message': '没有 %s 的可卖出持仓（需满足A股T+1：买入次日可卖）' % ts_code}

            total_available = sum(p['quantity'] for p in eligible_positions)
            if signal['quantity']:
                sell_qty = min(signal['quantity'], total_available)
            elif signal['target_ratio'] is not None:
                # target_ratio=0 表示清仓
                total_assets = self._calc_total_assets(trade_date)
                target_value = total_assets * signal['target_ratio']
                current_value = total_available * open_price
                sell_value = current_value - target_value
                sell_qty = int(sell_value / open_price / 100) * 100
            else:
                sell_qty = total_available

            sell_qty = int(sell_qty / 100) * 100
            if sell_qty <= 0:
                return {'signal_id': signal['id'], 'success': False,
                        'message': '卖出数量为0'}

            amount = sell_qty * open_price
            commission = max(amount * PORTFOLIO_COMMISSION_RATE, 5)
            stamp_tax = amount * PORTFOLIO_STAMP_TAX_RATE
            net_proceeds = amount - commission - stamp_tax

            with self._lock:
                with self._connect() as conn:
                    # 按FIFO减少持仓
                    remaining = sell_qty
                    for pos in eligible_positions:
                        if remaining <= 0:
                            break
                        if pos['quantity'] <= remaining:
                            conn.execute('DELETE FROM positions WHERE id=?', (pos['id'],))
                            remaining -= pos['quantity']
                        else:
                            conn.execute('UPDATE positions SET quantity=? WHERE id=?',
                                         (pos['quantity'] - remaining, pos['id']))
                            remaining = 0

                    # 写入交易记录
                    conn.execute('''INSERT INTO trade_history
                        (ts_code, name, direction, price, quantity, amount,
                         commission, stamp_tax, reason, proposed_by, approved_by,
                         trade_date, status, created_at)
                        VALUES (?, ?, 'sell', ?, ?, ?, ?, ?, ?, ?, 'director', ?, 'executed', ?)''',
                        (ts_code, signal['name'], open_price, sell_qty, amount,
                         commission, stamp_tax, signal['reason'], signal['proposed_by'],
                         trade_date, time.time()))
                    # 增加现金
                    conn.execute('UPDATE portfolio_config SET current_cash=current_cash+?, updated_at=? WHERE id=1',
                                 (net_proceeds, time.time()))
                    conn.execute('''UPDATE trade_signals SET
                        status="executed",
                        execute_price=?,
                        execute_qty=?,
                        execute_amount=?,
                        execute_commission=?,
                        execute_stamp_tax=?,
                        execute_cost=?,
                        execute_price_source=?,
                        executed_trade_date=?,
                        executed_at=?,
                        execute_message=?
                        WHERE id=?''',
                        (open_price, sell_qty, amount, commission, stamp_tax, commission + stamp_tax,
                         price_source, trade_date, time.time(),
                         '卖出 %s %d股 @ %.2f, 佣金%.2f, 印花税%.2f (价格来源: %s)' % (
                             ts_code, sell_qty, open_price, commission, stamp_tax, price_source),
                         signal['id']))

            return {'signal_id': signal['id'], 'success': True,
                    'message': '卖出 %s %d股 @ %.2f, 佣金%.2f, 印花税%.2f, 价格来源:%s' % (
                        ts_code, sell_qty, open_price, commission, stamp_tax, price_source)}

    # ━━━━━━━━━━━━━━━━ 持仓查询 ━━━━━━━━━━━━━━━━

    def get_positions(self):
        with self._connect() as conn:
            rows = conn.execute(
                'SELECT * FROM positions WHERE status="holding" ORDER BY buy_date'
            ).fetchall()
            return self._dict_rows(rows)

    def get_portfolio_status(self):
        """获取组合当前状态（供智能体和前端使用）"""
        config = self.get_config()
        if not config:
            return {'initialized': False}

        positions = self.get_positions()
        # 聚合同一股票的持仓
        holdings = {}
        for p in positions:
            code = p['ts_code']
            if code not in holdings:
                holdings[code] = {
                    'ts_code': code, 'name': p['name'],
                    'total_quantity': 0, 'total_cost': 0,
                    'earliest_buy': p['buy_date'],
                }
            holdings[code]['total_quantity'] += p['quantity']
            holdings[code]['total_cost'] += p['quantity'] * p['cost_price']

        # 计算每只股票的最新市值（用最近的收盘价估算）
        holding_list = []
        total_market_value = 0
        for code, h in holdings.items():
            avg_cost = h['total_cost'] / h['total_quantity'] if h['total_quantity'] else 0
            latest_price = self._get_latest_price(code)
            market_value = h['total_quantity'] * latest_price if latest_price else h['total_cost']
            pnl = market_value - h['total_cost']
            pnl_pct = pnl / h['total_cost'] if h['total_cost'] else 0
            total_market_value += market_value
            holding_list.append({
                'ts_code': code, 'name': h['name'],
                'quantity': h['total_quantity'],
                'avg_cost': round(avg_cost, 3),
                'latest_price': latest_price,
                'market_value': round(market_value, 2),
                'pnl': round(pnl, 2),
                'pnl_pct': round(pnl_pct * 100, 2),
                'weight': 0,  # 后面算
            })

        total_assets = config['current_cash'] + total_market_value
        nav = total_assets / config['initial_capital']

        # 计算权重
        for h in holding_list:
            h['weight'] = round(h['market_value'] / total_assets * 100, 2) if total_assets else 0

        # 最新日收益
        with self._connect() as conn:
            last_nav = conn.execute(
                'SELECT * FROM daily_nav ORDER BY trade_date DESC LIMIT 1'
            ).fetchone()

        daily_return = 0
        if last_nav:
            last_nav = dict(last_nav)
            daily_return = last_nav.get('daily_return', 0)

        return {
            'initialized': True,
            'mode': config['mode'],
            'target_code': config['target_code'],
            'initial_capital': config['initial_capital'],
            'current_cash': round(config['current_cash'], 2),
            'market_value': round(total_market_value, 2),
            'total_assets': round(total_assets, 2),
            'nav': round(nav, 4),
            'cumulative_return': round((nav - 1) * 100, 2),
            'daily_return': round(daily_return * 100, 2),
            'holdings': holding_list,
            'holdings_count': len(holding_list),
            'status': config['status'],
            'auto_run': config['auto_run'],
            'start_date': config['start_date'],
        }

    # ━━━━━━━━━━━━━━━━ 每日结算 ━━━━━━━━━━━━━━━━

    def daily_settlement(self, trade_date):
        """每日收盘后结算"""
        config = self.get_config()
        if not config:
            return {'success': False, 'message': '组合未初始化'}

        # 检查是否已结算
        with self._connect() as conn:
            existing = conn.execute(
                'SELECT id FROM daily_nav WHERE trade_date=?', (trade_date,)
            ).fetchone()
            if existing:
                return {'success': False, 'message': '%s 已结算' % trade_date}

        # 计算持仓市值
        positions = self.get_positions()
        market_value = 0
        snapshot = []
        for p in positions:
            close_price = self._get_close_price(p['ts_code'], trade_date)
            if close_price:
                mv = p['quantity'] * close_price
                market_value += mv
                snapshot.append({
                    'ts_code': p['ts_code'], 'name': p['name'],
                    'quantity': p['quantity'], 'cost': p['cost_price'],
                    'close': close_price, 'value': round(mv, 2)
                })
            else:
                # 无法获取价格，用成本价
                mv = p['quantity'] * p['cost_price']
                market_value += mv
                snapshot.append({
                    'ts_code': p['ts_code'], 'name': p['name'],
                    'quantity': p['quantity'], 'cost': p['cost_price'],
                    'close': p['cost_price'], 'value': round(mv, 2)
                })

        # 将薪资成本计入当日净值，而不是延迟到下一日
        daily_salary_total = PORTFOLIO_DAILY_SALARY * len(ALL_AGENTS)
        cash_before = config['current_cash']
        cash_after = cash_before - daily_salary_total
        total_assets = cash_after + market_value
        nav = total_assets / config['initial_capital']

        # 日收益率
        with self._connect() as conn:
            prev_nav = conn.execute(
                'SELECT nav FROM daily_nav ORDER BY trade_date DESC LIMIT 1'
            ).fetchone()
        prev_nav_val = prev_nav['nav'] if prev_nav else 1.0
        daily_return = (nav - prev_nav_val) / prev_nav_val if prev_nav_val else 0

        # 基准净值
        benchmark_nav, benchmark_return = self._calc_benchmark(trade_date, config)

        # 最大回撤
        max_drawdown = self._calc_max_drawdown(nav)

        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    'UPDATE portfolio_config SET current_cash=?, updated_at=? WHERE id=1',
                    (cash_after, time.time())
                )
                # 记录工资
                for agent_id in ALL_AGENTS:
                    conn.execute('''INSERT INTO agent_compensation
                        (agent_id, date, type, amount, reason, created_at)
                        VALUES (?, ?, 'salary', ?, '每日工资', ?)''',
                        (agent_id, trade_date, PORTFOLIO_DAILY_SALARY, time.time()))

                # 写入净值
                conn.execute('''INSERT INTO daily_nav
                    (trade_date, total_assets, cash, market_value, nav,
                     benchmark_nav, daily_return, benchmark_return,
                     max_drawdown, positions_snapshot, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (trade_date, total_assets, cash_after, market_value, nav,
                     benchmark_nav, daily_return, benchmark_return,
                     max_drawdown, json.dumps(snapshot, ensure_ascii=False),
                     time.time()))

        return {
            'success': True, 'trade_date': trade_date,
            'total_assets': round(total_assets, 2),
            'nav': round(nav, 4),
            'daily_return': round(daily_return * 100, 2),
            'benchmark_nav': round(benchmark_nav, 4),
            'max_drawdown': round(max_drawdown * 100, 2),
            'salary_paid': daily_salary_total,
        }

    def backfill_nav(self, from_date, to_date):
        """补算缺失交易日的净值"""
        trade_dates = self._get_trade_dates(from_date, to_date)
        filled = 0
        for td in trade_dates:
            with self._connect() as conn:
                existing = conn.execute(
                    'SELECT id FROM daily_nav WHERE trade_date=?', (td,)
                ).fetchone()
            if not existing:
                result = self.daily_settlement(td)
                if result.get('success'):
                    filled += 1
        return filled

    # ━━━━━━━━━━━━━━━━ 风险预警 ━━━━━━━━━━━━━━━━

    def submit_risk_warning(self, ts_code, risk_type, severity, description, warned_by):
        """提交风险预警"""
        if warned_by not in RISK_GROUP:
            return {'success': False, 'message': '只有风控组成员可提交预警'}

        import datetime
        today = datetime.datetime.now().strftime('%Y%m%d')

        with self._lock:
            with self._connect() as conn:
                conn.execute('''INSERT INTO risk_warnings
                    (ts_code, risk_type, severity, description, warned_by,
                     warn_date, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    (ts_code, risk_type, severity, description, warned_by,
                     today, time.time()))
                warning_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]

        return {'success': True, 'warning_id': warning_id,
                'message': '风险预警已记录 (ID: %d)' % warning_id}

    def verify_risk_warnings(self, current_date):
        """验证到期的风险预警，发放奖励"""
        with self._connect() as conn:
            warnings = conn.execute(
                'SELECT * FROM risk_warnings WHERE verified=0'
            ).fetchall()

        for w in warnings:
            w = dict(w)
            warn_date = w['warn_date']
            trade_dates = self._get_trade_dates(warn_date, current_date)
            if len(trade_dates) < PORTFOLIO_RISK_WARNING_VERIFY_DAYS:
                continue  # 还没到验证期

            # 检查价格变动
            warn_price = self._get_close_price(w['ts_code'], warn_date)
            verify_date = trade_dates[PORTFOLIO_RISK_WARNING_VERIFY_DAYS - 1]
            current_price = self._get_close_price(w['ts_code'], verify_date)

            verified = False
            if warn_price and current_price:
                drop = (warn_price - current_price) / warn_price
                if drop >= PORTFOLIO_RISK_WARNING_VERIFY_DROP:
                    verified = True

            with self._lock:
                with self._connect() as conn:
                    conn.execute(
                        'UPDATE risk_warnings SET verified=?, verified_date=? WHERE id=?',
                        (1 if verified else -1, verify_date, w['id'])
                    )
                    if verified:
                        conn.execute('''INSERT INTO agent_compensation
                            (agent_id, date, type, amount, reason, created_at)
                            VALUES (?, ?, 'bonus', ?, ?, ?)''',
                            (w['warned_by'], verify_date, PORTFOLIO_RISK_WARNING_REWARD,
                             '成功预警 %s (%s)' % (w['ts_code'], w['risk_type']),
                             time.time()))
                        conn.execute(
                            'UPDATE risk_warnings SET reward_paid=1 WHERE id=?',
                            (w['id'],)
                        )

    # ━━━━━━━━━━━━━━━━ 奖惩计算 ━━━━━━━━━━━━━━━━

    def calculate_monthly_compensation(self, year_month):
        """
        月度结算奖惩。year_month 格式: '202603'

        策略组：当月盈利 × 20% ÷ 3
        风控组：回撤惩罚检查
        总监：累计收益 - 累计工资
        """
        month_start = year_month + '01'
        month_end = year_month + '31'

        with self._connect() as conn:
            # 月初和月末净值
            first_nav = conn.execute(
                'SELECT nav FROM daily_nav WHERE trade_date>=? ORDER BY trade_date ASC LIMIT 1',
                (month_start,)
            ).fetchone()
            last_nav = conn.execute(
                'SELECT nav FROM daily_nav WHERE trade_date<=? ORDER BY trade_date DESC LIMIT 1',
                (month_end,)
            ).fetchone()

        if not first_nav or not last_nav:
            return {'message': '该月无净值数据'}

        # 防止同一月份奖惩重复发放（幂等）
        with self._connect() as conn:
            existing = conn.execute(
                'SELECT COUNT(*) FROM agent_compensation '
                'WHERE date=? AND ('
                '(type="bonus" AND reason LIKE "当月盈利%") OR '
                '(type="penalty" AND reason LIKE "个股%亏损%")'
                ')',
                (month_end,)
            ).fetchone()
        if existing and existing[0] > 0:
            return {'message': '该月奖惩已结算', 'month': year_month}

        config = self.get_config()
        month_return = (last_nav['nav'] - first_nav['nav']) / first_nav['nav']
        month_profit = month_return * config['initial_capital']

        results = {}

        # 策略组奖金
        if month_profit > 0:
            bonus_pool = month_profit * PORTFOLIO_STRATEGY_BONUS_RATE
            per_agent = bonus_pool / len(STRATEGY_GROUP)
            for agent_id in STRATEGY_GROUP:
                with self._lock:
                    with self._connect() as conn:
                        conn.execute('''INSERT INTO agent_compensation
                            (agent_id, date, type, amount, reason, created_at)
                            VALUES (?, ?, 'bonus', ?, ?, ?)''',
                            (agent_id, month_end, per_agent,
                             '当月盈利%.2f的20%%分成' % month_profit, time.time()))
                results[agent_id] = {'bonus': round(per_agent, 2)}

        # 检查策略组惩罚（个股亏损）
        with self._connect() as conn:
            trades = conn.execute(
                'SELECT * FROM trade_history WHERE trade_date>=? AND trade_date<=? AND direction="sell"',
                (month_start, month_end)
            ).fetchall()

        for t in trades:
            t = dict(t)
            # 查该股票的买入成本
            buy_trades = []
            with self._connect() as conn:
                buy_trades = conn.execute(
                    'SELECT * FROM trade_history WHERE ts_code=? AND direction="buy" AND trade_date<=?',
                    (t['ts_code'], t['trade_date'])
                ).fetchall()
            if buy_trades:
                avg_buy = sum(b['price'] * b['quantity'] for b in buy_trades) / sum(b['quantity'] for b in buy_trades)
                loss_pct = (t['price'] - avg_buy) / avg_buy
                if loss_pct < -PORTFOLIO_STOP_LOSS:
                    penalty_reason = '个股%s亏损%.1f%%' % (t['ts_code'], loss_pct * 100)
                    penalty_rate = 1.0 if loss_pct < -PORTFOLIO_SEVERE_LOSS else 0.5
                    proposer = t.get('proposed_by')
                    penalty_agents = [proposer] if proposer in STRATEGY_GROUP else list(STRATEGY_GROUP)
                    for agent_id in penalty_agents:
                        bonus = results.get(agent_id, {}).get('bonus', 0)
                        penalty = bonus * penalty_rate
                        if penalty > 0:
                            with self._lock:
                                with self._connect() as conn:
                                    conn.execute('''INSERT INTO agent_compensation
                                        (agent_id, date, type, amount, reason, related_trade_id, created_at)
                                        VALUES (?, ?, 'penalty', ?, ?, ?, ?)''',
                                        (agent_id, month_end, penalty,
                                         penalty_reason, t['id'], time.time()))

        return {'month': year_month, 'month_return': round(month_return * 100, 2),
                'month_profit': round(month_profit, 2), 'details': results}

    # ━━━━━━━━━━━━━━━━ 数据查询 ━━━━━━━━━━━━━━━━

    def get_nav_history(self, days=None):
        with self._connect() as conn:
            if days:
                rows = conn.execute(
                    'SELECT * FROM daily_nav ORDER BY trade_date DESC LIMIT ?', (days,)
                ).fetchall()
            else:
                rows = conn.execute(
                    'SELECT * FROM daily_nav ORDER BY trade_date'
                ).fetchall()
            result = self._dict_rows(rows)
            if days:
                result.reverse()
            return result

    def get_market_data_status(self):
        """返回行情库同步状态（用于前端提示数据库是否更新）。"""
        try:
            from AlphaFin.config import DB_ROOT
            import sqlite3 as _sqlite3
            db_path = '%s/daily_kline.db' % DB_ROOT
            conn2 = _sqlite3.connect(db_path)
            max_row = conn2.execute(
                'SELECT MAX(trade_date) FROM daily_kline'
            ).fetchone()
            today = datetime.datetime.now().strftime('%Y%m%d')
            today_row = conn2.execute(
                'SELECT COUNT(*) FROM daily_kline WHERE trade_date=? LIMIT 1',
                (today,)
            ).fetchone()
            conn2.close()
            max_trade_date = max_row[0] if max_row else None
            has_today = bool(today_row and today_row[0] and today_row[0] > 0)
            return {
                'daily_kline_max_trade_date': max_trade_date,
                'daily_kline_has_today': has_today,
            }
        except Exception:
            return {
                'daily_kline_max_trade_date': None,
                'daily_kline_has_today': False,
            }

    def get_trade_history(self, limit=50):
        with self._connect() as conn:
            rows = conn.execute(
                'SELECT * FROM trade_history ORDER BY id DESC LIMIT ?', (limit,)
            ).fetchall()
            return self._dict_rows(rows)

    def get_compensation_summary(self):
        """获取所有智能体的薪资汇总"""
        with self._connect() as conn:
            summary = {}
            for agent_id in ALL_AGENTS:
                rows = conn.execute(
                    'SELECT type, SUM(amount) as total FROM agent_compensation '
                    'WHERE agent_id=? GROUP BY type', (agent_id,)
                ).fetchall()
                agent_data = {'salary': 0, 'bonus': 0, 'penalty': 0}
                for r in rows:
                    agent_data[r['type']] = round(r['total'], 2)
                agent_data['net'] = round(
                    agent_data['salary'] + agent_data['bonus'] - agent_data['penalty'], 2
                )
                summary[agent_id] = agent_data
            return summary

    def get_performance_stats(self):
        """计算绩效统计"""
        nav_history = self.get_nav_history()
        if len(nav_history) < 2:
            return {'message': '数据不足，至少需要2个交易日'}

        navs = [n['nav'] for n in nav_history]
        returns = [n['daily_return'] for n in nav_history if n['daily_return'] is not None]
        benchmark_navs = [n['benchmark_nav'] for n in nav_history]

        import math
        # 年化收益率
        days = len(navs)
        total_return = navs[-1] / navs[0] - 1 if navs[0] else 0
        ann_return = (1 + total_return) ** (252 / days) - 1 if days > 0 else 0

        # 年化波动率
        if returns:
            avg_ret = sum(returns) / len(returns)
            var = sum((r - avg_ret) ** 2 for r in returns) / len(returns)
            daily_vol = math.sqrt(var)
            ann_vol = daily_vol * math.sqrt(252)
        else:
            ann_vol = 0

        # 夏普比率（无风险利率2%）
        sharpe = (ann_return - 0.02) / ann_vol if ann_vol > 0 else 0

        # 最大回撤
        max_dd = self._calc_max_drawdown_from_list(navs)

        # 胜率
        win_days = sum(1 for r in returns if r > 0)
        win_rate = win_days / len(returns) if returns else 0

        # 基准年化收益
        bench_return = benchmark_navs[-1] / benchmark_navs[0] - 1 if benchmark_navs[0] else 0
        bench_ann = (1 + bench_return) ** (252 / days) - 1 if days > 0 else 0

        # 超额收益
        excess = ann_return - bench_ann

        return {
            'trading_days': days,
            'total_return': round(total_return * 100, 2),
            'annualized_return': round(ann_return * 100, 2),
            'annualized_volatility': round(ann_vol * 100, 2),
            'sharpe_ratio': round(sharpe, 2),
            'max_drawdown': round(max_dd * 100, 2),
            'win_rate': round(win_rate * 100, 1),
            'benchmark_return': round(bench_return * 100, 2),
            'excess_return': round(excess * 100, 2),
        }

    # ━━━━━━━━━━━━━━━━ 内部辅助 ━━━━━━━━━━━━━━━━

    def _get_close_price(self, ts_code, trade_date):
        """从数据库获取收盘价"""
        try:
            from AlphaFin.config import DB_ROOT
            import sqlite3 as _sqlite3
            db_path = '%s/daily_kline.db' % DB_ROOT
            conn2 = _sqlite3.connect(db_path)
            row = conn2.execute(
                'SELECT close FROM daily_kline WHERE ts_code=? AND trade_date=?',
                (ts_code, trade_date)
            ).fetchone()
            conn2.close()
            return row[0] if row else None
        except Exception:
            return None

    def _get_open_price(self, ts_code, trade_date, with_source=False):
        """获取成交参考价（优先开盘价，缺失时兜底）。"""
        def _ret(price, source):
            return (price, source) if with_source else price

        try:
            from AlphaFin.config import DB_ROOT
            import sqlite3 as _sqlite3
            db_path = '%s/daily_kline.db' % DB_ROOT
            conn2 = _sqlite3.connect(db_path)
            row = conn2.execute(
                'SELECT open FROM daily_kline WHERE ts_code=? AND trade_date=?',
                (ts_code, trade_date)
            ).fetchone()
            conn2.close()
            if row and row[0] is not None:
                return _ret(row[0], 'daily_kline_open')
        except Exception:
            pass

        # 兜底1：尝试直接从 Tushare 拉取当日开盘价
        try:
            from AlphaFin.services.stock_service import pro
            df = pro.daily(
                ts_code=ts_code,
                start_date=trade_date,
                end_date=trade_date,
                fields='trade_date,open'
            )
            if df is not None and len(df) > 0:
                return _ret(float(df.iloc[0]['open']), 'tushare_daily_open')
        except Exception:
            pass

        # 兜底2：若是当日盘中，尝试分钟线最新价（Tushare doc_id=374，rt_min/stk_mins）
        try:
            today = datetime.datetime.now().strftime('%Y%m%d')
            if trade_date == today:
                from AlphaFin.ai_team.services.tushare_watch_service import fetch_intraday_stock_price
                rt_price = fetch_intraday_stock_price(ts_code)
                if rt_price:
                    return _ret(float(rt_price), 'tushare_rt_min')
        except Exception:
            pass

        # 兜底3：使用最近可得收盘价避免交易长期卡住
        latest_price = self._get_latest_price(ts_code)
        if latest_price is not None:
            return _ret(latest_price, 'latest_close_fallback')
        return _ret(None, '')

    def _get_latest_price(self, ts_code):
        """获取最新收盘价"""
        try:
            from AlphaFin.config import DB_ROOT
            import sqlite3 as _sqlite3
            db_path = '%s/daily_kline.db' % DB_ROOT
            conn2 = _sqlite3.connect(db_path)
            row = conn2.execute(
                'SELECT close FROM daily_kline WHERE ts_code=? ORDER BY trade_date DESC LIMIT 1',
                (ts_code,)
            ).fetchone()
            conn2.close()
            return row[0] if row else None
        except Exception:
            return None

    def _get_trade_dates(self, start_date, end_date):
        """获取交易日列表"""
        try:
            from AlphaFin.config import DB_ROOT
            import sqlite3 as _sqlite3
            db_path = '%s/daily_kline.db' % DB_ROOT
            conn2 = _sqlite3.connect(db_path)
            rows = conn2.execute(
                'SELECT DISTINCT trade_date FROM daily_kline '
                'WHERE trade_date>=? AND trade_date<=? ORDER BY trade_date',
                (start_date, end_date)
            ).fetchall()
            conn2.close()
            return [r[0] for r in rows]
        except Exception:
            return []

    def _get_next_trade_date(self, current_date):
        """获取下一个交易日"""
        try:
            from AlphaFin.config import DB_ROOT
            import sqlite3 as _sqlite3
            db_path = '%s/daily_kline.db' % DB_ROOT
            conn2 = _sqlite3.connect(db_path)
            row = conn2.execute(
                'SELECT DISTINCT trade_date FROM daily_kline '
                'WHERE trade_date>? ORDER BY trade_date LIMIT 1',
                (current_date,)
            ).fetchone()
            conn2.close()
            if row and row[0]:
                return row[0]
        except Exception:
            pass

        # 兜底：数据库尚未更新时，按自然周跳到下一个工作日
        try:
            import datetime as _dt
            d = _dt.datetime.strptime(current_date, '%Y%m%d')
            for _ in range(10):
                d = d + _dt.timedelta(days=1)
                if d.weekday() < 5:
                    return d.strftime('%Y%m%d')
        except Exception:
            pass
        return current_date

    def _is_trade_date(self, date_str):
        """判断是否为交易日"""
        try:
            from AlphaFin.config import DB_ROOT
            import sqlite3 as _sqlite3
            db_path = '%s/daily_kline.db' % DB_ROOT
            conn2 = _sqlite3.connect(db_path)
            row = conn2.execute(
                'SELECT COUNT(*) FROM daily_kline WHERE trade_date=? LIMIT 1',
                (date_str,)
            ).fetchone()
            conn2.close()
            if row and row[0] > 0:
                return True
        except Exception:
            pass

        # 兜底：若今天数据尚未入库，按工作日判定，避免调度停摆
        try:
            import datetime as _dt
            today = _dt.datetime.now().strftime('%Y%m%d')
            if date_str == today:
                d = _dt.datetime.strptime(date_str, '%Y%m%d')
                return d.weekday() < 5
        except Exception:
            pass
        return False

    def _calc_total_assets(self, trade_date):
        """计算指定日期的总资产"""
        config = self.get_config()
        cash = config['current_cash']
        positions = self.get_positions()
        market_value = 0
        for p in positions:
            price = self._get_close_price(p['ts_code'], trade_date)
            if price:
                market_value += p['quantity'] * price
            else:
                market_value += p['quantity'] * p['cost_price']
        return cash + market_value

    def _calc_benchmark(self, trade_date, config):
        """计算基准净值"""
        if config['mode'] == 'target' and config['target_code']:
            benchmark_code = config['target_code']
        else:
            benchmark_code = PORTFOLIO_BENCHMARK_CODE

        start_date = config['start_date']
        start_price = self._get_close_price(benchmark_code, start_date)
        current_price = self._get_close_price(benchmark_code, trade_date)

        if not start_price:
            # 如果是指数，尝试index表
            start_price = self._get_index_close(benchmark_code, start_date)
            current_price = self._get_index_close(benchmark_code, trade_date)

        if start_price and current_price:
            benchmark_nav = current_price / start_price
            # 计算基准日收益
            prev_dates = self._get_trade_dates(start_date, trade_date)
            if len(prev_dates) >= 2:
                prev_date = prev_dates[-2]
                prev_price = self._get_close_price(benchmark_code, prev_date)
                if not prev_price:
                    prev_price = self._get_index_close(benchmark_code, prev_date)
                benchmark_return = (current_price - prev_price) / prev_price if prev_price else 0
            else:
                benchmark_return = 0
            return benchmark_nav, benchmark_return

        return 1.0, 0

    def _get_index_close(self, ts_code, trade_date):
        """从Tushare获取指数收盘价"""
        try:
            from AlphaFin.indicators.shared_utils import pro
            df = pro.index_daily(ts_code=ts_code, start_date=trade_date, end_date=trade_date)
            if df is not None and len(df) > 0:
                return float(df.iloc[0]['close'])
        except Exception:
            pass
        return None

    def _calc_max_drawdown(self, current_nav):
        """计算到当前为止的最大回撤"""
        with self._connect() as conn:
            rows = conn.execute(
                'SELECT nav FROM daily_nav ORDER BY trade_date'
            ).fetchall()
        navs = [r['nav'] for r in rows] + [current_nav]
        return self._calc_max_drawdown_from_list(navs)

    def _calc_max_drawdown_from_list(self, navs):
        if not navs:
            return 0
        peak = navs[0]
        max_dd = 0
        for n in navs:
            if n > peak:
                peak = n
            dd = (peak - n) / peak if peak else 0
            if dd > max_dd:
                max_dd = dd
        return max_dd


# 全局单例
portfolio_manager = PortfolioManager()
