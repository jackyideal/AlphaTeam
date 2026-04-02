"""
AI 工具页 - 盘后复盘聚合服务
目标：轻量、快速、可解释、可回溯
"""
import copy
import datetime
import math
import os
import re
import sqlite3
import threading
import time
from collections import defaultdict, deque

from AlphaFin.config import DB_ROOT

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


_CACHE_LOCK = threading.Lock()
_SNAPSHOT_CACHE = {
    'key': '',
    'at': 0.0,
    'payload': None,
}
_CACHE_TTL_SECONDS = 18

_NAME_CACHE = {
    'loaded_at': 0.0,
    'code_to_name': {},
}
_NAME_CACHE_TTL = 6 * 3600

_HISTORY_LOCK = threading.Lock()
_HISTORY_MAX = 120
_HISTORY = {
    'index_pct': defaultdict(lambda: deque(maxlen=_HISTORY_MAX)),
    'stock_pct': defaultdict(lambda: deque(maxlen=_HISTORY_MAX)),
    'breadth_up_ratio': deque(maxlen=_HISTORY_MAX),
}

_DB_INDEX_READY = False
_DB_INDEX_LOCK = threading.Lock()


def _now_cn():
    if ZoneInfo:
        return datetime.datetime.now(ZoneInfo('Asia/Shanghai'))
    return datetime.datetime.now()


def _ensure_post_close_indexes():
    global _DB_INDEX_READY
    if _DB_INDEX_READY:
        return
    with _DB_INDEX_LOCK:
        if _DB_INDEX_READY:
            return
        # 为复盘查询添加必要索引，避免全表扫描导致页面卡顿。
        kline_db = os.path.join(DB_ROOT, 'daily_kline.db')
        if os.path.isfile(kline_db):
            conn = None
            try:
                conn = sqlite3.connect(kline_db)
                conn.execute(
                    'CREATE INDEX IF NOT EXISTS ix_daily_kline_trade_date '
                    'ON daily_kline(trade_date)'
                )
                conn.execute(
                    'CREATE INDEX IF NOT EXISTS ix_daily_kline_ts_code_trade_date '
                    'ON daily_kline(ts_code, trade_date)'
                )
                conn.commit()
            except Exception:
                pass
            finally:
                if conn is not None:
                    conn.close()

        basic_db = os.path.join(DB_ROOT, 'dailybasic.db')
        if os.path.isfile(basic_db):
            conn = None
            try:
                conn = sqlite3.connect(basic_db)
                conn.execute('CREATE INDEX IF NOT EXISTS ix_dailybasic_trade_date ON dailybasic(trade_date)')
                conn.execute('CREATE INDEX IF NOT EXISTS ix_sw_trade_date ON sw(trade_date)')
                conn.execute('CREATE INDEX IF NOT EXISTS ix_moneyflow_trade_date ON moneyflow(trade_date)')
                conn.execute('CREATE INDEX IF NOT EXISTS ix_margin_trade_date ON margin(trade_date)')
                conn.execute('CREATE INDEX IF NOT EXISTS ix_hk_hold_trade_date ON hk_hold(trade_date)')
                conn.commit()
            except Exception:
                pass
            finally:
                if conn is not None:
                    conn.close()
        _DB_INDEX_READY = True


def _safe_float(value, digits=None):
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(n):
        return None
    if digits is None:
        return n
    return round(n, int(digits))


def _sanitize_json_value(value):
    # 递归清洗 NaN/Infinity，确保返回严格 JSON。
    if isinstance(value, dict):
        return {str(k): _sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_value(v) for v in value]
    if isinstance(value, tuple):
        return [_sanitize_json_value(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if hasattr(value, 'item'):
        try:
            return _sanitize_json_value(value.item())
        except Exception:
            return None
    return value


def _normalize_ts_code(raw):
    code = str(raw or '').strip().upper()
    if not code:
        return ''
    if re.fullmatch(r'\d{6}', code):
        return code + ('.SH' if code.startswith(('5', '6', '9')) else '.SZ')
    if re.fullmatch(r'\d{6}\.(SH|SZ)', code):
        return code
    return ''


def _parse_watchlist(raw, max_items=8):
    text = str(raw or '')
    if not text.strip():
        return []
    parts = re.split(r'[\s,，;；|]+', text.strip())
    codes = []
    for p in parts:
        code = _normalize_ts_code(p)
        if not code:
            continue
        if code not in codes:
            codes.append(code)
        if len(codes) >= max(1, int(max_items)):
            break
    return codes


def _load_stock_name_map():
    now = time.time()
    if _NAME_CACHE['code_to_name'] and (now - _NAME_CACHE['loaded_at'] < _NAME_CACHE_TTL):
        return _NAME_CACHE['code_to_name']

    code_to_name = {}
    csv_path = os.path.join(DB_ROOT, 'data', 'nameschange.csv')
    if os.path.isfile(csv_path):
        try:
            import csv
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = _normalize_ts_code(row.get('ts_code'))
                    name = str(row.get('name') or '').strip()
                    end_date = str(row.get('end_date') or '').strip().lower()
                    active = (not end_date) or end_date in ('none', 'null', 'nan')
                    if not code or not name:
                        continue
                    old = code_to_name.get(code)
                    if old is None:
                        code_to_name[code] = {'name': name, 'active': active}
                    elif active and not old.get('active'):
                        code_to_name[code] = {'name': name, 'active': True}
        except Exception:
            pass

    flat = {}
    for code, item in code_to_name.items():
        flat[code] = item.get('name') or ''

    _NAME_CACHE['code_to_name'] = flat
    _NAME_CACHE['loaded_at'] = now
    return flat


def _get_stock_name(ts_code):
    mapping = _load_stock_name_map()
    return mapping.get(_normalize_ts_code(ts_code), '')


def _query_latest_close(ts_code):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return {}
    code = _normalize_ts_code(ts_code)
    if not code:
        return {}
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            'SELECT trade_date, close FROM daily_kline '
            'WHERE ts_code=? ORDER BY trade_date DESC LIMIT 1',
            (code,)
        ).fetchone()
        if not row:
            return {}
        return {
            'trade_date': str(row[0] or ''),
            'close': _safe_float(row[1]),
        }
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


def _query_latest_trade_date_local():
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return ''
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute('SELECT MAX(trade_date) FROM daily_kline').fetchone()
        return str(row[0] or '')
    except Exception:
        return ''
    finally:
        if conn is not None:
            conn.close()


def _query_local_active_codes(limit=6, trade_date=''):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        td = str(trade_date or '').strip()
        if td:
            row = conn.execute(
                'SELECT MAX(trade_date) FROM daily_kline WHERE trade_date<=?',
                (td,)
            ).fetchone()
            td = str((row or [''])[0] or '')
        else:
            td = _query_latest_trade_date_local()
        if not td:
            return []
        rows = conn.execute(
            'SELECT ts_code FROM daily_kline '
            'WHERE trade_date=? ORDER BY COALESCE(amount,0) DESC LIMIT ?',
            (td, max(1, int(limit)))
        ).fetchall()
        codes = []
        for r in rows:
            code = _normalize_ts_code(r[0])
            if code and code not in codes:
                codes.append(code)
        return codes
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _query_local_hotrank(limit=20, trade_date=''):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        td = str(trade_date or '').strip()
        if td:
            row = conn.execute(
                'SELECT MAX(trade_date) FROM daily_kline WHERE trade_date<=?',
                (td,)
            ).fetchone()
            td = str((row or [''])[0] or '')
        else:
            td = _query_latest_trade_date_local()
        if not td:
            return []
        rows = conn.execute(
            'SELECT ts_code, trade_date, pct_chg, amount '
            'FROM daily_kline WHERE trade_date=? '
            'ORDER BY COALESCE(amount,0) DESC LIMIT ?',
            (td, max(1, int(limit)))
        ).fetchall()
        result = []
        rank = 1
        for ts_code, tdate, pct_chg, amount in rows:
            code = _normalize_ts_code(ts_code)
            if not code:
                continue
            result.append({
                'trade_date': str(tdate or ''),
                'rank_time': (str(tdate or '') + ' 15:00:00').strip(),
                'market': '本地活跃度',
                'ts_code': code,
                'name': _get_stock_name(code),
                'rank': rank,
                'hot': _safe_float(amount, 2),
                'pct_change': _safe_float(pct_chg, 3),
            })
            rank += 1
        return result
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _query_local_sector_heat(limit=12, trade_date=''):
    db_path = os.path.join(DB_ROOT, 'dailybasic.db')
    if not os.path.isfile(db_path):
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        td = str(trade_date or '').strip()
        if td:
            row = conn.execute(
                'SELECT MAX(trade_date) FROM sw WHERE trade_date<=?',
                (td,)
            ).fetchone()
            td = str((row or [''])[0] or '')
        else:
            row = conn.execute('SELECT MAX(trade_date) FROM sw').fetchone()
            td = str((row or [''])[0] or '')
        if not td:
            return []
        rows = conn.execute(
            'SELECT ts_code, name, close, pct_change, amount '
            'FROM sw WHERE trade_date=? '
            'ORDER BY COALESCE(pct_change,0) DESC LIMIT ?',
            (td, max(1, int(limit)))
        ).fetchall()
        result = []
        for ts_code, name, close, pct_change, amount in rows:
            result.append({
                'ts_code': str(ts_code or ''),
                'name': str(name or ''),
                'trade_time': td + ' 15:00:00',
                'close': _safe_float(close),
                'pct_change': _safe_float(pct_change, 3),
                'amount': _safe_float(amount, 2),
                'source': 'sw_local',
            })
        return result
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _query_local_market_proxy_indexes(trade_date=''):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        td = str(trade_date or '').strip()
        if td:
            row = conn.execute(
                'SELECT MAX(trade_date) FROM daily_kline WHERE trade_date<=?',
                (td,)
            ).fetchone()
            td = str((row or [''])[0] or '')
        else:
            td = _query_latest_trade_date_local()
        if not td:
            return []

        def _avg_pct(where_sql, args):
            row = conn.execute(
                'SELECT AVG(COALESCE(pct_chg,0)), AVG(COALESCE(close,0)) '
                'FROM daily_kline WHERE trade_date=? AND ' + where_sql,
                (td,) + tuple(args)
            ).fetchone()
            if not row:
                return None, None
            return _safe_float(row[0], 3), _safe_float(row[1], 2)

        sh_pct, _ = _avg_pct("ts_code LIKE '%.SH'", ())
        sz_pct, _ = _avg_pct("ts_code LIKE '%.SZ'", ())
        cyb_pct, _ = _avg_pct("ts_code LIKE '300%.SZ'", ())
        row = conn.execute(
            'SELECT AVG(COALESCE(pct_chg,0)), AVG(COALESCE(close,0)) FROM ('
            'SELECT pct_chg, close FROM daily_kline WHERE trade_date=? '
            'ORDER BY COALESCE(amount,0) DESC LIMIT 300'
            ')',
            (td,)
        ).fetchone()
        hs300_pct = _safe_float((row or [None, None])[0], 3)

        pairs = [
            ('000001.SH', None, sh_pct),
            ('000300.SH', None, hs300_pct),
            ('399001.SZ', None, sz_pct),
            ('399006.SZ', None, cyb_pct),
        ]
        rows = []
        for code, close_val, pct_val in pairs:
            if pct_val is None:
                continue
            rows.append({
                'ts_code': code,
                'time': td + ' 15:00:00',
                'close': _safe_float(close_val),
                'high': None,
                'low': None,
                'vol': None,
                'amount': None,
                'intraday_pct': _safe_float(pct_val, 3),
                'source': 'daily_kline_proxy',
            })
        return rows
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _fallback_index_rows(index_codes, trade_date=''):
    from AlphaFin.ai_team.services.tushare_watch_service import fetch_intraday_stock_quote

    rows = []
    for code in index_codes or []:
        try:
            quote = fetch_intraday_stock_quote(code, freq='1MIN') or {}
        except Exception:
            quote = {}
        price = _safe_float(quote.get('price'))
        prev_close = _safe_float(quote.get('prev_close'))
        intraday_pct = None
        if price is not None and prev_close not in (None, 0):
            intraday_pct = _safe_float((price - prev_close) / prev_close * 100.0, 3)
        rows.append({
            'ts_code': code,
            'time': str(quote.get('time') or ''),
            'close': price,
            'high': None,
            'low': None,
            'vol': None,
            'amount': None,
            'intraday_pct': intraday_pct,
            'source': str(quote.get('source') or ''),
        })

    # 若公开行情不可用，再降级到本地 proxy（确保图表可视化有数据）。
    proxy = _query_local_market_proxy_indexes(trade_date=trade_date)
    proxy_map = {str(r.get('ts_code') or ''): r for r in proxy}
    filled = []
    for r in rows:
        code = str(r.get('ts_code') or '')
        if r.get('intraday_pct') is None and code in proxy_map:
            filled.append(proxy_map[code])
        elif r.get('intraday_pct') is not None:
            filled.append(r)
        elif code in proxy_map:
            filled.append(proxy_map[code])

    if filled:
        return filled
    return proxy


def _calc_market_breadth(trade_date=''):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return {}

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        td = str(trade_date or '').strip()
        if td:
            row = conn.execute(
                'SELECT MAX(trade_date) FROM daily_kline WHERE trade_date<=?',
                (td,)
            ).fetchone()
            td = str((row or [''])[0] or '')
        else:
            row = conn.execute('SELECT MAX(trade_date) FROM daily_kline').fetchone()
            td = str((row or [''])[0] or '')
        trade_date = td
        if not trade_date:
            return {}

        def _read_for_date(td_):
            return conn.execute(
                'SELECT '
                'COUNT(1) AS total_count, '
                'SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS up_count, '
                'SUM(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) AS down_count, '
                'SUM(CASE WHEN ABS(COALESCE(pct_chg,0)) < 0.0001 THEN 1 ELSE 0 END) AS flat_count, '
                'SUM(CASE WHEN pct_chg >= 9.8 THEN 1 ELSE 0 END) AS limit_up_count, '
                'SUM(CASE WHEN pct_chg <= -9.8 THEN 1 ELSE 0 END) AS limit_down_count, '
                'SUM(COALESCE(amount,0)) AS total_amount '
                'FROM daily_kline WHERE trade_date=?',
                (td_,)
            ).fetchone()

        row = _read_for_date(trade_date)
        if not row:
            return {}
        total_count = int(row[0] or 0)
        if total_count <= 0:
            latest_row = conn.execute('SELECT MAX(trade_date) FROM daily_kline').fetchone()
            latest_td = str((latest_row or [''])[0] or '')
            if latest_td and latest_td != trade_date:
                row = _read_for_date(latest_td)
                trade_date = latest_td
                total_count = int((row or [0])[0] or 0)

        if total_count <= 0:
            return {}

        up_count = int(row[1] or 0)
        down_count = int(row[2] or 0)
        flat_count = int(row[3] or 0)
        limit_up_count = int(row[4] or 0)
        limit_down_count = int(row[5] or 0)
        total_amount = _safe_float(row[6], 2) or 0.0
        up_ratio = (up_count / total_count * 100.0) if total_count > 0 else None
        return {
            'trade_date': trade_date,
            'total_count': total_count,
            'up_count': up_count,
            'down_count': down_count,
            'flat_count': flat_count,
            'limit_up_count': limit_up_count,
            'limit_down_count': limit_down_count,
            'up_ratio': _safe_float(up_ratio, 2),
            'total_amount': total_amount,
        }
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


def _trade_date_to_ts_ms(trade_date):
    s = str(trade_date or '').strip()
    if not re.fullmatch(r'\d{8}', s):
        return None
    try:
        dt = datetime.datetime.strptime(s, '%Y%m%d')
        if ZoneInfo:
            dt = dt.replace(hour=15, minute=0, second=0, tzinfo=ZoneInfo('Asia/Shanghai'))
        else:
            dt = dt.replace(hour=15, minute=0, second=0)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _query_recent_trade_dates(db_path, table_name, end_trade_date='', limit=60):
    if table_name not in ('daily_kline', 'dailybasic', 'moneyflow', 'margin'):
        return []
    if not os.path.isfile(db_path):
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        td = str(end_trade_date or '').strip()
        n = max(5, int(limit))
        if td:
            rows = conn.execute(
                'SELECT DISTINCT trade_date FROM %s WHERE trade_date<=? '
                'ORDER BY trade_date DESC LIMIT ?' % table_name,
                (td, n)
            ).fetchall()
        else:
            rows = conn.execute(
                'SELECT DISTINCT trade_date FROM %s ORDER BY trade_date DESC LIMIT ?' % table_name,
                (n,)
            ).fetchall()
        return [str(r[0]) for r in rows if r and r[0]]
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _query_breadth_series(limit=60, trade_date=''):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return []
    dates = _query_recent_trade_dates(db_path, 'daily_kline', end_trade_date=trade_date, limit=limit)
    if not dates:
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        placeholders = ','.join(['?'] * len(dates))
        sql = (
            'SELECT trade_date, '
            'COUNT(1) AS total_count, '
            'SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS up_count '
            'FROM daily_kline '
            'WHERE trade_date IN (%s) '
            'GROUP BY trade_date'
        ) % placeholders
        rows = conn.execute(sql, tuple(dates)).fetchall()
        result = []
        for tdate, total_count, up_count in rows:
            total_count = int(total_count or 0)
            up_count = int(up_count or 0)
            if total_count <= 0:
                continue
            ratio = _safe_float(up_count / total_count * 100.0, 3)
            ts_ms = _trade_date_to_ts_ms(tdate)
            if ratio is None or ts_ms is None:
                continue
            result.append({
                'ts': ts_ms,
                'time': str(tdate or ''),
                'value': ratio,
            })
        result.sort(key=lambda x: x.get('ts') or 0)
        return result
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _query_proxy_index_series(limit=60, trade_date=''):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return {}
    dates = _query_recent_trade_dates(db_path, 'daily_kline', end_trade_date=trade_date, limit=limit)
    if not dates:
        return {}
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        placeholders = ','.join(['?'] * len(dates))
        sql = (
            'SELECT trade_date, '
            'AVG(CASE WHEN ts_code LIKE \'%%.SH\' THEN pct_chg END) AS sh_pct, '
            'AVG(CASE WHEN ts_code LIKE \'%%.SZ\' THEN pct_chg END) AS sz_pct, '
            'AVG(CASE WHEN ts_code LIKE \'300%%.SZ\' THEN pct_chg END) AS cyb_pct, '
            'AVG(pct_chg) AS all_pct '
            'FROM daily_kline '
            'WHERE trade_date IN (%s) '
            'GROUP BY trade_date'
        ) % placeholders
        rows = conn.execute(sql, tuple(dates)).fetchall()

        out = {
            '000001.SH': [],
            '000300.SH': [],
            '399001.SZ': [],
            '399006.SZ': [],
        }
        for tdate, sh_pct, sz_pct, cyb_pct, all_pct in rows:
            ts_ms = _trade_date_to_ts_ms(tdate)
            if ts_ms is None:
                continue
            mapping = {
                '000001.SH': _safe_float(sh_pct, 3),
                '000300.SH': _safe_float(all_pct, 3),
                '399001.SZ': _safe_float(sz_pct, 3),
                '399006.SZ': _safe_float(cyb_pct, 3),
            }
            for code, value in mapping.items():
                if value is None:
                    continue
                out[code].append({
                    'ts': ts_ms,
                    'time': str(tdate or ''),
                    'value': value,
                })
        for code in list(out.keys()):
            out[code].sort(key=lambda x: x.get('ts') or 0)
            if not out[code]:
                del out[code]
        return out
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


def _query_stock_pct_series(ts_codes, limit=60, trade_date=''):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return {}
    codes = [_normalize_ts_code(c) for c in (ts_codes or [])]
    codes = [c for c in codes if c]
    if not codes:
        return {}

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        td = str(trade_date or '').strip()
        out = {}
        for code in codes[:10]:
            params = [code]
            where_tail = ''
            if td:
                where_tail = ' AND trade_date<=?'
                params.append(td)
            params.append(max(5, int(limit)))
            rows = conn.execute(
                'SELECT trade_date, pct_chg FROM daily_kline '
                'WHERE ts_code=?%s ORDER BY trade_date DESC LIMIT ?' % where_tail,
                tuple(params)
            ).fetchall()
            items = []
            for tdate, pct_chg in rows:
                ts_ms = _trade_date_to_ts_ms(tdate)
                value = _safe_float(pct_chg, 3)
                if ts_ms is None or value is None:
                    continue
                items.append({
                    'ts': ts_ms,
                    'time': str(tdate or ''),
                    'value': value,
                })
            items.sort(key=lambda x: x.get('ts') or 0)
            if items:
                out[code] = items
        return out
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


def _query_latest_trade_date_by_rule(base_date='', inclusive=True):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return ''
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        op = '<=' if inclusive else '<'
        if base_date:
            row = conn.execute(
                'SELECT MAX(trade_date) FROM daily_kline WHERE trade_date %s ?' % op,
                (str(base_date),)
            ).fetchone()
        else:
            row = conn.execute('SELECT MAX(trade_date) FROM daily_kline').fetchone()
        return str((row or [''])[0] or '')
    except Exception:
        return ''
    finally:
        if conn is not None:
            conn.close()


def _resolve_post_close_trade_date():
    """
    盘后复盘日选择规则：
    - 交易日 15:00 前：用上一交易日
    - 交易日 15:00 后：优先当日（若库中无当日，则退回最近交易日）
    - 周末：用最近交易日
    """
    now = _now_cn()
    today = now.strftime('%Y%m%d')
    is_weekday = now.weekday() < 5
    after_close = (now.hour > 15) or (now.hour == 15 and now.minute >= 0)

    if is_weekday and not after_close:
        target = _query_latest_trade_date_by_rule(base_date=today, inclusive=False)
    else:
        target = _query_latest_trade_date_by_rule(base_date=today, inclusive=True)

    if not target:
        target = _query_latest_trade_date_by_rule(base_date='', inclusive=True)

    return {
        'now': now.strftime('%Y-%m-%d %H:%M:%S'),
        'today': today,
        'is_weekday': is_weekday,
        'after_close': after_close,
        'trade_date': target,
        'used_previous': bool(target and target != today),
        'policy': 'today_if_closed_else_prev',
    }


def _query_post_close_index_rows(trade_date):
    # 本地数据库无标准指数日线，采用全市场代理指数用于复盘趋势。
    rows = _query_local_market_proxy_indexes(trade_date=trade_date)
    for r in rows:
        r['source'] = 'daily_kline_proxy'
    return rows


def _query_post_close_active_rows(trade_date, limit=12):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            'SELECT ts_code, close, pct_chg, amount FROM daily_kline '
            'WHERE trade_date=? ORDER BY COALESCE(amount,0) DESC LIMIT ?',
            (str(trade_date), max(1, int(limit)))
        ).fetchall()
        out = []
        for ts_code, close, pct_chg, amount in rows:
            code = _normalize_ts_code(ts_code)
            if not code:
                continue
            out.append({
                'ts_code': code,
                'name': _get_stock_name(code) or code,
                'trade_time': str(trade_date) + ' 15:00:00',
                'close': _safe_float(close),
                'pct_change': _safe_float(pct_chg, 3),
                'amount': _safe_float(amount, 2),
                'source': 'daily_kline_active',
            })
        return out
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _query_post_close_stock_rows(watch_codes, trade_date):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        out = []
        for code in watch_codes[:10]:
            row = conn.execute(
                'SELECT trade_date, close, pre_close, pct_chg, amount FROM daily_kline '
                'WHERE ts_code=? AND trade_date<=? ORDER BY trade_date DESC LIMIT 1',
                (code, str(trade_date))
            ).fetchone()
            if not row:
                continue
            tdate, close, pre_close, pct_chg, amount = row
            out.append({
                'ts_code': code,
                'name': _get_stock_name(code),
                'price': _safe_float(close),
                'quote_time': str(tdate) + ' 15:00:00',
                'source': 'daily_kline_local',
                'last_close': _safe_float(pre_close),
                'last_trade_date': str(tdate or ''),
                'pct_change': _safe_float(pct_chg, 3),
                'amount': _safe_float(amount, 2),
            })
        out.sort(key=lambda x: abs(x.get('pct_change') or 0), reverse=True)
        return out
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _nearest_table_trade_date(table_name, end_trade_date=''):
    db_path = os.path.join(DB_ROOT, 'dailybasic.db')
    if not os.path.isfile(db_path):
        return ''
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        td = str(end_trade_date or '').strip()
        if td:
            row = conn.execute(
                'SELECT MAX(trade_date) FROM %s WHERE trade_date<=?' % table_name,
                (td,)
            ).fetchone()
        else:
            row = conn.execute('SELECT MAX(trade_date) FROM %s' % table_name).fetchone()
        return str((row or [''])[0] or '')
    except Exception:
        return ''
    finally:
        if conn is not None:
            conn.close()


def _query_moneyflow_snapshot(end_trade_date):
    db_path = os.path.join(DB_ROOT, 'dailybasic.db')
    if not os.path.isfile(db_path):
        return {}
    trade_date = _nearest_table_trade_date('moneyflow', end_trade_date)
    if not trade_date:
        return {}
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            'SELECT '
            'SUM(COALESCE(net_mf_amount,0)) AS net_amount, '
            'SUM(COALESCE(buy_lg_amount,0)+COALESCE(buy_elg_amount,0)-COALESCE(sell_lg_amount,0)-COALESCE(sell_elg_amount,0)) AS main_net '
            'FROM moneyflow WHERE trade_date=?',
            (trade_date,)
        ).fetchone()
        return {
            'trade_date': trade_date,
            'net_mf_amount': _safe_float((row or [None, None])[0], 2),
            'main_net_amount': _safe_float((row or [None, None])[1], 2),
            'source': 'moneyflow_local',
        }
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


def _query_moneyflow_series(end_trade_date, limit=60):
    db_path = os.path.join(DB_ROOT, 'dailybasic.db')
    if not os.path.isfile(db_path):
        return []
    dates = _query_recent_trade_dates(db_path, 'moneyflow', end_trade_date=end_trade_date, limit=limit)
    if not dates:
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        placeholders = ','.join(['?'] * len(dates))
        rows = conn.execute(
            'SELECT trade_date, SUM(COALESCE(net_mf_amount,0)) AS net_amount '
            'FROM moneyflow '
            'WHERE trade_date IN (%s) '
            'GROUP BY trade_date' % placeholders,
            tuple(dates)
        ).fetchall()
        out = []
        for tdate, val in rows:
            ts_ms = _trade_date_to_ts_ms(tdate)
            value = _safe_float(val, 2)
            if ts_ms is None or value is None:
                continue
            out.append({'ts': ts_ms, 'time': str(tdate), 'value': value})
        out.sort(key=lambda x: x['ts'])
        return out
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _query_margin_snapshot(end_trade_date):
    db_path = os.path.join(DB_ROOT, 'dailybasic.db')
    if not os.path.isfile(db_path):
        return {}
    trade_date = _nearest_table_trade_date('margin', end_trade_date)
    if not trade_date:
        return {}
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            'SELECT SUM(COALESCE(rzye,0)), SUM(COALESCE(rqye,0)), SUM(COALESCE(rzrqye,0)) '
            'FROM margin WHERE trade_date=?',
            (trade_date,)
        ).fetchone()
        prev_date_row = conn.execute(
            'SELECT MAX(trade_date) FROM margin WHERE trade_date<?',
            (trade_date,)
        ).fetchone()
        prev_date = str((prev_date_row or [''])[0] or '')
        prev_total = None
        if prev_date:
            pr = conn.execute(
                'SELECT SUM(COALESCE(rzrqye,0)) FROM margin WHERE trade_date=?',
                (prev_date,)
            ).fetchone()
            prev_total = _safe_float((pr or [None])[0], 2)
        total = _safe_float((row or [None, None, None])[2], 2)
        delta = None
        if total is not None and prev_total is not None:
            delta = _safe_float(total - prev_total, 2)
        return {
            'trade_date': trade_date,
            'rzye': _safe_float((row or [None, None, None])[0], 2),
            'rqye': _safe_float((row or [None, None, None])[1], 2),
            'rzrqye': total,
            'delta_rzrqye': delta,
            'source': 'margin_local',
        }
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


def _query_margin_series(end_trade_date, limit=60):
    db_path = os.path.join(DB_ROOT, 'dailybasic.db')
    if not os.path.isfile(db_path):
        return []
    dates = _query_recent_trade_dates(db_path, 'margin', end_trade_date=end_trade_date, limit=limit)
    if not dates:
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        placeholders = ','.join(['?'] * len(dates))
        rows = conn.execute(
            'SELECT trade_date, SUM(COALESCE(rzrqye,0)) AS total_balance '
            'FROM margin '
            'WHERE trade_date IN (%s) '
            'GROUP BY trade_date' % placeholders,
            tuple(dates)
        ).fetchall()
        out = []
        for tdate, val in rows:
            ts_ms = _trade_date_to_ts_ms(tdate)
            value = _safe_float(val, 2)
            if ts_ms is None or value is None:
                continue
            out.append({'ts': ts_ms, 'time': str(tdate), 'value': value})
        out.sort(key=lambda x: x['ts'])
        return out
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _std(values):
    nums = []
    for v in values or []:
        n = _safe_float(v)
        if n is not None:
            nums.append(n)
    if len(nums) < 2:
        return None
    mean = sum(nums) / len(nums)
    var = sum((x - mean) ** 2 for x in nums) / (len(nums) - 1)
    if var < 0:
        return None
    return math.sqrt(var)


def _query_market_heat_series(end_trade_date, limit=60):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return []
    dates = _query_recent_trade_dates(db_path, 'daily_kline', end_trade_date=end_trade_date, limit=limit)
    if not dates:
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        placeholders = ','.join(['?'] * len(dates))
        rows = conn.execute(
            'SELECT trade_date, '
            'SUM(CASE WHEN pct_chg > 5 THEN 1 ELSE 0 END) AS up_5, '
            'SUM(CASE WHEN pct_chg > 7 THEN 1 ELSE 0 END) AS up_7, '
            'SUM(CASE WHEN pct_chg > 9.5 THEN 1 ELSE 0 END) AS up_10, '
            'SUM(CASE WHEN pct_chg < -5 THEN 1 ELSE 0 END) AS down_5, '
            'SUM(CASE WHEN pct_chg < -7 THEN 1 ELSE 0 END) AS down_7, '
            'SUM(CASE WHEN pct_chg < -9.5 THEN 1 ELSE 0 END) AS down_10, '
            'COUNT(1) AS total_count '
            'FROM daily_kline '
            'WHERE trade_date IN (%s) '
            'GROUP BY trade_date' % placeholders,
            tuple(dates)
        ).fetchall()
        out = []
        for tdate, up5, up7, up10, down5, down7, down10, total_count in rows:
            total_count = int(total_count or 0)
            if total_count <= 0:
                continue
            score = (
                int(up5 or 0) + int(up7 or 0) + int(up10 or 0)
                - int(down5 or 0) - int(down7 or 0) - int(down10 or 0)
            ) / float(total_count) * 100.0
            ts_ms = _trade_date_to_ts_ms(tdate)
            val = _safe_float(score, 3)
            if ts_ms is None or val is None:
                continue
            out.append({'ts': ts_ms, 'time': str(tdate), 'value': val})
        out.sort(key=lambda x: x['ts'])
        return out
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _query_market_heat_snapshot(end_trade_date):
    series = _query_market_heat_series(end_trade_date, limit=20)
    if not series:
        return {}
    latest = series[-1]
    prev = series[-2] if len(series) >= 2 else {}
    score = _safe_float(latest.get('value'), 2)
    delta = None
    if score is not None and _safe_float(prev.get('value')) is not None:
        delta = _safe_float(score - _safe_float(prev.get('value')), 2)

    phase = '中性'
    if score is not None:
        if score >= 6:
            phase = '偏热'
        elif score >= 2:
            phase = '偏强'
        elif score <= -6:
            phase = '偏冷'
        elif score <= -2:
            phase = '偏弱'
    return {
        'trade_date': str(latest.get('time') or ''),
        'score': score,
        'delta': delta,
        'phase': phase,
        'source': 'ind_24_market_heat',
    }


def _query_valuation_snapshot(end_trade_date):
    db_path = os.path.join(DB_ROOT, 'dailybasic.db')
    if not os.path.isfile(db_path):
        return {}
    trade_date = _nearest_table_trade_date('dailybasic', end_trade_date)
    if not trade_date:
        return {}
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            'SELECT '
            'SUM(CASE WHEN COALESCE(pb,0) > 0 THEN 1 ELSE 0 END) AS pb_valid, '
            'SUM(CASE WHEN COALESCE(pb,0) > 0 AND pb < 1 THEN 1 ELSE 0 END) AS pb_break, '
            'SUM(CASE WHEN COALESCE(pe_ttm,0) > 0 THEN 1 ELSE 0 END) AS pe_valid, '
            'SUM(CASE WHEN COALESCE(pe_ttm,0) > 0 AND pe_ttm < 30 THEN 1 ELSE 0 END) AS pe_low, '
            'AVG(COALESCE(turnover_rate_f, turnover_rate)) AS turnover_avg, '
            'AVG(COALESCE(pb,0)) AS pb_avg, '
            'AVG(CASE WHEN COALESCE(pe_ttm,0) > 0 THEN pe_ttm END) AS pe_avg '
            'FROM dailybasic WHERE trade_date=?',
            (trade_date,)
        ).fetchone()
        pb_valid = int((row or [0])[0] or 0)
        pb_break = int((row or [0, 0])[1] or 0)
        pe_valid = int((row or [0, 0, 0])[2] or 0)
        pe_low = int((row or [0, 0, 0, 0])[3] or 0)
        pb_break_ratio = _safe_float(pb_break / pb_valid * 100.0, 2) if pb_valid > 0 else None
        pe_low_ratio = _safe_float(pe_low / pe_valid * 100.0, 2) if pe_valid > 0 else None
        return {
            'trade_date': trade_date,
            'pb_break_ratio': pb_break_ratio,
            'pe_low_ratio': pe_low_ratio,
            'turnover_avg': _safe_float((row or [None] * 5)[4], 3),
            'pb_avg': _safe_float((row or [None] * 6)[5], 3),
            'pe_avg': _safe_float((row or [None] * 7)[6], 3),
            'source': 'ind_18_valuation',
        }
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


def _query_valuation_series(end_trade_date, limit=60):
    db_path = os.path.join(DB_ROOT, 'dailybasic.db')
    if not os.path.isfile(db_path):
        return []
    dates = _query_recent_trade_dates(db_path, 'dailybasic', end_trade_date=end_trade_date, limit=limit)
    if not dates:
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        placeholders = ','.join(['?'] * len(dates))
        rows = conn.execute(
            'SELECT trade_date, '
            'SUM(CASE WHEN COALESCE(pb,0) > 0 THEN 1 ELSE 0 END) AS pb_valid, '
            'SUM(CASE WHEN COALESCE(pb,0) > 0 AND pb < 1 THEN 1 ELSE 0 END) AS pb_break '
            'FROM dailybasic '
            'WHERE trade_date IN (%s) '
            'GROUP BY trade_date' % placeholders,
            tuple(dates)
        ).fetchall()
        out = []
        for tdate, pb_valid, pb_break in rows:
            pb_valid = int(pb_valid or 0)
            pb_break = int(pb_break or 0)
            if pb_valid <= 0:
                continue
            value = _safe_float(pb_break / pb_valid * 100.0, 3)
            ts_ms = _trade_date_to_ts_ms(tdate)
            if value is None or ts_ms is None:
                continue
            out.append({'ts': ts_ms, 'time': str(tdate), 'value': value})
        out.sort(key=lambda x: x['ts'])
        return out
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _query_proxy_volatility_snapshot(end_trade_date, window=20):
    n = max(5, int(window))
    series_map = _query_proxy_index_series(limit=max(60, n + 20), trade_date=end_trade_date)
    result = {}
    for code in ('000001.SH', '399001.SZ', '399006.SZ', '000300.SH'):
        points = series_map.get(code) or []
        vals = [_safe_float(p.get('value')) for p in points]
        vals = [v for v in vals if v is not None]
        if len(vals) < 5:
            continue
        recent = vals[-n:] if len(vals) >= n else vals
        std = _std(recent)
        if std is None:
            continue
        result[code] = _safe_float(std, 3)
    return {
        'trade_date': str((series_map.get('000001.SH') or [{'time': ''}])[-1].get('time') if series_map.get('000001.SH') else ''),
        'window': n,
        'values': result,
        'source': 'ind_26_market_volatility',
    }


def _build_indicator_panels(target_trade_date, index_rows, sector_rows, breadth, capital_flow, margin, northbound):
    heat = _query_market_heat_snapshot(target_trade_date)
    heat_series = _query_market_heat_series(target_trade_date, limit=60)
    valuation = _query_valuation_snapshot(target_trade_date)
    valuation_series = _query_valuation_series(target_trade_date, limit=60)
    volatility = _query_proxy_volatility_snapshot(target_trade_date, window=20)

    avg_index_pct = None
    idx_vals = [_safe_float(x.get('intraday_pct')) for x in (index_rows or [])]
    idx_vals = [v for v in idx_vals if v is not None]
    if idx_vals:
        avg_index_pct = _safe_float(sum(idx_vals) / len(idx_vals), 3)

    top_sector = (sector_rows or [{}])[0] if sector_rows else {}
    tail_sector = (sector_rows or [{}])[-1] if sector_rows else {}
    top_pct = _safe_float(top_sector.get('pct_change'))
    tail_pct = _safe_float(tail_sector.get('pct_change'))
    spread = None
    if top_pct is not None and tail_pct is not None:
        spread = _safe_float(top_pct - tail_pct, 2)

    panels = {
        'market': [
            {
                'indicator_id': 'ind_24_market_heat',
                'indicator_name': '市场热度指标',
                'metric': '热度扩散值',
                'value': _safe_float(heat.get('score')),
                'unit': '',
                'trade_date': str(heat.get('trade_date') or target_trade_date),
                'description': '以涨跌扩散强弱衡量市场温度，正值偏强、负值偏弱。',
            },
            {
                'indicator_id': 'ind_26_market_volatility',
                'indicator_name': '市场波动率指标',
                'metric': '20日波动率(上证代理)',
                'value': _safe_float((volatility.get('values') or {}).get('000001.SH')),
                'unit': '%',
                'trade_date': str(volatility.get('trade_date') or target_trade_date),
                'description': '基于近20交易日涨跌幅标准差，刻画波动扩张/收敛。',
            },
            {
                'indicator_id': 'ind_24_market_heat',
                'indicator_name': '市场热度指标',
                'metric': '四大指数平均涨跌',
                'value': avg_index_pct,
                'unit': '%',
                'trade_date': target_trade_date,
                'description': '上证/沪深300/深成指/创业板代理日表现均值。',
            },
        ],
        'industry': [
            {
                'indicator_id': 'ind_19_industry_rotation',
                'indicator_name': '理想行业轮动策略',
                'metric': '领涨板块涨幅',
                'value': _safe_float(top_pct),
                'unit': '%',
                'trade_date': target_trade_date,
                'description': '当日领涨板块：%s' % str(top_sector.get('name') or '--'),
            },
            {
                'indicator_id': 'ind_19_industry_rotation',
                'indicator_name': '理想行业轮动策略',
                'metric': '板块强弱差',
                'value': spread,
                'unit': '%',
                'trade_date': target_trade_date,
                'description': 'Top1 与末位板块的涨跌幅差值，衡量轮动分化程度。',
            },
        ],
        'macro': [
            {
                'indicator_id': 'ind_18_valuation',
                'indicator_name': '市场估值指标',
                'metric': '破净占比(PB<1)',
                'value': _safe_float(valuation.get('pb_break_ratio')),
                'unit': '%',
                'trade_date': str(valuation.get('trade_date') or target_trade_date),
                'description': '全市场 PB<1 个股占比，通常越高代表估值越低。',
            },
            {
                'indicator_id': 'ind_25_margin_financing',
                'indicator_name': '融资融券指标',
                'metric': '融资融券余额',
                'value': (
                    _safe_float(_safe_float(margin.get('rzrqye')) / 1e8, 2)
                    if _safe_float(margin.get('rzrqye')) is not None else None
                ),
                'unit': '亿元',
                'trade_date': str(margin.get('trade_date') or target_trade_date),
                'description': '两融余额及其变化反映杠杆资金风险偏好。',
            },
            {
                'indicator_id': 'ind_25_margin_financing',
                'indicator_name': '融资融券指标',
                'metric': '主力资金净流向',
                'value': (
                    _safe_float(_safe_float(capital_flow.get('net_mf_amount')) / 1e4, 2)
                    if _safe_float(capital_flow.get('net_mf_amount')) is not None else None
                ),
                'unit': '亿元',
                'trade_date': str(capital_flow.get('trade_date') or target_trade_date),
                'description': '全市场主力净流向，观察资金面边际变化。',
            },
        ],
    }

    snapshots = {
        'heat': heat,
        'valuation': valuation,
        'volatility': volatility,
        'northbound': {
            'trade_date': str(northbound.get('trade_date') or target_trade_date),
            'avg_ratio': _safe_float(northbound.get('avg_ratio')),
            'stock_count': int(northbound.get('stock_count') or 0),
            'source': str(northbound.get('source') or ''),
        },
        'breadth': {
            'trade_date': str(breadth.get('trade_date') or target_trade_date),
            'up_ratio': _safe_float(breadth.get('up_ratio')),
            'up_count': int(breadth.get('up_count') or 0),
            'down_count': int(breadth.get('down_count') or 0),
        },
    }
    trends = {
        'market_heat': heat_series,
        'valuation_pb_break': valuation_series,
    }
    return {
        'panels': panels,
        'snapshots': snapshots,
        'trends': trends,
    }


def _query_hk_hold_snapshot(end_trade_date):
    db_path = os.path.join(DB_ROOT, 'dailybasic.db')
    if not os.path.isfile(db_path):
        return {}
    trade_date = _nearest_table_trade_date('hk_hold', end_trade_date)
    if not trade_date:
        return {}
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            'SELECT COUNT(DISTINCT ts_code), SUM(COALESCE(vol,0)), AVG(COALESCE(ratio,0)) '
            'FROM hk_hold WHERE trade_date=?',
            (trade_date,)
        ).fetchone()
        return {
            'trade_date': trade_date,
            'stock_count': int((row or [0, 0, 0])[0] or 0),
            'hold_vol': _safe_float((row or [0, 0, 0])[1], 2),
            'avg_ratio': _safe_float((row or [0, 0, 0])[2], 4),
            'source': 'hk_hold_local',
        }
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


def _to_hhmmss(value):
    s = str(value or '').strip()
    if not s:
        return ''
    if len(s) >= 8 and ' ' in s:
        return s.split(' ')[-1][:8]
    return s[:8]


def _derive_watch_codes(watch_codes, hotrank_rows):
    if watch_codes:
        return watch_codes
    codes = []
    for row in hotrank_rows or []:
        code = _normalize_ts_code(row.get('ts_code'))
        if not code:
            continue
        if code not in codes:
            codes.append(code)
        if len(codes) >= 6:
            break
    if not codes:
        codes = _query_local_active_codes(limit=6)
    return codes


def _series_time_to_ts_ms(raw_time, trade_date=''):
    text = str(raw_time or '').strip()
    if not text:
        return None

    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y%m%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y%m%d %H:%M',
    ]
    dt = None
    for fmt in formats:
        try:
            dt = datetime.datetime.strptime(text, fmt)
            break
        except Exception:
            dt = None

    if dt is None and re.fullmatch(r'\d{2}:\d{2}:\d{2}', text):
        td = str(trade_date or '').strip()
        if re.fullmatch(r'\d{8}', td):
            try:
                dt = datetime.datetime.strptime(td + ' ' + text, '%Y%m%d %H:%M:%S')
            except Exception:
                dt = None

    if dt is None:
        return None
    if ZoneInfo:
        dt = dt.replace(tzinfo=ZoneInfo('Asia/Shanghai'))
    return int(dt.timestamp() * 1000)


def _sync_history_from_intraday_series(index_series, stock_series, trade_date=''):
    if not index_series and not stock_series:
        return
    with _HISTORY_LOCK:
        for code, points in (index_series or {}).items():
            norm_code = _normalize_ts_code(code)
            if not norm_code:
                continue
            dq = deque(maxlen=_HISTORY_MAX)
            for p in points[-_HISTORY_MAX:]:
                ts_ms = _series_time_to_ts_ms(p.get('time'), trade_date=trade_date)
                value = _safe_float(p.get('value'), 4)
                if ts_ms is None or value is None:
                    continue
                dq.append({
                    'ts': ts_ms,
                    'time': _to_hhmmss(p.get('time')),
                    'value': value,
                })
            if dq:
                _HISTORY['index_pct'][norm_code] = dq

        for code, points in (stock_series or {}).items():
            norm_code = _normalize_ts_code(code)
            if not norm_code:
                continue
            dq = deque(maxlen=_HISTORY_MAX)
            for p in points[-_HISTORY_MAX:]:
                ts_ms = _series_time_to_ts_ms(p.get('time'), trade_date=trade_date)
                value = _safe_float(p.get('value'), 4)
                if ts_ms is None or value is None:
                    continue
                dq.append({
                    'ts': ts_ms,
                    'time': _to_hhmmss(p.get('time')),
                    'value': value,
                })
            if dq:
                _HISTORY['stock_pct'][norm_code] = dq


def _collect_stock_rows(watch_codes):
    from AlphaFin.ai_team.services.tushare_watch_service import fetch_intraday_stock_quote

    rows = []
    for code in watch_codes:
        quote = {}
        try:
            quote = fetch_intraday_stock_quote(code, freq='1MIN') or {}
        except Exception:
            quote = {}

        latest = _query_latest_close(code)
        last_close = _safe_float(latest.get('close'))
        price = _safe_float(quote.get('price'))
        pct_change = None
        try:
            if price is not None and last_close not in (None, 0):
                pct_change = (price - last_close) / last_close * 100.0
        except Exception:
            pct_change = None

        rows.append({
            'ts_code': code,
            'name': _get_stock_name(code),
            'price': _safe_float(price),
            'quote_time': str(quote.get('time') or ''),
            'source': str(quote.get('source') or ''),
            'last_close': _safe_float(last_close),
            'last_trade_date': str(latest.get('trade_date') or ''),
            'pct_change': _safe_float(pct_change, 3),
        })
    rows.sort(key=lambda x: abs(x.get('pct_change') or 0), reverse=True)
    return rows


def _append_history(clock, index_rows, stock_rows, breadth):
    now = _now_cn()
    hhmm = now.strftime('%H:%M:%S')
    ts_ms = int(now.timestamp() * 1000)
    with _HISTORY_LOCK:
        for row in index_rows or []:
            code = _normalize_ts_code(row.get('ts_code'))
            val = _safe_float(row.get('intraday_pct'), 4)
            if not code or val is None:
                continue
            _HISTORY['index_pct'][code].append({
                'ts': ts_ms,
                'time': hhmm,
                'value': val,
            })

        for row in stock_rows or []:
            code = _normalize_ts_code(row.get('ts_code'))
            val = _safe_float(row.get('pct_change'), 4)
            if not code or val is None:
                continue
            _HISTORY['stock_pct'][code].append({
                'ts': ts_ms,
                'time': hhmm,
                'value': val,
            })

        ratio = _safe_float(breadth.get('up_ratio'), 4) if breadth else None
        if ratio is not None:
            _HISTORY['breadth_up_ratio'].append({
                'ts': ts_ms,
                'time': hhmm,
                'value': ratio,
            })


def _get_history_payload():
    with _HISTORY_LOCK:
        return {
            'index_pct': {
                k: list(v)
                for k, v in _HISTORY['index_pct'].items()
                if v
            },
            'stock_pct': {
                k: list(v)
                for k, v in _HISTORY['stock_pct'].items()
                if v
            },
            'breadth_up_ratio': list(_HISTORY['breadth_up_ratio']),
        }


def _build_summary(clock, index_rows, sector_rows, stock_rows, breadth, alerts):
    index_vals = []
    for r in index_rows:
        v = _safe_float(r.get('intraday_pct'))
        if v is not None:
            index_vals.append(v)
    avg_index_pct = (sum(index_vals) / len(index_vals)) if index_vals else 0.0
    breadth_ratio = _safe_float(breadth.get('up_ratio')) or 0.0
    top_sector = sector_rows[0] if sector_rows else {}
    hot_sector_name = str(top_sector.get('name') or '')
    hot_sector_pct = _safe_float(top_sector.get('pct_change'))
    high_alerts = len([a for a in alerts if a.get('severity') == 'high'])

    heat_score = 50.0
    heat_score += max(min(avg_index_pct * 8.0, 20.0), -20.0)
    heat_score += max(min((breadth_ratio - 50.0) * 0.4, 15.0), -15.0)
    if hot_sector_pct is not None:
        heat_score += max(min(hot_sector_pct * 2.0, 12.0), -12.0)
    heat_score -= high_alerts * 6.0
    heat_score = max(0.0, min(100.0, heat_score))

    market_bias = '震荡'
    if avg_index_pct >= 0.6 and breadth_ratio >= 55:
        market_bias = '偏强'
    elif avg_index_pct <= -0.6 and breadth_ratio <= 45:
        market_bias = '偏弱'

    return {
        'market_bias': market_bias,
        'market_heat_score': _safe_float(heat_score, 1),
        'avg_index_pct': _safe_float(avg_index_pct, 3),
        'breadth_up_ratio': _safe_float(breadth_ratio, 2),
        'hot_sector_name': hot_sector_name,
        'hot_sector_pct': _safe_float(hot_sector_pct, 3),
        'alerts_count': len(alerts),
        'high_alerts': high_alerts,
        'clock_phase': str(clock.get('phase') or ''),
    }


def _generate_alerts(index_rows, sector_rows, stock_rows, news_rows, breadth):
    alerts = []

    # 指数波动告警（盘后口径）
    for row in index_rows or []:
        code = row.get('ts_code', '')
        v = _safe_float(row.get('intraday_pct'))
        if v is None:
            continue
        if abs(v) >= 1.5:
            alerts.append({
                'severity': 'high',
                'category': 'index_volatility',
                'title': '%s 当日波动 %.2f%%' % (code, v),
                'detail': '指数日内波动较大，建议复核风险敞口变化',
                'source': row.get('source', ''),
                'time': _to_hhmmss(row.get('time')),
            })
        elif abs(v) >= 0.8:
            alerts.append({
                'severity': 'medium',
                'category': 'index_volatility',
                'title': '%s 当日波动 %.2f%%' % (code, v),
                'detail': '指数波动进入关注区间',
                'source': row.get('source', ''),
                'time': _to_hhmmss(row.get('time')),
            })

    # 行业分化告警
    if sector_rows:
        top = sector_rows[0]
        tail = sector_rows[-1]
        top_pct = _safe_float(top.get('pct_change')) or 0.0
        tail_pct = _safe_float(tail.get('pct_change')) or 0.0
        spread = top_pct - tail_pct
        if spread >= 4.0:
            alerts.append({
                'severity': 'medium',
                'category': 'sector_rotation',
                'title': '行业轮动加速，强弱差 %.2f%%' % spread,
                'detail': '热点集中在%s，注意高低切换风险' % (top.get('name') or '头部行业'),
                'source': top.get('source', ''),
                'time': _to_hhmmss(top.get('trade_time')),
            })

    # 个股异动告警
    for row in stock_rows or []:
        v = _safe_float(row.get('pct_change'))
        if v is None:
            continue
        if abs(v) >= 5.0:
            alerts.append({
                'severity': 'high',
                'category': 'stock_move',
                'title': '%s 异动 %.2f%%' % (row.get('ts_code', ''), v),
                'detail': '重点个股波动显著，请复核催化与流动性',
                'source': row.get('source', ''),
                'time': _to_hhmmss(row.get('quote_time')),
            })
        elif abs(v) >= 3.0:
            alerts.append({
                'severity': 'medium',
                'category': 'stock_move',
                'title': '%s 波动 %.2f%%' % (row.get('ts_code', ''), v),
                'detail': '重点个股进入观察区间',
                'source': row.get('source', ''),
                'time': _to_hhmmss(row.get('quote_time')),
            })

    # 新闻冲击告警
    key_words = ('停牌', '复牌', '减持', '重组', '并购', '加息', '降息', '监管', '地缘', '原油')
    for item in (news_rows or [])[:8]:
        title = str(item.get('title') or '')
        matched = [k for k in key_words if k in title]
        if not matched:
            continue
        alerts.append({
            'severity': 'low',
            'category': 'news_event',
            'title': '新闻事件：%s' % title[:38],
            'detail': '关键词：%s' % ('、'.join(matched)),
            'source': 'news_cls',
            'time': _to_hhmmss(item.get('datetime')),
        })
        if len(alerts) >= 24:
            break

    # 市场广度告警
    ratio = _safe_float(breadth.get('up_ratio')) if breadth else None
    if ratio is not None:
        if ratio >= 70:
            alerts.append({
                'severity': 'low',
                'category': 'breadth',
                'title': '市场广度偏强（上涨占比 %.1f%%）' % ratio,
                'detail': '短线风险偏好提升，但需防冲高回落',
                'source': 'daily_kline_local',
                'time': '',
            })
        elif ratio <= 30:
            alerts.append({
                'severity': 'medium',
                'category': 'breadth',
                'title': '市场广度偏弱（上涨占比 %.1f%%）' % ratio,
                'detail': '情绪偏弱，建议控制仓位波动',
                'source': 'daily_kline_local',
                'time': '',
            })

    # 按优先级排序：high > medium > low
    order = {'high': 0, 'medium': 1, 'low': 2}
    alerts.sort(key=lambda x: order.get(x.get('severity', 'low'), 9))
    return alerts[:20]


def _build_evidence(clock, status, index_rows, sector_rows, hotrank_rows, stock_rows, news_rows, breadth, errors):
    evidence = []
    evidence.append({
        'module': '市场时钟',
        'source': 'get_market_clock',
        'timestamp': str(clock.get('datetime') or ''),
        'status': 'ok' if clock else 'missing',
        'detail': 'phase=%s trade_date=%s' % (clock.get('phase', ''), clock.get('trade_date', '')),
    })
    evidence.append({
        'module': '数据健康',
        'source': 'get_realtime_data_status',
        'timestamp': str(status.get('checked_at') or ''),
        'status': str(status.get('mode') or ''),
        'detail': str(status.get('summary') or ''),
    })
    evidence.append({
        'module': '指数复盘',
        'source': 'daily_kline_proxy',
        'timestamp': _to_hhmmss(index_rows[0].get('time')) if index_rows else '',
        'status': 'ok' if index_rows else 'missing',
        'detail': '记录数=%d' % len(index_rows or []),
    })
    evidence.append({
        'module': '每日板块表现',
        'source': 'sw_local',
        'timestamp': _to_hhmmss(sector_rows[0].get('trade_time')) if sector_rows else '',
        'status': 'ok' if sector_rows else 'missing',
        'detail': '记录数=%d' % len(sector_rows or []),
    })
    evidence.append({
        'module': '同花顺热榜',
        'source': 'fetch_intraday_hotrank',
        'timestamp': _to_hhmmss(hotrank_rows[0].get('rank_time')) if hotrank_rows else '',
        'status': 'ok' if hotrank_rows else 'missing',
        'detail': '记录数=%d' % len(hotrank_rows or []),
    })
    evidence.append({
        'module': '重点个股',
        'source': 'daily_kline_local',
        'timestamp': _to_hhmmss(stock_rows[0].get('quote_time')) if stock_rows else '',
        'status': 'ok' if stock_rows else 'missing',
        'detail': '记录数=%d' % len(stock_rows or []),
    })
    if news_rows:
        evidence.append({
            'module': '资讯事件',
            'source': 'news_cls',
            'timestamp': _to_hhmmss(news_rows[0].get('datetime')),
            'status': 'ok',
            'detail': '记录数=%d' % len(news_rows or []),
        })
    evidence.append({
        'module': '市场广度',
        'source': 'daily_kline_local',
        'timestamp': str(breadth.get('trade_date') or ''),
        'status': 'ok' if breadth else 'missing',
        'detail': '上涨=%s 下跌=%s 涨停=%s 跌停=%s' % (
            breadth.get('up_count', '-'),
            breadth.get('down_count', '-'),
            breadth.get('limit_up_count', '-'),
            breadth.get('limit_down_count', '-'),
        ) if breadth else '',
    })
    if errors:
        evidence.append({
            'module': '异常',
            'source': 'runtime',
            'timestamp': _now_cn().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'warning',
            'detail': '；'.join(errors[:6]),
        })
    return evidence


def build_market_watch_snapshot(watchlist='', include_news=True, force=False):
    """
    生成盘后复盘聚合快照。
    兼容旧接口名：/api/ai_watch/snapshot
    """
    watch_codes = _parse_watchlist(watchlist)
    cache_key = '%s|post_close' % (','.join(watch_codes))

    now_ts = time.time()
    with _CACHE_LOCK:
        if (
            not force
            and _SNAPSHOT_CACHE.get('payload') is not None
            and _SNAPSHOT_CACHE.get('key') == cache_key
            and (now_ts - float(_SNAPSHOT_CACHE.get('at') or 0.0)) < _CACHE_TTL_SECONDS
        ):
            payload = copy.deepcopy(_SNAPSHOT_CACHE['payload'])
            payload['cached'] = True
            return _sanitize_json_value(payload)

    _ensure_post_close_indexes()

    generated_at = _now_cn().strftime('%Y-%m-%d %H:%M:%S')
    errors = []
    review = _resolve_post_close_trade_date()
    target_trade_date = str(review.get('trade_date') or '')
    if not target_trade_date:
        errors.append('本地日线库无可用交易日数据')
        payload = {
            'generated_at': generated_at,
            'cached': False,
            'review': review,
            'clock': {},
            'status': {
                'checked_at': generated_at,
                'mode': 'offline',
                'summary': '复盘数据不可用',
            },
            'summary': {},
            'watchlist': [],
            'indexes': [],
            'sectors': [],
            'active_stocks': [],
            'hotrank': [],
            'stocks': [],
            'news': [],
            'breadth': {},
            'alerts': [],
            'evidence': [],
            'history': {},
            'fallback_trend': {'mode': 'none', 'index_pct': {}, 'stock_pct': {}, 'breadth_up_ratio': []},
            'capital_flow': {},
            'margin': {},
            'northbound': {},
            'trends': {'moneyflow_net': [], 'margin_balance': []},
            'indicator_panels': {'market': [], 'industry': [], 'macro': []},
            'indicator_snapshots': {},
            'indicator_trends': {'market_heat': [], 'valuation_pb_break': []},
            'errors': errors,
        }
        payload = _sanitize_json_value(payload)
        with _CACHE_LOCK:
            _SNAPSHOT_CACHE['key'] = cache_key
            _SNAPSHOT_CACHE['at'] = time.time()
            _SNAPSHOT_CACHE['payload'] = copy.deepcopy(payload)
        return payload

    clock = {
        'datetime': str(review.get('now') or generated_at),
        'trade_date': target_trade_date,
        'is_weekday': bool(review.get('is_weekday')),
        'phase': 'post_close_review',
    }
    status = {
        'checked_at': generated_at,
        'mode': 'post_close',
        'summary': '盘后复盘（交易日 %s）' % target_trade_date,
        'clock_phase': 'post_close_review',
        'intraday_index_ok': False,
        'intraday_index_source': 'disabled',
        'intraday_index_time': '',
        'intraday_news_ok': False,
        'stock_quote_ok': False,
        'stock_quote_source': 'disabled',
        'stock_quote_time': '',
        'local_db_latest_trade_date': _query_latest_trade_date_local(),
        'errors': [],
    }

    index_rows = _query_post_close_index_rows(target_trade_date)
    sector_rows = _query_local_sector_heat(limit=20, trade_date=target_trade_date)
    active_rows = _query_post_close_active_rows(target_trade_date, limit=12)
    hotrank_rows = _query_local_hotrank(limit=20, trade_date=target_trade_date)
    news_rows = []

    if not watch_codes:
        watch_codes = _query_local_active_codes(limit=6, trade_date=target_trade_date)
    watch_codes = watch_codes[:8]
    stock_rows = _query_post_close_stock_rows(watch_codes, target_trade_date)

    # 广度
    breadth = _calc_market_breadth(target_trade_date)

    # 资金/杠杆/北向
    capital_flow = _query_moneyflow_snapshot(target_trade_date)
    margin = _query_margin_snapshot(target_trade_date)
    northbound = _query_hk_hold_snapshot(target_trade_date)
    trends = {
        'moneyflow_net': _query_moneyflow_series(target_trade_date, limit=60),
        'margin_balance': _query_margin_series(target_trade_date, limit=60),
    }
    indicator_pack = _build_indicator_panels(
        target_trade_date,
        index_rows=index_rows,
        sector_rows=sector_rows,
        breadth=breadth,
        capital_flow=capital_flow,
        margin=margin,
        northbound=northbound,
    )

    # 告警 + 摘要 + 证据（盘后口径）
    alerts = _generate_alerts(index_rows, sector_rows, stock_rows, news_rows, breadth)
    net_mf = _safe_float(capital_flow.get('net_mf_amount'))
    if net_mf is not None:
        if net_mf < 0:
            alerts.append({
                'severity': 'medium',
                'category': 'capital_flow',
                'title': '主力资金净流出',
                'detail': '当日资金面偏弱（净额 %.2f）' % net_mf,
                'source': capital_flow.get('source', ''),
                'time': '',
            })
        elif net_mf > 0:
            alerts.append({
                'severity': 'low',
                'category': 'capital_flow',
                'title': '主力资金净流入',
                'detail': '当日资金面偏强（净额 %.2f）' % net_mf,
                'source': capital_flow.get('source', ''),
                'time': '',
            })
    order = {'high': 0, 'medium': 1, 'low': 2}
    alerts.sort(key=lambda x: order.get(x.get('severity', 'low'), 9))
    alerts = alerts[:24]

    summary = _build_summary(clock, index_rows, sector_rows, stock_rows, breadth, alerts)
    summary['trade_date'] = target_trade_date
    summary['moneyflow_trade_date'] = str(capital_flow.get('trade_date') or '')
    summary['margin_trade_date'] = str(margin.get('trade_date') or '')
    summary['northbound_trade_date'] = str(northbound.get('trade_date') or '')
    evidence = _build_evidence(
        clock, status, index_rows, sector_rows, hotrank_rows, stock_rows, news_rows, breadth, errors
    )
    evidence.append({
        'module': '资金流向',
        'source': str(capital_flow.get('source') or ''),
        'timestamp': str(capital_flow.get('trade_date') or ''),
        'status': 'ok' if capital_flow else 'missing',
        'detail': '净额=%s' % str(capital_flow.get('net_mf_amount', '--')),
    })
    evidence.append({
        'module': '融资融券',
        'source': str(margin.get('source') or ''),
        'timestamp': str(margin.get('trade_date') or ''),
        'status': 'ok' if margin else 'missing',
        'detail': '余额=%s 变化=%s' % (
            str(margin.get('rzrqye', '--')),
            str(margin.get('delta_rzrqye', '--')),
        ),
    })
    evidence.append({
        'module': '北向持仓',
        'source': str(northbound.get('source') or ''),
        'timestamp': str(northbound.get('trade_date') or ''),
        'status': 'ok' if northbound else 'missing',
        'detail': '持仓标的=%s 平均占比=%s' % (
            str(northbound.get('stock_count', '--')),
            str(northbound.get('avg_ratio', '--')),
        ),
    })

    history = {
        'index_pct': _query_proxy_index_series(limit=60, trade_date=target_trade_date),
        'stock_pct': _query_stock_pct_series(watch_codes, limit=60, trade_date=target_trade_date),
        'breadth_up_ratio': _query_breadth_series(limit=60, trade_date=target_trade_date),
    }
    fallback_trend = {
        'mode': 'daily_post_close',
        'index_pct': history.get('index_pct', {}),
        'stock_pct': history.get('stock_pct', {}),
        'breadth_up_ratio': history.get('breadth_up_ratio', []),
    }

    payload = {
        'generated_at': generated_at,
        'cached': False,
        'review': review,
        'clock': clock,
        'status': status,
        'summary': summary,
        'watchlist': watch_codes,
        'indexes': index_rows,
        'sectors': sector_rows,
        'active_stocks': active_rows,
        'hotrank': hotrank_rows,
        'stocks': stock_rows,
        'news': news_rows,
        'breadth': breadth,
        'alerts': alerts,
        'evidence': evidence,
        'history': history,
        'fallback_trend': fallback_trend,
        'capital_flow': capital_flow,
        'margin': margin,
        'northbound': northbound,
        'trends': trends,
        'indicator_panels': indicator_pack.get('panels', {}),
        'indicator_snapshots': indicator_pack.get('snapshots', {}),
        'indicator_trends': indicator_pack.get('trends', {}),
        'errors': errors,
    }
    payload = _sanitize_json_value(payload)

    with _CACHE_LOCK:
        _SNAPSHOT_CACHE['key'] = cache_key
        _SNAPSHOT_CACHE['at'] = time.time()
        _SNAPSHOT_CACHE['payload'] = copy.deepcopy(payload)

    return payload
