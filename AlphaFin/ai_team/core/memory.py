"""
智能体持久化记忆 - 基于 SQLite
支持 HOT/WARM/COLD 分层记忆、反思日志、模式升级
"""
import json
import os
import re
import sqlite3
import threading
import time
import uuid
import datetime

from AlphaFin.ai_team.config import MEMORY_DB_PATH, DATA_DIR, KNOWLEDGE_RETENTION_DAYS

_lock = threading.Lock()

TIER_HOT = 'hot'
TIER_WARM = 'warm'
TIER_COLD = 'cold'
VALID_TIERS = (TIER_HOT, TIER_WARM, TIER_COLD)

OUTCOME_SUCCESS = 'success'
OUTCOME_FAILURE = 'failure'
OUTCOME_OBS = 'observation'


def _get_conn():
    """获取 SQLite 连接（WAL 模式）"""
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(MEMORY_DB_PATH, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA cache_size=-8000')
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn, table_name):
    rows = conn.execute('PRAGMA table_info(%s)' % table_name).fetchall()
    return set(r['name'] for r in rows)


def _ensure_column(conn, table_name, column_name, ddl):
    cols = _table_columns(conn, table_name)
    if column_name not in cols:
        conn.execute('ALTER TABLE %s ADD COLUMN %s %s' % (table_name, column_name, ddl))


def _normalize_tier(tier):
    t = (tier or TIER_WARM).strip().lower()
    if t not in VALID_TIERS:
        return TIER_WARM
    return t


def _safe_json_loads(text, default):
    try:
        return json.loads(text)
    except Exception:
        return default


def _safe_json_dumps(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return '[]'


def _normalize_pattern_key(text):
    raw = (text or '').strip().lower()
    raw = re.sub(r'\s+', '_', raw)
    raw = re.sub(r'[^a-z0-9_\-\u4e00-\u9fff:.]', '', raw)
    return raw[:120]


def _tokenize(text):
    text = (text or '').lower()
    ascii_tokens = re.findall(r'[a-z0-9_.]+', text)
    cjk_chars = ''.join(ch for ch in text if '\u4e00' <= ch <= '\u9fff')
    cjk_bigrams = [cjk_chars[i:i + 2] for i in range(max(0, len(cjk_chars) - 1))]
    if len(cjk_chars) == 1:
        cjk_bigrams = [cjk_chars]
    return set(ascii_tokens + cjk_bigrams)


def _relevance_score(query_tokens, text, updated_at):
    if not query_tokens:
        # 无查询词时以时间为主
        return float(updated_at or 0)
    doc_tokens = _tokenize(text)
    if not doc_tokens:
        return 0.0
    overlap = len(query_tokens.intersection(doc_tokens))
    coverage = overlap / max(1, len(query_tokens))
    age_days = (time.time() - float(updated_at or 0)) / 86400.0
    recency = max(0.0, 1.0 - age_days / 90.0)
    return overlap * 3.0 + coverage * 2.0 + recency


def _should_include_cold(query):
    q = (query or '').lower()
    if not q:
        return False
    keys = ('历史', '归档', '以前', '过去', '长期', 'archive', 'old', 'legacy')
    return any(k in q for k in keys)


def _ensure_schema(conn):
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS conversation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            tool_name TEXT,
            session_id TEXT,
            created_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS knowledge_base (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT NOT NULL,
            category TEXT NOT NULL,
            subject TEXT NOT NULL,
            content TEXT NOT NULL,
            confidence REAL DEFAULT 0.8,
            valid_until TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS research_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_type TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            participants TEXT,
            created_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS learning_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT NOT NULL,
            pattern_key TEXT NOT NULL,
            rule_text TEXT DEFAULT '',
            evidence TEXT DEFAULT '',
            success_count INTEGER DEFAULT 0,
            failure_count INTEGER DEFAULT 0,
            observation_count INTEGER DEFAULT 0,
            promoted INTEGER DEFAULT 0,
            hot_subject TEXT DEFAULT '',
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(agent_id, pattern_key)
        );

        CREATE TABLE IF NOT EXISTS reflection_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT NOT NULL,
            session_id TEXT,
            workflow TEXT DEFAULT '',
            task TEXT DEFAULT '',
            reply TEXT DEFAULT '',
            reflection TEXT DEFAULT '',
            created_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS user_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ask_id TEXT NOT NULL UNIQUE,
            session_id TEXT DEFAULT '',
            question TEXT DEFAULT '',
            answer TEXT DEFAULT '',
            workflow_mode TEXT DEFAULT '',
            workflow_name TEXT DEFAULT '',
            participants TEXT DEFAULT '[]',
            score INTEGER NOT NULL,
            suggestion TEXT DEFAULT '',
            created_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS llm_token_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT NOT NULL,
            session_id TEXT DEFAULT '',
            workflow TEXT DEFAULT '',
            model TEXT DEFAULT '',
            request_id TEXT DEFAULT '',
            prompt_tokens INTEGER DEFAULT 0,
            completion_tokens INTEGER DEFAULT 0,
            total_tokens INTEGER DEFAULT 0,
            estimated INTEGER DEFAULT 0,
            day_key TEXT DEFAULT '',
            meta_json TEXT DEFAULT '{}',
            created_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ask_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ask_id TEXT NOT NULL UNIQUE,
            status TEXT DEFAULT '',
            done INTEGER DEFAULT 0,
            payload_json TEXT DEFAULT '{}',
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trace_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL UNIQUE,
            session_id TEXT DEFAULT '',
            workflow TEXT DEFAULT '',
            origin TEXT DEFAULT '',
            topic TEXT DEFAULT '',
            status TEXT DEFAULT 'running',
            started_at REAL NOT NULL,
            ended_at REAL,
            duration_ms INTEGER DEFAULT 0,
            meta_json TEXT DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS trace_spans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            span_id TEXT NOT NULL UNIQUE,
            run_id TEXT DEFAULT '',
            parent_span_id TEXT DEFAULT '',
            session_id TEXT DEFAULT '',
            workflow TEXT DEFAULT '',
            agent_id TEXT DEFAULT '',
            span_type TEXT DEFAULT '',
            name TEXT DEFAULT '',
            status TEXT DEFAULT 'running',
            input_preview TEXT DEFAULT '',
            output_preview TEXT DEFAULT '',
            error_text TEXT DEFAULT '',
            data_json TEXT DEFAULT '{}',
            started_at REAL NOT NULL,
            ended_at REAL,
            duration_ms INTEGER DEFAULT 0
        );
    ''')

    # knowledge_base 兼容增量升级
    _ensure_column(conn, 'knowledge_base', 'tier', "TEXT DEFAULT 'warm'")
    _ensure_column(conn, 'knowledge_base', 'project', "TEXT DEFAULT ''")
    _ensure_column(conn, 'knowledge_base', 'domain', "TEXT DEFAULT ''")
    _ensure_column(conn, 'knowledge_base', 'tags', "TEXT DEFAULT '[]'")
    _ensure_column(conn, 'knowledge_base', 'pattern_key', "TEXT DEFAULT ''")
    _ensure_column(conn, 'knowledge_base', 'source_type', "TEXT DEFAULT 'manual'")
    _ensure_column(conn, 'knowledge_base', 'source_session', "TEXT DEFAULT ''")
    _ensure_column(conn, 'knowledge_base', 'usage_count', "INTEGER DEFAULT 0")
    _ensure_column(conn, 'knowledge_base', 'success_count', "INTEGER DEFAULT 0")
    _ensure_column(conn, 'knowledge_base', 'failure_count', "INTEGER DEFAULT 0")
    _ensure_column(conn, 'knowledge_base', 'version', "INTEGER DEFAULT 1")
    _ensure_column(conn, 'knowledge_base', 'parent_id', "INTEGER")
    _ensure_column(conn, 'knowledge_base', 'is_active', "INTEGER DEFAULT 1")

    conn.executescript('''
        CREATE INDEX IF NOT EXISTS idx_conv_agent ON conversation_history(agent_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_conv_agent_session ON conversation_history(agent_id, session_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_kb_agent ON knowledge_base(agent_id, category);
        CREATE INDEX IF NOT EXISTS idx_kb_agent_tier ON knowledge_base(agent_id, tier, updated_at);
        CREATE INDEX IF NOT EXISTS idx_kb_agent_subject ON knowledge_base(agent_id, subject, updated_at);
        CREATE INDEX IF NOT EXISTS idx_kb_active_valid ON knowledge_base(agent_id, is_active, valid_until);
        CREATE INDEX IF NOT EXISTS idx_reports_type ON research_reports(report_type, created_at);
        CREATE INDEX IF NOT EXISTS idx_patterns_agent ON learning_patterns(agent_id, updated_at);
        CREATE INDEX IF NOT EXISTS idx_patterns_key ON learning_patterns(agent_id, pattern_key);
        CREATE INDEX IF NOT EXISTS idx_reflection_agent ON reflection_logs(agent_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_feedback_session ON user_feedback(session_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_feedback_score ON user_feedback(score, created_at);
        CREATE INDEX IF NOT EXISTS idx_token_day ON llm_token_usage(day_key, created_at);
        CREATE INDEX IF NOT EXISTS idx_token_session ON llm_token_usage(session_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_token_agent ON llm_token_usage(agent_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_ask_tasks_askid ON ask_tasks(ask_id);
        CREATE INDEX IF NOT EXISTS idx_ask_tasks_updated ON ask_tasks(updated_at);
        CREATE INDEX IF NOT EXISTS idx_ask_tasks_done ON ask_tasks(done, updated_at);
        CREATE INDEX IF NOT EXISTS idx_trace_runs_session ON trace_runs(session_id, started_at);
        CREATE INDEX IF NOT EXISTS idx_trace_runs_started ON trace_runs(started_at);
        CREATE INDEX IF NOT EXISTS idx_trace_spans_run ON trace_spans(run_id, started_at);
        CREATE INDEX IF NOT EXISTS idx_trace_spans_session ON trace_spans(session_id, started_at);
        CREATE INDEX IF NOT EXISTS idx_trace_spans_agent ON trace_spans(agent_id, started_at);
    ''')


def init_db():
    """初始化数据库表"""
    conn = _get_conn()
    _ensure_schema(conn)
    conn.commit()
    conn.close()


def upsert_ask_task(ask_id, payload):
    """写入/更新直连问答任务状态（持久化）。"""
    init_db()
    aid = str(ask_id or '').strip()
    if not aid:
        return None
    base = dict(payload or {})
    now = time.time()
    created_at = float(base.get('created_at') or now)
    updated_at = float(base.get('updated_at') or now)
    status = str(base.get('status') or '')
    done = 1 if bool(base.get('done')) else 0
    base['created_at'] = created_at
    base['updated_at'] = updated_at
    with _lock:
        conn = _get_conn()
        conn.execute(
            'INSERT OR REPLACE INTO ask_tasks '
            '(id, ask_id, status, done, payload_json, created_at, updated_at) '
            'VALUES ((SELECT id FROM ask_tasks WHERE ask_id=?), ?, ?, ?, ?, '
            'COALESCE((SELECT created_at FROM ask_tasks WHERE ask_id=?), ?), ?)',
            (
                aid,
                aid,
                status,
                done,
                _safe_json_dumps(base),
                aid,
                created_at,
                updated_at
            )
        )
        conn.commit()
        row = conn.execute(
            'SELECT * FROM ask_tasks WHERE ask_id=?',
            (aid,)
        ).fetchone()
        conn.close()
    return dict(row) if row else None


def get_ask_task(ask_id):
    """按 ask_id 读取直连问答任务状态。"""
    init_db()
    aid = str(ask_id or '').strip()
    if not aid:
        return None
    conn = _get_conn()
    row = conn.execute(
        'SELECT * FROM ask_tasks WHERE ask_id=?',
        (aid,)
    ).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    payload = _safe_json_loads(d.get('payload_json') or '{}', {})
    if not isinstance(payload, dict):
        payload = {}
    payload['ask_id'] = aid
    payload['status'] = str(payload.get('status') or d.get('status') or '')
    payload['done'] = bool(payload.get('done') if 'done' in payload else d.get('done'))
    payload['created_at'] = float(payload.get('created_at') or d.get('created_at') or 0)
    payload['updated_at'] = float(payload.get('updated_at') or d.get('updated_at') or 0)
    return payload


def cleanup_ask_tasks(ttl_seconds):
    """清理过期 ask_tasks 记录。"""
    init_db()
    try:
        ttl = max(60, int(ttl_seconds or 0))
    except Exception:
        ttl = 6 * 3600
    cutoff = time.time() - ttl
    with _lock:
        conn = _get_conn()
        conn.execute(
            'DELETE FROM ask_tasks WHERE updated_at < ?',
            (cutoff,)
        )
        conn.commit()
        conn.close()


def mark_stale_running_ask_tasks(stale_after_seconds=300):
    """
    将长时间未更新且未完成的任务标记为失败，避免重启后“永远转圈”。
    返回修复条数。
    """
    init_db()
    try:
        stale_after = max(60, int(stale_after_seconds or 0))
    except Exception:
        stale_after = 300
    cutoff = time.time() - stale_after
    repaired = 0
    with _lock:
        conn = _get_conn()
        rows = conn.execute(
            'SELECT ask_id, payload_json, created_at FROM ask_tasks '
            'WHERE done=0 AND updated_at < ?',
            (cutoff,)
        ).fetchall()
        now = time.time()
        for r in rows:
            aid = str(r['ask_id'] or '')
            payload = _safe_json_loads(r['payload_json'] or '{}', {})
            if not isinstance(payload, dict):
                payload = {}
            payload['status'] = 'error'
            payload['done'] = True
            payload['error'] = '任务在服务重启或中断后未继续执行，已自动标记为失败，请重试。'
            payload['updated_at'] = now
            if not payload.get('created_at'):
                payload['created_at'] = float(r['created_at'] or now)
            conn.execute(
                'UPDATE ask_tasks SET status=?, done=1, payload_json=?, updated_at=? WHERE ask_id=?',
                ('error', _safe_json_dumps(payload), now, aid)
            )
            repaired += 1
        conn.commit()
        conn.close()
    return repaired


class AgentMemory:
    """单个智能体的记忆管理器"""

    def __init__(self, agent_id):
        self.agent_id = agent_id
        init_db()

    def save_conversation(self, role, content, tool_name=None, session_id=None):
        """保存一条对话记录"""
        with _lock:
            conn = _get_conn()
            conn.execute(
                'INSERT INTO conversation_history (agent_id, role, content, tool_name, session_id, created_at) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (self.agent_id, role, content, tool_name, session_id, time.time())
            )
            conn.commit()
            conn.close()

    def get_recent_conversations(self, limit=20, session_id=None, roles=None):
        """获取最近的对话记录"""
        try:
            limit = max(1, int(limit))
        except Exception:
            limit = 20

        conn = _get_conn()
        sql = 'SELECT role, content, tool_name, session_id, created_at FROM conversation_history WHERE agent_id=?'
        vals = [self.agent_id]
        if session_id:
            sql += ' AND session_id=?'
            vals.append(session_id)
        if roles:
            placeholders = ','.join(['?'] * len(roles))
            sql += ' AND role IN (%s)' % placeholders
            vals.extend(list(roles))
        sql += ' ORDER BY created_at DESC LIMIT ?'
        vals.append(limit)
        rows = conn.execute(sql, tuple(vals)).fetchall()
        conn.close()
        return [dict(r) for r in reversed(rows)]

    def get_relevant_conversations(self, query, limit=8, session_id=None):
        """按相关性召回历史对话，避免仅靠进程内短上下文"""
        try:
            limit = max(1, int(limit))
        except Exception:
            limit = 8
        conn = _get_conn()
        sql = (
            'SELECT role, content, tool_name, session_id, created_at '
            'FROM conversation_history '
            'WHERE agent_id=? AND role IN ("user","assistant") '
        )
        vals = [self.agent_id]
        if session_id:
            sql += 'AND (session_id=? OR session_id IS NULL OR session_id="") '
            vals.append(session_id)
        sql += 'ORDER BY created_at DESC LIMIT 200'
        rows = conn.execute(sql, tuple(vals)).fetchall()
        conn.close()
        query_tokens = _tokenize(query)
        scored = []
        for r in rows:
            text = '%s %s' % (r['role'] or '', r['content'] or '')
            score = _relevance_score(query_tokens, text, r['created_at'])
            if score <= 0:
                continue
            d = dict(r)
            d['_score'] = score
            scored.append(d)
        scored.sort(key=lambda x: (x['_score'], x['created_at']), reverse=True)
        picked = scored[:limit]
        picked.sort(key=lambda x: x['created_at'])
        return picked

    def _insert_knowledge_conn(
        self, conn, category, subject, content, confidence=0.8, valid_days=None,
        tier=TIER_WARM, project='', domain='', tags=None, pattern_key='',
        source_type='manual', source_session=None, success_count=0, failure_count=0
    ):
        now = time.time()
        tier = _normalize_tier(tier)

        if valid_days is None:
            valid_days = 0 if tier == TIER_HOT else KNOWLEDGE_RETENTION_DAYS
        valid_until = '' if not valid_days else str(int(now + int(valid_days) * 86400))

        try:
            confidence = float(confidence)
        except Exception:
            confidence = 0.8
        confidence = max(0.0, min(1.0, confidence))

        if tags is None:
            tags_list = []
        elif isinstance(tags, str):
            tags_list = [x.strip() for x in re.split(r'[,\s]+', tags) if x.strip()]
        elif isinstance(tags, (list, tuple)):
            tags_list = [str(x).strip() for x in tags if str(x).strip()]
        else:
            tags_list = []
        tags_json = _safe_json_dumps(tags_list)

        pkey = _normalize_pattern_key(pattern_key)
        source_session = source_session or ''

        prev = conn.execute(
            'SELECT id, version FROM knowledge_base '
            'WHERE agent_id=? AND category=? AND subject=? AND tier=? AND is_active=1 '
            'ORDER BY version DESC, id DESC LIMIT 1',
            (self.agent_id, category, subject, tier)
        ).fetchone()

        parent_id = None
        version = 1
        if prev:
            parent_id = prev['id']
            version = int(prev['version'] or 1) + 1
            # 不覆盖历史，旧版本保留为非活跃
            conn.execute('UPDATE knowledge_base SET is_active=0, updated_at=? WHERE id=?', (now, prev['id']))

        cur = conn.execute(
            'INSERT INTO knowledge_base '
            '(agent_id, category, subject, content, confidence, valid_until, created_at, updated_at, '
            ' tier, project, domain, tags, pattern_key, source_type, source_session, '
            ' usage_count, success_count, failure_count, version, parent_id, is_active) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, 1)',
            (
                self.agent_id, category, subject, content, confidence, valid_until, now, now,
                tier, project or '', domain or '', tags_json, pkey, source_type or 'manual', source_session,
                int(success_count or 0), int(failure_count or 0), version, parent_id
            )
        )
        return int(cur.lastrowid), version

    def _record_pattern_outcome_conn(self, conn, pattern_key, rule_text='', outcome=OUTCOME_OBS, evidence=''):
        pkey = _normalize_pattern_key(pattern_key)
        if not pkey:
            return None

        now = time.time()
        outcome = (outcome or OUTCOME_OBS).strip().lower()
        if outcome not in (OUTCOME_SUCCESS, OUTCOME_FAILURE, OUTCOME_OBS):
            outcome = OUTCOME_OBS

        row = conn.execute(
            'SELECT * FROM learning_patterns WHERE agent_id=? AND pattern_key=?',
            (self.agent_id, pkey)
        ).fetchone()

        if row:
            s = int(row['success_count'] or 0)
            f = int(row['failure_count'] or 0)
            o = int(row['observation_count'] or 0)
            promoted = int(row['promoted'] or 0)
            existing_rule = row['rule_text'] or ''
            if outcome == OUTCOME_SUCCESS:
                s += 1
            elif outcome == OUTCOME_FAILURE:
                f += 1
            else:
                o += 1
            conn.execute(
                'UPDATE learning_patterns '
                'SET rule_text=?, evidence=?, success_count=?, failure_count=?, observation_count=?, updated_at=? '
                'WHERE id=?',
                (
                    rule_text or existing_rule,
                    (evidence or row['evidence'] or '')[:1000],
                    s, f, o, now, row['id']
                )
            )
            pattern_id = row['id']
            hot_subject = row['hot_subject'] or ''
        else:
            s = 1 if outcome == OUTCOME_SUCCESS else 0
            f = 1 if outcome == OUTCOME_FAILURE else 0
            o = 1 if outcome == OUTCOME_OBS else 0
            promoted = 0
            hot_subject = ''
            cur = conn.execute(
                'INSERT INTO learning_patterns '
                '(agent_id, pattern_key, rule_text, evidence, success_count, failure_count, observation_count, promoted, hot_subject, created_at, updated_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, 0, "", ?, ?)',
                (
                    self.agent_id, pkey, (rule_text or '')[:400], (evidence or '')[:1000],
                    s, f, o, now, now
                )
            )
            pattern_id = int(cur.lastrowid)

        promoted_now = False
        hot_knowledge_id = None
        # 规则被验证成功>=3次，自动晋升为 HOT 永久规则
        if s >= 3 and not promoted:
            if not hot_subject:
                hot_subject = 'rule:' + pkey[:80]
            effective_rule = (rule_text or '').strip() or ('模式 %s 的执行规则' % pkey)
            promotion_content = (
                '规则：%s\n'
                '状态：已验证成功%d次（失败%d次，观察%d次）\n'
                '最近证据：%s\n'
                '要求：后续相关任务优先按该规则执行。'
            ) % (
                effective_rule[:220],
                s,
                f,
                o,
                (evidence or '无')[:280]
            )
            hot_knowledge_id, _ = self._insert_knowledge_conn(
                conn=conn,
                category='strategy_rule',
                subject=hot_subject,
                content=promotion_content,
                confidence=min(0.99, 0.78 + 0.05 * min(s, 5) - 0.03 * min(f, 5)),
                valid_days=0,
                tier=TIER_HOT,
                project='',
                domain='',
                tags=['auto_promoted', 'pattern_rule'],
                pattern_key=pkey,
                source_type='auto_promotion',
                source_session='',
                success_count=s,
                failure_count=f,
            )
            conn.execute(
                'UPDATE learning_patterns SET promoted=1, hot_subject=?, updated_at=? WHERE id=?',
                (hot_subject, now, pattern_id)
            )
            promoted_now = True

        return {
            'pattern_key': pkey,
            'success_count': s,
            'failure_count': f,
            'observation_count': o,
            'promoted': bool(promoted_now or promoted),
            'promoted_now': promoted_now,
            'hot_subject': hot_subject,
            'hot_knowledge_id': hot_knowledge_id,
        }

    def save_knowledge(
        self, category, subject, content, confidence=0.8, valid_days=None,
        tier=TIER_WARM, project='', domain='', tags=None, pattern_key='',
        source_type='manual', source_session=None, outcome=None, rule_text=''
    ):
        """
        保存知识条目（版本化，不覆盖历史）。
        支持 HOT/WARM/COLD 分层和模式学习统计。
        """
        with _lock:
            conn = _get_conn()
            knowledge_id, version = self._insert_knowledge_conn(
                conn=conn,
                category=category,
                subject=subject,
                content=content,
                confidence=confidence,
                valid_days=valid_days,
                tier=tier,
                project=project,
                domain=domain,
                tags=tags,
                pattern_key=pattern_key,
                source_type=source_type,
                source_session=source_session,
            )
            pattern_info = None
            if pattern_key:
                pattern_info = self._record_pattern_outcome_conn(
                    conn=conn,
                    pattern_key=pattern_key,
                    rule_text=(rule_text or content or '')[:400],
                    outcome=outcome or OUTCOME_OBS,
                    evidence=(content or '')[:500],
                )
            conn.commit()
            conn.close()
        return {
            'id': knowledge_id,
            'version': version,
            'tier': _normalize_tier(tier),
            'pattern': pattern_info,
        }

    def ensure_seed_knowledge(
        self, category, subject, content, confidence=0.95, valid_days=None,
        tier=TIER_HOT, project='', domain='', tags=None,
        source_type='seed', source_session=None
    ):
        """
        确保一条“种子记忆”存在。
        若当前激活版本内容未变化，则不重复写新版本；若内容变化，则自动升级为新版本。
        """
        raw_content = str(content or '').strip()
        if not raw_content:
            return {'ok': False, 'reason': 'empty_content'}

        tier_norm = _normalize_tier(tier)
        with _lock:
            conn = _get_conn()
            row = conn.execute(
                'SELECT id, content, version FROM knowledge_base '
                'WHERE agent_id=? AND category=? AND subject=? AND tier=? AND is_active=1 '
                'ORDER BY version DESC, id DESC LIMIT 1',
                (self.agent_id, category, subject, tier_norm)
            ).fetchone()
            if row and str(row['content'] or '').strip() == raw_content:
                conn.execute(
                    'UPDATE knowledge_base SET updated_at=? WHERE id=?',
                    (time.time(), row['id'])
                )
                conn.commit()
                conn.close()
                return {
                    'ok': True,
                    'id': int(row['id']),
                    'version': int(row['version'] or 1),
                    'created': False,
                    'tier': tier_norm,
                }

            knowledge_id, version = self._insert_knowledge_conn(
                conn=conn,
                category=category,
                subject=subject,
                content=raw_content,
                confidence=confidence,
                valid_days=valid_days,
                tier=tier_norm,
                project=project,
                domain=domain,
                tags=tags,
                pattern_key='',
                source_type=source_type,
                source_session=source_session,
            )
            conn.commit()
            conn.close()
            return {
                'ok': True,
                'id': knowledge_id,
                'version': version,
                'created': True,
                'tier': tier_norm,
            }

    def get_knowledge(
        self, category=None, limit=20, tier=None, query=None,
        include_inactive=False, include_expired=False, project=None, domain=None
    ):
        """按层级和相关性获取知识条目"""
        try:
            limit = max(1, int(limit))
        except Exception:
            limit = 20
        fetch_limit = min(max(limit * 6, 120), 600)

        conn = _get_conn()
        now = time.time()
        sql = (
            'SELECT id, category, subject, content, confidence, valid_until, created_at, updated_at, '
            'tier, project, domain, tags, pattern_key, source_type, source_session, '
            'usage_count, success_count, failure_count, version, parent_id, is_active '
            'FROM knowledge_base WHERE agent_id=?'
        )
        vals = [self.agent_id]
        if category:
            sql += ' AND category=?'
            vals.append(category)
        if tier:
            sql += ' AND tier=?'
            vals.append(_normalize_tier(tier))
        if project:
            sql += ' AND project=?'
            vals.append(project)
        if domain:
            sql += ' AND domain=?'
            vals.append(domain)
        if not include_inactive:
            sql += ' AND is_active=1'
        if not include_expired:
            sql += ' AND (valid_until="" OR CAST(valid_until AS REAL) > ?)'
            vals.append(now)
        sql += ' ORDER BY updated_at DESC LIMIT ?'
        vals.append(fetch_limit)
        rows = conn.execute(sql, tuple(vals)).fetchall()

        query_tokens = _tokenize(query)
        items = []
        for r in rows:
            d = dict(r)
            d['tags'] = _safe_json_loads(d.get('tags') or '[]', [])
            text = ' '.join([
                d.get('category', ''), d.get('subject', ''), d.get('content', ''),
                d.get('project', ''), d.get('domain', ''), ' '.join(d.get('tags') or [])
            ])
            d['_score'] = _relevance_score(query_tokens, text, d.get('updated_at'))
            items.append(d)

        if query_tokens:
            items.sort(key=lambda x: (x.get('_score', 0), x.get('updated_at', 0)), reverse=True)
        else:
            items.sort(key=lambda x: x.get('updated_at', 0), reverse=True)
        picked = items[:limit]

        if picked:
            ids = [x['id'] for x in picked]
            placeholders = ','.join(['?'] * len(ids))
            conn.execute(
                'UPDATE knowledge_base SET usage_count=COALESCE(usage_count,0)+1 WHERE id IN (%s)' % placeholders,
                tuple(ids)
            )
            conn.commit()
        conn.close()
        return picked

    def get_layered_knowledge(self, query='', max_hot=10, max_warm=10, max_cold=4, include_cold=False):
        """分层检索：HOT 每轮必载，WARM 按相关性，COLD 按需召回"""
        hot = self.get_knowledge(tier=TIER_HOT, limit=max_hot, query=query)
        warm = self.get_knowledge(tier=TIER_WARM, limit=max_warm, query=query)
        cold = []
        use_cold = include_cold or _should_include_cold(query)
        if use_cold or (not warm and query):
            cold = self.get_knowledge(tier=TIER_COLD, limit=max_cold, query=query)
        return {'hot': hot, 'warm': warm, 'cold': cold}

    @staticmethod
    def render_layered_knowledge_summary(layers, max_items=10):
        """将分层记忆转换为可注入 system prompt 的摘要文本。"""
        hot = list((layers or {}).get('hot') or [])
        warm = list((layers or {}).get('warm') or [])
        cold = list((layers or {}).get('cold') or [])
        if not hot and not warm and not cold:
            return ''

        lines = ['[分层长期记忆]']
        if hot:
            lines.append('HOT（核心规则，每轮必读）:')
            for item in hot[:100]:  # 热记忆强约束: <=100行
                lines.append('- [%s] %s' % (
                    item['subject'],
                    (item['content'] or '')[:180]
                ))
        if warm:
            lines.append('WARM（项目/领域相关经验）:')
            for item in warm[:max_items]:
                lines.append('- [%s/%s] %s (置信度:%.0f%%)' % (
                    item['category'],
                    item['subject'],
                    (item['content'] or '')[:180],
                    float(item.get('confidence') or 0) * 100
                ))
        if cold:
            lines.append('COLD（历史归档，谨慎参考）:')
            for item in cold[:max(1, int(max_items / 2))]:
                lines.append('- [%s] %s' % (item['subject'], (item['content'] or '')[:120]))
        return '\n'.join(lines)

    def get_knowledge_summary(self, max_items=10, query='', include_cold=False):
        """获取分层记忆摘要（用于注入智能体上下文）"""
        layers = self.get_layered_knowledge(
            query=query,
            max_hot=min(20, max_items),
            max_warm=max_items,
            max_cold=max(3, int(max_items / 2)),
            include_cold=include_cold,
        )
        return self.render_layered_knowledge_summary(layers, max_items=max_items)

    def get_memory_stats(self):
        """汇总当前智能体的长期记忆、反思与模式学习统计。"""
        conn = _get_conn()
        stats = {
            'conversation_count': 0,
            'knowledge_total': 0,
            'knowledge_hot': 0,
            'knowledge_warm': 0,
            'knowledge_cold': 0,
            'pattern_count': 0,
            'promoted_count': 0,
            'reflection_count': 0,
        }
        try:
            row = conn.execute(
                'SELECT COUNT(*) AS cnt FROM conversation_history WHERE agent_id=?',
                (self.agent_id,)
            ).fetchone()
            stats['conversation_count'] = int(row['cnt'] or 0) if row else 0
        except Exception:
            pass
        try:
            rows = conn.execute(
                'SELECT tier, COUNT(*) AS cnt FROM knowledge_base '
                'WHERE agent_id=? AND is_active=1 GROUP BY tier',
                (self.agent_id,)
            ).fetchall()
            total = 0
            for row in rows or []:
                tier = _normalize_tier(row['tier'])
                cnt = int(row['cnt'] or 0)
                total += cnt
                if tier == TIER_HOT:
                    stats['knowledge_hot'] = cnt
                elif tier == TIER_WARM:
                    stats['knowledge_warm'] = cnt
                elif tier == TIER_COLD:
                    stats['knowledge_cold'] = cnt
            stats['knowledge_total'] = total
        except Exception:
            pass
        try:
            row = conn.execute(
                'SELECT COUNT(*) AS cnt, SUM(CASE WHEN promoted=1 THEN 1 ELSE 0 END) AS promoted_cnt '
                'FROM learning_patterns WHERE agent_id=?',
                (self.agent_id,)
            ).fetchone()
            stats['pattern_count'] = int(row['cnt'] or 0) if row else 0
            stats['promoted_count'] = int(row['promoted_cnt'] or 0) if row else 0
        except Exception:
            pass
        try:
            row = conn.execute(
                'SELECT COUNT(*) AS cnt FROM reflection_logs WHERE agent_id=?',
                (self.agent_id,)
            ).fetchone()
            stats['reflection_count'] = int(row['cnt'] or 0) if row else 0
        except Exception:
            pass
        conn.close()
        return stats

    def get_recent_knowledge(self, limit=6):
        """获取近期激活知识，供前端记忆观测面板展示。"""
        try:
            limit = max(1, int(limit))
        except Exception:
            limit = 6
        conn = _get_conn()
        rows = conn.execute(
            'SELECT category, subject, content, tier, confidence, updated_at, source_type, source_session, pattern_key '
            'FROM knowledge_base WHERE agent_id=? AND is_active=1 ORDER BY updated_at DESC LIMIT ?',
            (self.agent_id, limit)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_recent_reflections(self, limit=6):
        """获取近期反思日志。"""
        try:
            limit = max(1, int(limit))
        except Exception:
            limit = 6
        conn = _get_conn()
        rows = conn.execute(
            'SELECT session_id, workflow, task, reply, reflection, created_at '
            'FROM reflection_logs WHERE agent_id=? ORDER BY created_at DESC LIMIT ?',
            (self.agent_id, limit)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def record_pattern_outcome(self, pattern_key, rule_text='', outcome=OUTCOME_OBS, evidence=''):
        """记录模式验证结果，并在成功>=3次时自动晋升 HOT 规则"""
        with _lock:
            conn = _get_conn()
            info = self._record_pattern_outcome_conn(
                conn=conn,
                pattern_key=pattern_key,
                rule_text=rule_text,
                outcome=outcome,
                evidence=evidence,
            )
            conn.commit()
            conn.close()
            return info

    def get_pattern_snapshot(self, limit=20):
        """获取模式学习快照（调试/审计）"""
        conn = _get_conn()
        rows = conn.execute(
            'SELECT pattern_key, success_count, failure_count, observation_count, promoted, hot_subject, updated_at '
            'FROM learning_patterns WHERE agent_id=? ORDER BY updated_at DESC LIMIT ?',
            (self.agent_id, limit)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def save_reflection(self, session_id, workflow, task, reply, reflection):
        """保存结构化反思日志"""
        with _lock:
            conn = _get_conn()
            conn.execute(
                'INSERT INTO reflection_logs (agent_id, session_id, workflow, task, reply, reflection, created_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?)',
                (
                    self.agent_id,
                    session_id or '',
                    workflow or '',
                    (task or '')[:800],
                    (reply or '')[:2000],
                    (reflection or '')[:2000],
                    time.time()
                )
            )
            conn.commit()
            conn.close()


def save_report(report_type, title, content, participants=None):
    """保存研究报告"""
    with _lock:
        conn = _get_conn()
        conn.execute(
            'INSERT INTO research_reports (report_type, title, content, participants, created_at) '
            'VALUES (?, ?, ?, ?, ?)',
            (report_type, title, content, json.dumps(participants or []), time.time())
        )
        conn.commit()
        conn.close()


def get_reports(report_type=None, limit=20):
    """获取研究报告列表"""
    conn = _get_conn()
    if report_type:
        rows = conn.execute(
            'SELECT id, report_type, title, created_at FROM research_reports '
            'WHERE report_type=? ORDER BY created_at DESC LIMIT ?',
            (report_type, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            'SELECT id, report_type, title, created_at FROM research_reports '
            'ORDER BY created_at DESC LIMIT ?',
            (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_report_by_id(report_id):
    """获取单篇报告详情"""
    conn = _get_conn()
    row = conn.execute(
        'SELECT * FROM research_reports WHERE id=?', (report_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def delete_report_by_id(report_id):
    """删除单篇研究报告。返回是否删除成功。"""
    with _lock:
        conn = _get_conn()
        cur = conn.execute(
            'DELETE FROM research_reports WHERE id=?', (report_id,)
        )
        conn.commit()
        deleted = (cur.rowcount or 0) > 0
        conn.close()
    return deleted


def save_user_feedback(ask_id, session_id, question, answer,
                       workflow_mode, workflow_name, participants,
                       score, suggestion):
    """保存直连问答用户评分反馈（ask_id 幂等更新）。"""
    init_db()
    with _lock:
        conn = _get_conn()
        now = time.time()
        participants_json = _safe_json_dumps(participants or [])
        conn.execute(
            'INSERT OR REPLACE INTO user_feedback '
            '(id, ask_id, session_id, question, answer, workflow_mode, workflow_name, participants, score, suggestion, created_at) '
            'VALUES ((SELECT id FROM user_feedback WHERE ask_id=?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (
                ask_id,
                ask_id,
                session_id or '',
                (question or '')[:2000],
                (answer or '')[:5000],
                (workflow_mode or '')[:100],
                (workflow_name or '')[:120],
                participants_json,
                int(score),
                (suggestion or '')[:4000],
                now
            )
        )
        conn.commit()
        row = conn.execute(
            'SELECT * FROM user_feedback WHERE ask_id=?',
            (ask_id,)
        ).fetchone()
        conn.close()
    return dict(row) if row else {}


def get_user_feedback(ask_id):
    """按 ask_id 获取用户反馈。"""
    init_db()
    conn = _get_conn()
    row = conn.execute(
        'SELECT * FROM user_feedback WHERE ask_id=?',
        (ask_id,)
    ).fetchone()
    conn.close()
    if not row:
        return None
    item = dict(row)
    item['participants'] = _safe_json_loads(item.get('participants') or '[]', [])
    return item


def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return int(default)


def _beijing_day_key(ts=None):
    base_ts = float(ts or time.time())
    dt = datetime.datetime.utcfromtimestamp(base_ts) + datetime.timedelta(hours=8)
    return dt.strftime('%Y%m%d')


def _merge_json_text(old_text, patch_dict):
    base = _safe_json_loads(old_text or '{}', {})
    if not isinstance(base, dict):
        base = {}
    if isinstance(patch_dict, dict):
        base.update(patch_dict)
    return _safe_json_dumps(base)


def record_token_usage(agent_id, session_id, workflow, model,
                       prompt_tokens, completion_tokens, total_tokens,
                       estimated=False, request_id='', meta=None):
    """记录一次 LLM token 消耗。"""
    init_db()
    now = time.time()
    day_key = _beijing_day_key(now)
    p = max(0, _safe_int(prompt_tokens, 0))
    c = max(0, _safe_int(completion_tokens, 0))
    t = max(0, _safe_int(total_tokens, p + c))
    with _lock:
        conn = _get_conn()
        conn.execute(
            'INSERT INTO llm_token_usage '
            '(agent_id, session_id, workflow, model, request_id, prompt_tokens, completion_tokens, '
            ' total_tokens, estimated, day_key, meta_json, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (
                str(agent_id or ''),
                str(session_id or ''),
                str(workflow or ''),
                str(model or ''),
                str(request_id or ''),
                p,
                c,
                t,
                1 if estimated else 0,
                day_key,
                _safe_json_dumps(meta or {}),
                now
            )
        )
        conn.commit()
        conn.close()
    return {
        'day_key': day_key,
        'prompt_tokens': p,
        'completion_tokens': c,
        'total_tokens': t,
        'estimated': bool(estimated),
    }


def get_token_usage_summary(session_id=None, day_key=None):
    """
    统计 token 使用量：
    - day_total: 当天（北京时间）累计
    - session_total: 指定 session 累计
    """
    init_db()
    dk = str(day_key or _beijing_day_key())
    sid = str(session_id or '')
    conn = _get_conn()
    day_row = conn.execute(
        'SELECT COALESCE(SUM(total_tokens),0) AS total, '
        'COALESCE(SUM(prompt_tokens),0) AS prompt, '
        'COALESCE(SUM(completion_tokens),0) AS completion '
        'FROM llm_token_usage WHERE day_key=?',
        (dk,)
    ).fetchone()
    if sid:
        session_row = conn.execute(
            'SELECT COALESCE(SUM(total_tokens),0) AS total, '
            'COALESCE(SUM(prompt_tokens),0) AS prompt, '
            'COALESCE(SUM(completion_tokens),0) AS completion '
            'FROM llm_token_usage WHERE session_id=?',
            (sid,)
        ).fetchone()
    else:
        session_row = {'total': 0, 'prompt': 0, 'completion': 0}
    conn.close()
    return {
        'day_key': dk,
        'day_total': _safe_int(day_row['total'], 0),
        'day_prompt': _safe_int(day_row['prompt'], 0),
        'day_completion': _safe_int(day_row['completion'], 0),
        'session_id': sid,
        'session_total': _safe_int(session_row['total'], 0),
        'session_prompt': _safe_int(session_row['prompt'], 0),
        'session_completion': _safe_int(session_row['completion'], 0),
    }


def get_token_budget_snapshot(session_id, daily_limit, session_limit,
                              warn_ratio=0.8, hard_ratio=0.95):
    """
    计算预算状态：normal / warning / critical / exhausted
    - daily_limit 或 session_limit <= 0 时视为不限制。
    """
    usage = get_token_usage_summary(session_id=session_id)
    day_used = usage['day_total']
    session_used = usage['session_total']

    daily_limit = max(0, _safe_int(daily_limit, 0))
    session_limit = max(0, _safe_int(session_limit, 0))
    warn_ratio = max(0.5, min(0.98, float(warn_ratio or 0.8)))
    hard_ratio = max(warn_ratio, min(0.995, float(hard_ratio or 0.95)))

    if daily_limit > 0:
        day_ratio = float(day_used) / float(daily_limit)
        day_remaining = max(0, daily_limit - day_used)
    else:
        day_ratio = 0.0
        day_remaining = None

    if session_limit > 0:
        session_ratio = float(session_used) / float(session_limit)
        session_remaining = max(0, session_limit - session_used)
    else:
        session_ratio = 0.0
        session_remaining = None

    level = 'normal'
    if (
        (daily_limit > 0 and day_used >= daily_limit) or
        (session_limit > 0 and session_used >= session_limit)
    ):
        level = 'exhausted'
    elif max(day_ratio, session_ratio) >= hard_ratio:
        level = 'critical'
    elif max(day_ratio, session_ratio) >= warn_ratio:
        level = 'warning'

    return {
        'level': level,
        'day_key': usage['day_key'],
        'day_used': day_used,
        'day_limit': daily_limit,
        'day_ratio': round(day_ratio, 4),
        'day_remaining': day_remaining,
        'session_id': usage['session_id'],
        'session_used': session_used,
        'session_limit': session_limit,
        'session_ratio': round(session_ratio, 4),
        'session_remaining': session_remaining,
        'warn_ratio': warn_ratio,
        'hard_ratio': hard_ratio,
    }


def trace_get_active_run_id(session_id):
    init_db()
    sid = str(session_id or '').strip()
    if not sid:
        return ''
    conn = _get_conn()
    row = conn.execute(
        'SELECT run_id FROM trace_runs '
        'WHERE session_id=? AND ended_at IS NULL '
        'ORDER BY started_at DESC LIMIT 1',
        (sid,)
    ).fetchone()
    conn.close()
    return str(row['run_id']) if row else ''


def trace_start_run(session_id='', workflow='', origin='', topic='', meta=None, run_id=None):
    """开启一条端到端 trace run。"""
    init_db()
    sid = str(session_id or '').strip()
    now = time.time()
    rid = str(run_id or ('run_' + uuid.uuid4().hex[:12]))
    with _lock:
        conn = _get_conn()
        conn.execute(
            'INSERT OR REPLACE INTO trace_runs '
            '(id, run_id, session_id, workflow, origin, topic, status, started_at, ended_at, duration_ms, meta_json) '
            'VALUES ((SELECT id FROM trace_runs WHERE run_id=?), ?, ?, ?, ?, ?, ?, '
            'COALESCE((SELECT started_at FROM trace_runs WHERE run_id=?), ?), '
            'NULL, 0, ?)',
            (
                rid,
                rid,
                sid,
                str(workflow or ''),
                str(origin or ''),
                str(topic or '')[:500],
                'running',
                rid,
                now,
                _safe_json_dumps(meta or {})
            )
        )
        conn.commit()
        conn.close()
    return rid


def trace_finish_run(run_id, status='ok', meta=None):
    """结束 trace run。"""
    if not run_id:
        return False
    init_db()
    now = time.time()
    with _lock:
        conn = _get_conn()
        row = conn.execute(
            'SELECT started_at, meta_json FROM trace_runs WHERE run_id=?',
            (run_id,)
        ).fetchone()
        if not row:
            conn.close()
            return False
        started_at = float(row['started_at'] or now)
        duration_ms = max(0, int((now - started_at) * 1000))
        old_meta = row['meta_json'] if 'meta_json' in row.keys() else '{}'
        merged_meta = _merge_json_text(old_meta, meta or {})
        conn.execute(
            'UPDATE trace_runs '
            'SET status=?, ended_at=?, duration_ms=?, meta_json=? '
            'WHERE run_id=?',
            (str(status or 'ok'), now, duration_ms, merged_meta, run_id)
        )
        conn.commit()
        conn.close()
    return True


def trace_start_span(run_id, session_id='', workflow='', span_type='',
                     name='', agent_id='', parent_span_id='',
                     input_preview='', data=None):
    """开启 trace span。"""
    init_db()
    sid = str(session_id or '').strip()
    span_id = 'span_' + uuid.uuid4().hex[:14]
    now = time.time()
    with _lock:
        conn = _get_conn()
        conn.execute(
            'INSERT INTO trace_spans '
            '(span_id, run_id, parent_span_id, session_id, workflow, agent_id, span_type, name, status, '
            ' input_preview, output_preview, error_text, data_json, started_at, ended_at, duration_ms) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "", "", ?, ?, NULL, 0)',
            (
                span_id,
                str(run_id or ''),
                str(parent_span_id or ''),
                sid,
                str(workflow or ''),
                str(agent_id or ''),
                str(span_type or ''),
                str(name or '')[:180],
                'running',
                str(input_preview or '')[:2000],
                _safe_json_dumps(data or {}),
                now
            )
        )
        conn.commit()
        conn.close()
    return span_id


def trace_finish_span(span_id, status='ok', output_preview='', error_text='', data=None):
    """结束 trace span。"""
    if not span_id:
        return False
    init_db()
    now = time.time()
    with _lock:
        conn = _get_conn()
        row = conn.execute(
            'SELECT started_at, data_json FROM trace_spans WHERE span_id=?',
            (span_id,)
        ).fetchone()
        if not row:
            conn.close()
            return False
        started_at = float(row['started_at'] or now)
        duration_ms = max(0, int((now - started_at) * 1000))
        merged_data = _merge_json_text(row['data_json'], data or {})
        conn.execute(
            'UPDATE trace_spans '
            'SET status=?, output_preview=?, error_text=?, data_json=?, ended_at=?, duration_ms=? '
            'WHERE span_id=?',
            (
                str(status or 'ok'),
                str(output_preview or '')[:3000],
                str(error_text or '')[:1200],
                merged_data,
                now,
                duration_ms,
                span_id
            )
        )
        conn.commit()
        conn.close()
    return True


def trace_event(run_id, session_id='', workflow='', span_type='event',
                name='', agent_id='', data=None, status='ok',
                output_preview='', error_text=''):
    """记录一个瞬时事件（start+finish）。"""
    sid = trace_start_span(
        run_id=run_id,
        session_id=session_id,
        workflow=workflow,
        span_type=span_type,
        name=name,
        agent_id=agent_id,
        parent_span_id='',
        input_preview='',
        data=data
    )
    trace_finish_span(
        sid,
        status=status,
        output_preview=output_preview,
        error_text=error_text,
        data=data
    )
    return sid


def get_trace_runs(session_id=None, limit=20):
    """查询 trace run 列表。"""
    init_db()
    try:
        limit = max(1, min(int(limit or 20), 200))
    except Exception:
        limit = 20
    conn = _get_conn()
    if session_id:
        rows = conn.execute(
            'SELECT * FROM trace_runs WHERE session_id=? ORDER BY started_at DESC LIMIT ?',
            (str(session_id), limit)
        ).fetchall()
    else:
        rows = conn.execute(
            'SELECT * FROM trace_runs ORDER BY started_at DESC LIMIT ?',
            (limit,)
        ).fetchall()
    conn.close()
    out = []
    for r in rows:
        d = dict(r)
        d['meta'] = _safe_json_loads(d.get('meta_json') or '{}', {})
        out.append(d)
    return out


def get_trace_run(run_id):
    """按 run_id 查询 trace run。"""
    init_db()
    conn = _get_conn()
    row = conn.execute(
        'SELECT * FROM trace_runs WHERE run_id=?',
        (str(run_id or ''),)
    ).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    d['meta'] = _safe_json_loads(d.get('meta_json') or '{}', {})
    return d


def get_trace_spans(run_id, limit=800):
    """查询某 run 的全部 span。"""
    init_db()
    try:
        limit = max(1, min(int(limit or 800), 5000))
    except Exception:
        limit = 800
    conn = _get_conn()
    rows = conn.execute(
        'SELECT * FROM trace_spans WHERE run_id=? ORDER BY started_at ASC LIMIT ?',
        (str(run_id or ''), limit)
    ).fetchall()
    conn.close()
    out = []
    for r in rows:
        d = dict(r)
        d['data'] = _safe_json_loads(d.get('data_json') or '{}', {})
        out.append(d)
    return out
