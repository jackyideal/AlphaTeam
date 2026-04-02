"""
投资周期调度器 - 管理每日投资决策流程
独立于 orchestrator.py（研究周期），专门处理投资组合的决策流程。
"""
import datetime
import time
import uuid
import threading
import traceback

from AlphaFin.ai_team.core.message_bus import bus
from AlphaFin.ai_team.core.session_control import (
    cancel_session,
    clear_session_cancel,
    is_session_cancelled,
    get_session_cancel_reason,
    set_session_deadline,
    clear_session_deadline,
    get_session_timing,
    is_session_converging,
    is_session_expired,
    set_session_progress,
    get_session_progress,
    clear_session_progress,
    request_session_overtime_decision,
    get_session_overtime_state,
    clear_session_overtime_state,
)
from AlphaFin.ai_team.config import (
    PORTFOLIO_AUTO_RUN_TIME,
    PORTFOLIO_WATCH_ENABLED,
    PORTFOLIO_WATCH_START,
    PORTFOLIO_WATCH_END,
    PORTFOLIO_WATCH_INTERVAL,
    PORTFOLIO_WATCH_WEEKDAY_FALLBACK,
    PORTFOLIO_DB_AUTO_UPDATE_ENABLED,
    PORTFOLIO_DB_AUTO_UPDATE_TIME,
    PORTFOLIO_MANUAL_DEFAULT_TIMEOUT,
    PORTFOLIO_WATCH_MANUAL_DEFAULT_TIMEOUT,
    WORKFLOW_DEADLINE_SOFT_RATIO,
)
from AlphaFin.ai_team.prompt_catalog import format_ai_team_prompt


class PortfolioScheduler:
    """投资周期调度器"""

    def __init__(self):
        self.running = False
        self.auto_enabled = False
        self.last_run_date = ''
        self.last_watch_time = 0
        self.last_watch_date = ''
        self.current_session = None
        self.watch_enabled = bool(PORTFOLIO_WATCH_ENABLED)
        self.watch_interval = max(300, int(PORTFOLIO_WATCH_INTERVAL))
        self.watch_start = PORTFOLIO_WATCH_START
        self.watch_end = PORTFOLIO_WATCH_END
        self.watch_weekday_fallback = bool(PORTFOLIO_WATCH_WEEKDAY_FALLBACK)
        self._watch_fallback_notice_date = ''
        self._auto_run_hour, self._auto_run_min = self._parse_hhmm(PORTFOLIO_AUTO_RUN_TIME, (15, 30))
        self.db_auto_update_enabled = bool(PORTFOLIO_DB_AUTO_UPDATE_ENABLED)
        self.db_auto_update_time = PORTFOLIO_DB_AUTO_UPDATE_TIME
        self._db_update_hour, self._db_update_min = self._parse_hhmm(PORTFOLIO_DB_AUTO_UPDATE_TIME, (6, 0))
        self.last_db_update_date = ''
        self.db_update_running = False
        self.strategy_agents = ['intel', 'quant', 'analyst', 'restructuring']
        self.review_agents = ['risk', 'auditor']
        self.specialists_all = list(self.strategy_agents + self.review_agents)
        self._lock = threading.Lock()
        self._deadline_notices = set()

    def start(self):
        """启动后台线程"""
        if self.running:
            return
        self._sync_auto_from_config()
        self.running = True
        t = threading.Thread(target=self._run_loop, daemon=True)
        t.start()
        print('[PortfolioScheduler] 投资调度器已启动')

    def stop(self):
        """停止后台线程循环（不改动持久化配置）。"""
        self.running = False
        self.current_session = None
        bus.post_activity('portfolio', 'status', '投资调度器后台循环已停止')

    def _sync_auto_from_config(self):
        """从持久化配置恢复自动运行开关"""
        try:
            from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
            cfg = pm.get_config()
            if cfg and cfg.get('auto_run') is not None:
                self.auto_enabled = bool(cfg.get('auto_run'))
        except Exception as e:
            print('[PortfolioScheduler] 恢复 auto_run 失败: %s' % str(e))

    def _run_loop(self):
        """后台循环：检查是否需要自动运行"""
        while self.running:
            try:
                now = datetime.datetime.now()
                today = now.strftime('%Y%m%d')
                self._maybe_run_daily_db_update(now, today)

                if self.auto_enabled and not self.current_session:
                    try:
                        from AlphaFin.ai_team.core.agent_registry import has_active_workflow
                        if has_active_workflow({'user_ask'}):
                            time.sleep(5)
                            continue
                    except Exception:
                        pass
                    if self._is_research_busy():
                        time.sleep(5)
                        continue
                    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
                    is_trade_day = pm._is_trade_date(today)
                    watch_trade_day = is_trade_day or self._fallback_watch_trade_day(now, today)

                    # 盘中盯盘：交易日 09:00-15:00，按固定间隔执行
                    if self.watch_enabled and watch_trade_day and self._in_watch_window(now):
                        if self.last_watch_date != today:
                            self.last_watch_date = today
                            self.last_watch_time = 0
                        if time.time() - self.last_watch_time >= self.watch_interval:
                            bus.post_activity('portfolio', 'status', '盘中自动盯盘启动')
                            self.run_market_watch_cycle(today)
                            continue

                    # 日终投资周期必须使用严格交易日判定，避免非交易日结算
                    if not is_trade_day:
                        time.sleep(30)
                        continue

                    # 收盘后自动执行完整投资周期（如果今天还没跑）
                    if self._is_after_auto_run_time(now) and today != self.last_run_date:
                        bus.post_activity('portfolio', 'status', '自动投资周期启动')
                        self.run_investment_cycle(today)
                time.sleep(30)
            except Exception as e:
                print('[PortfolioScheduler] 循环异常: %s' % str(e))
                traceback.print_exc()
                time.sleep(60)

    @staticmethod
    def _session_stop_requested(session_id):
        return bool(session_id) and is_session_cancelled(session_id)

    @staticmethod
    def _session_timing(session_id):
        return get_session_timing(session_id)

    @staticmethod
    def _session_progress(session_id):
        return get_session_progress(session_id)

    @staticmethod
    def _session_overtime(session_id):
        return get_session_overtime_state(session_id)

    @staticmethod
    def _session_should_converge(session_id):
        return bool(session_id) and (
            is_session_converging(session_id) or is_session_expired(session_id)
        )

    @staticmethod
    def _is_research_busy():
        try:
            from AlphaFin.ai_team.core.orchestrator import orchestrator
            return bool(orchestrator.current_session)
        except Exception:
            return False

    def _start_session_deadline(self, session_id, workflow, title='', time_limit_seconds=None, source='workflow'):
        seconds = max(0, int(time_limit_seconds or 0))
        if seconds <= 0:
            clear_session_deadline(session_id)
            return {'active': False, 'session_id': session_id or ''}
        timing = set_session_deadline(
            session_id,
            seconds,
            workflow=workflow,
            title=title,
            source=source,
            soft_ratio=WORKFLOW_DEADLINE_SOFT_RATIO,
        )
        bus.post_activity(
            'portfolio', 'status',
            '任务时限已设置：%d分钟（session=%s）' % (max(1, int(seconds / 60)), session_id),
            metadata={
                'session_id': session_id or '',
                'mode': workflow or '',
                'phase': 'session_deadline_started',
                'session_timing': timing,
            }
        )
        return timing

    def _clear_session_deadline(self, session_id):
        if session_id:
            clear_session_deadline(session_id)
        self._deadline_notices.discard(str(session_id or ''))

    def _set_session_progress(
            self,
            session_id,
            workflow,
            steps,
            current_index,
            current_step,
            detail='',
            state='running',
            actor='portfolio',
            title=''
    ):
        return set_session_progress(
            session_id=session_id,
            workflow=workflow,
            title=title,
            steps=steps,
            current_index=current_index,
            current_step=current_step,
            detail=detail,
            state=state,
            actor=actor,
        )

    def _clear_session_progress(self, session_id):
        if session_id:
            clear_session_progress(session_id)

    def _request_session_overtime(self, session_id, workflow, title='', message='', default_extend_seconds=300):
        return request_session_overtime_decision(
            session_id=session_id,
            workflow=workflow,
            title=title,
            message=message,
            default_extend_seconds=default_extend_seconds,
        )

    def _handle_session_expired(self, session_id, workflow, title=''):
        timing = self._session_timing(session_id)
        if not timing.get('is_expired'):
            return 'continue'
        overtime_message = '任务已达到设定时限，请选择继续等待，或立即停止任务。'
        overtime = self._request_session_overtime(
            session_id=session_id,
            workflow=workflow,
            title=title,
            message=overtime_message,
            default_extend_seconds=300,
        )
        self._set_session_progress(
            session_id=session_id,
            workflow=workflow,
            title=title,
            steps=(self._session_progress(session_id).get('steps') or []),
            current_index=self._session_progress(session_id).get('current_index') or 0,
            current_step=self._session_progress(session_id).get('current_step') or '等待用户决策',
            detail='已达到时限，等待用户选择继续等待或立即停止。',
            state='waiting_user',
            actor='portfolio',
        )
        if overtime.get('waiting'):
            bus.post_activity(
                'portfolio', 'status',
                '任务达到时限，等待用户决定：继续等待或立即停止。',
                metadata={
                    'session_id': session_id or '',
                    'mode': workflow or '',
                    'phase': 'session_overtime_waiting',
                    'session_timing': timing,
                    'session_progress': self._session_progress(session_id),
                    'session_overtime': overtime,
                }
            )
        recover_notice_sent = False
        while True:
            if self._session_stop_requested(session_id):
                return 'stop'
            state = get_session_overtime_state(session_id)
            if not state.get('active'):
                state = self._request_session_overtime(
                    session_id=session_id,
                    workflow=workflow,
                    title=title,
                    message=overtime_message,
                    default_extend_seconds=300,
                )
                if state.get('waiting') and not recover_notice_sent:
                    bus.post_activity(
                        'portfolio', 'status',
                        '超时决策状态异常已自动恢复，等待用户决定：继续等待或立即停止。',
                        metadata={
                            'session_id': session_id or '',
                            'mode': workflow or '',
                            'phase': 'session_overtime_recovered',
                            'session_timing': self._session_timing(session_id),
                            'session_progress': self._session_progress(session_id),
                            'session_overtime': state,
                        }
                    )
                    recover_notice_sent = True
                time.sleep(0.5)
                continue
            if state.get('waiting'):
                time.sleep(0.5)
                continue
            decision = str(state.get('decision') or '').strip().lower()
            extend_seconds = int(state.get('extend_seconds') or 0)
            clear_session_overtime_state(session_id)
            if decision == 'extend':
                try:
                    from AlphaFin.ai_team.core.agent_registry import clear_stop_all_agents
                    clear_stop_all_agents()
                except Exception:
                    pass
                bus.post_activity(
                    'portfolio', 'status',
                    '用户选择继续等待 %d 分钟，任务继续推进。' % max(1, int(extend_seconds / 60)),
                    metadata={
                        'session_id': session_id or '',
                        'mode': workflow or '',
                        'phase': 'session_overtime_extended',
                        'session_timing': self._session_timing(session_id),
                    }
                )
                return 'continue'
            if decision == 'stop':
                try:
                    from AlphaFin.ai_team.core.agent_registry import request_stop_agents_for_session
                    request_stop_agents_for_session(session_id, reason='用户在超时面板选择停止任务')
                except Exception:
                    pass
                cancel_session(session_id, reason='用户在超时面板选择停止任务')
                clear_session_deadline(session_id)
                bus.post_activity(
                    'portfolio', 'status',
                    '用户选择停止当前任务，系统已下发终止指令。',
                    metadata={
                        'session_id': session_id or '',
                        'mode': workflow or '',
                        'phase': 'session_overtime_stop',
                        'session_timing': self._session_timing(session_id),
                    }
                )
                return 'stop'
            if decision == 'summarize':
                try:
                    from AlphaFin.ai_team.core.agent_registry import request_stop_agents_for_session, clear_stop_agents
                    request_stop_agents_for_session(session_id, reason='用户要求立即汇总', exclude=['director'])
                    clear_stop_agents(['director'])
                except Exception:
                    pass
                # 用户已明确选择“立即汇总”，避免再次命中超时门控
                clear_session_deadline(session_id)
                bus.post_activity(
                    'portfolio', 'status',
                    '用户选择立即汇总，系统将基于当前已完成结果继续推进关键环节。',
                    metadata={
                        'session_id': session_id or '',
                        'mode': workflow or '',
                        'phase': 'session_overtime_summarize',
                        'session_timing': self._session_timing(session_id),
                    }
                )
                return 'summarize'

    def _maybe_announce_session_converging(self, session_id, mode):
        sid = str(session_id or '')
        if not sid:
            return {'active': False, 'session_id': ''}
        timing = self._session_timing(sid)
        if not timing.get('active'):
            return timing
        if timing.get('state') == 'running':
            self._deadline_notices.discard(sid)
            return timing
        if sid in self._deadline_notices:
            return timing
        self._deadline_notices.add(sid)
        bus.post_activity(
            'portfolio', 'status',
            '任务进入时间收敛阶段：剩余 %d 秒，将优先完成关键流程。' % int(timing.get('remaining_seconds') or 0),
            metadata={
                'session_id': sid,
                'mode': mode or '',
                'phase': 'session_converging',
                'session_timing': timing,
            }
        )
        return timing

    def _post_session_stopped(self, session_id, mode, title='任务已停止'):
        reason = get_session_cancel_reason(session_id, '用户手动停止')
        bus.post_activity(
            'portfolio', 'status',
            '%s：%s' % (title, reason),
            metadata={
                'session_id': session_id or '',
                'mode': mode or '',
                'phase': 'session_stopped',
                'reason': reason,
            }
        )
        return reason

    def _is_after_db_update_time(self, now):
        cur = now.hour * 60 + now.minute
        target = self._db_update_hour * 60 + self._db_update_min
        return cur >= target

    def _maybe_run_daily_db_update(self, now, today):
        """每天固定时刻自动更新数据库（默认 06:00）"""
        if not self.db_auto_update_enabled:
            return
        if self.db_update_running:
            return
        if self.last_db_update_date == today:
            return
        if not self._is_after_db_update_time(now):
            return

        self.last_db_update_date = today
        self.db_update_running = True
        task_id = 'auto_db_' + str(uuid.uuid4())[:8]

        def _job():
            try:
                from AlphaFin.services.update_service import run_update
                bus.post_activity('portfolio', 'status',
                                  '06:00 自动更新数据库启动（task=%s）' % task_id)
                run_update(task_id, include_fina=False)
                bus.post_activity('portfolio', 'status',
                                  '06:00 自动更新数据库完成（task=%s）' % task_id)
            except Exception as e:
                bus.post_activity('portfolio', 'error',
                                  '06:00 自动更新数据库失败: %s' % str(e))
            finally:
                self.db_update_running = False

        t = threading.Thread(target=_job, daemon=True)
        t.start()

    def run_market_watch_cycle(self, trade_date=None, time_limit_seconds=None):
        """
        执行一轮盘中盯盘（交易时段）。

        目标：
        1. 多位智能体协同盯盘，跟踪实时风险与机会
        2. 发现机会时提交交易信号
        3. 自动衔接风控审核与总监审批，避免与投资模块冲突
        """
        from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
        from AlphaFin.ai_team.core.agent_registry import get_agent

        try:
            from AlphaFin.ai_team.core.agent_registry import clear_stop_all_agents
            clear_stop_all_agents()
        except Exception:
            pass
        config = pm.get_config()
        if not config:
            bus.post_activity('portfolio', 'error', '投资组合未初始化，无法执行盘中盯盘')
            return {'success': False, 'message': '投资组合未初始化'}

        if not trade_date:
            trade_date = datetime.datetime.now().strftime('%Y%m%d')

        session_id = 'watch_' + str(uuid.uuid4())[:8]
        clear_session_cancel(session_id)
        self.current_session = session_id
        self.last_watch_time = time.time()
        self.last_watch_date = trade_date
        progress_steps = ['执行已批准信号', '盘中扫描', '团队会议', '风控审核', '总监简报']
        self._start_session_deadline(
            session_id,
            'market_watch',
            title='盘中盯盘',
            time_limit_seconds=time_limit_seconds,
            source='manual_watch' if time_limit_seconds else 'auto_watch'
        )
        self._set_session_progress(
            session_id=session_id,
            workflow='market_watch',
            title='盘中盯盘',
            steps=progress_steps,
            current_index=1,
            current_step='执行已批准信号',
            detail='系统正在先处理已批准但尚未执行的交易信号。',
            state='running',
            actor='portfolio',
        )

        now_str = datetime.datetime.now().strftime('%H:%M:%S')
        bus.post_activity(
            'portfolio', 'status',
            '盘中盯盘启动 (%s %s, session=%s)' % (trade_date, now_str, session_id)
        )

        try:
            if self._session_stop_requested(session_id):
                self._post_session_stopped(session_id, 'market_watch')
                return {'success': False, 'message': '当前任务已停止'}
            # 盘中先尝试执行已批准信号，避免交易积压
            results = pm.execute_approved_signals(trade_date)
            for r in results:
                status = 'status' if r.get('success') else 'error'
                bus.post_activity('portfolio', status, r.get('message', '盘中执行结果未知'))

            self._set_session_progress(
                session_id=session_id,
                workflow='market_watch',
                title='盘中盯盘',
                steps=progress_steps,
                current_index=2,
                current_step='盘中扫描',
                detail='智能体正在执行盘中盯盘与实时信息扫描。',
                state='running',
                actor='portfolio',
            )
            self._run_market_watch(session_id, trade_date, config)
            if self._session_stop_requested(session_id):
                self._post_session_stopped(session_id, 'market_watch')
                return {'success': False, 'message': '当前任务已停止'}
            deadline_action = self._handle_session_expired(session_id, 'market_watch', title='盘中盯盘')
            if deadline_action == 'stop':
                self._post_session_stopped(session_id, 'market_watch')
                return {'success': False, 'message': '当前任务已停止'}
            if self._session_should_converge(session_id):
                self._maybe_announce_session_converging(session_id, 'market_watch')
            elif deadline_action != 'summarize':
                self._set_session_progress(
                    session_id=session_id,
                    workflow='market_watch',
                    title='盘中盯盘',
                    steps=progress_steps,
                    current_index=3,
                    current_step='团队会议',
                    detail='团队正在进行盘中快速讨论与结论对齐。',
                    state='running',
                    actor='director',
                )
                self._run_portfolio_meeting(session_id, trade_date, stage='watch')
                if self._session_stop_requested(session_id):
                    self._post_session_stopped(session_id, 'market_watch')
                    return {'success': False, 'message': '当前任务已停止'}

            pending = pm.get_pending_signals(status='pending_risk')
            if pending:
                self._set_session_progress(
                    session_id=session_id,
                    workflow='market_watch',
                    title='盘中盯盘',
                    steps=progress_steps,
                    current_index=4,
                    current_step='风控审核',
                    detail='风控官正在审核盘中新增信号。',
                    state='running',
                    actor='risk',
                )
                bus.post_activity('portfolio', 'status', '盘中风控审核 %d 条信号' % len(pending))
                self._risk_review_phase(session_id, trade_date, pending)

            pending_dir = pm.get_pending_signals(status='pending_director')
            if pending_dir:
                discussion_map = {}
                disputed = [s for s in pending_dir if int(s.get('risk_approved') or 0) == 0]
                if disputed:
                    bus.post_activity('portfolio', 'status',
                                      '风控分歧讨论 %d 条信号' % len(disputed))
                    discussion_map = self._risk_discussion_phase(session_id, trade_date, disputed)
                bus.post_activity('portfolio', 'status', '盘中总监审批 %d 条信号' % len(pending_dir))
                self._director_approval_phase(session_id, trade_date, pending_dir, discussion_map=discussion_map)

            director = get_agent('director')
            if director:
                self._set_session_progress(
                    session_id=session_id,
                    workflow='market_watch',
                    title='盘中盯盘',
                    steps=progress_steps,
                    current_index=5,
                    current_step='总监简报',
                    detail='决策总监正在生成盘中指挥简报。',
                    state='running',
                    actor='director',
                )
                summary_prompt = format_ai_team_prompt(
                    'portfolio.market_watch.summary',
                    trade_date=trade_date
                )
                reply = director.think(summary_prompt, session_id=session_id)
                bus.save_result(session_id, 'director_watch', reply)
            self._set_session_progress(
                session_id=session_id,
                workflow='market_watch',
                title='盘中盯盘',
                steps=progress_steps,
                current_index=5,
                current_step='总监简报',
                detail='盘中指挥简报已生成。',
                state='completed',
                actor='director',
            )

            return {'success': True, 'trade_date': trade_date, 'session': session_id}
        except Exception as e:
            bus.post_activity('portfolio', 'error', '盘中盯盘异常: %s' % str(e))
            traceback.print_exc()
            return {'success': False, 'message': str(e)}
        finally:
            self.current_session = None
            self._clear_session_deadline(session_id)
            clear_session_overtime_state(session_id)
            self._clear_session_progress(session_id)
            bus.post_activity('portfolio', 'status', '盘中盯盘结束')

    def run_investment_cycle(self, trade_date=None, time_limit_seconds=None):
        """
        执行一轮完整投资决策。

        流程：
        1. 确定当前交易日
        2. 补算缺失净值
        3. 执行已批准的交易信号
        4. 分析与信号生成
        5. 风控审核
        6. 总监审批
        7. 收盘结算
        """
        from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
        from AlphaFin.ai_team.core.agent_registry import get_agent

        try:
            from AlphaFin.ai_team.core.agent_registry import clear_stop_all_agents
            clear_stop_all_agents()
        except Exception:
            pass
        config = pm.get_config()
        if not config:
            bus.post_activity('portfolio', 'error', '投资组合未初始化')
            return {'success': False, 'message': '投资组合未初始化'}

        if not trade_date:
            trade_date = datetime.datetime.now().strftime('%Y%m%d')

        session_id = 'inv_' + str(uuid.uuid4())[:8]
        clear_session_cancel(session_id)
        self.current_session = session_id
        self.last_run_date = trade_date
        progress_steps = ['补算净值', '执行已批准信号', '分析与监控', '团队讨论', '风控审核', '总监审批', '每日结算']
        self._start_session_deadline(
            session_id,
            'portfolio_investment',
            title='投资执行',
            time_limit_seconds=time_limit_seconds,
            source='manual_investment' if time_limit_seconds else 'auto_investment'
        )
        self._set_session_progress(
            session_id=session_id,
            workflow='portfolio_investment',
            title='投资执行',
            steps=progress_steps,
            current_index=1,
            current_step='补算净值',
            detail='系统正在补算缺失净值并准备进入投资分析。',
            state='running',
            actor='portfolio',
        )

        bus.post_activity('portfolio', 'status',
                          '投资周期启动 (日期=%s, session=%s)' % (trade_date, session_id))

        try:
            if self._session_stop_requested(session_id):
                self._post_session_stopped(session_id, 'portfolio_investment')
                return {'success': False, 'message': '当前任务已停止'}
            # ── 步骤1: 补算缺失净值 ──
            if config.get('start_date'):
                filled = pm.backfill_nav(config['start_date'], trade_date)
                if filled > 0:
                    bus.post_activity('portfolio', 'status', '补算了 %d 个交易日的净值' % filled)

            # ── 步骤2: 执行昨日已批准的信号 ──
            self._set_session_progress(
                session_id=session_id,
                workflow='portfolio_investment',
                title='投资执行',
                steps=progress_steps,
                current_index=2,
                current_step='执行已批准信号',
                detail='系统正在执行昨日已批准但尚未成交的信号。',
                state='running',
                actor='portfolio',
            )
            results = pm.execute_approved_signals(trade_date)
            for r in results:
                status = 'status' if r.get('success') else 'error'
                bus.post_activity('portfolio', status, r['message'])

            # ── 步骤3: 判断分析深度 ──
            self._set_session_progress(
                session_id=session_id,
                workflow='portfolio_investment',
                title='投资执行',
                steps=progress_steps,
                current_index=3,
                current_step='分析与监控',
                detail='系统正在判断采用深度分析还是常规监控。',
                state='running',
                actor='portfolio',
            )
            deep = self._should_deep_analyze(pm, trade_date)
            if deep and self._session_should_converge(session_id):
                self._maybe_announce_session_converging(session_id, 'portfolio_investment')
                deep = False

            if deep:
                bus.post_activity('portfolio', 'status', '触发深度分析')
                self._run_deep_analysis(session_id, trade_date, config)
            else:
                bus.post_activity('portfolio', 'status', '执行常规监控')
                self._run_routine_check(session_id, trade_date, config)
            if self._session_stop_requested(session_id):
                self._post_session_stopped(session_id, 'portfolio_investment')
                return {'success': False, 'message': '当前任务已停止'}
            deadline_action = self._handle_session_expired(session_id, 'portfolio_investment', title='投资执行')
            if deadline_action == 'stop':
                self._post_session_stopped(session_id, 'portfolio_investment')
                return {'success': False, 'message': '当前任务已停止'}
            if self._session_should_converge(session_id):
                self._maybe_announce_session_converging(session_id, 'portfolio_investment')
            elif deadline_action != 'summarize':
                self._set_session_progress(
                    session_id=session_id,
                    workflow='portfolio_investment',
                    title='投资执行',
                    steps=progress_steps,
                    current_index=4,
                    current_step='团队讨论',
                    detail='团队正在对分析结果进行讨论与分歧对齐。',
                    state='running',
                    actor='director',
                )
                self._run_portfolio_meeting(session_id, trade_date, stage='investment')
                if self._session_stop_requested(session_id):
                    self._post_session_stopped(session_id, 'portfolio_investment')
                    return {'success': False, 'message': '当前任务已停止'}

            # ── 步骤4: 风控审核 ──
            pending = pm.get_pending_signals(status='pending_risk')
            if pending:
                self._set_session_progress(
                    session_id=session_id,
                    workflow='portfolio_investment',
                    title='投资执行',
                    steps=progress_steps,
                    current_index=5,
                    current_step='风控审核',
                    detail='风控官正在审核待处理交易信号。',
                    state='running',
                    actor='risk',
                )
                bus.post_activity('portfolio', 'status',
                                  '风控审核 %d 条信号' % len(pending))
                self._risk_review_phase(session_id, trade_date, pending)

            # ── 步骤5: 总监审批 ──
            pending_dir = pm.get_pending_signals(status='pending_director')
            if pending_dir:
                self._set_session_progress(
                    session_id=session_id,
                    workflow='portfolio_investment',
                    title='投资执行',
                    steps=progress_steps,
                    current_index=6,
                    current_step='总监审批',
                    detail='决策总监正在审批最终交易信号。',
                    state='running',
                    actor='director',
                )
                discussion_map = {}
                disputed = [s for s in pending_dir if int(s.get('risk_approved') or 0) == 0]
                if disputed:
                    bus.post_activity('portfolio', 'status',
                                      '风控分歧讨论 %d 条信号' % len(disputed))
                    discussion_map = self._risk_discussion_phase(session_id, trade_date, disputed)
                bus.post_activity('portfolio', 'status',
                                  '总监审批 %d 条信号' % len(pending_dir))
                self._director_approval_phase(session_id, trade_date, pending_dir, discussion_map=discussion_map)

            # ── 步骤6: 验证风险预警 ──
            pm.verify_risk_warnings(trade_date)

            # ── 步骤7: 每日结算 ──
            self._set_session_progress(
                session_id=session_id,
                workflow='portfolio_investment',
                title='投资执行',
                steps=progress_steps,
                current_index=7,
                current_step='每日结算',
                detail='系统正在执行结算并更新净值表现。',
                state='running',
                actor='portfolio',
            )
            settlement = pm.daily_settlement(trade_date)
            if settlement.get('success'):
                bus.post_activity('portfolio', 'status',
                                  '每日结算完成: 净值=%.4f, 日收益=%.2f%%, 最大回撤=%.2f%%' % (
                                      settlement['nav'], settlement['daily_return'],
                                      settlement['max_drawdown']))
            else:
                bus.post_activity('portfolio', 'status',
                                  '结算: %s' % settlement.get('message', ''))

            # ── 步骤8: 月末奖惩 ──
            if trade_date[6:8] >= '28':
                year_month = trade_date[:6]
                comp = pm.calculate_monthly_compensation(year_month)
                if comp.get('month_return') is not None:
                    bus.post_activity('portfolio', 'status',
                                      '月度结算: 收益%.2f%%' % comp['month_return'])

            self._set_session_progress(
                session_id=session_id,
                workflow='portfolio_investment',
                title='投资执行',
                steps=progress_steps,
                current_index=7,
                current_step='每日结算',
                detail='投资执行主流程已完成。',
                state='completed',
                actor='portfolio',
            )
            return {'success': True, 'trade_date': trade_date, 'session': session_id}

        except Exception as e:
            bus.post_activity('portfolio', 'error', '投资周期异常: %s' % str(e))
            traceback.print_exc()
            return {'success': False, 'message': str(e)}

        finally:
            self.current_session = None
            self._clear_session_deadline(session_id)
            clear_session_overtime_state(session_id)
            self._clear_session_progress(session_id)
            bus.post_activity('portfolio', 'status', '投资周期结束')

    def _should_deep_analyze(self, pm, trade_date):
        """判断是否需要深度分析"""
        positions = pm.get_positions()

        # 空仓 → 深度（首次建仓）
        if not positions:
            return True

        # 月初 → 深度
        if trade_date[6:8] <= '03':
            return True

        # 持仓个股触发止损线
        for p in positions:
            current_price = pm._get_close_price(p['ts_code'], trade_date)
            if current_price and p['cost_price']:
                loss = (current_price - p['cost_price']) / p['cost_price']
                if loss < -0.05:
                    return True

        # 持仓超过20个交易日
        for p in positions:
            held_days = len(pm._get_trade_dates(p['buy_date'], trade_date))
            if held_days >= 20:
                return True

        return False

    def _run_routine_check(self, session_id, trade_date, config):
        """常规日：轻量级持仓监控"""
        from AlphaFin.ai_team.core.agent_registry import get_agent
        from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm

        status_text = self._build_portfolio_context(pm, config)

        # 量化师检查技术信号
        quant = get_agent('quant')
        if quant:
            prompt = format_ai_team_prompt(
                'portfolio.routine.quant',
                trade_date=trade_date,
                status_text=status_text
            )
            try:
                if self._session_stop_requested(session_id):
                    return
                reply = quant.think(prompt, session_id=session_id)
                bus.save_result(session_id, 'quant_routine', reply)
            except Exception as e:
                bus.post_activity('quant', 'error', '常规检查失败: %s' % str(e))

        # 资产重组专家检查事件催化变化
        restructuring = get_agent('restructuring')
        if restructuring:
            prompt = format_ai_team_prompt(
                'portfolio.routine.restructuring',
                trade_date=trade_date,
                status_text=status_text
            )
            try:
                if self._session_stop_requested(session_id):
                    return
                reply = restructuring.think(prompt, session_id=session_id)
                bus.save_result(session_id, 'restructuring_routine', reply)
            except Exception as e:
                bus.post_activity('restructuring', 'error', '重组跟踪失败: %s' % str(e))

        # 风控官检查风险
        risk = get_agent('risk')
        if risk:
            prompt = format_ai_team_prompt(
                'portfolio.routine.risk',
                trade_date=trade_date,
                status_text=status_text
            )
            try:
                if self._session_stop_requested(session_id):
                    return
                reply = risk.think(prompt, session_id=session_id)
                bus.save_result(session_id, 'risk_routine', reply)
            except Exception as e:
                bus.post_activity('risk', 'error', '风控检查失败: %s' % str(e))

    def _run_market_watch(self, session_id, trade_date, config):
        """交易时段的全员盯盘协作（快速版）"""
        from AlphaFin.ai_team.core.agent_registry import get_agent
        from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm

        status_text = self._build_portfolio_context(pm, config)
        mode_desc = '自由选股模式' if config['mode'] == 'free' else '指定标的: %s' % config['target_code']

        prompts = {
            'intel': format_ai_team_prompt(
                'portfolio.market_watch.intel',
                trade_date=trade_date,
                mode_desc=mode_desc,
                status_text=status_text
            ),
            'quant': format_ai_team_prompt(
                'portfolio.market_watch.quant',
                trade_date=trade_date,
                mode_desc=mode_desc,
                status_text=status_text
            ),
            'analyst': format_ai_team_prompt(
                'portfolio.market_watch.analyst',
                trade_date=trade_date,
                mode_desc=mode_desc,
                status_text=status_text
            ),
            'restructuring': format_ai_team_prompt(
                'portfolio.market_watch.restructuring',
                trade_date=trade_date,
                mode_desc=mode_desc,
                status_text=status_text
            ),
            'risk': format_ai_team_prompt(
                'portfolio.market_watch.risk',
                trade_date=trade_date,
                status_text=status_text
            ),
            'auditor': format_ai_team_prompt(
                'portfolio.market_watch.auditor',
                trade_date=trade_date,
                status_text=status_text
            ),
        }

        threads = []
        errors = {}

        def _agent_watch(agent_id, prompt):
            try:
                if self._session_stop_requested(session_id):
                    return
                agent = get_agent(agent_id)
                if not agent:
                    return
                reply = agent.think(prompt, session_id=session_id)
                bus.save_result(session_id, agent_id + '_watch', reply)
            except Exception as e:
                errors[agent_id] = str(e)
                bus.post_activity(agent_id, 'error', '盘中盯盘失败: %s' % str(e))

        for aid in ('intel', 'quant', 'analyst', 'restructuring', 'risk', 'auditor'):
            t = threading.Thread(target=_agent_watch, args=(aid, prompts[aid]), daemon=True)
            threads.append(t)
            t.start()

        for t in threads:
            waited = 0.0
            while t.is_alive() and waited < 300:
                if self._session_stop_requested(session_id):
                    break
                if is_session_expired(session_id):
                    try:
                        from AlphaFin.ai_team.core.agent_registry import request_stop_agents_for_session
                        request_stop_agents_for_session(
                            session_id,
                            reason='当前阶段达到时限，等待用户决策'
                        )
                    except Exception:
                        pass
                    self._request_session_overtime(
                        session_id=session_id,
                        workflow='market_watch',
                        title='盘中盯盘',
                        message='盘中盯盘已达到时限，请选择继续等待，或立即停止任务。',
                        default_extend_seconds=300,
                    )
                    bus.post_activity(
                        'portfolio', 'status',
                        '盘中盯盘达到时限，暂停推进并等待用户决策。',
                        metadata={
                            'session_id': session_id or '',
                            'mode': 'market_watch',
                            'phase': 'session_overtime_waiting',
                            'session_timing': self._session_timing(session_id),
                            'session_overtime': self._session_overtime(session_id),
                        }
                    )
                    break
                t.join(timeout=0.5)
                waited += 0.5
            if is_session_expired(session_id):
                break

        if errors:
            bus.post_activity('portfolio', 'error', '盘中盯盘部分失败: %s' % str(errors))

    def _run_deep_analysis(self, session_id, trade_date, config):
        """深度分析：完整团队讨论"""
        from AlphaFin.ai_team.core.agent_registry import get_agent
        from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm

        status_text = self._build_portfolio_context(pm, config)
        mode_desc = '自由选股模式' if config['mode'] == 'free' else '指定标的: %s' % config['target_code']

        # ── Phase 1: 策略组并行工作 ──
        threads = []
        errors = {}

        def _agent_analyze(agent_id, prompt):
            try:
                if self._session_stop_requested(session_id):
                    return
                agent = get_agent(agent_id)
                if agent:
                    reply = agent.think(prompt, session_id=session_id)
                    bus.save_result(session_id, agent_id, reply)
                    bus.post_activity(agent_id, 'speaking', reply[:200])
            except Exception as e:
                errors[agent_id] = str(e)
                bus.post_activity(agent_id, 'error', '分析失败: %s' % str(e))

        if config['mode'] == 'free':
            # 自由模式：情报员选行业，量化师筛股，分析师深度分析
            intel_prompt = format_ai_team_prompt(
                'portfolio.deep.free.intel',
                trade_date=trade_date,
                mode_desc=mode_desc,
                status_text=status_text
            )

            quant_prompt = format_ai_team_prompt(
                'portfolio.deep.free.quant',
                trade_date=trade_date,
                mode_desc=mode_desc,
                status_text=status_text
            )

            analyst_prompt = format_ai_team_prompt(
                'portfolio.deep.free.analyst',
                trade_date=trade_date,
                mode_desc=mode_desc,
                status_text=status_text
            )

            restructuring_prompt = format_ai_team_prompt(
                'portfolio.deep.free.restructuring',
                trade_date=trade_date,
                mode_desc=mode_desc,
                status_text=status_text
            )
        else:
            # 指定模式：围绕标的做择时
            target = config['target_code']
            intel_prompt = format_ai_team_prompt(
                'portfolio.deep.target.intel',
                trade_date=trade_date,
                target=target,
                status_text=status_text
            )

            quant_prompt = format_ai_team_prompt(
                'portfolio.deep.target.quant',
                trade_date=trade_date,
                target=target,
                status_text=status_text
            )

            analyst_prompt = format_ai_team_prompt(
                'portfolio.deep.target.analyst',
                trade_date=trade_date,
                target=target,
                status_text=status_text
            )

            restructuring_prompt = format_ai_team_prompt(
                'portfolio.deep.target.restructuring',
                trade_date=trade_date,
                target=target,
                status_text=status_text
            )

        for aid, prompt in [
            ('intel', intel_prompt),
            ('quant', quant_prompt),
            ('analyst', analyst_prompt),
            ('restructuring', restructuring_prompt),
        ]:
            t = threading.Thread(target=_agent_analyze, args=(aid, prompt), daemon=True)
            threads.append(t)
            t.start()

        for t in threads:
            waited = 0.0
            while t.is_alive() and waited < 600:
                if self._session_stop_requested(session_id):
                    break
                if is_session_expired(session_id):
                    try:
                        from AlphaFin.ai_team.core.agent_registry import request_stop_agents_for_session
                        request_stop_agents_for_session(
                            session_id,
                            reason='当前阶段达到时限，等待用户决策'
                        )
                    except Exception:
                        pass
                    self._request_session_overtime(
                        session_id=session_id,
                        workflow='portfolio_investment',
                        title='投资执行',
                        message='投资分析已达到时限，请选择继续等待，或立即停止任务。',
                        default_extend_seconds=300,
                    )
                    bus.post_activity(
                        'portfolio', 'status',
                        '投资分析达到时限，暂停推进并等待用户决策。',
                        metadata={
                            'session_id': session_id or '',
                            'mode': 'portfolio_investment',
                            'phase': 'session_overtime_waiting',
                            'session_timing': self._session_timing(session_id),
                            'session_overtime': self._session_overtime(session_id),
                        }
                    )
                    break
                t.join(timeout=0.5)
                waited += 0.5
            if is_session_expired(session_id):
                break

    def _run_portfolio_meeting(self, session_id, trade_date, stage='investment'):
        """投资流程会议：把并行结果拉进会议室二次对齐，再进入审核/审批。"""
        from AlphaFin.ai_team.core.agent_registry import get_agent

        if self._session_stop_requested(session_id):
            self._post_session_stopped(session_id, 'market_watch' if stage == 'watch' else 'portfolio_investment', title='会议已取消')
            return
        if self._session_should_converge(session_id):
            self._maybe_announce_session_converging(session_id, 'market_watch' if stage == 'watch' else 'portfolio_investment')
            return

        specialists = [aid for aid in self.specialists_all if get_agent(aid)]
        director = get_agent('director')
        participants = list(specialists)
        if director:
            participants.append('director')
        if len(participants) < 2:
            return

        rounds = 1 if stage == 'watch' else 2
        mode = 'market_watch' if stage == 'watch' else 'portfolio_investment'
        topic = '盘中盯盘' if stage == 'watch' else '投资决策'
        transcript = []

        bus.broadcast(
            'portfolio', 'status',
            '%s会议开始：团队进入会议室讨论' % topic,
            metadata={
                'phase': 'meeting_start',
                'title': '%s会议' % topic,
                'mode': mode,
                'session_id': session_id,
                'meeting_topic': '%s %s' % (trade_date, topic),
                'meeting_active': True,
                'participants': participants,
                'meeting_order': specialists,
                'meeting_round_total': rounds,
                'rounds': rounds,
            }
        )
        bus.post_activity(
            'portfolio', 'status',
            '%s会议发言顺序：%s；最终由 director 总结' % (topic, ' -> '.join(specialists)),
            metadata={
                'phase': 'meeting_order',
                'title': '%s会议顺序' % topic,
                'mode': mode,
                'session_id': session_id,
                'meeting_topic': '%s %s' % (trade_date, topic),
                'meeting_active': True,
                'participants': participants,
                'meeting_order': specialists,
                'meeting_round_total': rounds,
            }
        )

        for round_no in range(1, rounds + 1):
            if self._session_stop_requested(session_id):
                self._post_session_stopped(session_id, mode, title='%s会议已停止' % topic)
                break
            if self._session_should_converge(session_id):
                self._maybe_announce_session_converging(session_id, mode)
                break
            bus.post_activity(
                'portfolio', 'status',
                '%s会议第%d轮开始' % (topic, round_no),
                metadata={
                    'phase': 'meeting_round',
                    'title': '%s会议第%d轮' % (topic, round_no),
                    'mode': mode,
                    'session_id': session_id,
                    'meeting_topic': '%s %s' % (trade_date, topic),
                    'meeting_active': True,
                    'participants': participants,
                    'meeting_order': specialists,
                    'meeting_round': round_no,
                    'meeting_round_total': rounds,
                }
            )
            for seat_index, agent_id in enumerate(specialists):
                if self._session_stop_requested(session_id):
                    self._post_session_stopped(session_id, mode, title='%s会议已停止' % topic)
                    break
                if self._session_should_converge(session_id):
                    self._maybe_announce_session_converging(session_id, mode)
                    break
                agent = get_agent(agent_id)
                if not agent:
                    continue
                bus.post_activity(
                    'portfolio', 'status',
                    '%s会议第%d轮发言：%s（%d/%d）' % (
                        topic, round_no, agent_id, seat_index + 1, len(specialists)
                    ),
                    metadata={
                        'phase': 'meeting_turn',
                        'title': '%s会议发言顺序' % topic,
                        'mode': mode,
                        'session_id': session_id,
                        'meeting_topic': '%s %s' % (trade_date, topic),
                        'meeting_active': True,
                        'participants': participants,
                        'meeting_order': specialists,
                        'meeting_round': round_no,
                        'meeting_round_total': rounds,
                        'meeting_speaker': agent_id,
                        'meeting_speaker_seq': seat_index + 1,
                        'meeting_speaker_total': len(specialists),
                    }
                )
                agent.process_incoming_messages(session_id)

                results = bus.get_session_results(session_id)
                context_rows = []
                for rid, rcontent in results.items():
                    text = (rcontent or '').replace('\n', ' ').strip()
                    if text:
                        context_rows.append('【%s】%s' % (rid, text[:220]))
                context = '\n'.join(context_rows[-12:]) if context_rows else '暂无前置成果'
                history = '\n'.join(transcript[-8:]) if transcript else '（暂无前序发言）'

                prompt = format_ai_team_prompt(
                    'portfolio.meeting_turn',
                    topic=topic,
                    trade_date=trade_date,
                    round_no=round_no,
                    rounds=rounds,
                    context=context,
                    history=history
                )
                try:
                    if self._session_stop_requested(session_id):
                        break
                    reply = agent.think(prompt, session_id=session_id)
                    short_reply = (reply or '').strip()
                    transcript.append('%s: %s' % (agent_id, short_reply))
                    bus.save_result(session_id, '%s_%s_meeting_r%d' % (agent_id, stage, round_no), short_reply)
                    bus.broadcast(
                        agent_id, 'speaking', short_reply,
                        metadata={
                            'phase': 'meeting',
                            'title': '%s会议发言' % topic,
                            'mode': mode,
                            'session_id': session_id,
                            'meeting_topic': '%s %s' % (trade_date, topic),
                            'meeting_active': True,
                            'participants': participants,
                            'meeting_order': specialists,
                            'meeting_round': round_no,
                            'meeting_round_total': rounds,
                            'seat_index': seat_index,
                            'meeting_speaker': agent_id,
                            'meeting_speaker_seq': seat_index + 1,
                            'meeting_speaker_total': len(specialists),
                        }
                    )
                except Exception as e:
                    bus.post_activity(agent_id, 'error', '%s会议发言失败: %s' % (topic, str(e)))

            if self._session_stop_requested(session_id):
                self._post_session_stopped(session_id, mode, title='%s会议已停止' % topic)
                break

        if director:
            try:
                if self._session_stop_requested(session_id):
                    self._post_session_stopped(session_id, mode, title='%s会议总结已取消' % topic)
                    return
                director.process_incoming_messages(session_id)
                summary_prompt = format_ai_team_prompt(
                    'portfolio.meeting_summary',
                    topic=topic,
                    trade_date=trade_date,
                    transcript='\n'.join(transcript[-16:])
                )
                summary = director.think(summary_prompt, session_id=session_id)
                bus.save_result(session_id, 'director_%s_meeting' % stage, summary)
                bus.broadcast(
                    'director', 'consensus', summary,
                    metadata={
                        'phase': 'meeting_summary',
                        'title': '%s会议共识' % topic,
                        'mode': mode,
                        'session_id': session_id,
                        'meeting_topic': '%s %s' % (trade_date, topic),
                        'meeting_active': False,
                        'participants': participants,
                        'meeting_order': specialists,
                        'meeting_round': rounds,
                        'meeting_round_total': rounds,
                    }
                )
                summary_preview = (summary or '').replace('\n', ' ').strip()
                if len(summary_preview) > 160:
                    summary_preview = summary_preview[:160] + '...'
                bus.post_activity(
                    'portfolio', 'status',
                    '%s会议结果已生成：%s' % (topic, summary_preview or '请查看会议共识详情'),
                    metadata={
                        'phase': 'meeting_result',
                        'title': '%s会议结果通知' % topic,
                        'mode': mode,
                        'session_id': session_id,
                        'meeting_topic': '%s %s' % (trade_date, topic),
                        'meeting_active': False,
                        'participants': participants,
                        'meeting_order': specialists,
                        'meeting_round': rounds,
                        'meeting_round_total': rounds,
                        'summary_preview': summary_preview,
                    }
                )
            except Exception as e:
                bus.post_activity('director', 'error', '%s会议总结失败: %s' % (topic, str(e)))
                bus.broadcast(
                    'portfolio', 'status', '%s会议异常结束' % topic,
                    metadata={
                        'phase': 'meeting_end',
                        'title': '%s会议结束' % topic,
                        'mode': mode,
                        'session_id': session_id,
                        'meeting_topic': '%s %s' % (trade_date, topic),
                        'meeting_active': False,
                        'participants': participants,
                        'meeting_order': specialists,
                        'meeting_round': rounds,
                        'meeting_round_total': rounds,
                    }
                )
                bus.post_activity(
                    'portfolio', 'status', '%s会议结果：异常结束' % topic,
                    metadata={
                        'phase': 'meeting_result',
                        'title': '%s会议结果通知' % topic,
                        'mode': mode,
                        'session_id': session_id,
                        'meeting_topic': '%s %s' % (trade_date, topic),
                        'meeting_active': False,
                        'participants': participants,
                        'meeting_order': specialists,
                        'meeting_round': rounds,
                        'meeting_round_total': rounds,
                    }
                )
        else:
            bus.broadcast(
                'portfolio', 'status', '%s会议结束' % topic,
                metadata={
                    'phase': 'meeting_end',
                    'title': '%s会议结束' % topic,
                    'mode': mode,
                    'session_id': session_id,
                    'meeting_topic': '%s %s' % (trade_date, topic),
                    'meeting_active': False,
                    'participants': participants,
                    'meeting_order': specialists,
                    'meeting_round': rounds,
                    'meeting_round_total': rounds,
                }
            )
            bus.post_activity(
                'portfolio', 'status', '%s会议结果：会议已结束' % topic,
                metadata={
                    'phase': 'meeting_result',
                    'title': '%s会议结果通知' % topic,
                    'mode': mode,
                    'session_id': session_id,
                    'meeting_topic': '%s %s' % (trade_date, topic),
                    'meeting_active': False,
                    'participants': participants,
                    'meeting_order': specialists,
                    'meeting_round': rounds,
                    'meeting_round_total': rounds,
                }
            )

    def _risk_review_phase(self, session_id, trade_date, pending_signals):
        """风控官审核信号"""
        from AlphaFin.ai_team.core.agent_registry import get_agent

        risk = get_agent('risk')
        if not risk:
            return

        signals_desc = '\n'.join([
            '  ID=%d: %s %s, 理由: %s (提交者: %s)' % (
                s['id'], s['direction'], s['ts_code'], s['reason'][:100], s['proposed_by']
            ) for s in pending_signals
        ])

        prompt = format_ai_team_prompt(
            'portfolio.risk_review',
            trade_date=trade_date,
            signals_desc=signals_desc
        )

        try:
            risk.think(prompt, session_id=session_id)
        except Exception as e:
            bus.post_activity('risk', 'error', '风控审核异常: %s' % str(e))

    def _risk_discussion_phase(self, session_id, trade_date, disputed_signals):
        """
        风控与策略方分歧讨论阶段：
        1) 策略提交方补充论据
        2) 风控给出复核意见
        3) 最终交由总监裁决
        """
        from AlphaFin.ai_team.core.agent_registry import get_agent
        discussion_map = {}
        risk = get_agent('risk')
        if not risk:
            return discussion_map

        def _short(text, n=240):
            t = (text or '').replace('\n', ' ').strip()
            if len(t) <= n:
                return t
            return t[:n] + '...'

        for s in disputed_signals:
            sid = s.get('id')
            proposer_id = s.get('proposed_by')
            proposer = get_agent(proposer_id) if proposer_id else None
            strategy_view = ''

            if proposer:
                proposer_prompt = format_ai_team_prompt(
                    'portfolio.risk_discussion_proposer',
                    trade_date=trade_date,
                    signal_id=sid,
                    ts_code=s.get('ts_code', ''),
                    direction=s.get('direction', ''),
                    reason=s.get('reason', ''),
                    risk_review=s.get('risk_review', '')
                )
                try:
                    strategy_view = proposer.think(proposer_prompt, session_id=session_id) or ''
                except Exception as e:
                    strategy_view = '策略方补充失败: %s' % str(e)
            else:
                strategy_view = '未找到策略提交方智能体，无法补充观点。'

            risk_prompt = format_ai_team_prompt(
                'portfolio.risk_discussion_risk',
                trade_date=trade_date,
                signal_id=sid,
                ts_code=s.get('ts_code', ''),
                direction=s.get('direction', ''),
                risk_review=s.get('risk_review', ''),
                strategy_view=_short(strategy_view, 300)
            )
            try:
                risk_reply = risk.think(risk_prompt, session_id=session_id) or ''
            except Exception as e:
                risk_reply = '风控复核失败: %s' % str(e)

            summary = '策略方: %s\n风控复核: %s' % (_short(strategy_view, 300), _short(risk_reply, 300))
            discussion_map[sid] = summary
            bus.post_activity(
                'portfolio', 'status',
                '信号#%s 分歧讨论完成，提交总监裁决' % sid,
                metadata={'signal_id': sid, 'discussion': summary}
            )
        return discussion_map

    def _director_approval_phase(self, session_id, trade_date, pending_signals, discussion_map=None):
        """总监最终审批"""
        from AlphaFin.ai_team.core.agent_registry import get_agent

        director = get_agent('director')
        if not director:
            return

        discussion_map = discussion_map or {}
        rows = []
        for s in pending_signals:
            risk_flag = '支持' if int(s.get('risk_approved') or 0) == 1 else '反对'
            line = '  ID=%d: %s %s, 理由: %s, 风控结论:%s, 风控意见:%s' % (
                s['id'], s['direction'], s['ts_code'], s['reason'][:80], risk_flag,
                s['risk_review'][:80] if s['risk_review'] else '无'
            )
            if s['id'] in discussion_map:
                line += '\n      讨论纪要: %s' % discussion_map[s['id']]
            rows.append(line)
        signals_desc = '\n'.join(rows)

        prompt = format_ai_team_prompt(
            'portfolio.director_approval',
            trade_date=trade_date,
            signals_desc=signals_desc
        )

        try:
            director.think(prompt, session_id=session_id)
        except Exception as e:
            bus.post_activity('director', 'error', '总监审批异常: %s' % str(e))

    def _build_portfolio_context(self, pm, config):
        """构建组合状态文本供智能体使用"""
        status = pm.get_portfolio_status()
        if not status.get('initialized'):
            return '投资组合尚未初始化'

        lines = [
            '当前组合状态:',
            '  模式: %s' % ('自由选股' if status['mode'] == 'free' else '指定: %s' % status['target_code']),
            '  总资产: %.0f元, 现金: %.0f元, 持仓市值: %.0f元' % (
                status['total_assets'], status['current_cash'], status['market_value']),
            '  净值: %.4f, 累计收益: %.2f%%' % (status['nav'], status['cumulative_return']),
        ]
        if status['holdings']:
            lines.append('  持仓:')
            for h in status['holdings']:
                lines.append('    %s(%s): %d股, 成本%.2f, 浮盈%.1f%%' % (
                    h['ts_code'], h['name'], h['quantity'], h['avg_cost'], h['pnl_pct']))
        else:
            lines.append('  持仓: 空仓')
        return '\n'.join(lines)

    def set_auto(self, enabled, persist=True):
        enabled = bool(enabled)
        self.auto_enabled = enabled
        if enabled:
            # 开启后立即允许进入盘中盯盘循环
            self.last_watch_time = 0
        if persist:
            try:
                from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
                pm.update_config(auto_run=1 if enabled else 0)
            except Exception:
                pass
        bus.post_activity('portfolio', 'status',
                          '自动投资: %s' % ('已开启' if enabled else '已关闭'))

    def set_watch(self, enabled=None, interval=None):
        """更新盘中盯盘配置"""
        if enabled is not None:
            self.watch_enabled = bool(enabled)
            bus.post_activity(
                'portfolio', 'status',
                '盘中盯盘已%s' % ('开启' if self.watch_enabled else '关闭')
            )
        if interval is not None:
            self.watch_interval = max(300, int(interval))
            bus.post_activity(
                'portfolio', 'status',
                '盘中盯盘间隔已更新为 %d 秒' % self.watch_interval
            )

    @staticmethod
    def _parse_hhmm(value, fallback):
        """解析 HH:MM 字符串为 (hour, minute)"""
        try:
            parts = (value or '').split(':')
            if len(parts) != 2:
                return fallback
            h = int(parts[0])
            m = int(parts[1])
            if h < 0 or h > 23 or m < 0 or m > 59:
                return fallback
            return h, m
        except Exception:
            return fallback

    def _is_after_auto_run_time(self, now):
        """是否已到收盘后自动投资时间"""
        cur = now.hour * 60 + now.minute
        target = self._auto_run_hour * 60 + self._auto_run_min
        return cur >= target

    def _in_watch_window(self, now):
        """是否处于盘中盯盘时间窗口"""
        start_h, start_m = self._parse_hhmm(self.watch_start, (9, 0))
        end_h, end_m = self._parse_hhmm(self.watch_end, (15, 0))
        cur = now.hour * 60 + now.minute
        start = start_h * 60 + start_m
        end = end_h * 60 + end_m
        return start <= cur < end

    def _fallback_watch_trade_day(self, now, today):
        """
        盘中盯盘的交易日兜底：
        若本地日线库尚未写入当天数据，但当前是工作日，则允许盯盘继续运行。
        """
        if not self.watch_weekday_fallback:
            return False
        # 周末不兜底
        if now.weekday() >= 5:
            return False
        # 同一天只提示一次，避免刷屏
        if self._watch_fallback_notice_date != today:
            self._watch_fallback_notice_date = today
            bus.post_activity(
                'portfolio', 'status',
                '盘中盯盘启用工作日兜底模式（本地日线库尚未写入当日数据）'
            )
        return True

    def get_state(self):
        return {
            'running': self.running,
            'auto_enabled': self.auto_enabled,
            'last_run_date': self.last_run_date,
            'watch_enabled': self.watch_enabled,
            'watch_interval': self.watch_interval,
            'watch_start': self.watch_start,
            'watch_end': self.watch_end,
            'watch_weekday_fallback': self.watch_weekday_fallback,
            'last_watch_time': self.last_watch_time,
            'last_watch_date': self.last_watch_date,
            'db_auto_update_enabled': self.db_auto_update_enabled,
            'db_auto_update_time': self.db_auto_update_time,
            'last_db_update_date': self.last_db_update_date,
            'db_update_running': self.db_update_running,
            'current_session': self.current_session,
            'session_timing': self._session_timing(self.current_session),
            'session_progress': self._session_progress(self.current_session),
            'session_overtime': self._session_overtime(self.current_session),
        }


# 全局单例
portfolio_scheduler = PortfolioScheduler()
