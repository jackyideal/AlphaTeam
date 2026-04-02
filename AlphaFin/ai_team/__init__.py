"""
AlphaFin 智能分析团队 - 模块入口
提供 Flask Blueprint 注册和后台调度器启动
"""
import os
import time
import threading

from AlphaFin.ai_team.config import DATA_DIR, REPORTS_DIR, TEAM_MODULE_AUTO_START

_runtime_lock = threading.Lock()
_runtime = {
    'started': False,
    'started_at': 0.0,
}


def _start_background_runtime():
    """实际启动后台运行时线程（幂等）。"""
    from AlphaFin.ai_team.core.orchestrator import orchestrator
    from AlphaFin.ai_team.core.portfolio_scheduler import portfolio_scheduler

    if not orchestrator.running:
        t = threading.Thread(target=orchestrator.run_forever, daemon=True)
        t.start()
        print('[AI Team] 智能分析团队调度器已启动')

    if not portfolio_scheduler.running:
        portfolio_scheduler.start()
        print('[AI Team] 投资周期调度器已启动')


def start_team_module():
    """手动启动智能分析模块。"""
    with _runtime_lock:
        _start_background_runtime()
        _runtime['started'] = True
        _runtime['started_at'] = time.time()
    return get_team_module_state()


def stop_team_module():
    """手动停止智能分析模块后台循环。"""
    from AlphaFin.ai_team.core.orchestrator import orchestrator
    from AlphaFin.ai_team.core.portfolio_scheduler import portfolio_scheduler

    with _runtime_lock:
        try:
            orchestrator.stop()
        except Exception as e:
            print('[AI Team] 停止 orchestrator 失败: %s' % str(e))
        try:
            portfolio_scheduler.stop()
        except Exception as e:
            print('[AI Team] 停止 portfolio scheduler 失败: %s' % str(e))
        _runtime['started'] = False
    return get_team_module_state()


def get_team_module_state():
    """获取智能分析模块运行状态。"""
    from AlphaFin.ai_team.core.orchestrator import orchestrator
    from AlphaFin.ai_team.core.portfolio_scheduler import portfolio_scheduler

    running = bool(orchestrator.running or portfolio_scheduler.running)
    with _runtime_lock:
        if running:
            _runtime['started'] = True
            if not _runtime['started_at']:
                _runtime['started_at'] = time.time()
        return {
            'running': running,
            'started': bool(_runtime['started']),
            'started_at': _runtime['started_at'],
            'auto_start': bool(TEAM_MODULE_AUTO_START),
            'orchestrator_running': bool(orchestrator.running),
            'portfolio_running': bool(portfolio_scheduler.running),
        }


def create_team_blueprint():
    """创建并返回 Flask Blueprint。"""
    # 确保数据目录存在
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)

    # 导入路由 Blueprint
    from AlphaFin.ai_team.routes import bp

    # 可选自动启动（默认关闭，交由首页手动按钮控制）
    if TEAM_MODULE_AUTO_START:
        from threading import Timer
        timer = Timer(3.0, start_team_module)
        timer.daemon = True
        timer.start()

    return bp
