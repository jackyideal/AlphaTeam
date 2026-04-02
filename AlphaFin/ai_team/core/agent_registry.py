"""
智能体注册表 - 实例化所有智能体
"""
from AlphaFin.ai_team.config import AGENT_API_KEYS
from AlphaFin.services.prompt_config_service import get_prompt
from AlphaFin.services.model_config_service import (
    get_team_default_model,
    get_team_agent_model,
    set_team_default_model,
    set_team_agent_model,
    get_team_model_config,
)

_agents = {}


def get_agents():
    """获取所有智能体实例（懒加载单例）"""
    if not _agents:
        _init_agents()
    return _agents


def get_agent(agent_id):
    """获取单个智能体"""
    agents = get_agents()
    return agents.get(agent_id)


def get_agent_prompt_defaults():
    """返回所有团队智能体的默认系统提示词。"""
    from AlphaFin.ai_team.agents.decision_director import SYSTEM_PROMPT as director_prompt
    from AlphaFin.ai_team.agents.investment_analyst import SYSTEM_PROMPT as analyst_prompt
    from AlphaFin.ai_team.agents.risk_officer import SYSTEM_PROMPT as risk_prompt
    from AlphaFin.ai_team.agents.market_intelligence import SYSTEM_PROMPT as intel_prompt
    from AlphaFin.ai_team.agents.quant_strategist import SYSTEM_PROMPT as quant_prompt
    from AlphaFin.ai_team.agents.audit_reviewer import SYSTEM_PROMPT as auditor_prompt
    from AlphaFin.ai_team.agents.restructuring_specialist import SYSTEM_PROMPT as restructuring_prompt

    return {
        'director': director_prompt,
        'analyst': analyst_prompt,
        'risk': risk_prompt,
        'intel': intel_prompt,
        'quant': quant_prompt,
        'auditor': auditor_prompt,
        'restructuring': restructuring_prompt,
    }


def update_agent_system_prompt(agent_id, system_prompt):
    """
    更新已初始化智能体的运行时系统提示词。
    返回 True 表示已更新运行中实例；False 表示实例尚未初始化。
    """
    agent = _agents.get(agent_id)
    if not agent:
        return False
    agent.system_prompt = str(system_prompt or '').strip()
    return True


def update_agent_model(agent_id, model_name):
    """
    更新指定智能体模型（持久化 + 运行时生效）。
    返回: {'runtime_applied': bool, 'model': str}
    """
    saved = set_team_agent_model(agent_id, model_name)
    agent = _agents.get(agent_id)
    runtime_applied = False
    if agent:
        try:
            agent.set_model(saved)
            runtime_applied = True
        except Exception:
            runtime_applied = False
    return {'runtime_applied': runtime_applied, 'model': saved}


def update_team_default_model(model_name):
    """
    更新团队默认模型（持久化）。
    注意：仅对未单独配置模型的智能体自动同步。
    """
    saved = set_team_default_model(model_name)
    applied = []
    for aid, agent in (_agents or {}).items():
        try:
            # 仅当该智能体当前模型与旧默认一致时，才跟随默认模型更新
            current = str(get_team_agent_model(aid, default=saved) or '')
            if current == saved:
                agent.set_model(saved)
                applied.append(aid)
        except Exception:
            continue
    return {'model': saved, 'runtime_applied_agents': applied}


def get_runtime_model_settings():
    cfg = get_team_model_config()
    runtime = {}
    for aid in cfg.get('team_agent_models', {}):
        agent = _agents.get(aid)
        if agent:
            runtime[aid] = agent.get_model()
        else:
            runtime[aid] = {
                'model': get_team_agent_model(aid, default=cfg.get('team_default_model')),
                'fallback_model': '',
            }
    cfg['runtime'] = runtime
    return cfg


def _init_agents():
    """初始化所有智能体"""
    from AlphaFin.ai_team.agents.decision_director import create_agent as create_director
    from AlphaFin.ai_team.agents.investment_analyst import create_agent as create_analyst
    from AlphaFin.ai_team.agents.risk_officer import create_agent as create_risk
    from AlphaFin.ai_team.agents.market_intelligence import create_agent as create_intel
    from AlphaFin.ai_team.agents.quant_strategist import create_agent as create_quant
    from AlphaFin.ai_team.agents.audit_reviewer import create_agent as create_auditor
    from AlphaFin.ai_team.agents.restructuring_specialist import create_agent as create_restructuring

    creators = {
        'director': create_director,
        'analyst': create_analyst,
        'risk': create_risk,
        'intel': create_intel,
        'quant': create_quant,
        'auditor': create_auditor,
        'restructuring': create_restructuring,
    }
    prompt_defaults = get_agent_prompt_defaults()
    team_default_model = get_team_default_model()

    for agent_id, creator in creators.items():
        api_key = AGENT_API_KEYS.get(agent_id, '')
        if api_key:
            agent = creator(api_key)
            default_prompt = prompt_defaults.get(agent_id, agent.system_prompt)
            agent.system_prompt = get_prompt('ai_team', agent_id, default_prompt)
            agent_model = get_team_agent_model(agent_id, default=team_default_model)
            try:
                agent.set_model(agent_model)
            except Exception:
                pass
            _agents[agent_id] = agent
            print('[AI Team] 已初始化智能体: %s (%s)' % (agent_id, _agents[agent_id].name))
        else:
            print('[AI Team] 跳过智能体 %s: 无 API key' % agent_id)


def get_all_status():
    """获取所有智能体状态（供前端显示）"""
    from AlphaFin.ai_team.config import AGENT_META
    agents = get_agents()
    result = []
    for agent_id, meta in AGENT_META.items():
        agent = agents.get(agent_id)
        status = agent.get_status() if agent else {'agent_id': agent_id, 'status': 'offline'}
        status['color'] = meta['color']
        status['icon'] = meta['icon']
        status['description'] = meta['description']
        if 'name' not in status:
            status['name'] = meta['name']
        result.append(status)
    return result


def has_active_workflow(workflows):
    """是否有智能体正在执行指定工作流。"""
    targets = set(workflows or [])
    if not targets:
        return False
    for row in get_all_status():
        if str(row.get('status') or '') == 'idle':
            continue
        if str(row.get('current_workflow') or '') in targets:
            return True
    return False


def request_stop_all_agents(reason=''):
    """向全部已初始化智能体下发停止指令。"""
    agents = get_agents()
    stopped = []
    for aid, agent in (agents or {}).items():
        try:
            agent.request_stop(reason=reason)
            stopped.append(aid)
        except Exception:
            continue
    return stopped


def clear_stop_all_agents():
    """清除全部智能体的停止标记。"""
    agents = get_agents()
    cleared = []
    for aid, agent in (agents or {}).items():
        try:
            agent.clear_stop_request()
            cleared.append(aid)
        except Exception:
            continue
    return cleared


def clear_stop_agents(agent_ids):
    """清除指定智能体的停止标记。"""
    ids = list(agent_ids or [])
    agents = get_agents()
    cleared = []
    for aid in ids:
        agent = agents.get(aid)
        if not agent:
            continue
        try:
            agent.clear_stop_request()
            cleared.append(aid)
        except Exception:
            continue
    return cleared


def request_stop_agents_for_session(session_id, reason='', exclude=None):
    """仅向指定 session 上正在工作的智能体下发停止指令。"""
    sid = str(session_id or '').strip()
    skip = set(exclude or [])
    if not sid:
        return []
    agents = get_agents()
    stopped = []
    for aid, agent in (agents or {}).items():
        if aid in skip:
            continue
        if str(getattr(agent, 'current_session_id', '') or '') != sid:
            continue
        try:
            agent.request_stop(reason=reason or '会话已进入汇总模式')
            stopped.append(aid)
        except Exception:
            continue
    return stopped
