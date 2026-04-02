# -*- coding: utf-8 -*-
"""
统一管理 AI 模块与智能体模型配置（可持久化）。

数据格式:
{
  "modules": {
    "ai_chat": "qwen3-max",
    "sector_news": "qwen3-max",
    "ai_team": "qwen3-max"
  },
  "team": {
    "default_model": "qwen3-max",
    "agents": {
      "director": "qwen3-max"
    }
  }
}
"""
import json
import os
import threading

from AlphaFin.config import BASE_DIR


MODEL_CONFIG_PATH = os.path.join(BASE_DIR, 'AlphaFin', 'data', 'model_overrides.json')
_MODEL_LOCK = threading.Lock()

ALLOWED_MODELS = [
    'qwen3-max',
    'Moonshot-Kimi-K2-Instruct',
    'deepseek-r1',
    'MiniMax-M2.1',
]

DEFAULT_MODEL = 'qwen3-max'

MODULE_DEFAULTS = {
    'ai_chat': DEFAULT_MODEL,
    'sector_news': DEFAULT_MODEL,
    'ai_team': DEFAULT_MODEL,
}

TEAM_AGENT_IDS = (
    'director', 'analyst', 'risk', 'intel', 'quant', 'restructuring', 'auditor'
)


def _ensure_parent_dir():
    parent = os.path.dirname(MODEL_CONFIG_PATH)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def _load_unlocked():
    if not os.path.exists(MODEL_CONFIG_PATH):
        return {}
    try:
        with open(MODEL_CONFIG_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def _save_unlocked(payload):
    _ensure_parent_dir()
    tmp_path = MODEL_CONFIG_PATH + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, MODEL_CONFIG_PATH)


def get_allowed_models():
    return list(ALLOWED_MODELS)


def normalize_model_name(model_name, fallback=''):
    name = str(model_name or '').strip()
    if name in ALLOWED_MODELS:
        return name
    fb = str(fallback or '').strip()
    if fb in ALLOWED_MODELS:
        return fb
    return DEFAULT_MODEL


def get_module_model(module_name, default=''):
    module = str(module_name or '').strip()
    fallback = default or MODULE_DEFAULTS.get(module, DEFAULT_MODEL)
    with _MODEL_LOCK:
        data = _load_unlocked()
        modules = data.get('modules')
        if isinstance(modules, dict):
            return normalize_model_name(modules.get(module), fallback=fallback)
    return normalize_model_name(fallback, fallback=DEFAULT_MODEL)


def set_module_model(module_name, model_name):
    module = str(module_name or '').strip()
    if not module:
        raise ValueError('模块名称不能为空')
    model = normalize_model_name(model_name)
    with _MODEL_LOCK:
        data = _load_unlocked()
        modules = data.get('modules')
        if not isinstance(modules, dict):
            modules = {}
        modules[module] = model
        data['modules'] = modules
        _save_unlocked(data)
    return model


def get_team_default_model():
    with _MODEL_LOCK:
        data = _load_unlocked()
        team = data.get('team')
        if isinstance(team, dict):
            return normalize_model_name(team.get('default_model'), fallback=MODULE_DEFAULTS.get('ai_team'))
    return normalize_model_name(MODULE_DEFAULTS.get('ai_team'), fallback=DEFAULT_MODEL)


def set_team_default_model(model_name):
    model = normalize_model_name(model_name)
    with _MODEL_LOCK:
        data = _load_unlocked()
        team = data.get('team')
        if not isinstance(team, dict):
            team = {}
        team['default_model'] = model
        data['team'] = team
        _save_unlocked(data)
    return model


def get_team_agent_model(agent_id, default=''):
    aid = str(agent_id or '').strip()
    if not aid:
        return normalize_model_name(default or get_team_default_model())
    fallback = default or get_team_default_model()
    with _MODEL_LOCK:
        data = _load_unlocked()
        team = data.get('team')
        if isinstance(team, dict):
            agents = team.get('agents')
            if isinstance(agents, dict):
                return normalize_model_name(agents.get(aid), fallback=fallback)
    return normalize_model_name(fallback, fallback=DEFAULT_MODEL)


def set_team_agent_model(agent_id, model_name):
    aid = str(agent_id or '').strip()
    if not aid:
        raise ValueError('agent_id 不能为空')
    model = normalize_model_name(model_name)
    with _MODEL_LOCK:
        data = _load_unlocked()
        team = data.get('team')
        if not isinstance(team, dict):
            team = {}
        agents = team.get('agents')
        if not isinstance(agents, dict):
            agents = {}
        agents[aid] = model
        team['agents'] = agents
        data['team'] = team
        _save_unlocked(data)
    return model


def get_team_model_config(agent_ids=None):
    ids = list(agent_ids or TEAM_AGENT_IDS)
    default_model = get_team_default_model()
    per_agent = {}
    for aid in ids:
        sid = str(aid or '').strip()
        if not sid:
            continue
        per_agent[sid] = get_team_agent_model(sid, default=default_model)
    return {
        'allowed_models': get_allowed_models(),
        'team_default_model': default_model,
        'team_agent_models': per_agent,
    }
