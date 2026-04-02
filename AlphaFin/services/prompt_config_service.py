# -*- coding: utf-8 -*-
"""
统一管理 AI 模块系统提示词的持久化覆盖配置。

数据格式:
{
  "ai_chat": {"direct": "...", "grounded": "..."},
  "sector_news": {"report": "..."},
  "ai_team": {"director": "...", "analyst": "..."}
}
"""
import json
import os
import threading

from AlphaFin.config import BASE_DIR


PROMPT_OVERRIDES_PATH = os.path.join(BASE_DIR, 'AlphaFin', 'data', 'prompt_overrides.json')
_PROMPT_LOCK = threading.Lock()


def _ensure_parent_dir():
    parent = os.path.dirname(PROMPT_OVERRIDES_PATH)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def _load_overrides_unlocked():
    if not os.path.exists(PROMPT_OVERRIDES_PATH):
        return {}
    try:
        with open(PROMPT_OVERRIDES_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def _save_overrides_unlocked(payload):
    _ensure_parent_dir()
    tmp_path = PROMPT_OVERRIDES_PATH + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, PROMPT_OVERRIDES_PATH)


def get_prompt(module_name, prompt_key, default_text):
    """
    读取指定模块/键的提示词，若未覆盖则返回默认值。
    """
    with _PROMPT_LOCK:
        data = _load_overrides_unlocked()
        module_data = data.get(module_name, {})
        if isinstance(module_data, dict):
            value = module_data.get(prompt_key)
            if isinstance(value, str) and value.strip():
                return value
    return default_text


def set_prompt(module_name, prompt_key, prompt_text):
    """
    持久化更新提示词。
    """
    text = str(prompt_text or '').strip()
    if not text:
        raise ValueError('提示词不能为空')

    with _PROMPT_LOCK:
        data = _load_overrides_unlocked()
        module_data = data.get(module_name)
        if not isinstance(module_data, dict):
            module_data = {}
        module_data[prompt_key] = text
        data[module_name] = module_data
        _save_overrides_unlocked(data)
    return text


def reset_prompt(module_name, prompt_key):
    """
    删除提示词覆盖项，恢复为默认值。
    """
    with _PROMPT_LOCK:
        data = _load_overrides_unlocked()
        module_data = data.get(module_name)
        if isinstance(module_data, dict):
            module_data.pop(prompt_key, None)
            if module_data:
                data[module_name] = module_data
            else:
                data.pop(module_name, None)
            _save_overrides_unlocked(data)
    return True


def get_module_overrides(module_name):
    """
    返回某个模块全部覆盖项（仅覆盖值，不含默认值）。
    """
    with _PROMPT_LOCK:
        data = _load_overrides_unlocked()
        module_data = data.get(module_name)
        if isinstance(module_data, dict):
            return dict(module_data)
    return {}
