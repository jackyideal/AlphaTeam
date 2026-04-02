# -*- coding: utf-8 -*-
"""
个股新闻资讯服务 — 基于 Qwen AI 联网搜索
获取4类新闻：行业政策、公司公告、公司相关新闻、世界大事
"""
import json
import re
import time
try:
    from urllib.parse import quote
except ImportError:
    from urllib import quote

from AlphaFin.services.ai_chat_service import ai_chat

# 内存缓存: {ts_code: {'data': dict, 'ts': float}}
_news_cache = {}
CACHE_TTL = 7200  # 2小时


def fetch_stock_news(ts_code):
    """
    获取个股相关的4类新闻（AI联网搜索）。

    Returns:
        dict: {industry_policy: [...], company_announcements: [...],
               company_news: [...], world_events: [...]}
        每条: {title, date, summary, impact, url}
    """
    # 检查缓存
    if ts_code in _news_cache:
        cached = _news_cache[ts_code]
        if time.time() - cached['ts'] < CACHE_TTL:
            return cached['data']

    prompt = f"""请搜索关于股票代码 {ts_code} 对应公司的最新新闻和信息，按以下4个类别整理。

请严格以JSON格式返回（不要包含其他文字），格式如下：
{{
  "industry_policy": [
    {{"title": "新闻标题", "date": "YYYY-MM-DD", "summary": "2-3句话的新闻摘要", "impact": true}},
    ...
  ],
  "company_announcements": [...],
  "company_news": [...],
  "world_events": [...]
}}

各类别要求：
1. industry_policy（行业与政策动态）：该公司所在行业的政策法规、行业趋势、产业链变化，近1年内，5-8条
2. company_announcements（公司公告）：该公司过去2年内的重要公告，包括但不限于：季报/半年报/年报、业绩预告、资产重组、资产注入、分红派息、股权变动、技术突破、人事变动、重大合同等官方公告，尽可能多列，15-20条
3. company_news（公司相关新闻）：媒体报道、分析师评论、市场传闻、竞争动态等，5-8条
4. world_events（世界大事）：可能影响该股票的宏观经济事件、国际局势、全球行业动态，3-5条

impact 字段规则：如果该新闻对公司近期股价走势有密切关系（如直接导致涨跌、重大利好利空、业绩大幅变化等），设为 true，否则设为 false。每个类别中标记为 true 的新闻不超过3条。

按时间从新到旧排列。只返回JSON，不要其他任何解释文字。"""

    try:
        raw = ai_chat(prompt, history=None, enable_search=True)
    except Exception as e:
        return {'error': 'AI调用失败: ' + str(e)}

    # 解析 JSON
    data = _parse_json_response(raw)

    # 为每条新闻生成百度搜索链接（AI 无法提供可靠的原文URL）
    if 'error' not in data:
        for category in ['industry_policy', 'company_announcements', 'company_news', 'world_events']:
            items = data.get(category, [])
            for item in items:
                title = item.get('title', '')
                if title:
                    item['url'] = 'https://www.baidu.com/s?wd=' + quote(title)
                else:
                    item['url'] = ''
        _news_cache[ts_code] = {'data': data, 'ts': time.time()}

    return data


def _parse_json_response(raw):
    """从 AI 返回的文本中解析 JSON，兼容 markdown 代码块"""
    # 直接解析
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        pass

    # 尝试从 markdown 代码块中提取
    m = re.search(r'```(?:json)?\s*([\s\S]*?)```', raw or '')
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # 尝试找到第一个 { 到最后一个 } 之间的内容
    if raw:
        start = raw.find('{')
        end = raw.rfind('}')
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(raw[start:end + 1])
            except json.JSONDecodeError:
                pass

    # 检查是否 AI 本身返回了错误信息
    if raw and ('请求超时' in raw or '调用失败' in raw or '设置' in raw):
        return {'error': raw}
    return {'error': 'AI返回格式异常，请重试'}
