# -*- coding: utf-8 -*-
"""
每日新闻提取 & 板块热点分析服务
基于 Qwen AI 联网搜索，提取当日重大新闻，分析当前热度最高的A股板块
"""
import time
from datetime import datetime
from AlphaFin.services.ai_chat_service import ai_chat_direct
from AlphaFin.services.prompt_config_service import get_prompt
from AlphaFin.services.model_config_service import normalize_model_name, get_module_model

# 内存缓存: {'daily_report': {'report': str, 'timestamp': str, 'ts': float}}
_sector_cache = {}
CACHE_TTL = 14400  # 4小时
SECTOR_NEWS_PROMPT_MODULE = 'sector_news'

SECTOR_REPORT_PROMPT = """请帮我做一份专业的A股板块热点分析报告，要求如下：

## 任务说明
搜索今天（当日）的国际时事、国内政策、行业政策等重大新闻，分析这些新闻对A股各板块的潜在影响，找出当前热度最高的板块。

## 输出格式（Markdown）
请严格按以下格式返回，只返回符合此格式的Markdown文本，不要包含其他文字：

### 📰 今日重大新闻概览

#### 🌍 国际时事（3-5条关键新闻）
- **新闻标题1**: 简要描述
- **新闻标题2**: 简要描述
...

#### 🏛️ 国内政策（3-5条关键政策动向）
- **政策标题1**: 简要描述
- **政策标题2**: 简要描述
...

#### 🏭 行业动态（3-5条行业重大事件）
- **行业事件1**: 简要描述
- **行业事件2**: 简要描述
...

### 🔥 热点板块排名 TOP 10

按热度从高到低排列，每个板块包含热度评分（10分制）和驱动因素：

| 排名 | 板块名称 | 热度评分 | 主要驱动因素 |
|------|--------|--------|-----------|
| 1 | 板块名1 | 9.5/10 | 具体驱动因素 |
| 2 | 板块名2 | 8.8/10 | 具体驱动因素 |
| 3 | 板块名3 | 8.2/10 | 具体驱动因素 |
| ... | ... | ... | ... |

### 📊 重点板块深度分析

#### TOP 1: 板块名称

**热度评分**: X.X/10

**核心逻辑**:
- 利好因素分析（2-3点）
- 政策支持情况
- 短期催化剂

**相关龙头股票**（举例前3-5家）:
- 股票代码（股票简称）- 简要理由

**风险提示**:
- 主要风险点分析

---

#### TOP 2: 板块名称
[同上格式]

---

#### TOP 3: 板块名称
[同上格式]

### 💡 投资策略建议

1. **短期重点关注方向**:
   - 方向1及其理由
   - 方向2及其理由

2. **风险提醒**:
   - 主要风险点
   - 防范建议

3. **配置建议**:
   - 建议重点关注的行业/主题

---

## 质量要求
1. 新闻来源须真实可查，时间须为当日（如果无当日新闻，说明日期较早）
2. 板块分析要基于真实的A股申万行业分类
3. 龙头股票要选择该板块内实际的领军企业
4. 评分合理，驱动因素具体可信
5. 不要编造数据，如果无法搜到相关信息，请说明

开始生成报告吧！"""


def _now_cn_str(fmt='%Y-%m-%d %H:%M:%S'):
    return datetime.now().strftime(fmt)


def _today_cn_str():
    return datetime.now().strftime('%Y-%m-%d')


def _looks_like_ai_failure(text):
    s = str(text or '').strip()
    if not s:
        return True
    prefixes = (
        'AI 调用失败',
        'AI 请求超时',
        '请在 config.py 中设置 QWEN_API_KEY',
    )
    return any(s.startswith(p) for p in prefixes)


def _normalize_items(rows, limit=8):
    out = []
    seen = set()
    max_n = max(1, int(limit or 8))
    for row in (rows or []):
        if not isinstance(row, dict):
            continue
        title = str(row.get('title') or '').strip()
        url = str(row.get('url') or row.get('link') or '').strip()
        source = str(row.get('source') or '').strip()
        published_at = str(row.get('published_at') or '').strip()
        if not title and not url:
            continue
        key = (url.lower(), title)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            'title': title or url or '来源',
            'url': url,
            'source': source,
            'published_at': published_at,
        })
        if len(out) >= max_n:
            break
    return out


def _format_items_for_prompt(rows, limit=8):
    items = _normalize_items(rows, limit=limit)
    if not items:
        return ''
    lines = []
    for i, row in enumerate(items, 1):
        lines.append(
            '%d) %s | %s | %s | %s' % (
                i,
                row.get('published_at') or '-',
                row.get('source') or '-',
                (row.get('title') or '-')[:120],
                row.get('url') or '-',
            )
        )
    return '\n'.join(lines)


def _build_sources_section(rows, limit=12):
    items = _normalize_items(rows, limit=limit)
    if not items:
        return ''
    lines = ['### 🔗 联网检索来源（系统快照）', '']
    for i, row in enumerate(items, 1):
        line = '%d. %s | %s | %s | %s' % (
            i,
            row.get('published_at') or '-',
            row.get('source') or '-',
            row.get('title') or '-',
            row.get('url') or '-',
        )
        lines.append(line)
    return '\n'.join(lines)


def get_sector_news_prompt_definitions():
    """返回板块热点分析可管理的系统提示词定义。"""
    return [
        {
            'key': 'report',
            'name': '板块热点分析系统提示词',
            'description': '用于生成“每日板块热点分析报告”',
            'default_prompt': SECTOR_REPORT_PROMPT,
        }
    ]


def get_sector_news_prompt_configs():
    """返回板块热点分析提示词（含当前生效值）。"""
    result = []
    for item in get_sector_news_prompt_definitions():
        current_prompt = get_prompt(
            SECTOR_NEWS_PROMPT_MODULE,
            item['key'],
            item['default_prompt']
        )
        row = dict(item)
        row['prompt'] = current_prompt
        row['is_overridden'] = current_prompt != item['default_prompt']
        result.append(row)
    return result


def fetch_sector_report(context_text='', context_file_ids=None, model_name=''):
    """
    获取每日板块热点分析报告（基于当日新闻）。

    Returns:
        dict: {
            'report': 'Markdown格式的分析报告',
            'timestamp': '生成时间（格式：YYYY-MM-DD HH:MM:SS）',
            'status': 'success' or 'error'
        }
    """
    prompt = get_prompt(SECTOR_NEWS_PROMPT_MODULE, 'report', SECTOR_REPORT_PROMPT)
    resolved_model = normalize_model_name(
        model_name or get_module_model('sector_news')
    )
    extra_context = str(context_text or '').strip()
    file_ids = []
    for fid in (context_file_ids or []):
        s = str(fid or '').strip()
        if s and s not in file_ids:
            file_ids.append(s)
    today_str = _today_cn_str()
    full_prompt = prompt
    if extra_context:
        full_prompt = (
            prompt +
            '\n\n【用户上传上下文材料】\n' +
            extra_context +
            '\n\n请将上述上传材料作为优先证据来源之一，并在报告中体现引用依据。'
        )

    ws_items = []
    ws_error = ''

    # 检查缓存（提示词变化时自动失效）
    if 'daily_report' in _sector_cache:
        cached = _sector_cache['daily_report']
        if (
            time.time() - cached['ts'] < CACHE_TTL
            and cached.get('day_key') == today_str
            and cached.get('prompt') == prompt
            and cached.get('context_text', '') == extra_context
            and cached.get('context_file_ids', []) == file_ids
            and cached.get('model', '') == resolved_model
        ):
            return {
                'report': cached['report'],
                'timestamp': cached['timestamp'],
                'status': 'success',
                'from_cache': True,
                'search_items_count': int(cached.get('search_items_count') or 0),
                'search_error': cached.get('search_error', ''),
                'model': cached.get('model', ''),
            }

    try:
        chat_ret = ai_chat_direct(
            full_prompt,
            history=None,
            enable_search=True,
            context_file_ids=file_ids,
            model_name=resolved_model,
        )
        report_text = str((chat_ret or {}).get('reply') or '')
        model_used = str((chat_ret or {}).get('model') or '')
        ws_items = _normalize_items((chat_ret or {}).get('search_links') or [], limit=12)
        ws_error = str((chat_ret or {}).get('search_error') or '').strip()
    except Exception as e:
        return {
            'report': f'生成报告失败: {str(e)}',
            'timestamp': _now_cn_str(),
            'status': 'error'
        }

    # 失败场景识别：避免把“AI调用失败文本”当成成功报告
    if _looks_like_ai_failure(report_text):
        return {
            'report': report_text or '生成报告失败：AI返回为空',
            'timestamp': _now_cn_str(),
            'status': 'error',
            'search_items_count': len(ws_items),
            'search_error': ws_error,
            'model': model_used,
        }

    # 在报告尾部附来源清单，提升可核验性
    sources_md = _build_sources_section(ws_items, limit=12)
    if sources_md:
        if '联网检索来源（系统快照）' not in report_text:
            report_text = (report_text.rstrip() + '\n\n---\n\n' + sources_md).strip()
    elif ws_error:
        warn = (
            '\n\n---\n\n'
            '### ⚠️ 联网检索状态\n'
            '本次外部检索失败：%s\n'
            '本报告可能不完整，请谨慎使用并二次核验。'
        ) % ws_error
        report_text = (report_text.rstrip() + warn).strip()

    # 生成时间戳
    timestamp = _now_cn_str()

    # 缓存结果（仅缓存成功）
    _sector_cache['daily_report'] = {
        'report': report_text,
        'timestamp': timestamp,
        'prompt': prompt,
        'context_text': extra_context,
        'context_file_ids': file_ids,
        'day_key': today_str,
        'search_items_count': len(ws_items),
        'search_error': ws_error,
        'model': model_used,
        'ts': time.time()
    }

    return {
        'report': report_text,
        'timestamp': timestamp,
        'status': 'success',
        'search_items_count': len(ws_items),
        'search_error': ws_error,
        'model': model_used,
    }
