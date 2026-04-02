"""
Qwen API 集成服务 - 图表分析对话（通义千问，OpenAI兼容模式）
使用 requests 直接调用，兼容 Python 3.7
"""
import base64
import os
import requests

from AlphaFin.config import QWEN_API_KEY, QWEN_BASE_URL, QWEN_MODEL, CHART_DIR

SYSTEM_PROMPT = """你是一位专业的中国A股市场分析师。用户会向你展示股票市场的技术指标图表。
请基于图表内容进行专业分析，包括：
1. 当前指标所处的历史位置（高估/低估/中性）
2. 趋势判断（上升/下降/震荡）
3. 可能的交易信号或风险提示
4. 与其他宏观/市场指标的联动关系
请用中文回答，语言简洁专业。"""


def analyze_charts(chart_paths, user_message, history=None):
    """
    发送图表图片 + 用户问题到 Qwen API 进行分析。

    Args:
        chart_paths: 图表路径列表（相对路径如 '/static/charts/xxx.png'）
        user_message: 用户问题
        history: 对话历史 [{'role': 'user'|'assistant', 'content': str}]

    Returns:
        str: Qwen 的回复文本
    """
    if not QWEN_API_KEY:
        return '请在 config.py 中设置 QWEN_API_KEY 后使用此功能。'

    messages = [{'role': 'system', 'content': SYSTEM_PROMPT}]

    # 添加对话历史
    if history:
        for h in history:
            messages.append({'role': h['role'], 'content': h['content']})

    # 构建当前消息（图片 + 文本）
    content = []
    img_count = 0
    for path in chart_paths:
        fname = os.path.basename(path)
        abs_path = os.path.join(CHART_DIR, fname)
        if os.path.exists(abs_path):
            img_count += 1
            with open(abs_path, 'rb') as f:
                img_data = base64.standard_b64encode(f.read()).decode('utf-8')
            content.append({
                'type': 'image_url',
                'image_url': {
                    'url': 'data:image/png;base64,' + img_data,
                }
            })

    print(f'[Qwen] 收到 {len(chart_paths)} 个路径，成功加载 {img_count} 张图片')
    content.append({'type': 'text', 'text': user_message})
    messages.append({'role': 'user', 'content': content})

    try:
        url = QWEN_BASE_URL.rstrip('/') + '/chat/completions'
        headers = {
            'Authorization': 'Bearer ' + QWEN_API_KEY,
            'Content-Type': 'application/json',
        }
        payload = {
            'model': QWEN_MODEL,
            'messages': messages,
            'temperature': 0,
            'seed': 42,
        }
        resp = requests.post(url, json=payload, headers=headers, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        return data['choices'][0]['message']['content'].strip()
    except requests.exceptions.Timeout:
        return 'Qwen API 请求超时，请稍后重试。'
    except Exception as e:
        return 'Qwen API 调用失败: ' + str(e)
