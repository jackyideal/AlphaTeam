"""
智能体协同策略 — 多专家AI协同分析系统
GPT-4o + DeepSeek 双专家辩论，涵盖技术面、基本面、估值、资金流、舆情、策略回测，最终MOE决策
"""
import os
import sys
import time
import textwrap
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

# ──────────────── 指标元数据 ────────────────

INDICATOR_META = {
    'id': 'ind_21_agent_strategy',
    'name': '智能体协同策略',
    'group': '策略模型',
    'description': '多专家AI协同决策系统：GPT-4o与DeepSeek双专家从技术面、基本面、估值、资金流、舆情五维度独立分析并交叉辩论，最终通过MOE(混合专家)机制加权融合，输出综合投资决策',
    'input_type': 'query',
    'default_query': '请帮我分析一下贵州茅台，是否值得投资？',
    'requires_db': False,
    'slow': True,
    'chart_count': 10,
    'chart_descriptions': [
        'AI深度思考逻辑链路图，展示分析推理的完整思维过程',
        '日K线技术分析图，标注关键支撑阻力位与技术形态',
        '策略构建分析，展示AI生成的量化策略逻辑与参数',
        'MOE最终投资决策，混合专家模型的加权融合结论',
        '专家辩论记录，GPT-4o与DeepSeek的交叉质询与观点碰撞',
        '专家辩论最终结论，辩论后的共识与分歧总结',
        '数据综合报告，基本面、技术面、资金面等多维数据汇总',
        '市场情报综合，舆情、政策、行业动态等外部信息聚合',
        '专家1(GPT-4o)完整分析报告',
        '专家2(DeepSeek)完整分析报告',
    ],
}

# stockagent 路径（可通过环境变量覆盖）
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STOCKAGENT_DIR = os.getenv(
    'STOCKAGENT_DIR',
    os.path.join(_REPO_ROOT, 'third_party', 'stockagent')
)


# ──────────────── 工具函数 ────────────────


def _safe_call(func, *args, default=None, label="unknown"):
    """安全调用函数，失败返回默认值"""
    try:
        result = func(*args)
        print(f'[OK] {label} 完成')
        return result
    except Exception as e:
        print(f'[WARNING] {label} 失败: {e}')
        traceback.print_exc()
        return default if default is not None else f'[{label} 数据获取失败: {str(e)[:100]}]'


def _render_text_figure(text, title, figsize=(20, 16), font_size=11):
    """将长文本渲染为 matplotlib 图表"""
    if not text or not isinstance(text, str):
        text = '（无数据）'

    fig, ax = plt.subplots(figsize=figsize, facecolor='white')
    ax.axis('off')

    # 标题
    ax.text(0.5, 0.97, title,
            transform=ax.transAxes, fontsize=18, fontweight='bold',
            ha='center', va='top',
            fontfamily='Arial Unicode MS',
            color='#1a365d')

    # 分隔线
    ax.axhline(y=0.95, xmin=0.05, xmax=0.95, color='#3182ce',
               linewidth=1.5, transform=ax.transAxes)

    # 文本处理：按行分割，中文自动换行
    lines = text.split('\n')
    wrapped_lines = []
    for line in lines:
        if len(line) > 90:
            wrapped_lines.extend(textwrap.wrap(line, width=90))
        else:
            wrapped_lines.append(line)

    # 限制总行数以适应图表尺寸
    max_lines = int(figsize[1] * 4.5)
    if len(wrapped_lines) > max_lines:
        wrapped_lines = wrapped_lines[:max_lines]
        wrapped_lines.append('\n... (内容过长，已截断) ...')

    display_text = '\n'.join(wrapped_lines)

    ax.text(0.03, 0.93, display_text,
            transform=ax.transAxes, fontsize=font_size,
            va='top', ha='left',
            fontfamily='Arial Unicode MS',
            linespacing=1.5,
            color='#2d3748')

    fig.tight_layout(pad=1.0)
    return fig


def _image_to_figure(image_path, title, figsize=(16, 10)):
    """将磁盘上的 PNG 图片加载为 matplotlib figure"""
    try:
        from PIL import Image
        img = Image.open(image_path)
        fig, ax = plt.subplots(figsize=figsize, facecolor='white')
        ax.imshow(np.array(img))
        ax.axis('off')
        ax.set_title(title, fontsize=16, fontweight='bold', pad=12,
                     fontfamily='Arial Unicode MS', color='#1a365d')
        fig.tight_layout(pad=0.5)
        return fig
    except Exception as e:
        print(f'[WARNING] 加载图片失败 {image_path}: {e}')
        return _render_text_figure(f'图片加载失败: {image_path}\n错误: {str(e)}', title)


def _render_combined_report(sections, title, figsize=(20, 20)):
    """将多个文本报告合并渲染为一张图"""
    combined_text = ''
    for section_title, section_text in sections:
        if section_text and isinstance(section_text, str):
            combined_text += f'━━━━━ {section_title} ━━━━━\n\n'
            combined_text += section_text.strip()
            combined_text += '\n\n'
    return _render_text_figure(combined_text, title, figsize=figsize)


# ──────────────── 主生成函数 ────────────────

def generate(query='请帮我分析一下贵州茅台，是否值得投资？', progress_callback=None, **kwargs):
    """
    多专家AI协同分析系统
    Phase 1: 并行数据采集 (11项)
    Phase 2: 专家1 (GPT-4o) 分析
    Phase 3: 专家2 (DeepSeek) 分析
    Phase 4: 专家辩论 + MOE决策
    """
    total_steps = 18
    t0 = time.time()

    def _progress(step, msg):
        elapsed = time.time() - t0
        if progress_callback:
            progress_callback(step, total_steps, f'{msg} (已用时 {elapsed:.0f}s)')

    # ── 延迟导入 stockagent 模块 ──
    if STOCKAGENT_DIR not in sys.path:
        sys.path.insert(0, STOCKAGENT_DIR)

    from function import (
        extract_stock_name, k_line_read1, fetch_stock_and_industry_data,
        fetch_value_data, fetch_money_data, analyse_A_markt,
        decision_tool, final_reflection, final_decision1, final_decision2,
        strategy_construction
    )
    from function3 import expert_debate, format_debate_for_ui
    from kimi_web import (
        kimi_web_search, kimi_web_market_feeling,
        kimi_web_reconstuction_search, kimi_web_stock_report
    )
    from annual_report_forecast import predict_future_performance, extract_business_and_development_info
    from deepseek.deepseek_reasoner import generate_and_save_analysis_graph

    # ═══════════════ Step 0: 初始化 ═══════════════
    _progress(0, f'初始化，从问题中提取股票名称...')
    task = query
    stock_name = extract_stock_name(query)
    if not stock_name:
        # 尝试用整个query作为股票名称
        stock_name = query.strip()
    print(f'[Agent] 问题: {query}, 识别股票: {stock_name}')

    # ═══════════════ Phase 1: 并行数据采集 ═══════════════
    _progress(1, f'并行采集数据（共11项）: K线、网络搜索、基本面、估值、资金流、策略回测...')

    data = {}
    completed_count = [0]  # 用列表以便在闭包内修改
    model_type_1 = 'deepseek'

    def _on_complete(key, result):
        data[key] = result
        completed_count[0] += 1
        n = completed_count[0]
        elapsed = time.time() - t0
        if n <= 4:
            _progress(2, f'数据采集中... 已完成 {n}/11 项 (已用时 {elapsed:.0f}s)')
        elif n <= 8:
            _progress(3, f'数据采集中... 已完成 {n}/11 项 (已用时 {elapsed:.0f}s)')
        else:
            _progress(4, f'数据采集中... 已完成 {n}/11 项 (已用时 {elapsed:.0f}s)')

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {}

        # K线分析（专家1视角）
        futures[executor.submit(
            _safe_call, k_line_read1, stock_name, 'o1',
            default=('', ''), label='K线分析(o1)'
        )] = 'kline1'

        # 网络搜索（4项）
        futures[executor.submit(
            _safe_call, kimi_web_search, task,
            default='', label='网络新闻搜索'
        )] = 'web_search'

        futures[executor.submit(
            _safe_call, kimi_web_market_feeling, task,
            default='', label='市场舆情分析'
        )] = 'market_feeling'

        futures[executor.submit(
            _safe_call, kimi_web_reconstuction_search, task,
            default='', label='资本运作搜索'
        )] = 'web_reconstuction'

        futures[executor.submit(
            _safe_call, kimi_web_stock_report, stock_name,
            default='', label='券商报告搜索'
        )] = 'stock_report'

        # 基本面
        futures[executor.submit(
            _safe_call, predict_future_performance, stock_name,
            default='', label='年报预测分析'
        )] = 'annual_report'

        futures[executor.submit(
            _safe_call, extract_business_and_development_info, stock_name,
            default='', label='商业模式提取'
        )] = 'business_model'

        # 估值 / 行情 / 资金流
        futures[executor.submit(
            _safe_call, fetch_value_data, stock_name, model_type_1,
            default='', label='估值分析'
        )] = 'value_report'

        futures[executor.submit(
            _safe_call, fetch_stock_and_industry_data, stock_name, model_type_1,
            default='', label='行情数据分析'
        )] = 'data_report'

        futures[executor.submit(
            _safe_call, fetch_money_data, stock_name, model_type_1,
            default='', label='资金流分析'
        )] = 'money_report'

        # 策略回测
        futures[executor.submit(
            _safe_call, strategy_construction, stock_name, model_type_1,
            default=('', ''), label='策略构建与回测'
        )] = 'strategy'

        # 收集结果
        for future in as_completed(futures):
            key = futures[future]
            try:
                result = future.result()
                _on_complete(key, result)
            except Exception as e:
                print(f'[ERROR] {key}: {e}')
                _on_complete(key, None)

    # 解包结果
    kline1_result = data.get('kline1', ('', ''))
    if isinstance(kline1_result, tuple) and len(kline1_result) == 2:
        k_line_image_path, k_line_prompt = kline1_result
    else:
        k_line_image_path, k_line_prompt = '', ''

    strategy_result = data.get('strategy', ('', ''))
    if isinstance(strategy_result, tuple) and len(strategy_result) == 2:
        strategy_analysis_text, strategy_image_path = strategy_result
    else:
        strategy_analysis_text, strategy_image_path = '', ''

    web_search = data.get('web_search', '')
    market_feeling = data.get('market_feeling', '')
    web_reconstuction = data.get('web_reconstuction', '')
    stock_report = data.get('stock_report', '')
    annual_report = data.get('annual_report', '')
    business_model = data.get('business_model', '')
    value_report = data.get('value_report', '')
    data_report = data.get('data_report', '')
    money_report = data.get('money_report', '')

    _progress(4, f'数据采集完成，共11项 (已用时 {time.time()-t0:.0f}s)')

    # ═══════════════ Phase 2: 专家1 (GPT-4o) ═══════════════
    _progress(5, '专家1(GPT-4o): 分析A股大盘走势...')
    A_share_analyse = _safe_call(analyse_A_markt, stock_name,
                                  default='', label='A股大盘分析')

    _progress(6, '专家1(GPT-4o): 初步投资决策分析...')
    model_type_expert1 = 'deepseek'
    full_result1 = _safe_call(
        decision_tool, stock_name, A_share_analyse,
        k_line_prompt, data_report, money_report, value_report,
        annual_report, business_model,
        web_search, market_feeling, stock_report, web_reconstuction,
        model_type_expert1,
        default='', label='专家1初步决策'
    )

    _progress(7, '专家1(GPT-4o): 反思与改进...')
    reflection1 = _safe_call(final_reflection, full_result1, model_type_expert1,
                              default='', label='专家1反思')

    _progress(8, '专家1(GPT-4o): 最终决策生成...')
    decision1_result = _safe_call(
        final_decision1, stock_name, A_share_analyse,
        k_line_prompt, data_report, value_report, money_report,
        annual_report, business_model, full_result1,
        web_search, market_feeling, stock_report, reflection1,
        web_reconstuction, model_type_expert1,
        default=('', ''), label='专家1最终决策'
    )

    if isinstance(decision1_result, tuple) and len(decision1_result) == 2:
        final_decision_resp1, _ = decision1_result
    else:
        final_decision_resp1 = decision1_result if isinstance(decision1_result, str) else ''

    if isinstance(final_decision_resp1, str) and final_decision_resp1:
        full_result1 = final_decision_resp1

    # ═══════════════ Phase 3: 专家2 (DeepSeek) ═══════════════
    _progress(9, '专家2(DeepSeek): K线图视觉分析（第二视角）...')
    kline2_result = _safe_call(k_line_read1, stock_name, 'kimi',
                                default=('', ''), label='K线分析(kimi)')
    if isinstance(kline2_result, tuple) and len(kline2_result) == 2:
        k_line_image_path2, k_line_prompt2 = kline2_result
    else:
        k_line_image_path2, k_line_prompt2 = '', ''

    model_type_expert2 = 'deepseek'

    _progress(10, '专家2(DeepSeek): 初步投资决策分析...')
    # 复用 Phase 1 的 data_report/value_report/money_report（与原代码一致）
    full_result2 = _safe_call(
        decision_tool, stock_name, A_share_analyse,
        k_line_prompt2, data_report, money_report, value_report,
        annual_report, business_model,
        web_search, market_feeling, stock_report, web_reconstuction,
        model_type_expert2,
        default='', label='专家2初步决策'
    )

    _progress(11, '专家2(DeepSeek): 反思与改进...')
    reflection2 = _safe_call(final_reflection, full_result2, model_type_expert2,
                              default='', label='专家2反思')

    _progress(12, '专家2(DeepSeek): 最终决策 + 构建思维链路图...')
    decision2_result = _safe_call(
        final_decision1, stock_name, A_share_analyse,
        k_line_prompt2, data_report, value_report, money_report,
        annual_report, business_model, full_result2,
        web_search, market_feeling, stock_report, reflection2,
        web_reconstuction, model_type_expert2,
        default=('', ''), label='专家2最终决策'
    )

    reasoning_content = ''
    if isinstance(decision2_result, tuple) and len(decision2_result) == 2:
        final_decision_resp2, reasoning_content = decision2_result
    else:
        final_decision_resp2 = decision2_result if isinstance(decision2_result, str) else ''

    if isinstance(final_decision_resp2, str) and final_decision_resp2:
        full_result2 = final_decision_resp2

    # 生成逻辑链路图
    logical_path = None
    if reasoning_content:
        try:
            logical_path_result = generate_and_save_analysis_graph(
                task, reasoning_content,
                filename=f"investment_analysis_{stock_name}_logic.png"
            )
            if isinstance(logical_path_result, tuple) and len(logical_path_result) == 2:
                logical_path = logical_path_result[0]
            else:
                logical_path = logical_path_result
            print(f'[OK] 逻辑链路图生成: {logical_path}')
        except Exception as e:
            print(f'[WARNING] 逻辑链路图生成失败: {e}')

    # ═══════════════ Phase 4: 专家辩论 + MOE决策 ═══════════════
    _progress(13, '混合专家辩论系统: 第1轮辩论...')

    formatted_debate = ''
    final_conclusion = ''
    final_moe_decision = ''

    try:
        final_conclusion_result, debate_history = expert_debate(
            initial_response1=full_result1,
            initial_response2=full_result2,
            task=task
        )
        final_conclusion = final_conclusion_result

        _progress(14, '混合专家辩论系统: 辩论进行中...')
        _progress(15, '混合专家辩论系统: 辩论完成，汇总结论...')

        # 格式化辩论记录
        formatted_debate = format_debate_for_ui(debate_history)

        print(f'[OK] 专家辩论完成，共 {len(debate_history)} 条记录')
    except Exception as e:
        print(f'[WARNING] 专家辩论失败: {e}')
        traceback.print_exc()
        _progress(15, '专家辩论出错，跳过...')

    # MOE 最终决策
    _progress(16, 'MOE最终决策生成...')
    final_moe_decision = _safe_call(
        final_decision2, stock_name, final_conclusion,
        default='', label='MOE最终决策'
    )

    # ═══════════════ Step 17: 生成可视化图表 ═══════════════
    _progress(17, '生成可视化图表（共10张）...')

    figures = []

    # 图1: 逻辑链路图
    if logical_path and os.path.exists(str(logical_path)):
        figures.append((_image_to_figure(logical_path, '深度思考逻辑链路图'), '深度思考逻辑链路图'))
    else:
        figures.append((_render_text_figure(
            '逻辑链路图未能生成（可能因为 DeepSeek 推理内容缺失）',
            '深度思考逻辑链路图'), '深度思考逻辑链路图'))

    # 图2: K线技术分析图
    if k_line_image_path and os.path.exists(str(k_line_image_path)):
        figures.append((_image_to_figure(k_line_image_path, '日K线技术分析图'), '日K线技术分析图'))
    elif k_line_image_path2 and os.path.exists(str(k_line_image_path2)):
        figures.append((_image_to_figure(k_line_image_path2, '日K线技术分析图'), '日K线技术分析图'))
    else:
        figures.append((_render_text_figure('K线图未能生成', '日K线技术分析图'), '日K线技术分析图'))

    # 图3: 策略回测效果图
    if strategy_image_path and os.path.exists(str(strategy_image_path)):
        figures.append((_image_to_figure(strategy_image_path, '策略回测效果图'), '策略回测效果图'))
    else:
        fig_strategy = _render_text_figure(
            strategy_analysis_text if strategy_analysis_text else '策略回测未能生成',
            '策略构建分析')
        figures.append((fig_strategy, '策略构建分析'))

    # 图4: MOE 最终投资决策
    decision_display = ''
    if final_moe_decision:
        decision_display += '【MOE 混合专家最终决策】\n\n' + final_moe_decision
    else:
        decision_display = '（MOE决策未能生成）'
    figures.append((_render_text_figure(decision_display, f'MOE最终投资决策 — {stock_name}',
                                         figsize=(20, 14)), f'MOE最终投资决策 — {stock_name}'))

    # 图5: 专家辩论记录
    debate_display = formatted_debate if formatted_debate else '（辩论记录为空）'
    figures.append((_render_text_figure(debate_display, '专家辩论记录',
                                         figsize=(20, 22)), '专家辩论记录'))

    # 图6: 专家辩论最终结论
    conclusion_display = final_conclusion if final_conclusion else '（最终结论为空）'
    figures.append((_render_text_figure(conclusion_display, '专家辩论最终结论',
                                         figsize=(20, 16)), '专家辩论最终结论'))

    # 图7: 数据综合报告（行情 + 估值 + 资金流）
    figures.append((_render_combined_report([
        ('行情数据分析', data_report),
        ('估值分析报告', value_report),
        ('资金流分析报告', money_report),
    ], f'数据综合报告 — {stock_name}', figsize=(20, 22)),
        f'数据综合报告 — {stock_name}'))

    # 图8: 市场情报综合（新闻 + 舆情 + 券商 + 资本运作）
    figures.append((_render_combined_report([
        ('网络新闻搜索', web_search),
        ('市场舆情信息', market_feeling),
        ('券商分析师报告', stock_report),
        ('资本运作情报', web_reconstuction),
    ], f'市场情报综合 — {stock_name}', figsize=(20, 22)),
        f'市场情报综合 — {stock_name}'))

    # 图9: 专家1分析报告
    expert1_display = f'【A股大盘分析】\n{A_share_analyse}\n\n【专家1决策分析】\n{full_result1}'
    figures.append((_render_text_figure(expert1_display, '专家1(GPT-4o) 分析报告',
                                         figsize=(20, 20)), '专家1(GPT-4o) 分析报告'))

    # 图10: 专家2分析报告
    expert2_display = f'【专家2决策分析】\n{full_result2}'
    figures.append((_render_text_figure(expert2_display, '专家2(DeepSeek) 分析报告',
                                         figsize=(20, 20)), '专家2(DeepSeek) 分析报告'))

    total_time = time.time() - t0
    _progress(18, f'全部完成（总用时 {total_time:.0f}s，共 {len(figures)} 张图表）')

    print(f'[Agent] 智能体协同策略完成: {stock_name}, 总用时 {total_time:.0f}s, 生成 {len(figures)} 张图表')

    return figures
