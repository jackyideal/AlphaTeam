"""
FinEvolver 策略 — 进化式多智能体协同分析系统
基于遗传算法的专家进化 + CoT/ToT/GoT 三策略推理 + 拍卖机制任务分配 + 梯度优化
"""
import os
import sys
import time
import json
import textwrap
import traceback
import glob

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

# ──────────────── 指标元数据 ────────────────

INDICATOR_META = {
    'id': 'ind_22_fin_evolver',
    'name': 'FinEvolver 策略',
    'group': '策略模型',
    'description': '进化式多智能体协同分析框架：基于遗传算法的专家群体进化、CoT/ToT/GoT三策略自适应推理、拍卖机制驱动的子任务分配与文本梯度优化，实现持续进化的智能投研',
    'input_type': 'query',
    'default_query': '青松建化短期是否值得买入？',
    'requires_db': False,
    'slow': True,
    'chart_count': 7,
    'chart_descriptions': [
        'FinEvolver最终分析结论，多智能体协同推理的综合投资建议',
        'K线技术分析图，基于量价形态的技术面判断',
        '分析过程详情，展示CoT/ToT/GoT推理策略的执行链路',
        '专家讨论与辩论记录，进化后专家群体的观点交锋',
        'FinEvolver系统状态，当前专家群体的进化代数与适应度分布',
        '（动态生成）附加分析图表',
        '（动态生成）附加分析图表',
    ],
}

# FinEvolver 路径
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EVOLVER_DIR = os.getenv(
    'FIN_EVOLVER_DIR',
    os.path.join(_REPO_ROOT, 'third_party', 'Evolve_Fin', 'script')
)

# API 配置
QWEN_API_KEY = os.getenv('QWEN_API_KEY', '')
QWEN_BASE_URL = os.getenv('QWEN_BASE_URL', 'https://dashscope.aliyuncs.com/compatible-mode/v1')
QWEN_MODEL = os.getenv('QWEN_MODEL', 'qwen-plus-latest')


# ──────────────── 工具函数 ────────────────

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
               linewidth=1.5)

    # 文本处理
    lines = text.split('\n')
    wrapped_lines = []
    for line in lines:
        if len(line) > 90:
            wrapped_lines.extend(textwrap.wrap(line, width=90))
        else:
            wrapped_lines.append(line)

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


def _find_kline_image():
    """查找最近生成的 K线图 PNG 文件"""
    # 搜索 FinEvolver 工作目录和当前目录
    search_dirs = [EVOLVER_DIR, os.getcwd(), os.path.dirname(EVOLVER_DIR)]
    for d in search_dirs:
        pattern = os.path.join(d, '*_kline.png')
        files = glob.glob(pattern)
        if files:
            # 返回最新的文件
            return max(files, key=os.path.getmtime)
    return None


def _extract_solving_details(solving_process):
    """从 solving_process 中提取可读文本"""
    if isinstance(solving_process, str):
        return solving_process

    if isinstance(solving_process, dict):
        parts = []

        # 单专家模式
        if 'expert' in solving_process:
            parts.append(f"专家: {solving_process.get('expert', '未知')}")
            parts.append(f"策略: {solving_process.get('chosen_strategy', '未知')}")
            parts.append(f"评价: {solving_process.get('evaluation', '未知')}")

            intent = solving_process.get('intent_analysis', '')
            if intent:
                parts.append(f"\n━━━ 意图分析 ━━━\n{intent}")

            expanded = solving_process.get('expanded_task', '')
            if expanded:
                parts.append(f"\n━━━ 扩展分析 ━━━\n{expanded}")

            details = solving_process.get('solving_details', {})
            if isinstance(details, dict):
                for key, val in details.items():
                    if isinstance(val, str) and len(val) > 10:
                        parts.append(f"\n━━━ {key} ━━━\n{val}")
                    elif isinstance(val, list):
                        for i, item in enumerate(val):
                            parts.append(f"\n━━━ {key} (步骤{i+1}) ━━━\n{item}")

        # 多专家模式
        if 'subtasks' in solving_process:
            subtasks = solving_process.get('subtasks', [])
            parts.append(f"子任务分解 ({len(subtasks)} 项):")
            for i, st in enumerate(subtasks):
                parts.append(f"  {i+1}. {st}")

            allocation = solving_process.get('allocation', {})
            if allocation:
                parts.append(f"\n任务分配:")
                for task_id, expert in allocation.items():
                    parts.append(f"  {task_id} → {expert}")

            results = solving_process.get('subtask_results', [])
            for r in results:
                if isinstance(r, dict):
                    expert = r.get('expert_name', '未知')
                    result_text = r.get('result', '')
                    parts.append(f"\n━━━ {expert} 的分析 ━━━\n{result_text}")

        return '\n'.join(parts)

    return str(solving_process)


def _extract_discussion_text(expert_discussion):
    """从 expert_discussion 中提取可读文本"""
    if not isinstance(expert_discussion, dict):
        return str(expert_discussion) if expert_discussion else '（无讨论）'

    parts = []
    conducted = expert_discussion.get('conducted', False)

    if not conducted:
        boss_answer = expert_discussion.get('final_boss_answer', '未进行专家讨论')
        return f"讨论状态: 未进行\n说明: {boss_answer}"

    parts.append("讨论状态: 已进行")

    consensus = expert_discussion.get('consensus_reached')
    if consensus is not None:
        parts.append(f"共识达成: {'是' if consensus else '否'}")

    boss_answer = expert_discussion.get('final_boss_answer', '')
    if boss_answer:
        parts.append(f"\n━━━ 最终整合结论 ━━━\n{boss_answer}")

    debate_history = expert_discussion.get('debate_history', [])
    if debate_history:
        parts.append(f"\n━━━ 辩论记录 ({len(debate_history)} 条) ━━━")
        for item in debate_history:
            if isinstance(item, dict):
                expert = item.get('expert', '未知')
                content = item.get('content', '')
                parts.append(f"\n【{expert}】\n{content}")
            elif isinstance(item, str):
                parts.append(item)

    return '\n'.join(parts)


def _build_system_info(result, elapsed_time):
    """构建系统信息摘要"""
    parts = [
        f"任务类型: {result.get('task_type', '未知')}",
        f"使用策略: {result.get('strategy_used', '未知')}",
        f"总用时: {elapsed_time:.0f} 秒",
        '',
    ]

    # 进化状态
    try:
        state_path = os.path.join(EVOLVER_DIR, 'evolution_state.json')
        if os.path.exists(state_path):
            with open(state_path, 'r') as f:
                state = json.load(f)
            parts.append('━━━ 进化系统状态 ━━━')
            parts.append(f"当前代数: 第 {state.get('generation', 0)} 代")
            parts.append(f"累计任务: {state.get('task_count', 0)} 个")
            experts = state.get('experts', [])
            if experts:
                parts.append(f"\n专家团队 ({len(experts)} 人):")
                for exp in experts:
                    name = exp.get('name', '?')
                    role = exp.get('role', '?')
                    fitness = exp.get('fitness', 0)
                    genes = exp.get('genes', {})
                    strategy = genes.get('strategy_prob', {})
                    parts.append(f"  {name} [{role}] 适应度={fitness:.2f}")
                    if strategy:
                        probs = ', '.join([f'{k}:{v:.1%}' for k, v in strategy.items()])
                        parts.append(f"    策略概率: {probs}")
    except Exception as e:
        parts.append(f'进化状态读取失败: {e}')

    return '\n'.join(parts)


# ──────────────── 主生成函数 ────────────────

def generate(query='青松建化短期是否值得买入？', progress_callback=None, **kwargs):
    """
    FinEvolver 进化式多智能体协同分析
    """
    total_steps = 7
    t0 = time.time()

    def _progress(step, msg):
        elapsed = time.time() - t0
        if progress_callback:
            progress_callback(step, total_steps, f'{msg} (已用时 {elapsed:.0f}s)')

    # ── 延迟导入 ──
    if EVOLVER_DIR not in sys.path:
        sys.path.insert(0, EVOLVER_DIR)

    # 记录 K线图修改时间，用于后续检测新生成的图片
    old_kline = _find_kline_image()
    old_mtime = os.path.getmtime(old_kline) if old_kline else 0

    # ═══════════════ Step 0: 初始化 TaskSolver ═══════════════
    _progress(0, f'初始化 FinEvolver 系统（提取股票、加载专家团队、进化状态）...')

    try:
        # 切换到 FinEvolver 工作目录，因为它会在当前目录保存文件
        original_cwd = os.getcwd()
        os.chdir(EVOLVER_DIR)

        from try_new import TaskSolver

        solver = TaskSolver(
            api_key=QWEN_API_KEY,
            base_url=QWEN_BASE_URL,
            model=QWEN_MODEL,
            query=query
        )
        stock_name = solver.stock_name or query
        print(f'[FinEvolver] 初始化完成，股票: {stock_name}')
    except Exception as e:
        print(f'[ERROR] TaskSolver 初始化失败: {e}')
        traceback.print_exc()
        os.chdir(original_cwd)
        _progress(total_steps, '初始化失败')
        fig = _render_text_figure(f'FinEvolver 初始化失败:\n{str(e)}', '系统错误')
        return [(fig, '系统错误')]

    # ═══════════════ Step 1: 意图解析 ═══════════════
    _progress(1, f'解析用户意图，判断任务复杂度（股票: {stock_name}）...')

    # ═══════════════ Step 2-4: 执行分析 ═══════════════
    _progress(2, '多智能体协同分析中（专家选择/拍卖、策略推理、工具调用）...')

    result = None
    try:
        result = solver.solve_task(query)
        print(f'[FinEvolver] 分析完成，任务类型: {result.get("task_type", "未知")}')
    except Exception as e:
        print(f'[ERROR] solve_task 失败: {e}')
        traceback.print_exc()
        result = {
            'task': query,
            'task_type': '执行失败',
            'strategy_used': '无',
            'solving_process': f'错误: {str(e)}',
            'expert_discussion': {'conducted': False, 'final_boss_answer': f'执行失败: {str(e)}'},
            'final_result': f'分析过程中出现错误: {str(e)}'
        }
    finally:
        os.chdir(original_cwd)

    _progress(3, '分析完成，整理专家结论...')

    # ═══════════════ Step 4: 提取结果 ═══════════════
    _progress(4, '提取分析结果...')

    final_result = result.get('final_result', '（无结果）')
    task_type = result.get('task_type', '未知')
    strategy_used = result.get('strategy_used', '未知')
    solving_process = result.get('solving_process', {})
    expert_discussion = result.get('expert_discussion', {})

    # ═══════════════ Step 5: 查找 K线图 ═══════════════
    _progress(5, '检索生成的K线技术分析图...')

    # 查找本次运行新生成的 K线图
    new_kline = _find_kline_image()
    kline_path = None
    if new_kline:
        new_mtime = os.path.getmtime(new_kline)
        if new_mtime > old_mtime:
            kline_path = new_kline
            print(f'[FinEvolver] 发现新K线图: {kline_path}')

    # ═══════════════ Step 6: 生成可视化图表 ═══════════════
    _progress(6, '生成可视化图表...')

    total_time = time.time() - t0
    figures = []

    # 图1: 最终分析结论
    header = f'【{task_type}】使用策略: {strategy_used}\n'
    header += f'分析对象: {stock_name} | 总用时: {total_time:.0f}s\n'
    header += '═' * 60 + '\n\n'
    figures.append((
        _render_text_figure(header + final_result,
                            f'FinEvolver 最终分析结论 — {stock_name}',
                            figsize=(20, 18)),
        f'FinEvolver 最终分析结论 — {stock_name}'
    ))

    # 图2: K线技术分析图（如果有）
    if kline_path and os.path.exists(kline_path):
        figures.append((
            _image_to_figure(kline_path, f'{stock_name} K线技术分析图'),
            f'{stock_name} K线技术分析图'
        ))

    # 图3: 分析过程详情
    process_text = _extract_solving_details(solving_process)
    if process_text and len(process_text) > 20:
        figures.append((
            _render_text_figure(process_text, '分析过程详情',
                                figsize=(20, 20)),
            '分析过程详情'
        ))

    # 图4: 专家讨论记录
    discussion_text = _extract_discussion_text(expert_discussion)
    if discussion_text and len(discussion_text) > 30:
        figures.append((
            _render_text_figure(discussion_text, '专家讨论与辩论记录',
                                figsize=(20, 18)),
            '专家讨论与辩论记录'
        ))

    # 图5: 系统信息（进化状态、专家团队）
    system_info = _build_system_info(result, total_time)
    figures.append((
        _render_text_figure(system_info, 'FinEvolver 系统状态',
                            figsize=(18, 12)),
        'FinEvolver 系统状态'
    ))

    # 确保至少有1张图
    if not figures:
        figures.append((
            _render_text_figure('未能生成任何分析结果', '系统提示'),
            '系统提示'
        ))

    _progress(7, f'全部完成（总用时 {total_time:.0f}s，共 {len(figures)} 张图表）')
    print(f'[FinEvolver] 完成: {stock_name}, 用时 {total_time:.0f}s, {len(figures)} 张图表')

    return figures
