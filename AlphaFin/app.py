"""
AlphaFin - 股票市场指标可视化系统
运行方式: python app.py 或 flask run
"""
import sys
import os

# 确保项目根目录在路径中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, render_template, jsonify, request, Response, send_from_directory, redirect
import json
import time
import uuid
from threading import Thread

from AlphaFin.config import SECRET_KEY, DEBUG, CHART_DIR
from AlphaFin.indicators.indicator_registry import REGISTRY, get_grouped
from AlphaFin.services.progress_service import progress_store, get_progress, update_progress

app = Flask(__name__)
app.secret_key = SECRET_KEY
ASSET_VERSION = str(int(time.time()))

# 注册智能分析团队 Blueprint
from AlphaFin.ai_team import create_team_blueprint
app.register_blueprint(create_team_blueprint())


@app.context_processor
def inject_asset_version():
    """静态资源版本号，用于前端强制刷新缓存。"""
    return {'asset_version': ASSET_VERSION}


def _build_goal_projection(current_price=4.5, total_shares_override=None, total_debt_override=None,
                           fixed_assets_override=None):
    """Ideal 目标测算（个人页面展示，不参与智能体工具链）"""
    # 参考: 各种指标/目标/目标.ipynb
    dad_shares = 861
    mom_shares = 192.2
    outside_shares = 460

    debt_dad_margin = 1920
    debt_mom_margin = 430
    debt_outside_personal = 750
    debt_other_funds = 500
    debt_apartment_pledge = 800
    debt_office_pledge = 470
    debt_home_pledge = 800
    debt_outside_account = 2500

    goal_villa = 2000
    goal_renovation = 1000
    goal_alphard = 80
    goal_cayenne = 80
    goal_yiyi_house = 250

    purchase_target = goal_villa + goal_renovation + goal_alphard + goal_cayenne + goal_yiyi_house
    base_total_debt = (
        debt_dad_margin + debt_mom_margin + debt_outside_personal + debt_other_funds +
        debt_apartment_pledge + debt_office_pledge + debt_home_pledge + debt_outside_account
    )
    default_fixed_assets = 5000.0

    base_total_shares = dad_shares + mom_shares + outside_shares
    default_total_shares = 2000.0
    default_total_debt = 0.0

    if total_shares_override is not None and float(total_shares_override) > 0:
        total_shares = float(total_shares_override)
    else:
        total_shares = default_total_shares

    # 支持手动设置为 0（此前逻辑会被回退到旧负债）
    if total_debt_override is not None and float(total_debt_override) >= 0:
        total_debt = float(total_debt_override)
    else:
        total_debt = default_total_debt

    if fixed_assets_override is not None and float(fixed_assets_override) >= 0:
        fixed_assets = float(fixed_assets_override)
    else:
        fixed_assets = default_fixed_assets

    current_price = float(current_price) if current_price and current_price > 0 else 4.5

    price_list = [4.35, 4.43, 4.8, 5, 5.5, 6, 6.5, 7, 7.5, 8, 8.5, 9, 9.5, 10, 10.5, 11, 12]
    price_list = sorted(set(price_list + [round(current_price, 2)]))
    rows = []
    for price in price_list:
        stock_assets = total_shares * price
        total_assets = stock_assets + fixed_assets
        net_assets = total_assets - total_debt
        target_gap = net_assets - purchase_target
        rows.append({
            'price': round(price, 2),
            'stock_assets': round(stock_assets, 2),
            'total_assets': round(total_assets, 2),
            'total_debt': round(total_debt, 2),
            'net_assets': round(net_assets, 2),
            'target_gap': round(target_gap, 2),
        })

    current_stock_assets = total_shares * current_price
    current_total_assets = current_stock_assets + fixed_assets
    current_net_assets = current_total_assets - total_debt
    current_target_gap = current_net_assets - purchase_target
    current_row = {
        'price': round(current_price, 2),
        'stock_assets': round(current_stock_assets, 2),
        'total_assets': round(current_total_assets, 2),
        'total_debt': round(total_debt, 2),
        'net_assets': round(current_net_assets, 2),
        'target_gap': round(current_target_gap, 2),
    }
    break_even_price = round((purchase_target + total_debt - fixed_assets) / total_shares, 2) if total_shares else 0

    return {
        'rows': rows,
        'current_price': round(current_price, 2),
        'current_row': current_row,
        'break_even_price': break_even_price,
        'total_shares': round(total_shares, 2),
        'purchase_target': round(purchase_target, 2),
        'fixed_assets': round(fixed_assets, 2),
        'total_debt': round(total_debt, 2),
    }


# ──────────────── 页面路由 ────────────────

@app.route('/')
def index():
    """首页 - 指标概览"""
    groups = get_grouped()
    return render_template('index.html', groups=groups, registry=REGISTRY)


@app.route('/ai')
def ai_page():
    """AI 智能分析页面"""
    groups = get_grouped()
    return render_template('ai_chat.html', groups=groups, show_watch_panel=False)


@app.route('/post_review')
def post_review_page():
    """盘后复盘页面"""
    groups = get_grouped()
    return render_template('post_review.html', groups=groups)


@app.route('/sector_news')
def sector_news_page():
    """板块热点分析页面"""
    groups = get_grouped()
    return render_template('sector_news.html', groups=groups)


@app.route('/stock')
def stock_page():
    """个股通用分析页面"""
    groups = get_grouped()
    return render_template('stock_analysis.html', groups=groups)


@app.route('/stock_pattern')
def stock_pattern_page():
    """K线结构匹配预测页面"""
    groups = get_grouped()
    return render_template('stock_pattern.html', groups=groups)


@app.route('/stock_resonance')
def stock_resonance_page():
    """多周期共振系统页面"""
    groups = get_grouped()
    return render_template('stock_resonance.html', groups=groups)


@app.route('/stock_ml')
def stock_ml_page():
    """机器学习集成预测页面"""
    groups = get_grouped()
    return render_template('stock_ml.html', groups=groups)


@app.route('/industry_map')
def industry_map_page():
    """行业时空图谱页面"""
    groups = get_grouped()
    return render_template('industry_map.html', groups=groups)


@app.route('/market_hub')
def market_hub_page():
    """市场顶底指标（牛市逃顶指标 + 铜油比）"""
    groups = get_grouped()
    return render_template('market_hub.html', groups=groups)


@app.route('/goal')
def goal_page():
    """Ideal 目标页面（个人规划，不纳入指标工具链）"""
    groups = get_grouped()
    def _to_float(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    current_price = _to_float(request.args.get('current_price', 4.5))
    total_shares = _to_float(request.args.get('total_shares'))
    total_debt = _to_float(request.args.get('total_debt'))
    fixed_assets = _to_float(request.args.get('fixed_assets'))

    goal = _build_goal_projection(
        current_price=current_price if current_price and current_price > 0 else 4.5,
        total_shares_override=total_shares,
        total_debt_override=total_debt,
        fixed_assets_override=fixed_assets,
    )
    return render_template('goal.html', groups=groups, goal=goal)


@app.route('/master')
def master_strategies_page():
    """大师选股策略专属页面"""
    groups = get_grouped()
    return render_template('master_strategies.html', groups=groups)


@app.route('/indicator/<indicator_id>')
def indicator_page(indicator_id):
    """指标详情页"""
    # 大师选股策略重定向到专属页面
    if indicator_id == 'ind_23_master_strategies':
        return redirect('/master')
    meta = REGISTRY.get(indicator_id)
    if not meta:
        return '指标不存在', 404
    groups = get_grouped()
    # 排除内部字段，避免序列化和前端展示噪声
    meta_json = {k: v for k, v in meta.items() if not str(k).startswith('_')}
    return render_template('indicator.html', meta=meta_json, indicator_id=indicator_id, groups=groups)


# ──────────────── API 路由 ────────────────

@app.route('/api/compute/<indicator_id>', methods=['POST'])
def compute_indicator(indicator_id):
    """触发指标计算"""
    from AlphaFin.services.chart_service import generate_charts

    params = request.json or {}
    task_id = str(uuid.uuid4())

    t = Thread(target=generate_charts, args=(indicator_id, params, task_id))
    t.daemon = True
    t.start()

    return jsonify({'task_id': task_id})


@app.route('/api/progress/<task_id>')
def progress_stream(task_id):
    """SSE 实时进度推送"""
    def event_stream():
        last_signature = None
        timeout_count = 0
        while timeout_count < 7200:  # 最多等60分钟，避免慢任务在前端无响应
            info = get_progress(task_id)
            signature = (
                info.get('step', -1),
                info.get('total', 0),
                info.get('message', ''),
                bool(info.get('done', False)),
            )
            if signature != last_signature:
                data = json.dumps(info, ensure_ascii=False)
                yield f"data: {data}\n\n"
                last_signature = signature
                timeout_count = 0
            if info.get('done'):
                break
            time.sleep(0.5)
            timeout_count += 1

    return Response(event_stream(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/api/charts/<task_id>')
def get_charts(task_id):
    """获取已生成的图表路径"""
    info = get_progress(task_id)
    return jsonify({
        'charts': info.get('chart_paths', []),
        'titles': info.get('chart_titles', []),
        'done': info.get('done', False),
        'message': info.get('message', ''),
    })


@app.route('/api/chat', methods=['POST'])
def chat():
    """Qwen API 对话"""
    from AlphaFin.services.claude_service import analyze_charts

    data = request.json
    chart_paths = data.get('chart_paths', [])
    user_message = data.get('message', '')
    history = data.get('history', [])

    response = analyze_charts(chart_paths, user_message, history)
    return jsonify({'reply': response})


@app.route('/api/ai_chat', methods=['POST'])
def ai_chat_api():
    """独立 AI 对话（内部数据增强 + 联网搜索）"""
    from AlphaFin.services.ai_chat_service import ai_chat_grounded
    from AlphaFin.services.context_upload_service import build_context_text, build_context_file_refs

    data = request.json or {}
    user_message = data.get('message', '')
    history = data.get('history', [])
    enable_search = data.get('enable_search', True)
    model_name = str(data.get('model') or '').strip()
    context_ids = data.get('context_ids', [])
    ctx_payload = build_context_text(context_ids)
    context_text = ctx_payload.get('context_text', '')
    ctx_refs = build_context_file_refs(context_ids)
    context_file_ids = ctx_refs.get('file_ids') or []

    # 统一走“内部数据证据链”主链路：
    # - enable_search=True 时：内部数据 + 联网搜索双重证据
    # - enable_search=False 时：仅内部数据
    # 这样可避免“联网波动时只靠模型自由发挥”导致结果不稳。
    response = ai_chat_grounded(
        user_message, history, enable_search=bool(enable_search),
        context_text=context_text, context_file_ids=context_file_ids,
        model_name=model_name,
    )
    if isinstance(response, dict):
        response['context_file_count'] = int(ctx_refs.get('file_count') or 0)
        response['context_used_ids'] = ctx_refs.get('used_ids') or []
        return jsonify(response)
    return jsonify({'reply': response})


@app.route('/api/ai_chat/status')
def ai_chat_status_api():
    """获取 AI 智能分析的数据源状态。"""
    from AlphaFin.services.ai_chat_service import get_realtime_data_status
    return jsonify(get_realtime_data_status())


@app.route('/api/ai_chat/models')
def ai_chat_models_api():
    """获取 AI 智能分析可选模型与当前模型。"""
    from AlphaFin.services.model_config_service import get_allowed_models, get_module_model
    return jsonify({
        'module': 'ai_chat',
        'allowed_models': get_allowed_models(),
        'current_model': get_module_model('ai_chat'),
    })


@app.route('/api/ai_chat/model', methods=['PUT'])
def ai_chat_model_update_api():
    """更新 AI 智能分析默认模型。"""
    from AlphaFin.services.model_config_service import set_module_model, get_module_model
    data = request.json or {}
    model_name = str(data.get('model') or '').strip()
    if not model_name:
        return jsonify({'error': '模型不能为空'}), 400
    try:
        saved = set_module_model('ai_chat', model_name)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    return jsonify({
        'success': True,
        'model': saved,
        'current_model': get_module_model('ai_chat'),
    })


@app.route('/api/ai_chat/prompts')
def ai_chat_prompts_api():
    """获取 AI 智能分析系统提示词配置。"""
    from AlphaFin.services.ai_chat_service import get_ai_chat_prompt_configs
    return jsonify({
        'module': 'ai_chat',
        'prompts': get_ai_chat_prompt_configs(),
    })


@app.route('/api/ai_chat/prompts/<prompt_key>', methods=['PUT'])
def ai_chat_prompt_update_api(prompt_key):
    """更新 AI 智能分析系统提示词。"""
    from AlphaFin.services.ai_chat_service import get_ai_chat_prompt_configs
    from AlphaFin.services.prompt_config_service import set_prompt

    prompt_items = get_ai_chat_prompt_configs()
    prompt_map = {item.get('key'): item for item in prompt_items}
    if prompt_key not in prompt_map:
        return jsonify({'error': '提示词不存在: %s' % prompt_key}), 404

    data = request.json or {}
    prompt_text = str(data.get('prompt', '')).strip()
    if not prompt_text:
        return jsonify({'error': '提示词不能为空'}), 400

    try:
        set_prompt('ai_chat', prompt_key, prompt_text)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    return jsonify({
        'success': True,
        'key': prompt_key,
        'prompts': get_ai_chat_prompt_configs(),
    })


@app.route('/api/ai_chat/prompts/<prompt_key>/reset', methods=['POST'])
def ai_chat_prompt_reset_api(prompt_key):
    """重置 AI 智能分析系统提示词。"""
    from AlphaFin.services.ai_chat_service import get_ai_chat_prompt_configs
    from AlphaFin.services.prompt_config_service import reset_prompt

    prompt_items = get_ai_chat_prompt_configs()
    prompt_map = {item.get('key'): item for item in prompt_items}
    if prompt_key not in prompt_map:
        return jsonify({'error': '提示词不存在: %s' % prompt_key}), 404

    reset_prompt('ai_chat', prompt_key)
    return jsonify({
        'success': True,
        'key': prompt_key,
        'prompts': get_ai_chat_prompt_configs(),
    })


@app.route('/api/ai_watch/snapshot')
def ai_watch_snapshot_api():
    """获取 AI 工具页的盘后复盘聚合快照。"""
    from AlphaFin.services.ai_watch_service import build_market_watch_snapshot

    watchlist = request.args.get('watchlist', '')
    include_news_raw = str(request.args.get('include_news', '1')).strip().lower()
    force_raw = str(request.args.get('force', '0')).strip().lower()

    include_news = include_news_raw not in ('0', 'false', 'off', 'no')
    force = force_raw in ('1', 'true', 'on', 'yes')

    payload = build_market_watch_snapshot(
        watchlist=watchlist,
        include_news=include_news,
        force=force,
    )
    return jsonify(payload)


@app.route('/api/ai_chat/trace/<trace_id>')
def ai_chat_trace_api(trace_id):
    """获取 AI 智能分析的回溯证据。"""
    from AlphaFin.services.ai_chat_service import get_ai_chat_trace
    trace = get_ai_chat_trace(trace_id)
    if not trace:
        return jsonify({'error': 'trace 不存在'}), 404
    return jsonify(trace)


@app.route('/api/sector_news', methods=['POST'])
def sector_news_api():
    """板块热点分析报告 API"""
    from AlphaFin.services.sector_news_service import fetch_sector_report
    from AlphaFin.services.context_upload_service import build_context_text, build_context_file_refs

    data = request.json or {}
    model_name = str(data.get('model') or '').strip()
    context_ids = data.get('context_ids', [])
    ctx_payload = build_context_text(context_ids)
    ctx_refs = build_context_file_refs(context_ids)
    result = fetch_sector_report(
        context_text=ctx_payload.get('context_text', ''),
        context_file_ids=ctx_refs.get('file_ids') or [],
        model_name=model_name,
    )
    result['context_file_count'] = int(ctx_refs.get('file_count') or 0)
    result['context_used_ids'] = ctx_refs.get('used_ids') or []
    return jsonify(result)


@app.route('/api/sector_news/models')
def sector_news_models_api():
    """获取板块热点分析可选模型与当前模型。"""
    from AlphaFin.services.model_config_service import get_allowed_models, get_module_model
    return jsonify({
        'module': 'sector_news',
        'allowed_models': get_allowed_models(),
        'current_model': get_module_model('sector_news'),
    })


@app.route('/api/sector_news/model', methods=['PUT'])
def sector_news_model_update_api():
    """更新板块热点分析默认模型。"""
    from AlphaFin.services.model_config_service import set_module_model, get_module_model
    data = request.json or {}
    model_name = str(data.get('model') or '').strip()
    if not model_name:
        return jsonify({'error': '模型不能为空'}), 400
    try:
        saved = set_module_model('sector_news', model_name)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    return jsonify({
        'success': True,
        'model': saved,
        'current_model': get_module_model('sector_news'),
    })


@app.route('/api/context/upload', methods=['POST'])
def context_upload_api():
    """统一上下文上传接口（AI智能分析/板块热点/智能团队共用）。"""
    try:
        from AlphaFin.services.context_upload_service import upload_context_files

        files = request.files.getlist('files')
        module = request.form.get('module', 'general')
        result = upload_context_files(files, module=module)
        code = 200 if result.get('ok') else 400
        return jsonify(result), code
    except Exception as e:
        return jsonify({
            'ok': False,
            'error': '上下文上传接口异常: %s' % str(e),
        }), 500


@app.route('/api/sector_news/prompts')
def sector_news_prompts_api():
    """获取板块热点分析系统提示词配置。"""
    from AlphaFin.services.sector_news_service import get_sector_news_prompt_configs
    return jsonify({
        'module': 'sector_news',
        'prompts': get_sector_news_prompt_configs(),
    })


@app.route('/api/sector_news/prompts/<prompt_key>', methods=['PUT'])
def sector_news_prompt_update_api(prompt_key):
    """更新板块热点分析系统提示词。"""
    from AlphaFin.services.sector_news_service import get_sector_news_prompt_configs
    from AlphaFin.services.prompt_config_service import set_prompt

    prompt_items = get_sector_news_prompt_configs()
    prompt_map = {item.get('key'): item for item in prompt_items}
    if prompt_key not in prompt_map:
        return jsonify({'error': '提示词不存在: %s' % prompt_key}), 404

    data = request.json or {}
    prompt_text = str(data.get('prompt', '')).strip()
    if not prompt_text:
        return jsonify({'error': '提示词不能为空'}), 400

    try:
        set_prompt('sector_news', prompt_key, prompt_text)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    return jsonify({
        'success': True,
        'key': prompt_key,
        'prompts': get_sector_news_prompt_configs(),
    })


@app.route('/api/sector_news/prompts/<prompt_key>/reset', methods=['POST'])
def sector_news_prompt_reset_api(prompt_key):
    """重置板块热点分析系统提示词。"""
    from AlphaFin.services.sector_news_service import get_sector_news_prompt_configs
    from AlphaFin.services.prompt_config_service import reset_prompt

    prompt_items = get_sector_news_prompt_configs()
    prompt_map = {item.get('key'): item for item in prompt_items}
    if prompt_key not in prompt_map:
        return jsonify({'error': '提示词不存在: %s' % prompt_key}), 404

    reset_prompt('sector_news', prompt_key)
    return jsonify({
        'success': True,
        'key': prompt_key,
        'prompts': get_sector_news_prompt_configs(),
    })


@app.route('/api/update', methods=['POST'])
def update_db():
    """触发数据库更新"""
    from AlphaFin.services.update_service import run_update

    data = request.json or {}
    include_fina = data.get('include_fina', False)
    task_id = str(uuid.uuid4())
    t = Thread(target=run_update, args=(task_id, include_fina))
    t.daemon = True
    t.start()
    return jsonify({'task_id': task_id})


@app.route('/api/stock/kline', methods=['POST'])
def stock_kline():
    """获取个股K线数据（日/周/月）"""
    from AlphaFin.services.stock_service import get_daily_data, get_weekly_monthly_data

    data = request.json or {}
    ts_code = data.get('ts_code', '600425.SH')
    freq = data.get('freq', 'D')
    start_date = data.get('start_date', '20200101')
    end_date = data.get('end_date', '')

    if freq == 'D':
        result = get_daily_data(ts_code, start_date, end_date=end_date)
    else:
        result = get_weekly_monthly_data(ts_code, start_date, freq, end_date=end_date)
    return jsonify(result)


@app.route('/api/stock/fina', methods=['POST'])
def stock_fina():
    """获取个股财务指标"""
    from AlphaFin.services.stock_service import get_fina_indicator

    data = request.json or {}
    ts_code = data.get('ts_code', '600425.SH')
    result = get_fina_indicator(ts_code)
    return jsonify(result)


@app.route('/api/stock/nineturn', methods=['POST'])
def stock_nineturn():
    """获取神奇九转信号"""
    from AlphaFin.services.stock_service import get_nineturn

    data = request.json or {}
    ts_code = data.get('ts_code', '600425.SH')
    freq = data.get('freq', 'D')
    result = get_nineturn(ts_code, freq)
    return jsonify(result)


@app.route('/api/stock/cyq', methods=['POST'])
def stock_cyq():
    """获取个股筹码分布及胜率数据"""
    from AlphaFin.services.stock_service import get_cyq_perf

    data = request.json or {}
    ts_code = data.get('ts_code', '600425.SH')
    start_date = data.get('start_date', '20180101')
    end_date = data.get('end_date', '')
    result = get_cyq_perf(ts_code, start_date, end_date=end_date)
    return jsonify(result)


@app.route('/api/stock/pattern_match', methods=['POST'])
def stock_pattern_match():
    """K线结构相似预测：历史相似阶段 TopK + 未来收益统计"""
    from AlphaFin.services.stock_service import get_stock_pattern_match

    data = request.json or {}
    ts_code = data.get('ts_code', '600425.SH')
    freq = data.get('freq', 'D')
    start_date = data.get('start_date', '19900101')
    window = data.get('window', 40)
    top_k = data.get('top_k', 5)
    horizons = data.get('horizons', [5, 10, 20])
    weights = data.get('weights', {'price': 1, 'volume': 0, 'macd': 0, 'kdj': 0})

    result = get_stock_pattern_match(
        ts_code=ts_code,
        freq=freq,
        window=window,
        top_k=top_k,
        horizons=horizons,
        start_date=start_date,
        weights=weights,
    )
    return jsonify(result)


@app.route('/api/stock/news', methods=['POST'])
def stock_news():
    """获取个股相关新闻（AI联网搜索）"""
    from AlphaFin.services.news_service import fetch_stock_news

    data = request.json or {}
    ts_code = data.get('ts_code', '600425.SH')
    result = fetch_stock_news(ts_code)
    return jsonify(result)


@app.route('/api/stock/resonance', methods=['POST'])
def stock_resonance():
    """多周期共振系统：返回多周期 MACD/KDJ 状态与预测"""
    from AlphaFin.services.stock_service import get_multi_cycle_resonance

    data = request.json or {}
    ts_code = data.get('ts_code', '600425.SH')
    asset_type = data.get('asset_type', 'stock')
    start_date = data.get('start_date', '20200101')
    result = get_multi_cycle_resonance(ts_code=ts_code, asset_type=asset_type, start_date=start_date)
    return jsonify(result)


@app.route('/api/stock/industry_map', methods=['POST'])
def stock_industry_map():
    """行业时空图谱：行业多维散点 + 个股定位"""
    from AlphaFin.services.industry_map_service import get_industry_rotation_map

    data = request.json or {}
    level = data.get('level', 'L1')
    start_date = data.get('start_date', '20240924')
    end_date = data.get('end_date', '')
    x_metric = data.get('x_metric', 'pct_change')
    x_start_date = data.get('x_start_date', '')
    x_end_date = data.get('x_end_date', '')
    bubble_metric = data.get('bubble_metric', 'amount')
    target_code = data.get('target_code', '600425.SH')

    result = get_industry_rotation_map(
        level=level,
        start_date=start_date,
        end_date=end_date,
        x_metric=x_metric,
        x_start_date=x_start_date,
        x_end_date=x_end_date,
        bubble_metric=bubble_metric,
        target_code=target_code,
    )
    return jsonify(result)


@app.route('/api/stock/ml_nextday', methods=['POST'])
def stock_ml_nextday():
    """个股机器学习集成：预测下一交易日涨跌概率"""
    from AlphaFin.services.stock_service import get_stock_ml_ensemble_prediction

    data = request.json or {}
    ts_code = data.get('ts_code', '600425.SH')
    start_date = data.get('start_date', '20160101')
    end_date = data.get('end_date', '')
    threshold_up = data.get('threshold_up', 0.58)
    threshold_down = data.get('threshold_down', 0.42)
    min_train_size = data.get('min_train_size', 360)
    enabled_models = data.get('enabled_models', [])
    auto_threshold = bool(data.get('auto_threshold', False))
    if not isinstance(enabled_models, (list, tuple, set)):
        enabled_models = []

    result = get_stock_ml_ensemble_prediction(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        threshold_up=threshold_up,
        threshold_down=threshold_down,
        min_train_size=min_train_size,
        enabled_models=enabled_models,
        auto_threshold=auto_threshold,
    )
    return jsonify(result)


@app.route('/api/stock/ml_nextday/start', methods=['POST'])
def stock_ml_nextday_start():
    """启动个股机器学习集成异步任务，返回 task_id（可配合 /api/progress/<task_id> 轮询）"""
    from AlphaFin.services.stock_service import get_stock_ml_ensemble_prediction

    data = request.json or {}
    ts_code = data.get('ts_code', '600425.SH')
    start_date = data.get('start_date', '20160101')
    end_date = data.get('end_date', '')
    threshold_up = data.get('threshold_up', 0.58)
    threshold_down = data.get('threshold_down', 0.42)
    min_train_size = data.get('min_train_size', 360)
    enabled_models = data.get('enabled_models', [])
    auto_threshold = bool(data.get('auto_threshold', False))
    if not isinstance(enabled_models, (list, tuple, set)):
        enabled_models = []

    task_id = str(uuid.uuid4())
    update_progress(task_id, 0, 100, '任务已创建，准备读取数据...')

    def _run_ml_async(tid):
        try:
            def _cb(step, total, msg):
                update_progress(tid, step, total, msg)

            result = get_stock_ml_ensemble_prediction(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                threshold_up=threshold_up,
                threshold_down=threshold_down,
                min_train_size=min_train_size,
                enabled_models=enabled_models,
                auto_threshold=auto_threshold,
                progress_callback=_cb,
            )
            if result.get('ok'):
                update_progress(tid, 100, 100, '预测完成', done=True, result=result)
            else:
                update_progress(tid, 100, 100, '预测失败: ' + str(result.get('message', 'unknown')), done=True, result=result)
        except Exception as e:
            err = {'ok': False, 'message': '预测异常: ' + str(e)}
            update_progress(tid, 100, 100, err['message'], done=True, result=err)

    t = Thread(target=_run_ml_async, args=(task_id,))
    t.daemon = True
    t.start()
    return jsonify({'task_id': task_id})


@app.route('/api/stock/ml_nextday/result/<task_id>')
def stock_ml_nextday_result(task_id):
    """获取异步任务结果"""
    info = get_progress(task_id)
    return jsonify({
        'step': info.get('step', 0),
        'total': info.get('total', 100),
        'done': bool(info.get('done', False)),
        'message': info.get('message', ''),
        'result': info.get('result'),
    })


@app.route('/api/stock/ai_strategy', methods=['POST'])
def ai_strategy():
    """AI 自动设计交易策略（基于前端暴力搜索结果）"""
    from AlphaFin.services.ai_chat_service import ai_chat

    data = request.json or {}
    stock_summary = data.get('stock_summary', '')
    strategy_results = data.get('strategy_results', '')
    strategy_ids = data.get('strategy_ids', [])

    ids_list = ', '.join(strategy_ids) if strategy_ids else '无'

    response = ai_chat(
        f"""你是一个顶级量化策略设计专家。系统已对一只股票自动回测了所有可用的单因子策略，结果如下：

【股票概况】
{stock_summary}

【各策略回测绩效】（按夏普比率降序排列）
策略ID | 策略名称 | 累积收益 | 年化收益 | 夏普比率 | 最大回撤 | 交易笔数
{strategy_results}

【可选策略ID列表】
{ids_list}

请你作为量化专家，分析上述回测数据，设计最优交易策略。你可以：
1. 选择单个最优策略
2. 组合多个策略（利用因子间的互补性降低回撤、提高收益）
3. 选择不同的买入和卖出因子（如用MACD金叉买入，用KDJ死叉卖出）

请以严格 JSON 格式返回（不要包含其他文字）：
{{
    "strategy_name": "策略名称",
    "description": "策略设计思路和逻辑说明（150字以内，解释为什么选择这些因子组合）",
    "buy_strategies": ["策略ID1", "策略ID2"],
    "sell_strategies": ["策略ID1", "策略ID2"],
    "buy_logic": "any" 或 "all",
    "sell_logic": "any" 或 "all"
}}

重要规则：
- buy_strategies 和 sell_strategies 必须从上面的【可选策略ID列表】中选择，不能自创
- buy_strategies: 当这些策略发出持仓信号时视为买入条件
- sell_strategies: 当这些策略发出空仓信号时视为卖出条件
- buy_logic/sell_logic: "any"=任一策略满足即触发, "all"=所有策略同时满足才触发
- 目标：最大化夏普比率，同时控制回撤
- 只返回JSON，不要其他解释""",
        history=None,
        enable_search=False
    )
    return jsonify({'reply': response})


@app.route('/api/indicators')
def list_indicators():
    """列出所有指标（供前端调用）"""
    result = {}
    for id_, meta in REGISTRY.items():
        result[id_] = {k: v for k, v in meta.items() if not str(k).startswith('_')}
    return jsonify(result)


# ──────────────── 大师选股策略 API ────────────────

def _load_master_strategy_api():
    """
    延迟导入大师策略模块，避免应用启动时加载重依赖（pandas/matplotlib/数据库）。
    """
    from AlphaFin.indicators.ind_23_master_strategies import (
        load_data as master_load_data_func,
        run_strategy as master_run_strategy,
        get_strategies_info,
        backtest_all_strategies as master_backtest_func,
        get_backtest_result as master_get_backtest_result,
    )
    return {
        'load_data': master_load_data_func,
        'run_strategy': master_run_strategy,
        'get_strategies_info': get_strategies_info,
        'backtest_all': master_backtest_func,
        'get_backtest_result': master_get_backtest_result,
    }


@app.route('/api/master/strategies')
def master_strategies_list():
    """获取所有大师策略的元信息"""
    api = _load_master_strategy_api()
    return jsonify(api['get_strategies_info']())


@app.route('/api/master/load', methods=['POST'])
def master_load():
    """第一阶段：加载全市场数据（后台线程+SSE进度）"""
    api = _load_master_strategy_api()
    task_id = str(uuid.uuid4())

    def _do_load(tid):
        def progress_cb(step, total, msg):
            update_progress(tid, step, total, msg)
        try:
            api['load_data'](progress_callback=progress_cb)
            update_progress(tid, 6, 6, '数据加载完成', done=True)
        except Exception as e:
            update_progress(tid, 0, 1, f'数据加载失败: {str(e)}', done=True)

    t = Thread(target=_do_load, args=(task_id,))
    t.daemon = True
    t.start()
    return jsonify({'task_id': task_id})


@app.route('/api/master/run', methods=['POST'])
def master_run():
    """第二阶段：运行指定策略（同步返回JSON结果）"""
    api = _load_master_strategy_api()
    data = request.json or {}
    strategy_id = data.get('strategy_id', '')
    result = api['run_strategy'](strategy_id)
    return jsonify(result)


@app.route('/api/master/backtest', methods=['POST'])
def master_backtest():
    """运行历史回测（后台线程+SSE进度）"""
    api = _load_master_strategy_api()
    task_id = str(uuid.uuid4())

    def _do_backtest(tid):
        def progress_cb(step, total, msg):
            update_progress(tid, step, total, msg)
        try:
            api['backtest_all'](progress_callback=progress_cb)
            update_progress(tid, 5, 5, '回测完成', done=True)
        except Exception as e:
            update_progress(tid, 0, 1, f'回测失败: {str(e)}', done=True)

    t = Thread(target=_do_backtest, args=(task_id,))
    t.daemon = True
    t.start()
    return jsonify({'task_id': task_id})


@app.route('/api/master/backtest_result')
def master_backtest_result():
    """获取缓存的回测结果"""
    api = _load_master_strategy_api()
    result = api['get_backtest_result']()
    if result is None:
        return jsonify({'error': '回测尚未完成'}), 404
    return jsonify(result)


# ──────────────── 启动 ────────────────

if __name__ == '__main__':
    os.makedirs(CHART_DIR, exist_ok=True)
    # 关闭 reloader，避免开发模式下双进程重复加载导致启动卡顿和内存翻倍
    app.run(host='0.0.0.0', port=5002, debug=DEBUG, threaded=True, use_reloader=False)
