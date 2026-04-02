/* ═══════════════ ML 涨跌预测 — 独立页面逻辑 ═══════════════ */

var mlFeatureChart = null;
var mlBacktestChart = null;
var mlResizeBound = false;
var mlProgressEvt = null;
var mlProgressTimer = null;
var mlProgressStartedAt = 0;
var mlLastPercent = 0;
var mlTaskCompleted = false;

function _mlTheme() {
    if (typeof getEchartsTheme === 'function') return getEchartsTheme();
    return null;
}

function _mlFmtPct(v, digits) {
    if (v === null || v === undefined || isNaN(v)) return '--';
    var n = Number(v);
    var d = (digits === undefined ? 2 : digits);
    var sign = n >= 0 ? '+' : '';
    return sign + n.toFixed(d) + '%';
}

function _mlFmtProb(v) {
    if (v === null || v === undefined || isNaN(v)) return '--';
    return (Number(v) * 100).toFixed(2) + '%';
}

function _mlSourceLabel(raw) {
    var s = String(raw || '').toLowerCase();
    if (s === 'local_daily_kline') return '本地日线库';
    if (s === 'local_dailybasic') return '本地估值库';
    if (s === 'tushare_daily') return 'Tushare 日线';
    if (s === 'tushare_daily_basic') return 'Tushare 估值';
    if (!s || s === 'none') return '无';
    return String(raw || '--');
}

function setMlStatus(level, text) {
    var el = document.getElementById('ml-status');
    if (!el) return;
    el.className = 'stock-data-status level-' + (level || 'ok');
    el.textContent = text || '';
    el.style.display = 'block';
}

function ensureMlResizeBinding() {
    if (mlResizeBound) return;
    mlResizeBound = true;
    window.addEventListener('resize', function() {
        if (mlFeatureChart) mlFeatureChart.resize();
        if (mlBacktestChart) mlBacktestChart.resize();
    });
}

function renderMlSummary(data) {
    var box = document.getElementById('ml-summary');
    if (!box) return;
    var pred = data.prediction || {};
    var perf = data.performance || {};
    var cls = pred.signal > 0 ? 'positive' : (pred.signal < 0 ? 'negative' : '');
    var html = '' +
        '<div class="ml-stat-card">' +
            '<div class="ml-stat-title">下一交易日上涨概率</div>' +
            '<div class="ml-stat-main ' + cls + '">' + _mlFmtProb(pred.up_prob) + '</div>' +
            '<div class="ml-stat-sub">信号：' + (pred.signal_text || '--') + ' ｜ 置信度：' + _mlFmtProb(pred.confidence) + '</div>' +
        '</div>' +
        '<div class="ml-stat-card">' +
            '<div class="ml-stat-title">集成概率分解</div>' +
            '<div class="ml-stat-main">' + _mlFmtProb(pred.soft_up_prob) + ' / ' + _mlFmtProb(pred.stack_up_prob) + '</div>' +
            '<div class="ml-stat-sub">Soft / Stacking（' + (pred.stack_status || '--') + '）</div>' +
        '</div>' +
        '<div class="ml-stat-card">' +
            '<div class="ml-stat-title">策略回测累计收益</div>' +
            '<div class="ml-stat-main ' + ((perf.strategy_total_return_pct || 0) >= 0 ? 'positive' : 'negative') + '">' + _mlFmtPct(perf.strategy_total_return_pct || 0, 2) + '</div>' +
            '<div class="ml-stat-sub">基准：' + _mlFmtPct(perf.benchmark_total_return_pct || 0, 2) + '</div>' +
        '</div>' +
        '<div class="ml-stat-card">' +
            '<div class="ml-stat-title">命中率 / 覆盖率</div>' +
            '<div class="ml-stat-main">' + _mlFmtProb(perf.direction_hit_rate) + ' / ' + _mlFmtProb(perf.coverage) + '</div>' +
            '<div class="ml-stat-sub">夏普：' + (perf.strategy_sharpe === undefined || perf.strategy_sharpe === null ? '--' : Number(perf.strategy_sharpe).toFixed(3)) +
                ' ｜ 最大回撤：' + _mlFmtPct(perf.strategy_max_drawdown_pct || 0, 2) + '</div>' +
        '</div>';
    box.innerHTML = html;
    box.style.display = 'grid';
}

function renderMlModelTable(models) {
    var table = document.getElementById('ml-model-table');
    if (!table) return;
    if (!models || !models.length) {
        table.innerHTML = '';
        return;
    }
    var head = '<thead><tr>' +
        '<th>模型</th><th>权重</th><th>上涨概率</th><th>ACC</th><th>F1</th><th>AUC</th><th>覆盖率</th><th>状态</th>' +
        '</tr></thead><tbody>';
    var body = '';
    for (var i = 0; i < models.length; i++) {
        var m = models[i] || {};
        var mm = m.metrics || {};
        body += '<tr>' +
            '<td>' + (m.name || '--') + '</td>' +
            '<td>' + (m.weight === null || m.weight === undefined ? '--' : (Number(m.weight) * 100).toFixed(1) + '%') + '</td>' +
            '<td>' + _mlFmtProb(m.latest_up_prob) + '</td>' +
            '<td>' + _mlFmtProb(mm.accuracy) + '</td>' +
            '<td>' + _mlFmtProb(mm.f1) + '</td>' +
            '<td>' + _mlFmtProb(mm.auc) + '</td>' +
            '<td>' + _mlFmtProb(mm.coverage) + '</td>' +
            '<td>' + (m.status || '--') + '</td>' +
        '</tr>';
    }
    body += '</tbody>';
    table.innerHTML = head + body;
}

function renderMlFeatureChart(topFeatures) {
    var container = document.getElementById('ml-feature-chart');
    if (!container) return;
    if (mlFeatureChart) mlFeatureChart.dispose();
    mlFeatureChart = echarts.init(container, _mlTheme());
    ensureMlResizeBinding();

    var feats = topFeatures || [];
    if (!feats.length) {
        mlFeatureChart.setOption({
            title: { text: '暂无特征贡献数据', left: 'center', top: 'middle', textStyle: { fontSize: 14, fontWeight: 500 } }
        });
        return;
    }
    var names = feats.map(function(x) { return x.feature; }).reverse();
    var vals = feats.map(function(x) { return Number(x.score || 0); }).reverse();
    mlFeatureChart.setOption({
        animation: false,
        tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
        grid: { left: '20%', right: '4%', top: '8%', bottom: '8%' },
        xAxis: { type: 'value', splitLine: { lineStyle: { opacity: 0.28 } } },
        yAxis: { type: 'category', data: names, axisLabel: { fontSize: 11 } },
        series: [{
            type: 'bar',
            data: vals.map(function(v) {
                return {
                    value: Number(v.toFixed(6)),
                    itemStyle: { color: '#f97316', borderRadius: [0, 4, 4, 0] }
                };
            }),
            label: {
                show: true,
                position: 'right',
                formatter: function(p) { return Number(p.value || 0).toFixed(4); },
                fontSize: 10
            }
        }]
    });
}

function renderMlBacktestChart(backtest) {
    var container = document.getElementById('ml-backtest-chart');
    if (!container) return;
    if (mlBacktestChart) mlBacktestChart.dispose();
    mlBacktestChart = echarts.init(container, _mlTheme());
    ensureMlResizeBinding();

    var dates = (backtest && backtest.dates) ? backtest.dates : [];
    var strategy = (backtest && backtest.strategy_curve) ? backtest.strategy_curve : [];
    var benchmark = (backtest && backtest.benchmark_curve) ? backtest.benchmark_curve : [];
    var prob = (backtest && backtest.prob_up) ? backtest.prob_up : [];

    if (!dates.length) {
        mlBacktestChart.setOption({
            title: { text: '暂无回测数据', left: 'center', top: 'middle', textStyle: { fontSize: 14, fontWeight: 500 } }
        });
        return;
    }

    mlBacktestChart.setOption({
        animation: false,
        tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
        legend: { data: ['策略净值', '基准净值', '上涨概率'], top: 8 },
        grid: { left: '6%', right: '5%', top: '14%', bottom: '10%' },
        xAxis: { type: 'category', data: dates, boundaryGap: false, axisLabel: { fontSize: 10 } },
        yAxis: [
            { type: 'value', scale: true, name: '净值', splitLine: { lineStyle: { opacity: 0.25 } } },
            { type: 'value', min: 0, max: 1, name: '概率', splitLine: { show: false } }
        ],
        dataZoom: [
            { type: 'inside', start: 0, end: 100 },
            { type: 'slider', bottom: 0, height: 16, start: 0, end: 100 }
        ],
        series: [
            { name: '策略净值', type: 'line', yAxisIndex: 0, data: strategy, symbol: 'none', lineStyle: { width: 2, color: '#ef4444' } },
            { name: '基准净值', type: 'line', yAxisIndex: 0, data: benchmark, symbol: 'none', lineStyle: { width: 2, color: '#0ea5e9' } },
            { name: '上涨概率', type: 'line', yAxisIndex: 1, data: prob, symbol: 'none', lineStyle: { width: 1.5, color: '#f59e0b', opacity: 0.85 } }
        ]
    });
}

function renderMlRecentTable(backtest) {
    var table = document.getElementById('ml-recent-table');
    if (!table) return;
    var rows = (backtest && backtest.recent_signals) ? backtest.recent_signals : [];
    if (!rows.length) {
        table.innerHTML = '';
        return;
    }

    var html = '<thead><tr><th>日期</th><th>上涨概率</th><th>信号</th><th>次日收益</th></tr></thead><tbody>';
    for (var i = rows.length - 1; i >= 0; i--) {
        var r = rows[i] || {};
        var sig = Number(r.signal || 0);
        var sigText = sig > 0 ? '买入' : (sig < 0 ? '减仓' : '观望');
        var ret = Number(r.next_ret_pct || 0);
        var cls = ret >= 0 ? 'positive' : 'negative';
        html += '<tr>' +
            '<td>' + (r.trade_date || '--') + '</td>' +
            '<td>' + _mlFmtProb(r.up_prob) + '</td>' +
            '<td>' + sigText + '</td>' +
            '<td class="' + cls + '">' + _mlFmtPct(ret, 2) + '</td>' +
        '</tr>';
    }
    html += '</tbody>';
    table.innerHTML = html;
}

function _mlFormatDuration(seconds) {
    var sec = Math.max(0, Math.round(Number(seconds || 0)));
    var h = Math.floor(sec / 3600);
    var m = Math.floor((sec % 3600) / 60);
    var s = sec % 60;
    if (h > 0) return h + 'h ' + m + 'm';
    return String(m).padStart(2, '0') + ':' + String(s).padStart(2, '0');
}

function stopMlProgressStream() {
    if (mlProgressEvt) {
        try { mlProgressEvt.close(); } catch (e) {}
        mlProgressEvt = null;
    }
    if (mlProgressTimer) {
        clearInterval(mlProgressTimer);
        mlProgressTimer = null;
    }
}

function initMlProgressUi() {
    var wrap = document.getElementById('ml-progress-wrap');
    var bar = document.getElementById('ml-progress-bar');
    var text = document.getElementById('ml-progress-text');
    var timeEl = document.getElementById('ml-progress-time');
    if (wrap) wrap.style.display = 'block';
    if (bar) bar.style.width = '2%';
    if (text) text.textContent = '任务已创建，准备开始...';
    if (timeEl) timeEl.textContent = '预计剩余：计算中...';
    mlProgressStartedAt = Date.now();
    mlLastPercent = 2;
    mlTaskCompleted = false;
    if (mlProgressTimer) clearInterval(mlProgressTimer);
    mlProgressTimer = setInterval(function() {
        var tEl = document.getElementById('ml-progress-time');
        if (!tEl) return;
        if (mlLastPercent <= 1) {
            tEl.textContent = '预计剩余：计算中...';
            return;
        }
        var elapsed = (Date.now() - mlProgressStartedAt) / 1000.0;
        var remain = elapsed * (100.0 - mlLastPercent) / Math.max(1.0, mlLastPercent);
        tEl.textContent = '预计剩余：' + _mlFormatDuration(remain);
    }, 1000);
}

function updateMlProgressUi(percent, msg) {
    var wrap = document.getElementById('ml-progress-wrap');
    var bar = document.getElementById('ml-progress-bar');
    var text = document.getElementById('ml-progress-text');
    if (wrap) wrap.style.display = 'block';
    var p = Math.max(0, Math.min(100, Number(percent || 0)));
    mlLastPercent = p;
    if (bar) bar.style.width = p.toFixed(1) + '%';
    if (text) text.textContent = (msg || '处理中...') + ' (' + p.toFixed(0) + '%)';
}

function finishMlProgressUi(msg) {
    updateMlProgressUi(100, msg || '任务完成');
    mlTaskCompleted = true;
    var timeEl = document.getElementById('ml-progress-time');
    if (timeEl) {
        var elapsed = (Date.now() - mlProgressStartedAt) / 1000.0;
        timeEl.textContent = '总耗时：' + _mlFormatDuration(elapsed);
    }
    stopMlProgressStream();
}

function getMlSelectedModels() {
    var nodes = document.querySelectorAll('#ml-model-switch input[type="checkbox"]');
    var out = [];
    for (var i = 0; i < nodes.length; i++) {
        if (nodes[i].checked) out.push(String(nodes[i].value || '').trim());
    }
    return out;
}

function renderMlResult(data) {
    var info = data.data_info || {};
    var th = data.thresholds || {};
    var thInfo = th.auto_info || {};
    var thText = '阈值(看多/看空): ' + (th.up === undefined ? '--' : Number(th.up).toFixed(2)) +
        ' / ' + (th.down === undefined ? '--' : Number(th.down).toFixed(2));
    if (th.auto && thInfo.optimized) {
        thText += ' ｜ 自动优化: 已启用';
    } else if (th.auto) {
        thText += ' ｜ 自动优化: 未调整';
    }
    setMlStatus(
        'ok',
        '最新交易日: ' + (data.latest_trade_date || '--') +
        ' ｜ 数据源(日线/估值): ' + _mlSourceLabel(info.price_source) + ' / ' + _mlSourceLabel(info.basic_source) +
        ' ｜ 样本: ' + (data.sample_count || '--') + ' ｜ 特征: ' + (data.feature_count || '--') +
        ' ｜ ' + thText +
        ' ｜ 更新时间: ' + (data.generated_at || '--')
    );
    renderMlSummary(data);
    renderMlModelTable(data.models || []);
    renderMlFeatureChart(data.top_features || []);
    renderMlBacktestChart(data.backtest || {});
    renderMlRecentTable(data.backtest || {});
}

function fetchMlTaskResult(taskId, btn) {
    fetch('/api/stock/ml_nextday/result/' + encodeURIComponent(taskId))
        .then(function(resp) { return resp.json(); })
        .then(function(payload) {
            var result = payload && payload.result ? payload.result : null;
            if (!result || !result.ok) {
                setMlStatus('error', '预测失败：' + ((result && result.message) || payload.message || 'unknown'));
                return;
            }
            renderMlResult(result);
        })
        .catch(function(err) {
            setMlStatus('error', '结果获取失败：' + String(err || 'unknown'));
        })
        .finally(function() {
            if (btn) {
                btn.disabled = false;
                btn.textContent = '开始预测';
            }
        });
}

function listenMlTaskProgress(taskId, btn) {
    stopMlProgressStream();
    initMlProgressUi();
    mlProgressEvt = new EventSource('/api/progress/' + encodeURIComponent(taskId));
    mlProgressEvt.onmessage = function(event) {
        var info = {};
        try { info = JSON.parse(event.data || '{}'); } catch (e) {}
        var step = Number(info.step || 0);
        var total = Number(info.total || 100);
        var percent = total > 0 ? (step / total) * 100 : 0;
        if (info.done) percent = 100;
        updateMlProgressUi(percent, info.message || '处理中...');

        if (info.done) {
            finishMlProgressUi(info.message || '任务完成');
            fetchMlTaskResult(taskId, btn);
        }
    };
    mlProgressEvt.onerror = function() {
        if (mlTaskCompleted || mlLastPercent >= 100) return;
        stopMlProgressStream();
        setMlStatus('error', '进度连接中断，请重试。');
        if (btn) {
            btn.disabled = false;
            btn.textContent = '开始预测';
        }
    };
}

function runStockMlPredict() {
    var tsCode = document.getElementById('ml-stock-code').value.trim();
    var startDate = document.getElementById('ml-start-date').value.replace(/-/g, '') || '20160101';
    var thUp = parseFloat(document.getElementById('ml-th-up').value);
    var thDown = parseFloat(document.getElementById('ml-th-down').value);
    var minTrain = parseInt(document.getElementById('ml-min-train').value, 10);
    var autoThreshold = !!(document.getElementById('ml-auto-threshold') && document.getElementById('ml-auto-threshold').checked);
    var enabledModels = getMlSelectedModels();

    if (!tsCode) return;
    if (!Number.isFinite(thUp)) thUp = 0.58;
    if (!Number.isFinite(thDown)) thDown = 0.42;
    if (!Number.isFinite(minTrain) || minTrain < 180) minTrain = 360;
    if (!enabledModels.length) {
        setMlStatus('warning', '请至少勾选一个模型。');
        return;
    }

    var btn = document.getElementById('btn-ml-run');
    btn.disabled = true;
    btn.textContent = '任务进行中...';
    setMlStatus('warning', '任务已提交，正在进入训练队列...');

    fetch('/api/stock/ml_nextday/start', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            ts_code: tsCode,
            start_date: startDate,
            threshold_up: thUp,
            threshold_down: thDown,
            min_train_size: minTrain,
            enabled_models: enabledModels,
            auto_threshold: autoThreshold
        })
    }).then(function(resp) {
        return resp.json();
    }).then(function(data) {
        if (!data || !data.task_id) {
            setMlStatus('error', '任务启动失败：未返回 task_id');
            btn.disabled = false;
            btn.textContent = '开始预测';
            return;
        }
        listenMlTaskProgress(data.task_id, btn);
    }).catch(function(err) {
        setMlStatus('error', '请求失败：' + String(err || 'unknown'));
        btn.disabled = false;
        btn.textContent = '开始预测';
    });
}

document.addEventListener('DOMContentLoaded', function() {
    var wrap = document.getElementById('ml-progress-wrap');
    var bar = document.getElementById('ml-progress-bar');
    var text = document.getElementById('ml-progress-text');
    var timeEl = document.getElementById('ml-progress-time');
    if (wrap) wrap.style.display = 'block';
    if (bar && !bar.style.width) bar.style.width = '0%';
    if (text && !text.textContent) text.textContent = '等待开始预测...';
    if (timeEl && !timeEl.textContent) timeEl.textContent = '预计剩余：--';
});
