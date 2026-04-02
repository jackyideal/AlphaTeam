/* ═══════════════ K线匹配预测 — 独立页面逻辑 ═══════════════ */

var patternPageCharts = [];
var patternPageResizeBound = false;

function _patternPageSourceLabel(raw) {
    var s = String(raw || '').toLowerCase();
    if (s === 'tushare_daily') return 'Tushare 日线';
    if (s === 'tushare_weekly') return 'Tushare 周线';
    if (s === 'tushare_monthly') return 'Tushare 月线';
    if (s.indexOf('local_daily_kline') >= 0) return '本地库日线';
    if (s.indexOf('local_daily_resample_weekly') >= 0) return '本地库重采样周线';
    if (s.indexOf('local_daily_resample_monthly') >= 0) return '本地库重采样月线';
    if (!s || s === 'none') return '无';
    return String(raw || '--');
}

function _patternPageFmtYmd(ymd) {
    var s = String(ymd || '');
    if (s.length === 8) return s.slice(0, 4) + '-' + s.slice(4, 6) + '-' + s.slice(6, 8);
    return s || '--';
}

function _patternPageFmtPct(v) {
    if (v === null || v === undefined || isNaN(v)) return '--';
    var n = parseFloat(v);
    return (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
}

function _patternPageParseHorizons(text) {
    var s = String(text || '').trim();
    if (!s) return [5, 10, 20];
    var arr = s.split(',').map(function(x) { return parseInt(String(x).trim(), 10); })
        .filter(function(x) { return Number.isFinite(x) && x > 0 && x <= 260; });
    var out = [];
    var seen = {};
    for (var i = 0; i < arr.length; i++) {
        if (!seen[arr[i]]) {
            seen[arr[i]] = true;
            out.push(arr[i]);
        }
    }
    return out.length ? out : [5, 10, 20];
}

function _patternPageTheme() {
    if (typeof getEchartsTheme === 'function') {
        return getEchartsTheme();
    }
    return null;
}

function clearPatternPageCharts() {
    if (!patternPageCharts || !patternPageCharts.length) return;
    for (var i = 0; i < patternPageCharts.length; i++) {
        try { patternPageCharts[i].dispose(); } catch (e) {}
    }
    patternPageCharts = [];
}

function ensurePatternPageResizeBinding() {
    if (patternPageResizeBound) return;
    patternPageResizeBound = true;
    window.addEventListener('resize', function() {
        for (var i = 0; i < patternPageCharts.length; i++) {
            try { if (patternPageCharts[i]) patternPageCharts[i].resize(); } catch (e) {}
        }
    });
}

function setPatternPageStatus(level, text) {
    var el = document.getElementById('pattern-page-status');
    if (!el) return;
    el.className = 'stock-data-status level-' + (level || 'ok');
    el.textContent = text || '';
    el.style.display = 'block';
}

function fetchPatternPageData(payload) {
    return fetch('/api/stock/pattern_match', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
    }).then(function(r) { return r.json(); });
}

function renderPatternPageSegmentChart(containerId, chartData, title, subtitle) {
    var el = document.getElementById(containerId);
    if (!el || !chartData) return;
    var dates = chartData.dates || [];
    var ohlc = chartData.ohlc || [];
    var volumes = chartData.volumes || [];
    var macd = chartData.macd || {};
    var kdj = chartData.kdj || {};
    if (!dates.length || !ohlc.length) return;

    var echartsOhlc = ohlc.map(function(d) { return [d[0], d[3], d[2], d[1]]; });
    var volColors = volumes.map(function(v, i) {
        if (!ohlc[i]) return '#ef5350';
        return (ohlc[i][3] >= ohlc[i][0]) ? '#ef5350' : '#26a69a';
    });

    var chart = echarts.init(el, _patternPageTheme());
    patternPageCharts.push(chart);

    var option = {
        animation: false,
        title: {
            text: title || '',
            subtext: subtitle || '',
            left: 'center',
            textStyle: { fontSize: 14, fontWeight: 600 },
            subtextStyle: { fontSize: 12, color: '#64748b' }
        },
        tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
        axisPointer: { link: [{xAxisIndex: 'all'}] },
        legend: {
            data: ['K线', '成交量', 'DIF', 'DEA', 'MACD', 'K', 'D', 'J'],
            top: 30,
            textStyle: { fontSize: 11 }
        },
        grid: [
            { left: '8%', right: '4%', top: '14%', height: '36%' },
            { left: '8%', right: '4%', top: '53%', height: '10%' },
            { left: '8%', right: '4%', top: '66%', height: '14%' },
            { left: '8%', right: '4%', top: '83%', height: '12%' }
        ],
        xAxis: [
            { type: 'category', data: dates, gridIndex: 0, axisLabel: { show: false }, axisTick: { show: false }, axisLine: { show: false } },
            { type: 'category', data: dates, gridIndex: 1, axisLabel: { show: false }, axisTick: { show: false }, axisLine: { show: false } },
            { type: 'category', data: dates, gridIndex: 2, axisLabel: { show: false }, axisTick: { show: false }, axisLine: { show: false } },
            { type: 'category', data: dates, gridIndex: 3, axisLabel: { fontSize: 10 } }
        ],
        yAxis: [
            { scale: true, gridIndex: 0, splitLine: { lineStyle: { opacity: 0.25 } } },
            { scale: true, gridIndex: 1, splitLine: { lineStyle: { opacity: 0.2 } }, axisLabel: { fontSize: 10 } },
            { scale: true, gridIndex: 2, splitLine: { lineStyle: { opacity: 0.2 } }, axisLabel: { fontSize: 10 } },
            { scale: true, gridIndex: 3, splitLine: { lineStyle: { opacity: 0.2 } }, axisLabel: { fontSize: 10 } }
        ],
        dataZoom: [
            { type: 'inside', xAxisIndex: [0, 1, 2, 3], start: 0, end: 100 },
            { type: 'slider', xAxisIndex: [0, 1, 2, 3], bottom: 0, height: 16, start: 0, end: 100 }
        ],
        series: [
            {
                name: 'K线', type: 'candlestick', data: echartsOhlc, xAxisIndex: 0, yAxisIndex: 0,
                itemStyle: { color: '#ef5350', color0: '#26a69a', borderColor: '#ef5350', borderColor0: '#26a69a' }
            },
            {
                name: '成交量', type: 'bar', xAxisIndex: 1, yAxisIndex: 1,
                data: volumes.map(function(v, i) { return { value: v, itemStyle: { color: volColors[i] } }; })
            },
            { name: 'DIF', type: 'line', xAxisIndex: 2, yAxisIndex: 2, data: macd.dif || [], symbol: 'none', lineStyle: { width: 1, color: '#1d4ed8' } },
            { name: 'DEA', type: 'line', xAxisIndex: 2, yAxisIndex: 2, data: macd.dea || [], symbol: 'none', lineStyle: { width: 1, color: '#f59e0b' } },
            {
                name: 'MACD', type: 'bar', xAxisIndex: 2, yAxisIndex: 2,
                data: (macd.hist || []).map(function(v) { return { value: v, itemStyle: { color: (v >= 0 ? '#ef5350' : '#26a69a') } }; })
            },
            { name: 'K', type: 'line', xAxisIndex: 3, yAxisIndex: 3, data: kdj.k || [], symbol: 'none', lineStyle: { width: 1, color: '#1d4ed8' } },
            { name: 'D', type: 'line', xAxisIndex: 3, yAxisIndex: 3, data: kdj.d || [], symbol: 'none', lineStyle: { width: 1, color: '#f59e0b' } },
            { name: 'J', type: 'line', xAxisIndex: 3, yAxisIndex: 3, data: kdj.j || [], symbol: 'none', lineStyle: { width: 1, color: '#8b5cf6' } }
        ]
    };
    chart.setOption(option);
}

function renderPatternPageSummary(prediction, unit) {
    var box = document.getElementById('pattern-page-summary');
    if (!box) return;
    var keys = Object.keys(prediction || {});
    if (!keys.length) {
        box.style.display = 'none';
        return;
    }
    keys.sort(function(a, b) { return parseInt(a, 10) - parseInt(b, 10); });
    var html = '';
    for (var i = 0; i < keys.length; i++) {
        var k = keys[i];
        var p = prediction[k] || {};
        var m = parseFloat(p.weighted_mean_pct || 0);
        var cls = m >= 0 ? 'positive' : 'negative';
        html += '<div class="pattern-stat-card">' +
            '<div class="pattern-stat-title">未来' + k + unit + '（相似度加权）</div>' +
            '<div class="pattern-stat-main ' + cls + '">' + _patternPageFmtPct(m) + '</div>' +
            '<div class="pattern-stat-sub">中位数: ' + _patternPageFmtPct(p.median_pct) +
            ' ｜ 胜率: ' + (p.up_ratio_pct === undefined ? '--' : p.up_ratio_pct + '%') + '<br>' +
            '区间: ' + _patternPageFmtPct(p.min_pct) + ' ~ ' + _patternPageFmtPct(p.max_pct) + '</div>' +
            '</div>';
    }
    box.innerHTML = html;
    box.style.display = 'grid';
}

function renderPatternPageTable(matches, horizons, unit) {
    var wrap = document.getElementById('pattern-page-table-wrap');
    var table = document.getElementById('pattern-page-table');
    if (!wrap || !table) return;
    if (!matches || !matches.length) {
        wrap.style.display = 'none';
        return;
    }
    var hs = (horizons || []).slice().sort(function(a, b) { return a - b; });
    var head = '<thead><tr><th>排名</th><th>历史阶段</th><th>相似度</th>';
    for (var i = 0; i < hs.length; i++) head += '<th>未来' + hs[i] + unit + '</th>';
    head += '</tr></thead><tbody>';
    var body = '';
    for (var r = 0; r < matches.length; r++) {
        var m = matches[r];
        body += '<tr><td>' + (m.rank || (r + 1)) + '</td>' +
            '<td>' + (m.start_date || '--') + ' ~ ' + (m.end_date || '--') + '</td>' +
            '<td>' + (m.similarity === undefined ? '--' : Number(m.similarity).toFixed(4)) + '</td>';
        for (var j = 0; j < hs.length; j++) {
            var key = String(hs[j]);
            var v = m.future_returns ? m.future_returns[key] : null;
            var cls = (v === null || v === undefined || isNaN(v)) ? '' : (parseFloat(v) >= 0 ? 'positive' : 'negative');
            body += '<td class="' + cls + '">' + _patternPageFmtPct(v) + '</td>';
        }
        body += '</tr>';
    }
    body += '</tbody>';
    table.innerHTML = head + body;
    wrap.style.display = 'block';
}

function renderPatternPageMatches(matches, horizons, unit) {
    var grid = document.getElementById('pattern-page-matches');
    if (!grid) return;
    if (!matches || !matches.length) {
        grid.innerHTML = '';
        return;
    }
    var hs = (horizons || []).slice().sort(function(a, b) { return a - b; });
    var html = '';
    for (var i = 0; i < matches.length; i++) {
        var m = matches[i];
        var tags = '';
        for (var j = 0; j < hs.length; j++) {
            var key = String(hs[j]);
            var v = m.future_returns ? m.future_returns[key] : null;
            var cls = (v === null || v === undefined || isNaN(v)) ? '' : (parseFloat(v) >= 0 ? 'positive' : 'negative');
            tags += '<span class="pattern-ret-tag ' + cls + '">' + hs[j] + unit + ': ' + _patternPageFmtPct(v) + '</span>';
        }
        html += '<div class="pattern-match-card">' +
            '<div class="pattern-match-head">' +
            '<div class="pattern-match-title">相似阶段 #' + (m.rank || (i + 1)) + '：' + (m.start_date || '--') + ' ~ ' + (m.end_date || '--') + '</div>' +
            '<div class="pattern-sim-tag">相似度 ' + (m.similarity === undefined ? '--' : Number(m.similarity).toFixed(4)) + '</div>' +
            '</div>' +
            '<div class="pattern-ret-tags">' + tags + '</div>' +
            '<div id="pattern-page-match-chart-' + i + '" class="stock-chart-container pattern-chart-box"></div>' +
            '</div>';
    }
    grid.innerHTML = html;

    for (var k = 0; k < matches.length; k++) {
        var item = matches[k] || {};
        renderPatternPageSegmentChart(
            'pattern-page-match-chart-' + k,
            item.chart || {},
            '相似阶段 #' + (item.rank || (k + 1)),
            (item.start_date || '--') + ' ~ ' + (item.end_date || '--')
        );
    }
}

function runPatternPageMatch() {
    var tsCode = document.getElementById('pattern-stock-code').value.trim();
    var startDate = document.getElementById('pattern-start-date').value.replace(/-/g, '') || '19900101';
    var freq = document.getElementById('pattern-freq').value || 'D';
    var windowN = parseInt(document.getElementById('pattern-window').value, 10);
    var topK = parseInt(document.getElementById('pattern-topk').value, 10);
    var horizons = _patternPageParseHorizons(document.getElementById('pattern-horizons').value);

    if (!tsCode) return;
    if (!Number.isFinite(windowN) || windowN < 12) windowN = 40;
    if (!Number.isFinite(topK) || topK < 1) topK = 5;
    topK = Math.min(10, topK);

    var btn = document.getElementById('btn-pattern-query');
    btn.disabled = true;
    btn.textContent = '匹配中...';
    setPatternPageStatus('ok', '正在进行历史结构匹配，请稍候...');

    fetchPatternPageData({
        ts_code: tsCode,
        freq: freq,
        start_date: startDate,
        window: windowN,
        top_k: topK,
        horizons: horizons,
        weights: {price: 1, volume: 0, macd: 0, kdj: 0}
    }).then(function(data) {
        btn.disabled = false;
        btn.textContent = '开始匹配';

        if (!data || data.ok === false) {
            setPatternPageStatus('warning', '匹配失败：' + (data && data.message ? data.message : 'unknown'));
            clearPatternPageCharts();
            return;
        }

        clearPatternPageCharts();
        ensurePatternPageResizeBinding();

        setPatternPageStatus(
            'ok',
            '匹配完成：频率=' + (data.freq_label || freq) +
            ' ｜ 窗口=' + (data.window || windowN) +
            ' ｜ 候选样本=' + (data.candidate_count || 0) +
            ' ｜ 数据源=' + _patternPageSourceLabel(data.data_source) +
            ' ｜ 最新日期=' + _patternPageFmtYmd(data.latest_trade_date || '') +
            ' ｜ 更新时间=' + (data.generated_at || '--')
        );

        renderPatternPageSummary(data.prediction || {}, data.freq_unit || '天');
        renderPatternPageTable(data.matches || [], data.horizons || horizons, data.freq_unit || '天');

        var targetWrap = document.getElementById('pattern-page-target-wrap');
        if (targetWrap) targetWrap.style.display = 'block';
        renderPatternPageSegmentChart(
            'pattern-page-target-chart',
            ((data.target || {}).chart) || {},
            '当前结构（目标片段）',
            ((data.target || {}).start_date || '--') + ' ~ ' + ((data.target || {}).end_date || '--')
        );
        renderPatternPageMatches(data.matches || [], data.horizons || horizons, data.freq_unit || '天');
    }).catch(function(err) {
        btn.disabled = false;
        btn.textContent = '开始匹配';
        clearPatternPageCharts();
        setPatternPageStatus('error', '请求失败：' + String(err || 'unknown'));
    });
}

window.addEventListener('beforeunload', function() {
    clearPatternPageCharts();
});

(function() {
    var params = new URLSearchParams(window.location.search);
    var code = params.get('code');
    if (code) {
        var el = document.getElementById('pattern-stock-code');
        if (el) el.value = code;
    }
})();

