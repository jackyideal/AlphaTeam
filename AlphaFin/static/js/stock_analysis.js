/* ═══════════════ 个股通用分析 — ECharts 前端逻辑 ═══════════════ */

var stockData = null;        // 当前K线原始数据
var nineTurnData = null;     // 九转信号数据
var currentFreq = 'D';       // 当前频率
var klineChart = null;       // ECharts 主图实例（K线+成交量+MACD+KDJ+CCI）
var valuationChart = null;   // ECharts 估值图表实例
var intrinsicChart = null;   // ECharts 内在价值图实例
var finaChartInstance = null; // 财务弹窗图表实例
var finaData = null;         // 财务数据
var cyqData = null;          // 筹码分布数据
var cyqDistChart = null;     // 筹码分布图实例
var cyqTimelineChart = null; // 筹码时序图实例
var cyqBtChart = null;       // 筹码回测净值图实例
var newsData = null;         // 新闻资讯数据
var patternData = null;      // 结构相似预测结果
var patternCharts = [];      // 结构相似模块图表实例
var patternResizeBound = false; // 结构相似模块 resize 监听
var intrinsicResizeBound = false; // 内在价值图 resize 监听
var stockAutoRefreshTimer = null; // 自动刷新计时器
var stockAutoRefreshing = false;  // 自动刷新并发锁
var STOCK_AUTO_REFRESH_MS = 5 * 60 * 1000; // 5分钟自动刷新一次

// ── 主入口：加载数据 ──
function loadStockData() {
    var tsCode = document.getElementById('stock-code').value.trim();
    var startDate = document.getElementById('stock-start-date').value.replace(/-/g, '');
    if (!tsCode) return;

    var btn = document.getElementById('btn-stock-query');
    btn.disabled = true;
    btn.textContent = '加载中...';

    currentFreq = 'D';
    var freqBtns = document.querySelectorAll('.freq-btn');
    freqBtns.forEach(function(b) { b.classList.remove('active'); });
    document.querySelector('.freq-btn[data-freq="D"]').classList.add('active');

    // 重置新闻状态
    newsData = null;
    var newsPanels = document.getElementById('news-panels');
    if (newsPanels) newsPanels.style.display = 'none';
    var btnNews = document.getElementById('btn-news-load');
    if (btnNews) { btnNews.textContent = '加载新闻'; btnNews.disabled = false; btnNews.style.display = 'inline-block'; }
    var newsLoading = document.getElementById('news-loading');
    if (newsLoading) newsLoading.style.display = 'none';

    // 重置筹码状态（避免跨股票残留）
    cyqData = null;
    var cyqStatus = document.getElementById('cyq-data-status');
    if (cyqStatus) cyqStatus.style.display = 'none';

    // 重置结构相似结果（避免跨股票残留）
    patternData = null;
    clearPatternCharts();
    var patternStatus = document.getElementById('pattern-status');
    if (patternStatus) patternStatus.style.display = 'none';
    var patternSummary = document.getElementById('pattern-summary');
    if (patternSummary) patternSummary.style.display = 'none';
    var patternTableWrap = document.getElementById('pattern-table-wrap');
    if (patternTableWrap) patternTableWrap.style.display = 'none';
    var patternTargetWrap = document.getElementById('pattern-target-wrap');
    if (patternTargetWrap) patternTargetWrap.style.display = 'none';
    var patternMatchesGrid = document.getElementById('pattern-matches-grid');
    if (patternMatchesGrid) patternMatchesGrid.innerHTML = '';
    var valuationSection = document.getElementById('valuation-section');
    if (valuationSection) valuationSection.style.display = 'none';
    var finaSection = document.getElementById('fina-section');
    if (finaSection) finaSection.style.display = 'none';
    var intrinsicSection = document.getElementById('intrinsic-section');
    if (intrinsicSection) {
        intrinsicSection.style.display = 'none';
        var intrinsicCards = document.getElementById('intrinsic-cards');
        if (intrinsicCards) intrinsicCards.innerHTML = '';
        var intrinsicTable = document.getElementById('intrinsic-table');
        if (intrinsicTable) intrinsicTable.innerHTML = '';
    }

    Promise.all([
        fetchKline(tsCode, currentFreq, startDate),
        fetchNineturn(tsCode, currentFreq),
        fetchFina(tsCode)
    ]).then(function(results) {
        stockData = results[0];
        nineTurnData = results[1];
        finaData = results[2];

        showUI();
        renderAll();
        setStockDataStatus('ok');
        startStockAutoRefresh();

        btn.disabled = false;
        btn.textContent = '查询';
    }).catch(function(err) {
        btn.disabled = false;
        btn.textContent = '查询';
        setStockDataStatus('error', '加载失败：' + String(err || 'unknown'));
        alert('数据加载失败: ' + err);
    });
}

function fetchKline(tsCode, freq, startDate) {
    var endDate = _todayYmd();
    return fetch('/api/stock/kline', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ts_code: tsCode, freq: freq, start_date: startDate, end_date: endDate})
    }).then(function(r) { return r.json(); });
}

function fetchNineturn(tsCode, freq) {
    return fetch('/api/stock/nineturn', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ts_code: tsCode, freq: freq})
    }).then(function(r) { return r.json(); });
}

function fetchFina(tsCode) {
    return fetch('/api/stock/fina', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ts_code: tsCode})
    }).then(function(r) { return r.json(); });
}

function fetchCyq(tsCode, startDate) {
    var endDate = _todayYmd();
    return fetch('/api/stock/cyq', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ts_code: tsCode, start_date: startDate, end_date: endDate})
    }).then(function(r) { return r.json(); });
}

function fetchPatternMatch(tsCode, freq, startDate, windowN, topK, horizons) {
    return fetch('/api/stock/pattern_match', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            ts_code: tsCode,
            freq: freq,
            start_date: startDate,
            window: windowN,
            top_k: topK,
            horizons: horizons,
            weights: {price: 1, volume: 0, macd: 0, kdj: 0}
        })
    }).then(function(r) { return r.json(); });
}

function _todayYmd() {
    var now = new Date();
    var y = now.getFullYear();
    var m = String(now.getMonth() + 1).padStart(2, '0');
    var d = String(now.getDate()).padStart(2, '0');
    return String(y) + m + d;
}

function _fmtYmd(ymd) {
    var s = String(ymd || '');
    if (s.length === 8) return s.slice(0, 4) + '-' + s.slice(4, 6) + '-' + s.slice(6, 8);
    return s || '--';
}

function _sourceLabel(raw) {
    var s = String(raw || '').toLowerCase();
    if (s === 'tushare_daily') return 'Tushare 日线';
    if (s === 'tushare_weekly') return 'Tushare 周线';
    if (s === 'tushare_monthly') return 'Tushare 月线';
    if (s === 'tushare_daily_basic') return 'Tushare 估值';
    if (s.indexOf('local_daily_kline') >= 0) return '本地库日线';
    if (s.indexOf('local_daily_resample_weekly') >= 0) return '本地库重采样周线';
    if (s.indexOf('local_daily_resample_monthly') >= 0) return '本地库重采样月线';
    if (s.indexOf('local_dailybasic') >= 0) return '本地库估值';
    if (s.indexOf('local_cyq_cache') >= 0) return '本地筹码缓存';
    if (s === 'none' || !s) return '无';
    return raw;
}

function setStockDataStatus(level, message) {
    var el = document.getElementById('stock-data-status');
    if (!el) return;

    var lv = level || 'ok';
    var freqLabel = currentFreq === 'W' ? '周线' : (currentFreq === 'M' ? '月线' : '日线');
    var latest = stockData && stockData.latest_trade_date ? _fmtYmd(stockData.latest_trade_date) : '--';
    var source = stockData ? _sourceLabel(stockData.data_source) : '--';
    var basicLatest = stockData && stockData.latest_basic_trade_date ? _fmtYmd(stockData.latest_basic_trade_date) : '--';
    var basicSource = stockData ? _sourceLabel(stockData.basic_data_source) : '--';
    var generatedAt = stockData && stockData.generated_at ? stockData.generated_at : '--';

    var text = message || (
        '最新' + freqLabel + '日期: ' + latest +
        ' ｜ 估值日期: ' + basicLatest +
        ' ｜ K线源: ' + source +
        ' ｜ 估值源: ' + basicSource +
        ' ｜ 更新时间: ' + generatedAt +
        ' ｜ 自动刷新: 每5分钟'
    );
    el.textContent = text;
    el.className = 'stock-data-status level-' + lv;
    el.style.display = 'block';
}

function setCyqDataStatus(level, message) {
    var el = document.getElementById('cyq-data-status');
    if (!el) return;

    var lv = level || 'ok';
    var latest = cyqData && cyqData.latest_trade_date ? _fmtYmd(cyqData.latest_trade_date) : '--';
    var source = cyqData ? _sourceLabel(cyqData.data_source) : '--';
    var generatedAt = cyqData && cyqData.generated_at ? cyqData.generated_at : '--';
    var text = message || ('最新筹码日期: ' + latest + ' ｜ 数据源: ' + source + ' ｜ 更新时间: ' + generatedAt);

    el.textContent = text;
    el.className = 'stock-data-status level-' + lv;
    el.style.display = 'block';
}

function setPatternStatus(level, message) {
    var el = document.getElementById('pattern-status');
    if (!el) return;
    el.textContent = message || '';
    el.className = 'stock-data-status level-' + (level || 'ok');
    el.style.display = 'block';
}

function startStockAutoRefresh() {
    if (stockAutoRefreshTimer) clearInterval(stockAutoRefreshTimer);
    stockAutoRefreshTimer = setInterval(function() {
        if (document.hidden) return;
        if (!stockData) return;
        refreshStockDataSilently();
    }, STOCK_AUTO_REFRESH_MS);
}

function refreshStockDataSilently() {
    if (stockAutoRefreshing) return;
    var tsCode = document.getElementById('stock-code').value.trim();
    var startDate = document.getElementById('stock-start-date').value.replace(/-/g, '');
    if (!tsCode) return;

    stockAutoRefreshing = true;
    Promise.all([
        fetchKline(tsCode, currentFreq, startDate),
        fetchNineturn(tsCode, currentFreq)
    ]).then(function(results) {
        var newData = results[0] || {};
        if (!newData.dates || !newData.dates.length) {
            setStockDataStatus('warning', '自动刷新未获取到新数据，已保留当前结果。');
            return;
        }
        stockData = newData;
        nineTurnData = results[1];
        renderInfoPanel();
        renderMainChart();
        renderNineturnAlert();
        if (currentFreq === 'D' && stockData.basic_history && stockData.basic_history.dates && stockData.basic_history.dates.length > 0) {
            document.getElementById('valuation-section').style.display = 'block';
            renderValuationCharts();
        } else {
            document.getElementById('valuation-section').style.display = 'none';
        }
        var intrinsicSec = document.getElementById('intrinsic-section');
        if (intrinsicSec) {
            if (currentFreq === 'D' && shouldShowIntrinsicValuation()) {
                intrinsicSec.style.display = 'block';
                renderIntrinsicValuation();
            } else {
                intrinsicSec.style.display = 'none';
            }
        }
        // 如果用户已加载筹码模块，自动刷新时同步刷新筹码数据，保持日期一致
        if (cyqData && cyqData.dates && cyqData.dates.length) {
            fetchCyq(tsCode, startDate).then(function(cyq) {
                if (cyq && cyq.dates && cyq.dates.length) {
                    cyqData = cyq;
                    if (cyqDistChart) renderCyqDistChart();
                    if (cyqTimelineChart) renderCyqTimelineChart();
                    setCyqDataStatus('ok');
                }
            }).catch(function() {
                // cyq 同步刷新失败时不阻断主流程
                setCyqDataStatus('warning', '筹码自动刷新失败，已保留当前结果。');
            });
        }
        setStockDataStatus('ok');
    }).catch(function(err) {
        setStockDataStatus('warning', '自动刷新失败：' + String(err || 'unknown'));
    }).finally(function() {
        stockAutoRefreshing = false;
    });
}

function showUI() {
    document.getElementById('chart-section-title').style.display = 'block';
    document.getElementById('freq-btns').style.display = 'flex';
    document.getElementById('kline-chart').style.display = 'block';
    if (stockData && stockData.basic && Object.keys(stockData.basic).length > 0) {
        document.getElementById('stock-info-panel').style.display = 'flex';
    }
    if (stockData && stockData.basic_history && stockData.basic_history.dates && stockData.basic_history.dates.length > 0) {
        document.getElementById('valuation-section').style.display = 'block';
    }
    if (shouldShowIntrinsicValuation()) {
        document.getElementById('intrinsic-section').style.display = 'block';
    }
    if (finaData && finaData.data && finaData.data.length > 0) {
        document.getElementById('fina-section').style.display = 'block';
    }
    var patternSection = document.getElementById('pattern-section');
    if (patternSection) patternSection.style.display = 'block';
    document.getElementById('backtest-section').style.display = 'block';
    document.getElementById('cyq-section').style.display = 'block';
    document.getElementById('news-section').style.display = 'block';
    onFactorChange(); // 初始化信号说明
}

function renderAll() {
    renderInfoPanel();
    renderMainChart();
    renderNineturnAlert();
    renderValuationCharts();
    renderIntrinsicValuation();
    renderFinaTable();
}

// ── 频率切换 ──
function switchFreq(freq, btn) {
    currentFreq = freq;
    document.querySelectorAll('.freq-btn').forEach(function(b) { b.classList.remove('active'); });
    btn.classList.add('active');

    var tsCode = document.getElementById('stock-code').value.trim();
    var startDate = document.getElementById('stock-start-date').value.replace(/-/g, '');

    Promise.all([
        fetchKline(tsCode, freq, startDate),
        fetchNineturn(tsCode, freq)
    ]).then(function(results) {
        stockData = results[0];
        nineTurnData = results[1];
        renderMainChart();
        renderNineturnAlert();
        renderInfoPanel();
        setStockDataStatus('ok');
        if (patternData && patternData.matches && patternData.matches.length) {
            setPatternStatus('warning', '已切换到' + (freq === 'W' ? '周线' : (freq === 'M' ? '月线' : '日线')) + '，请重新点击“开始匹配”以刷新结构对比。');
        }
        // 估值图表仅在日线时显示
        if (freq === 'D' && stockData.basic_history && stockData.basic_history.dates.length > 0) {
            document.getElementById('valuation-section').style.display = 'block';
            renderValuationCharts();
        } else {
            document.getElementById('valuation-section').style.display = 'none';
        }
        if (freq === 'D' && shouldShowIntrinsicValuation()) {
            document.getElementById('intrinsic-section').style.display = 'block';
            renderIntrinsicValuation();
        } else {
            document.getElementById('intrinsic-section').style.display = 'none';
        }
    }).catch(function(err) {
        setStockDataStatus('warning', '切换频率失败：' + String(err || 'unknown'));
    });
}

// ── 信息面板 ──
function renderInfoPanel() {
    var panel = document.getElementById('stock-info-panel');
    if (!stockData || !stockData.basic) { panel.style.display = 'none'; return; }
    var b = stockData.basic;
    var labels = {
        pe: 'PE', pe_ttm: 'PE(TTM)', pb: 'PB', ps: 'PS',
        total_mv: '总市值(万)', circ_mv: '流通市值(万)', turnover_rate: '换手率(%)'
    };
    var html = '';
    for (var key in labels) {
        if (b[key] !== null && b[key] !== undefined) {
            var val = b[key];
            if (key === 'total_mv' || key === 'circ_mv') {
                val = (val / 10000).toFixed(2) + '亿';
            }
            html += '<div class="stock-info-item"><span class="info-label">' +
                    labels[key] + '</span><span class="info-value">' + val + '</span></div>';
        }
    }
    panel.innerHTML = html;
    panel.style.display = html ? 'flex' : 'none';
}

// ══════════════════════════════════════════════════════════════
// ── 合并主图：K线 + 成交量 + MACD + KDJ + CCI ──
// ══════════════════════════════════════════════════════════════
function renderMainChart() {
    if (!stockData || !stockData.dates || stockData.dates.length === 0) return;

    var container = document.getElementById('kline-chart');
    if (klineChart) klineChart.dispose();
    klineChart = echarts.init(container, getEchartsTheme());

    var dates = stockData.dates;
    var ohlc = stockData.ohlc;
    var volumes = stockData.volumes;
    var closes = ohlc.map(function(d) { return d[3]; });
    var highs = ohlc.map(function(d) { return d[1]; });
    var lows = ohlc.map(function(d) { return d[2]; });

    // 转换为 ECharts candlestick 格式: [open, close, low, high]
    var echartsOhlc = ohlc.map(function(d) { return [d[0], d[3], d[2], d[1]]; });

    // 计算 MA
    var ma5 = calcMA(closes, 5);
    var ma10 = calcMA(closes, 10);
    var ma20 = calcMA(closes, 20);

    // BOLL
    var boll = calcBOLL(closes, 20);

    // 九转标注
    var markPoints = buildNineturnMarks(dates);

    // 成交量颜色（基于原始数据的 close vs open）
    var volColors = volumes.map(function(v, i) {
        if (i === 0) return '#ef5350';
        return ohlc[i][3] >= ohlc[i][0] ? '#ef5350' : '#26a69a';
    });

    // 计算技术指标
    var macd = calcMACD(closes);
    var kdj = calcKDJ(closes, highs, lows);
    var cci = calcCCI(closes, highs, lows, 14);

    // 5个grid布局: K线(38%) + 成交量(8%) + MACD(14%) + KDJ(14%) + CCI(14%)
    var option = {
        animation: false,
        tooltip: {
            trigger: 'axis',
            axisPointer: { type: 'cross' },
            formatter: function(params) {
                if (!params || params.length === 0) return '';
                var d = params[0].axisValue;
                var idx = dates.indexOf(d);
                if (idx < 0) return d;
                var o = ohlc[idx];
                var vol = volumes[idx];
                var lines = ['<b>' + d + '</b>'];
                lines.push('开盘: ' + o[0].toFixed(2));
                lines.push('最高: ' + o[1].toFixed(2));
                lines.push('最低: ' + o[2].toFixed(2));
                lines.push('收盘: ' + o[3].toFixed(2));
                lines.push('成交量: ' + (vol / 100).toFixed(0) + '手');
                if (macd.dif[idx] != null) {
                    lines.push('DIF: ' + macd.dif[idx].toFixed(2) + '  DEA: ' + macd.dea[idx].toFixed(2) + '  MACD: ' + macd.histogram[idx].toFixed(2));
                }
                if (kdj.k[idx] != null) {
                    lines.push('K: ' + kdj.k[idx].toFixed(1) + '  D: ' + kdj.d[idx].toFixed(1) + '  J: ' + kdj.j[idx].toFixed(1));
                }
                if (cci[idx] != null) {
                    lines.push('CCI: ' + cci[idx].toFixed(1));
                }
                return lines.join('<br>');
            }
        },
        axisPointer: { link: [{xAxisIndex: 'all'}] },
        legend: {
            data: ['K线', 'MA5', 'MA10', 'MA20', 'BOLL上轨', 'BOLL中轨', 'BOLL下轨', 'DIF', 'DEA', 'MACD', 'K', 'D', 'J', 'CCI'],
            top: 0,
            textStyle: { fontSize: 11 },
            selected: {
                'BOLL上轨': false, 'BOLL中轨': false, 'BOLL下轨': false
            }
        },
        grid: [
            { left: '8%', right: '4%', top: '5%', height: '35%' },   // K线
            { left: '8%', right: '4%', top: '43%', height: '7%' },   // 成交量
            { left: '8%', right: '4%', top: '54%', height: '12%' },  // MACD
            { left: '8%', right: '4%', top: '69%', height: '10%' },  // KDJ
            { left: '8%', right: '4%', top: '82%', height: '10%' }   // CCI
        ],
        xAxis: [
            { type: 'category', data: dates, gridIndex: 0, axisLabel: {show: false}, axisTick: {show: false}, axisLine: {show: false} },
            { type: 'category', data: dates, gridIndex: 1, axisLabel: {show: false}, axisTick: {show: false}, axisLine: {show: false} },
            { type: 'category', data: dates, gridIndex: 2, axisLabel: {show: false}, axisTick: {show: false}, axisLine: {show: false} },
            { type: 'category', data: dates, gridIndex: 3, axisLabel: {show: false}, axisTick: {show: false}, axisLine: {show: false} },
            { type: 'category', data: dates, gridIndex: 4, axisLabel: {fontSize: 11} }
        ],
        yAxis: [
            { scale: true, gridIndex: 0, splitLine: {lineStyle: {opacity: 0.3}} },
            { scale: true, gridIndex: 1, splitLine: {lineStyle: {opacity: 0.3}}, axisLabel: {formatter: function(v) { return (v/100).toFixed(0); }} },
            { scale: true, gridIndex: 2, splitLine: {lineStyle: {opacity: 0.3}}, axisLabel: {fontSize: 10} },
            { scale: true, gridIndex: 3, splitLine: {lineStyle: {opacity: 0.3}}, axisLabel: {fontSize: 10} },
            { scale: true, gridIndex: 4, splitLine: {lineStyle: {opacity: 0.3}}, axisLabel: {fontSize: 10} }
        ],
        dataZoom: [
            { type: 'inside', xAxisIndex: [0, 1, 2, 3, 4], start: Math.max(0, 100 - 3000 / dates.length * 100), end: 100 },
            { type: 'slider', xAxisIndex: [0, 1, 2, 3, 4], bottom: 2, height: 18, start: Math.max(0, 100 - 3000 / dates.length * 100), end: 100 }
        ],
        series: [
            // ── Grid 0: K线 + MA + BOLL ──
            {
                name: 'K线', type: 'candlestick', data: echartsOhlc, xAxisIndex: 0, yAxisIndex: 0,
                itemStyle: { color: '#ef5350', color0: '#26a69a', borderColor: '#ef5350', borderColor0: '#26a69a' },
                markPoint: markPoints
            },
            { name: 'MA5', type: 'line', data: ma5, xAxisIndex: 0, yAxisIndex: 0, symbol: 'none', lineStyle: {width: 1, color: '#f39c12'} },
            { name: 'MA10', type: 'line', data: ma10, xAxisIndex: 0, yAxisIndex: 0, symbol: 'none', lineStyle: {width: 1, color: '#3498db'} },
            { name: 'MA20', type: 'line', data: ma20, xAxisIndex: 0, yAxisIndex: 0, symbol: 'none', lineStyle: {width: 1, color: '#9b59b6'} },
            { name: 'BOLL上轨', type: 'line', data: boll.upper, xAxisIndex: 0, yAxisIndex: 0, lineStyle: {width: 1, type: 'dashed', color: '#e74c3c'}, symbol: 'none', z: 1 },
            { name: 'BOLL中轨', type: 'line', data: boll.mid, xAxisIndex: 0, yAxisIndex: 0, lineStyle: {width: 1, type: 'dashed', color: '#f39c12'}, symbol: 'none', z: 1 },
            { name: 'BOLL下轨', type: 'line', data: boll.lower, xAxisIndex: 0, yAxisIndex: 0, lineStyle: {width: 1, type: 'dashed', color: '#27ae60'}, symbol: 'none', z: 1 },
            // ── Grid 1: 成交量 ──
            {
                name: '成交量', type: 'bar', data: volumes.map(function(v, i) {
                    return { value: v, itemStyle: {color: volColors[i]} };
                }),
                xAxisIndex: 1, yAxisIndex: 1
            },
            // ── Grid 2: MACD ──
            { name: 'DIF', type: 'line', data: macd.dif, xAxisIndex: 2, yAxisIndex: 2, symbol: 'none', lineStyle: {width: 1, color: '#3498db'} },
            { name: 'DEA', type: 'line', data: macd.dea, xAxisIndex: 2, yAxisIndex: 2, symbol: 'none', lineStyle: {width: 1, color: '#f39c12'} },
            {
                name: 'MACD', type: 'bar', data: macd.histogram.map(function(v) {
                    return { value: v, itemStyle: { color: v >= 0 ? '#ef5350' : '#26a69a' } };
                }),
                xAxisIndex: 2, yAxisIndex: 2
            },
            // ── Grid 3: KDJ ──
            { name: 'K', type: 'line', data: kdj.k, xAxisIndex: 3, yAxisIndex: 3, symbol: 'none', lineStyle: {width: 1, color: '#3498db'} },
            { name: 'D', type: 'line', data: kdj.d, xAxisIndex: 3, yAxisIndex: 3, symbol: 'none', lineStyle: {width: 1, color: '#f39c12'} },
            { name: 'J', type: 'line', data: kdj.j, xAxisIndex: 3, yAxisIndex: 3, symbol: 'none', lineStyle: {width: 1, color: '#9b59b6'} },
            // ── Grid 4: CCI ──
            { name: 'CCI', type: 'line', data: cci, xAxisIndex: 4, yAxisIndex: 4, symbol: 'none', lineStyle: {width: 1.5, color: '#e67e22'} },
            { type: 'line', data: dates.map(function() { return 100; }), xAxisIndex: 4, yAxisIndex: 4, symbol: 'none', lineStyle: {width: 1, type: 'dashed', color: '#e74c3c'} },
            { type: 'line', data: dates.map(function() { return -100; }), xAxisIndex: 4, yAxisIndex: 4, symbol: 'none', lineStyle: {width: 1, type: 'dashed', color: '#27ae60'} }
        ]
    };

    klineChart.setOption(option);
    window.addEventListener('resize', function() { if (klineChart) klineChart.resize(); });
}

// ══════════════════════════════════════════════════════════════
// ── 估值分析图表：PE / PS / PB / 总市值 ──
// ══════════════════════════════════════════════════════════════
function renderValuationCharts() {
    if (!stockData || !stockData.basic_history || !stockData.basic_history.dates || stockData.basic_history.dates.length === 0) {
        var sec = document.getElementById('valuation-section');
        if (sec) sec.style.display = 'none';
        return;
    }

    var bh = stockData.basic_history;
    var dates = bh.dates;

    var container = document.getElementById('valuation-chart');
    if (valuationChart) valuationChart.dispose();
    valuationChart = echarts.init(container, getEchartsTheme());

    var metrics = [
        { key: 'pe', label: 'PE (市盈率)' },
        { key: 'ps', label: 'PS (市销率)' },
        { key: 'pb', label: 'PB (市净率)' },
        { key: 'total_mv', label: '总市值 (万元)' }
    ];

    var grids = [];
    var xAxes = [];
    var yAxes = [];
    var series = [];
    var titles = [];
    var windowEl = document.getElementById('valuation-window');
    var WINDOW = windowEl ? parseInt(windowEl.value) : 252;

    for (var m = 0; m < metrics.length; m++) {
        var metric = metrics[m];
        var rawData = bh[metric.key];
        var topPct = 3 + m * 24; // 每个子图间隔

        grids.push({ left: '8%', right: '12%', top: topPct + '%', height: '20%' });
        xAxes.push({
            type: 'category', data: dates, gridIndex: m,
            axisLabel: { show: m === metrics.length - 1, fontSize: 11 },
            axisTick: { show: m === metrics.length - 1 },
            axisLine: { show: m === metrics.length - 1 }
        });
        yAxes.push({ scale: true, gridIndex: m, splitLine: {lineStyle: {opacity: 0.3}}, axisLabel: {fontSize: 10} });

        // 计算滚动均值和标准差
        var rolling = calcRollingStats(rawData, WINDOW);

        // 计算历史百分位
        var lastVal = null;
        for (var vi = rawData.length - 1; vi >= 0; vi--) {
            if (rawData[vi] !== null) { lastVal = rawData[vi]; break; }
        }
        var percentile = calcPercentile(rawData, lastVal);
        var percentileText = lastVal !== null
            ? metric.label + '=' + (metric.key === 'total_mv' ? (lastVal / 10000).toFixed(1) + '亿' : lastVal.toFixed(2)) + '  历史百分位: ' + percentile + '%'
            : '';

        // 标题
        titles.push({
            text: metric.label + (percentileText ? '  (' + percentileText + ')' : ''),
            left: '8%',
            top: (topPct - 0.5) + '%',
            textStyle: { fontSize: 12, fontWeight: 'normal' }
        });

        // 主线
        series.push({
            name: metric.label,
            type: 'line', data: rawData, xAxisIndex: m, yAxisIndex: m,
            symbol: 'none', lineStyle: {width: 1.5, color: '#1a56db'}, z: 3
        });

        // 滚动均值线
        series.push({
            name: metric.label + ' 均值',
            type: 'line', data: rolling.mean, xAxisIndex: m, yAxisIndex: m,
            symbol: 'none', lineStyle: {width: 1, color: '#f39c12', type: 'dashed'}, z: 2
        });

        // +1 标准差（上边界，用于 areaStyle 填充）
        series.push({
            name: metric.label + ' +1σ',
            type: 'line', data: rolling.upper, xAxisIndex: m, yAxisIndex: m,
            symbol: 'none', lineStyle: {width: 0.5, color: '#ef5350', opacity: 0.5},
            z: 1
        });

        // -1 标准差（下边界，填充与上界之间的区域）
        series.push({
            name: metric.label + ' -1σ',
            type: 'line', data: rolling.lower, xAxisIndex: m, yAxisIndex: m,
            symbol: 'none', lineStyle: {width: 0.5, color: '#26a69a', opacity: 0.5},
            areaStyle: { color: 'rgba(26,86,219,0.08)', origin: 'auto' },
            z: 1
        });
    }

    var option = {
        animation: false,
        title: titles,
        tooltip: { trigger: 'axis', axisPointer: {type: 'line'} },
        axisPointer: { link: [{xAxisIndex: 'all'}] },
        grid: grids,
        xAxis: xAxes,
        yAxis: yAxes,
        dataZoom: [
            { type: 'inside', xAxisIndex: [0, 1, 2, 3], start: 0, end: 100 },
            { type: 'slider', xAxisIndex: [0, 1, 2, 3], bottom: 2, height: 18, start: 0, end: 100 }
        ],
        series: series
    };

    valuationChart.setOption(option);
    window.addEventListener('resize', function() { if (valuationChart) valuationChart.resize(); });
}

// ══════════════════════════════════════════════════════════════
// ── 内在价值估算：RI / DCF / Graham / 估值锚 ──
// ══════════════════════════════════════════════════════════════
function shouldShowIntrinsicValuation() {
    if (currentFreq !== 'D') return false;
    if (!stockData || !stockData.ohlc || stockData.ohlc.length === 0) return false;
    var hasBasic = !!(stockData.basic && Object.keys(stockData.basic).length > 0);
    var hasFina = !!(finaData && Array.isArray(finaData.data) && finaData.data.length > 0);
    return hasBasic || hasFina;
}

function ivToNum(v) {
    var n = Number(v);
    return Number.isFinite(n) ? n : null;
}

function ivClamp(v, minV, maxV) {
    return Math.max(minV, Math.min(maxV, v));
}

function ivRound(v, digits) {
    var n = ivToNum(v);
    if (n === null) return null;
    return Number(n.toFixed(digits === undefined ? 2 : digits));
}

function ivValidSeries(arr) {
    if (!Array.isArray(arr)) return [];
    var out = [];
    for (var i = 0; i < arr.length; i++) {
        var n = ivToNum(arr[i]);
        if (n !== null) out.push(n);
    }
    return out;
}

function ivWeightedRecent(values, weights) {
    if (!values || !values.length) return null;
    var sumW = 0;
    var sumV = 0;
    for (var i = 0; i < values.length; i++) {
        var n = ivToNum(values[i]);
        if (n === null) continue;
        var w = weights && weights[i] !== undefined ? Number(weights[i]) : 1;
        if (!Number.isFinite(w) || w <= 0) w = 1;
        sumW += w;
        sumV += n * w;
    }
    if (sumW <= 0) return null;
    return sumV / sumW;
}

function ivPercentileValue(values, pct) {
    var arr = ivValidSeries(values).slice().sort(function(a, b) { return a - b; });
    if (!arr.length) return null;
    if (arr.length === 1) return arr[0];
    var p = ivClamp(Number(pct), 0, 100) / 100;
    var idx = (arr.length - 1) * p;
    var lo = Math.floor(idx);
    var hi = Math.ceil(idx);
    if (lo === hi) return arr[lo];
    var w = idx - lo;
    return arr[lo] * (1 - w) + arr[hi] * w;
}

function ivMedian(values) {
    var arr = ivValidSeries(values).slice().sort(function(a, b) { return a - b; });
    if (!arr.length) return null;
    var mid = Math.floor(arr.length / 2);
    if (arr.length % 2 === 1) return arr[mid];
    return (arr[mid - 1] + arr[mid]) / 2;
}

function ivExtractFinaSeries(field, limit) {
    var out = [];
    if (!finaData || !Array.isArray(finaData.data)) return out;
    var maxN = Math.max(1, Number(limit || 8));
    for (var i = 0; i < finaData.data.length && out.length < maxN; i++) {
        var row = finaData.data[i] || {};
        var n = ivToNum(row[field]);
        if (n !== null) out.push(n);
    }
    return out;
}

function ivLatestFinaRow() {
    if (!finaData || !Array.isArray(finaData.data) || !finaData.data.length) return null;
    return finaData.data[0] || null;
}

function ivCalcRIValuePerShare(bps0, roe, discount, growth, years, persistence) {
    if (!(bps0 > 0) || !(discount > 0)) return null;
    var r = ivClamp(discount, 0.04, 0.25);
    var ro = ivClamp(roe, 0.0, 0.40);
    var maxG = Math.min(0.12, r - 0.025);
    if (maxG < -0.02) maxG = -0.02;
    var g = ivClamp(growth, -0.03, maxG);
    var p = ivClamp(persistence || 0.75, 0.35, 0.92);
    var nYears = Math.max(3, Math.min(10, Math.round(years || 5)));

    var bv = bps0;
    var value = bps0;
    var residualN = 0;
    for (var t = 1; t <= nYears; t++) {
        var residual = (ro - r) * bv;
        value += residual / Math.pow(1 + r, t);
        residualN = residual;
        bv = bv * (1 + g);
    }
    var terminalDenom = 1 + r - p;
    if (terminalDenom > 0.05) {
        var terminalResidual = residualN * p;
        value += (terminalResidual / terminalDenom) / Math.pow(1 + r, nYears);
    }
    if (value < bps0 * 0.3) value = bps0 * 0.3;
    return value > 0 ? value : null;
}

function ivCalcGrahamPerShare(eps, growthPct, bondYieldPct) {
    if (!(eps > 0) || !(bondYieldPct > 0)) return null;
    var g = ivClamp(growthPct, 0, 20);
    var y = ivClamp(bondYieldPct, 1.0, 10.0);
    return eps * (8.5 + 2 * g) * (4.4 / y);
}

function ivCalcDCFValuePerShare(fcfe0, discountPct, growthPct, years, terminalGrowthPct) {
    if (!(fcfe0 > 0) || !(discountPct > 0)) return null;
    var nYears = Math.max(3, Math.min(10, Math.round(years || 5)));
    var r = ivClamp(discountPct / 100, 0.04, 0.25);
    var gMax = Math.min(0.20, r - 0.015);
    var g = ivClamp((growthPct || 0) / 100, -0.05, gMax);
    var gtMax = Math.min(0.05, r - 0.015);
    var gt = ivClamp((terminalGrowthPct || 2.0) / 100, 0.0, gtMax);

    var pv = 0;
    var cf = fcfe0;
    for (var t = 1; t <= nYears; t++) {
        cf = cf * (1 + g);
        pv += cf / Math.pow(1 + r, t);
    }
    if (r - gt > 0.01) {
        var terminalCf = cf * (1 + gt);
        var terminalValue = terminalCf / (r - gt);
        pv += terminalValue / Math.pow(1 + r, nYears);
    }
    return pv > 0 ? pv : null;
}

function ivBuildValuationAnchor(price, epsTtm, bps) {
    var bh = stockData && stockData.basic_history ? stockData.basic_history : null;
    if (!bh) return null;

    var peSeries = ivValidSeries(bh.pe || []);
    var pbSeries = ivValidSeries(bh.pb || []);

    var peBand = null;
    if (epsTtm && epsTtm > 0 && peSeries.length) {
        var pe30 = ivPercentileValue(peSeries, 30);
        var pe50 = ivPercentileValue(peSeries, 50);
        var pe70 = ivPercentileValue(peSeries, 70);
        if (pe30 !== null && pe50 !== null && pe70 !== null) {
            peBand = {
                low: epsTtm * pe30,
                mid: epsTtm * pe50,
                high: epsTtm * pe70,
                p30: pe30,
                p50: pe50,
                p70: pe70
            };
        }
    }

    var pbBand = null;
    if (bps && bps > 0 && pbSeries.length) {
        var pb30 = ivPercentileValue(pbSeries, 30);
        var pb50 = ivPercentileValue(pbSeries, 50);
        var pb70 = ivPercentileValue(pbSeries, 70);
        if (pb30 !== null && pb50 !== null && pb70 !== null) {
            pbBand = {
                low: bps * pb30,
                mid: bps * pb50,
                high: bps * pb70,
                p30: pb30,
                p50: pb50,
                p70: pb70
            };
        }
    }

    var baseCandidates = [];
    var lowCandidates = [];
    var highCandidates = [];
    if (peBand) {
        baseCandidates.push(peBand.mid);
        lowCandidates.push(peBand.low);
        highCandidates.push(peBand.high);
    }
    if (pbBand) {
        baseCandidates.push(pbBand.mid);
        lowCandidates.push(pbBand.low);
        highCandidates.push(pbBand.high);
    }
    if (!baseCandidates.length) return null;

    var base = ivWeightedRecent(baseCandidates, null);
    var low = lowCandidates.length ? Math.min.apply(null, lowCandidates) : null;
    var high = highCandidates.length ? Math.max.apply(null, highCandidates) : null;

    return {
        low: low,
        base: base,
        high: high,
        peBand: peBand,
        pbBand: pbBand
    };
}

function ivBuildResult() {
    var latestClose = null;
    if (stockData && stockData.ohlc && stockData.ohlc.length) {
        latestClose = ivToNum(stockData.ohlc[stockData.ohlc.length - 1][3]);
    }
    var basic = stockData && stockData.basic ? stockData.basic : {};
    var peTtm = ivToNum(basic.pe_ttm !== undefined ? basic.pe_ttm : basic.pe);
    var pb = ivToNum(basic.pb);
    var latestFina = ivLatestFinaRow();

    var bps = latestFina ? ivToNum(latestFina.bps) : null;
    var epsLatest = latestFina ? ivToNum(latestFina.eps) : null;
    var bpsSource = bps && bps > 0 ? '财务BPS' : '未获取';
    var epsSource = epsLatest && epsLatest > 0 ? '财务EPS' : '未获取';
    var epsTtm = (latestClose && peTtm && peTtm > 0) ? latestClose / peTtm : null;
    if (epsTtm && epsTtm > 0) {
        epsSource = '价格/PE(TTM)推导';
    } else {
        epsTtm = epsLatest;
    }
    if (!(bps > 0) && latestClose && pb && pb > 0) {
        bps = latestClose / pb;
        bpsSource = '价格/PB推导';
    }

    var discountPct = ivClamp(ivToNum(document.getElementById('iv-discount').value) || 10, 4, 20);
    var years = ivClamp(ivToNum(document.getElementById('iv-years').value) || 5, 3, 10);
    var bondYieldPct = ivClamp(ivToNum(document.getElementById('iv-bond-yield').value) || 3.0, 1.0, 8.0);
    var marginPct = ivClamp(ivToNum(document.getElementById('iv-margin').value) || 25, 5, 60);
    var growthCapPct = ivClamp(ivToNum(document.getElementById('iv-growth-cap').value) || 8, 2, 20);

    var roeSeries = ivExtractFinaSeries('roe', 8);
    var yoySeries = ivExtractFinaSeries('netprofit_yoy', 8);
    var roeWeights = [0.30, 0.24, 0.18, 0.10, 0.08, 0.05, 0.03, 0.02];
    var yoyWeights = [0.28, 0.23, 0.18, 0.12, 0.08, 0.06, 0.03, 0.02];

    var roeBasePct = ivWeightedRecent(roeSeries, roeWeights);
    var roeMedianPct = ivMedian(roeSeries);
    if (!(roeBasePct !== null)) {
        if (epsTtm && bps && bps > 0) roeBasePct = (epsTtm / bps) * 100;
        else roeBasePct = 8;
    }
    if (roeMedianPct !== null) {
        roeBasePct = roeBasePct * 0.7 + roeMedianPct * 0.3;
    }
    roeBasePct = ivClamp(roeBasePct, 2, 22);

    var yoyBasePct = ivWeightedRecent(yoySeries, yoyWeights);
    if (yoyBasePct === null) yoyBasePct = roeBasePct * 0.6;
    // 增长上限的“有效值”会受折现率约束（防止估值终值失真）
    var growthMaxByDiscount = Math.max(2.0, discountPct - 2.5);
    var effectiveGrowthCapPct = Math.min(growthCapPct, growthMaxByDiscount);
    var growthBaseRawPct = yoyBasePct * 0.25;
    var growthBasePct = ivClamp(growthBaseRawPct, -2, effectiveGrowthCapPct);
    // 让“增长上限”不仅仅是截断上界，对基准场景也保留温和影响
    if (growthBasePct >= 0 && effectiveGrowthCapPct > 0) {
        growthBasePct = ivClamp(growthBasePct * 0.85 + effectiveGrowthCapPct * 0.15, -2, effectiveGrowthCapPct);
    }

    var growthConPct = growthBasePct >= 0 ? growthBasePct * 0.7 : growthBasePct * 1.2;
    var growthOptPct = growthBasePct >= 0 ? growthBasePct * 1.25 : growthBasePct * 0.8;
    growthConPct = ivClamp(growthConPct, -3, effectiveGrowthCapPct);
    growthOptPct = ivClamp(growthOptPct, -1, effectiveGrowthCapPct);

    var fcfeSeries = ivExtractFinaSeries('fcfe_ps', 8);
    var ocfpsSeries = ivExtractFinaSeries('ocfps', 8);
    var fcfeBasePs = ivWeightedRecent(fcfeSeries.slice(0, 3), [0.55, 0.30, 0.15]);
    var fcfeSource = 'FCFE/股';
    if (!(fcfeBasePs > 0)) {
        var ocfpsBase = ivWeightedRecent(ocfpsSeries.slice(0, 3), [0.55, 0.30, 0.15]);
        if (ocfpsBase && ocfpsBase > 0) {
            fcfeBasePs = ocfpsBase * 0.70;
            fcfeSource = 'OCFPS*0.70';
        }
    }
    if (!(fcfeBasePs > 0) && epsTtm && epsTtm > 0) {
        fcfeBasePs = epsTtm * 0.80;
        fcfeSource = 'EPS(TTM)*0.80';
    }
    var dcfStageGrowthPct = growthBasePct;
    var dcfTerminalGrowthPct = ivClamp(Math.max(1.2, dcfStageGrowthPct * 0.35), 0.5, Math.min(4.0, growthMaxByDiscount));
    var dcfCon = ivCalcDCFValuePerShare(fcfeBasePs ? fcfeBasePs * 0.90 : null, discountPct + 0.8, growthConPct, years, Math.max(0.5, dcfTerminalGrowthPct - 0.4));
    var dcfBase = ivCalcDCFValuePerShare(fcfeBasePs, discountPct, dcfStageGrowthPct, years, dcfTerminalGrowthPct);
    var dcfOpt = ivCalcDCFValuePerShare(fcfeBasePs ? fcfeBasePs * 1.08 : null, Math.max(4, discountPct - 0.6), growthOptPct, years, Math.min(4.5, dcfTerminalGrowthPct + 0.4));

    var riCon = (bps && bps > 0)
        ? ivCalcRIValuePerShare(
            bps,
            ivClamp((roeBasePct * 0.85) / 100, 0.01, 0.35),
            discountPct / 100,
            growthConPct / 100,
            years,
            0.65
        )
        : null;
    var riBase = (bps && bps > 0)
        ? ivCalcRIValuePerShare(
            bps,
            ivClamp(roeBasePct / 100, 0.01, 0.35),
            discountPct / 100,
            growthBasePct / 100,
            years,
            0.75
        )
        : null;
    var riOpt = (bps && bps > 0)
        ? ivCalcRIValuePerShare(
            bps,
            ivClamp((roeBasePct * 1.15) / 100, 0.01, 0.38),
            discountPct / 100,
            growthOptPct / 100,
            years,
            0.85
        )
        : null;

    var graham = ivCalcGrahamPerShare(epsTtm, ivClamp(growthBasePct, 0, 15), bondYieldPct);
    var anchor = ivBuildValuationAnchor(latestClose, epsTtm, bps);

    var weighted = [];
    if (riBase && riBase > 0) weighted.push({v: riBase, w: 0.40});
    if (dcfBase && dcfBase > 0) weighted.push({v: dcfBase, w: 0.30});
    if (graham && graham > 0) weighted.push({v: graham, w: 0.15});
    if (anchor && anchor.base && anchor.base > 0) weighted.push({v: anchor.base, w: 0.15});
    var composite = null;
    if (weighted.length) {
        var sumW = 0;
        var sumV = 0;
        for (var wi = 0; wi < weighted.length; wi++) {
            sumW += weighted[wi].w;
            sumV += weighted[wi].v * weighted[wi].w;
        }
        composite = sumW > 0 ? sumV / sumW : null;
    }

    var safeEntry = composite ? composite * (1 - marginPct / 100) : null;
    var upsidePct = (composite && latestClose && latestClose > 0)
        ? (composite / latestClose - 1) * 100
        : null;

    return {
        price: latestClose,
        bps: bps,
        bpsSource: bpsSource,
        epsTtm: epsTtm,
        epsSource: epsSource,
        peTtm: peTtm,
        pb: pb,
        discountPct: discountPct,
        years: years,
        bondYieldPct: bondYieldPct,
        marginPct: marginPct,
        growthCapPct: growthCapPct,
        effectiveGrowthCapPct: effectiveGrowthCapPct,
        roeBasePct: roeBasePct,
        growthBasePct: growthBasePct,
        fcfeBasePs: fcfeBasePs,
        fcfeSource: fcfeSource,
        ri: { conservative: riCon, base: riBase, optimistic: riOpt },
        dcf: {
            conservative: dcfCon,
            base: dcfBase,
            optimistic: dcfOpt,
            stageGrowthPct: dcfStageGrowthPct,
            terminalGrowthPct: dcfTerminalGrowthPct
        },
        graham: graham,
        anchor: anchor,
        composite: composite,
        safeEntry: safeEntry,
        upsidePct: upsidePct
    };
}

function ivFmtPrice(v) {
    var n = ivToNum(v);
    if (n === null) return '--';
    return n.toFixed(2);
}

function ivFmtPct(v) {
    var n = ivToNum(v);
    if (n === null) return '--';
    var s = n >= 0 ? '+' : '';
    return s + n.toFixed(2) + '%';
}

function setIntrinsicStatus(level, message) {
    var el = document.getElementById('intrinsic-status');
    if (!el) return;
    el.textContent = message || '';
    el.className = 'stock-data-status level-' + (level || 'ok');
    el.style.display = message ? 'block' : 'none';
}

function renderIntrinsicCards(result) {
    var box = document.getElementById('intrinsic-cards');
    if (!box) return;
    var price = ivToNum(result.price);
    var compositeDiff = (result.composite && price) ? (result.composite / price - 1) * 100 : null;
    var safeDiff = (result.safeEntry && price) ? (result.safeEntry / price - 1) * 100 : null;
    var anchorBase = result.anchor && result.anchor.base ? result.anchor.base : null;

    var cards = [
        {
            title: '当前股价',
            value: ivFmtPrice(price),
            klass: '',
            sub: '最新收盘价'
        },
        {
            title: '每股净资产(BPS)',
            value: ivFmtPrice(result.bps),
            klass: '',
            sub: '来源: ' + (result.bpsSource || '--')
        },
        {
            title: 'RI中性内在价值',
            value: ivFmtPrice(result.ri.base),
            klass: (result.ri.base && price && result.ri.base >= price) ? 'positive' : 'negative',
            sub: '折现率 ' + result.discountPct.toFixed(1) + '% · ' + result.years + '年预测'
        },
        {
            title: 'DCF 内在价值',
            value: ivFmtPrice(result.dcf.base),
            klass: (result.dcf.base && price && result.dcf.base >= price) ? 'positive' : 'negative',
            sub: 'FCFE=' + ivFmtPrice(result.fcfeBasePs) + ' (' + (result.fcfeSource || '--') + ')'
        },
        {
            title: 'Graham 内在价值',
            value: ivFmtPrice(result.graham),
            klass: (result.graham && price && result.graham >= price) ? 'positive' : 'negative',
            sub: 'EPS=' + ivFmtPrice(result.epsTtm) + ' · g=' + result.growthBasePct.toFixed(2) + '%'
        },
        {
            title: '估值锚中枢',
            value: ivFmtPrice(anchorBase),
            klass: (anchorBase && price && anchorBase >= price) ? 'positive' : 'negative',
            sub: 'PE/PB 历史50分位映射'
        },
        {
            title: '综合内在价值',
            value: ivFmtPrice(result.composite),
            klass: compositeDiff !== null ? (compositeDiff >= 0 ? 'positive' : 'negative') : '',
            sub: '相对现价: ' + ivFmtPct(compositeDiff)
        },
        {
            title: '安全边际买点',
            value: ivFmtPrice(result.safeEntry),
            klass: safeDiff !== null ? (safeDiff >= 0 ? 'positive' : 'negative') : '',
            sub: '安全边际 ' + result.marginPct.toFixed(0) + '%'
        }
    ];

    box.innerHTML = cards.map(function(c) {
        return '<div class="intrinsic-stat-card">' +
            '<div class="intrinsic-stat-title">' + c.title + '</div>' +
            '<div class="intrinsic-stat-value ' + (c.klass || '') + '">' + c.value + '</div>' +
            '<div class="intrinsic-stat-sub">' + c.sub + '</div>' +
            '</div>';
    }).join('');
}

function renderIntrinsicChart(result) {
    var container = document.getElementById('intrinsic-chart');
    if (!container) return;
    if (intrinsicChart) intrinsicChart.dispose();
    intrinsicChart = echarts.init(container, getEchartsTheme());

    var rows = [
        { name: '当前股价', value: result.price, color: '#64748b' },
        { name: 'RI保守', value: result.ri.conservative, color: '#14b8a6' },
        { name: 'RI中性', value: result.ri.base, color: '#1d4ed8' },
        { name: 'RI乐观', value: result.ri.optimistic, color: '#7c3aed' },
        { name: 'DCF保守', value: result.dcf.conservative, color: '#22c55e' },
        { name: 'DCF中性', value: result.dcf.base, color: '#16a34a' },
        { name: 'DCF乐观', value: result.dcf.optimistic, color: '#15803d' },
        { name: 'Graham', value: result.graham, color: '#f59e0b' },
        { name: '估值锚中枢', value: result.anchor && result.anchor.base, color: '#0ea5e9' },
        { name: '综合内在价值', value: result.composite, color: '#ef4444' },
        { name: '安全边际买点', value: result.safeEntry, color: '#10b981' }
    ];
    var validRows = rows.filter(function(r) { return ivToNum(r.value) !== null; });

    var option = {
        animation: false,
        tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
        grid: { left: '6%', right: '4%', top: '12%', bottom: '12%' },
        xAxis: {
            type: 'category',
            data: validRows.map(function(r) { return r.name; }),
            axisLabel: { interval: 0, fontSize: 11 }
        },
        yAxis: {
            type: 'value',
            scale: true,
            splitLine: { lineStyle: { opacity: 0.3 } }
        },
        series: [{
            type: 'bar',
            barWidth: '52%',
            data: validRows.map(function(r) {
                return {
                    value: ivRound(r.value, 2),
                    itemStyle: { color: r.color, borderRadius: [6, 6, 0, 0] }
                };
            }),
            label: {
                show: true,
                position: 'top',
                formatter: function(p) { return Number(p.value || 0).toFixed(2); },
                fontSize: 11
            }
        }]
    };

    intrinsicChart.setOption(option);
    if (!intrinsicResizeBound) {
        intrinsicResizeBound = true;
        window.addEventListener('resize', function() {
            if (intrinsicChart) intrinsicChart.resize();
        });
    }
}

function renderIntrinsicTable(result) {
    var table = document.getElementById('intrinsic-table');
    if (!table) return;

    var price = ivToNum(result.price);
    function diffText(v) {
        if (!(price > 0) || ivToNum(v) === null) return '--';
        return ivFmtPct((v / price - 1) * 100);
    }
    function classByDiff(v) {
        if (!(price > 0) || ivToNum(v) === null) return '';
        return (v / price - 1) >= 0 ? 'positive' : 'negative';
    }

    var anchor = result.anchor || {};
    var rows = [
        {
            method: 'RI 保守',
            value: result.ri.conservative,
            assume: 'ROE=' + (result.roeBasePct * 0.85).toFixed(2) + '%, g=' +
                (result.growthBasePct >= 0 ? result.growthBasePct * 0.7 : result.growthBasePct * 1.2).toFixed(2) + '%'
        },
        {
            method: 'RI 中性',
            value: result.ri.base,
            assume: 'ROE=' + result.roeBasePct.toFixed(2) + '%, g=' + result.growthBasePct.toFixed(2) + '%'
        },
        {
            method: 'RI 乐观',
            value: result.ri.optimistic,
            assume: 'ROE=' + (result.roeBasePct * 1.15).toFixed(2) + '%, g=' +
                (result.growthBasePct >= 0 ? result.growthBasePct * 1.25 : result.growthBasePct * 0.8).toFixed(2) + '%'
        },
        {
            method: 'DCF 保守',
            value: result.dcf.conservative,
            assume: 'FCFE=' + ivFmtPrice(result.fcfeBasePs ? result.fcfeBasePs * 0.9 : null) +
                ', g=' + (result.growthBasePct >= 0 ? result.growthBasePct * 0.7 : result.growthBasePct * 1.2).toFixed(2) +
                '%, 永续g=' + Math.max(0.5, result.dcf.terminalGrowthPct - 0.4).toFixed(2) + '%'
        },
        {
            method: 'DCF 中性',
            value: result.dcf.base,
            assume: 'FCFE=' + ivFmtPrice(result.fcfeBasePs) + ' (' + (result.fcfeSource || '--') + '), g=' +
                result.dcf.stageGrowthPct.toFixed(2) + '%, 永续g=' + result.dcf.terminalGrowthPct.toFixed(2) + '%'
        },
        {
            method: 'DCF 乐观',
            value: result.dcf.optimistic,
            assume: 'FCFE=' + ivFmtPrice(result.fcfeBasePs ? result.fcfeBasePs * 1.08 : null) +
                ', g=' + (result.growthBasePct >= 0 ? result.growthBasePct * 1.25 : result.growthBasePct * 0.8).toFixed(2) +
                '%, 永续g=' + Math.min(4.5, result.dcf.terminalGrowthPct + 0.4).toFixed(2) + '%'
        },
        {
            method: 'Graham',
            value: result.graham,
            assume: 'EPS=' + ivFmtPrice(result.epsTtm) + ', 国债=' + result.bondYieldPct.toFixed(2) + '%'
        },
        {
            method: '估值锚低位',
            value: anchor.low,
            assume: 'PE/PB 30分位映射'
        },
        {
            method: '估值锚中枢',
            value: anchor.base,
            assume: 'PE/PB 50分位映射'
        },
        {
            method: '估值锚高位',
            value: anchor.high,
            assume: 'PE/PB 70分位映射'
        },
        {
            method: '综合内在价值',
            value: result.composite,
            assume: '权重: RI 40% + DCF 30% + Graham 15% + 估值锚 15%（按可用项归一）'
        },
        {
            method: '安全边际买点',
            value: result.safeEntry,
            assume: '综合价值 × (1 - ' + result.marginPct.toFixed(0) + '%)'
        }
    ];

    var html = '<thead><tr><th>方法</th><th>估算价格</th><th>相对现价</th><th>关键假设</th></tr></thead><tbody>';
    for (var i = 0; i < rows.length; i++) {
        var r = rows[i];
        html += '<tr>' +
            '<td>' + r.method + '</td>' +
            '<td>' + ivFmtPrice(r.value) + '</td>' +
            '<td class="' + classByDiff(r.value) + '">' + diffText(r.value) + '</td>' +
            '<td>' + r.assume + '</td>' +
            '</tr>';
    }
    html += '</tbody>';
    table.innerHTML = html;
}

function renderIntrinsicValuation() {
    var section = document.getElementById('intrinsic-section');
    if (!section) return;
    if (!shouldShowIntrinsicValuation()) {
        section.style.display = 'none';
        return;
    }
    section.style.display = 'block';

    var result = ivBuildResult();
    if (!result || !result.price) {
        setIntrinsicStatus('warning', '缺少核心输入（价格/BPS/EPS），无法估算内在价值。');
        return;
    }

    renderIntrinsicCards(result);
    renderIntrinsicChart(result);
    renderIntrinsicTable(result);

    var status = '输入参数：折现率 ' + result.discountPct.toFixed(2) + '%，预测 ' + result.years +
        ' 年，国债 ' + result.bondYieldPct.toFixed(2) + '%，安全边际 ' + result.marginPct.toFixed(0) + '%' +
        ' ｜ 核心因子：ROE基准 ' + result.roeBasePct.toFixed(2) + '%，增长基准 ' + result.growthBasePct.toFixed(2) + '%' +
        ' ｜ 增长上限(输入/有效): ' + result.growthCapPct.toFixed(2) + '% / ' + result.effectiveGrowthCapPct.toFixed(2) + '%' +
        ' ｜ BPS来源: ' + (result.bpsSource || '--') +
        ' ｜ EPS来源: ' + (result.epsSource || '--') +
        ' ｜ FCFE来源: ' + (result.fcfeSource || '--');
    setIntrinsicStatus('ok', status);
}

// ── 滚动均值 + 标准差计算 ──
function calcRollingStats(data, window) {
    var mean = [], upper = [], lower = [];
    for (var i = 0; i < data.length; i++) {
        if (i < window - 1 || data[i] === null) {
            mean.push(null); upper.push(null); lower.push(null);
            continue;
        }
        // 收集窗口内非 null 值
        var vals = [];
        for (var j = 0; j < window; j++) {
            if (data[i - j] !== null) vals.push(data[i - j]);
        }
        if (vals.length < window * 0.5) {
            mean.push(null); upper.push(null); lower.push(null);
            continue;
        }
        var sum = 0;
        for (var k = 0; k < vals.length; k++) sum += vals[k];
        var avg = sum / vals.length;
        var variance = 0;
        for (var k = 0; k < vals.length; k++) variance += Math.pow(vals[k] - avg, 2);
        var std = Math.sqrt(variance / vals.length);
        mean.push(parseFloat(avg.toFixed(2)));
        upper.push(parseFloat((avg + std).toFixed(2)));
        lower.push(parseFloat((avg - std).toFixed(2)));
    }
    return { mean: mean, upper: upper, lower: lower };
}

// ── 历史百分位计算 ──
function calcPercentile(data, value) {
    if (value === null || value === undefined) return '-';
    var valid = [];
    for (var i = 0; i < data.length; i++) {
        if (data[i] !== null) valid.push(data[i]);
    }
    if (valid.length === 0) return '-';
    var count = 0;
    for (var i = 0; i < valid.length; i++) {
        if (valid[i] <= value) count++;
    }
    return (count / valid.length * 100).toFixed(1);
}

// ── 九转信号提示 ──
function renderNineturnAlert() {
    var alertDiv = document.getElementById('nineturn-alert');
    if (!nineTurnData || !stockData || !stockData.dates) {
        alertDiv.style.display = 'none';
        return;
    }

    var signals = nineTurnData.signals;
    if (nineTurnData.source === 'frontend' && stockData.ohlc.length > 13) {
        signals = calcNineturnFrontend(stockData.dates, stockData.ohlc);
    }

    if (!signals || signals.length === 0) {
        alertDiv.style.display = 'none';
        return;
    }

    var recentDates = stockData.dates.slice(-5);
    var recentSignals = signals.filter(function(s) {
        return recentDates.indexOf(s.trade_date) >= 0 && (s.buy === 9 || s.sell === 9);
    });

    if (recentSignals.length > 0) {
        var s = recentSignals[recentSignals.length - 1];
        var freqName = currentFreq === 'D' ? '日线' : (currentFreq === 'W' ? '周线' : '月线');
        if (s.buy === 9) {
            alertDiv.className = 'nineturn-alert buy';
            alertDiv.innerHTML = '🟢 <strong>' + freqName + '出现神奇九转买入第9信号！</strong>（' + s.trade_date + '）建议关注买入机会';
        } else if (s.sell === 9) {
            alertDiv.className = 'nineturn-alert sell';
            alertDiv.innerHTML = '🔴 <strong>' + freqName + '出现神奇九转卖出第9信号！</strong>（' + s.trade_date + '）建议关注卖出风险';
        }
        alertDiv.style.display = 'block';
    } else {
        alertDiv.style.display = 'none';
    }
}

// ── 九转标注构建 ──
function buildNineturnMarks(dates) {
    var signals = [];
    if (nineTurnData && nineTurnData.signals && nineTurnData.signals.length > 0) {
        signals = nineTurnData.signals;
    } else if (nineTurnData && nineTurnData.source === 'frontend' && stockData && stockData.ohlc.length > 13) {
        signals = calcNineturnFrontend(stockData.dates, stockData.ohlc);
    }

    if (signals.length === 0) return {};

    var markData = [];
    signals.forEach(function(s) {
        var idx = dates.indexOf(s.trade_date);
        if (idx < 0) return;
        if (s.buy) {
            markData.push({
                coord: [idx, stockData.ohlc[idx][2]],
                value: s.buy,
                symbol: 'circle',
                symbolSize: s.buy === 9 ? 22 : 14,
                itemStyle: { color: '#26a69a' },
                label: { show: true, formatter: String(s.buy), fontSize: s.buy === 9 ? 14 : 10, fontWeight: s.buy === 9 ? 'bold' : 'normal', color: '#26a69a', position: 'bottom' }
            });
        }
        if (s.sell) {
            markData.push({
                coord: [idx, stockData.ohlc[idx][1]],
                value: s.sell,
                symbol: 'circle',
                symbolSize: s.sell === 9 ? 22 : 14,
                itemStyle: { color: '#ef5350' },
                label: { show: true, formatter: String(s.sell), fontSize: s.sell === 9 ? 14 : 10, fontWeight: s.sell === 9 ? 'bold' : 'normal', color: '#ef5350', position: 'top' }
            });
        }
    });

    return { data: markData };
}

// ── 前端九转计算 ──
function calcNineturnFrontend(dates, ohlc) {
    var closes = ohlc.map(function(d) { return d[3]; });
    var signals = [];
    var buyCount = 0;
    var sellCount = 0;

    for (var i = 4; i < closes.length; i++) {
        if (closes[i] < closes[i - 4]) {
            buyCount++;
            if (buyCount <= 9) {
                signals.push({ trade_date: dates[i], buy: buyCount });
            }
            if (buyCount >= 9) buyCount = 0;
        } else {
            buyCount = 0;
        }

        if (closes[i] > closes[i - 4]) {
            sellCount++;
            if (sellCount <= 9) {
                signals.push({ trade_date: dates[i], sell: sellCount });
            }
            if (sellCount >= 9) sellCount = 0;
        } else {
            sellCount = 0;
        }
    }
    return signals;
}

// ── 财务表格 ──
function renderFinaTable() {
    if (!finaData || !finaData.data || finaData.data.length === 0) return;

    var table = document.getElementById('fina-table');
    var cols = finaData.columns;
    var data = finaData.data;

    var colLabels = {
        end_date: '报告期', eps: 'EPS', bps: '每股净资产',
        roe: 'ROE(%)', roe_waa: 'ROE加权(%)', roa: 'ROA(%)',
        grossprofit_margin: '毛利率(%)', netprofit_margin: '净利率(%)',
        netprofit_yoy: '净利润增速(%)', or_yoy: '营收增速(%)',
        debt_to_assets: '资产负债率(%)', current_ratio: '流动比率',
        quick_ratio: '速动比率',
        revenue: '营收(亿)', total_profit: '利润总额(亿)', n_income: '净利润(亿)'
    };

    var thead = '<thead><tr>';
    cols.forEach(function(col) {
        var label = colLabels[col] || col;
        if (col === 'end_date') {
            thead += '<th>' + label + '</th>';
        } else {
            thead += '<th class="fina-clickable" onclick="showFinaChart(\'' + col + '\', \'' + label + '\')">' + label + '</th>';
        }
    });
    thead += '</tr></thead>';

    var tbody = '<tbody>';
    data.forEach(function(row) {
        tbody += '<tr>';
        cols.forEach(function(col) {
            var val = row[col];
            if (val === null || val === undefined) {
                tbody += '<td>-</td>';
            } else {
                tbody += '<td>' + val + '</td>';
            }
        });
        tbody += '</tr>';
    });
    tbody += '</tbody>';

    table.innerHTML = thead + tbody;
}

// ── 财务指标历史趋势弹窗 ──
function showFinaChart(field, label) {
    if (!finaData || !finaData.data) return;

    var modal = document.getElementById('fina-chart-modal');
    document.getElementById('fina-chart-title').textContent = label + ' 历史趋势';
    modal.style.display = 'flex';

    var data = finaData.data.slice().reverse();
    var dates = data.map(function(d) { return d.end_date || ''; });
    var values = data.map(function(d) { return d[field]; });

    var container = document.getElementById('fina-chart');
    if (finaChartInstance) finaChartInstance.dispose();
    finaChartInstance = echarts.init(container, getEchartsTheme());

    var option = {
        tooltip: { trigger: 'axis' },
        xAxis: { type: 'category', data: dates, axisLabel: {rotate: 45, fontSize: 11} },
        yAxis: { scale: true, splitLine: {lineStyle: {opacity: 0.3}} },
        dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 5, height: 18 }],
        series: [{
            name: label, type: 'bar',
            data: values.map(function(v) {
                return { value: v, itemStyle: { color: v >= 0 ? '#3498db' : '#ef5350' } };
            }),
            barMaxWidth: 40
        }],
        grid: { left: '12%', right: '5%', top: '10%', bottom: '22%' }
    };

    finaChartInstance.setOption(option);
}

function closeFinaModal() {
    document.getElementById('fina-chart-modal').style.display = 'none';
    if (finaChartInstance) { finaChartInstance.dispose(); finaChartInstance = null; }
}

// ══════════════════════════════════════════════════════════════
// ── 技术指标计算函数 ──
// ══════════════════════════════════════════════════════════════

function calcMA(data, period) {
    var result = [];
    for (var i = 0; i < data.length; i++) {
        if (i < period - 1) { result.push(null); continue; }
        var sum = 0;
        for (var j = 0; j < period; j++) sum += data[i - j];
        result.push(parseFloat((sum / period).toFixed(2)));
    }
    return result;
}

function calcEMA(data, period) {
    var result = [];
    var k = 2 / (period + 1);
    for (var i = 0; i < data.length; i++) {
        if (i === 0) { result.push(data[i]); continue; }
        result.push(parseFloat((data[i] * k + result[i - 1] * (1 - k)).toFixed(4)));
    }
    return result;
}

function calcMACD(closes) {
    var ema12 = calcEMA(closes, 12);
    var ema26 = calcEMA(closes, 26);
    var dif = [];
    for (var i = 0; i < closes.length; i++) {
        dif.push(parseFloat((ema12[i] - ema26[i]).toFixed(4)));
    }
    var dea = calcEMA(dif, 9);
    var histogram = [];
    for (var i = 0; i < closes.length; i++) {
        histogram.push(parseFloat(((dif[i] - dea[i]) * 2).toFixed(4)));
    }
    return { dif: dif, dea: dea, histogram: histogram };
}

function calcKDJ(closes, highs, lows, period) {
    period = period || 9;
    var k = [], d = [], j = [];
    var prevK = 50, prevD = 50;

    for (var i = 0; i < closes.length; i++) {
        if (i < period - 1) {
            k.push(null); d.push(null); j.push(null);
            continue;
        }
        var hh = -Infinity, ll = Infinity;
        for (var n = 0; n < period; n++) {
            if (highs[i - n] > hh) hh = highs[i - n];
            if (lows[i - n] < ll) ll = lows[i - n];
        }
        var rsv = hh === ll ? 50 : (closes[i] - ll) / (hh - ll) * 100;
        var curK = 2 / 3 * prevK + 1 / 3 * rsv;
        var curD = 2 / 3 * prevD + 1 / 3 * curK;
        var curJ = 3 * curK - 2 * curD;
        k.push(parseFloat(curK.toFixed(2)));
        d.push(parseFloat(curD.toFixed(2)));
        j.push(parseFloat(curJ.toFixed(2)));
        prevK = curK;
        prevD = curD;
    }
    return { k: k, d: d, j: j };
}

function calcCCI(closes, highs, lows, period) {
    period = period || 14;
    var result = [];
    for (var i = 0; i < closes.length; i++) {
        if (i < period - 1) { result.push(null); continue; }
        var tps = [];
        for (var j = 0; j < period; j++) {
            tps.push((highs[i - j] + lows[i - j] + closes[i - j]) / 3);
        }
        var tp = tps[0];
        var sma = 0;
        for (var j = 0; j < tps.length; j++) sma += tps[j];
        sma /= period;
        var mad = 0;
        for (var j = 0; j < tps.length; j++) mad += Math.abs(tps[j] - sma);
        mad /= period;
        var cciVal = mad === 0 ? 0 : (tp - sma) / (0.015 * mad);
        result.push(parseFloat(cciVal.toFixed(2)));
    }
    return result;
}

function calcBOLL(closes, period) {
    period = period || 20;
    var mid = [], upper = [], lower = [];
    for (var i = 0; i < closes.length; i++) {
        if (i < period - 1) {
            mid.push(null); upper.push(null); lower.push(null);
            continue;
        }
        var sum = 0;
        for (var j = 0; j < period; j++) sum += closes[i - j];
        var ma = sum / period;
        var variance = 0;
        for (var j = 0; j < period; j++) variance += Math.pow(closes[i - j] - ma, 2);
        var std = Math.sqrt(variance / period);
        mid.push(parseFloat(ma.toFixed(2)));
        upper.push(parseFloat((ma + 2 * std).toFixed(2)));
        lower.push(parseFloat((ma - 2 * std).toFixed(2)));
    }
    return { mid: mid, upper: upper, lower: lower };
}

// ── ECharts 主题 ──
function getEchartsTheme() {
    var isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    return isDark ? 'dark' : null;
}

// 监听主题切换，刷新图表
var _origToggleTheme = window.toggleTheme;
window.toggleTheme = function() {
    if (_origToggleTheme) _origToggleTheme();
    setTimeout(function() {
        if (stockData) {
            renderMainChart();
            renderValuationCharts();
            renderIntrinsicValuation();
        }
    }, 100);
};

// ══════════════════════════════════════════════════════════════
// ── 单因子回测引擎 ──
// ══════════════════════════════════════════════════════════════

var backtestChart = null;

var FACTOR_INFO = {
    macd: {
        name: 'MACD 金叉死叉',
        buy: 'DIF 上穿 DEA（MACD 金叉）',
        sell: 'MACD 红柱开始缩短（多头动能减弱）',
        params: []
    },
    kdj: {
        name: 'KDJ 金叉死叉',
        buy: 'K 线上穿 D 线（KDJ 金叉）',
        sell: 'K 线下穿 D 线（KDJ 死叉）',
        params: []
    },
    cci: {
        name: 'CCI 超买超卖',
        buy: 'CCI 上穿 -100（从超卖区回升）',
        sell: 'CCI 下穿 +100（从超买区回落）',
        params: []
    },
    ma: {
        name: '均线金叉死叉',
        buy: '短期均线上穿长期均线（金叉）',
        sell: '短期均线下穿长期均线（死叉）',
        params: ['ma']
    },
    pe: {
        name: 'PE 百分位',
        buy: 'PE 滚动百分位低于买入阈值（低估）',
        sell: 'PE 滚动百分位高于卖出阈值（高估）',
        params: ['pct']
    },
    pb: {
        name: 'PB 百分位',
        buy: 'PB 滚动百分位低于买入阈值（低估）',
        sell: 'PB 滚动百分位高于卖出阈值（高估）',
        params: ['pct']
    },
    ps: {
        name: 'PS 百分位',
        buy: 'PS 滚动百分位低于买入阈值（低估）',
        sell: 'PS 滚动百分位高于卖出阈值（高估）',
        params: ['pct']
    },
    ma_opt: {
        name: '均线最优搜索',
        buy: '最优短期均线上穿最优长期均线（遍历所有组合自动寻找）',
        sell: '最优短期均线下穿最优长期均线',
        params: []
    },
    ai_opt: {
        name: 'AI 智能寻优',
        buy: 'AI 根据股票数据特征自动设计买入条件（调用 Qwen 模型）',
        sell: 'AI 自动设计卖出条件',
        params: []
    }
};

// ── 因子切换 ──
function onFactorChange() {
    var factor = document.getElementById('bt-factor').value;
    var info = FACTOR_INFO[factor];

    // 显示/隐藏参数
    var maParams = document.querySelectorAll('.bt-param-ma');
    var pctParams = document.querySelectorAll('.bt-param-pct');
    maParams.forEach(function(el) { el.style.display = info.params.indexOf('ma') >= 0 ? '' : 'none'; });
    pctParams.forEach(function(el) { el.style.display = info.params.indexOf('pct') >= 0 ? '' : 'none'; });

    // 更新信号说明
    var descEl = document.getElementById('bt-signal-desc');
    descEl.innerHTML =
        '<div class="signal-desc-title">' + info.name + ' — 信号定义</div>' +
        '<div class="signal-desc-row"><span class="signal-buy-tag">买入</span> ' + info.buy + '</div>' +
        '<div class="signal-desc-row"><span class="signal-sell-tag">卖出</span> ' + info.sell + '</div>' +
        '<div class="signal-desc-row" style="color:var(--text-secondary,#64748b);font-size:12px;">仓位：满仓(1)或空仓(0)，不做空。信号当天收盘后产生，<b>次日开盘价</b>执行交易。手续费在每次开仓/平仓时单边扣除。</div>';
}

// ── 信号生成器 ──
function generateSignals_MACD(closes) {
    var macd = calcMACD(closes);
    var signals = [];
    var pos = 0;
    for (var i = 0; i < closes.length; i++) {
        if (i > 0 && macd.dif[i] != null && macd.dea[i] != null && macd.dif[i - 1] != null) {
            // 买入: DIF上穿DEA（金叉）
            if (macd.dif[i] > macd.dea[i] && macd.dif[i - 1] <= macd.dea[i - 1]) pos = 1;
            // 卖出: 红柱缩短（MACD柱>0 但开始缩小）
            if (pos === 1 && macd.histogram[i] > 0 && macd.histogram[i - 1] > 0 && macd.histogram[i] < macd.histogram[i - 1]) pos = 0;
            // 也在死叉时卖出（兜底）
            if (macd.dif[i] < macd.dea[i] && macd.dif[i - 1] >= macd.dea[i - 1]) pos = 0;
        }
        signals.push(pos);
    }
    return signals;
}

function generateSignals_KDJ(closes, highs, lows) {
    var kdj = calcKDJ(closes, highs, lows);
    var signals = [];
    var pos = 0;
    for (var i = 0; i < closes.length; i++) {
        if (i > 0 && kdj.k[i] != null && kdj.d[i] != null && kdj.k[i - 1] != null) {
            if (kdj.k[i] > kdj.d[i] && kdj.k[i - 1] <= kdj.d[i - 1]) pos = 1;
            if (kdj.k[i] < kdj.d[i] && kdj.k[i - 1] >= kdj.d[i - 1]) pos = 0;
        }
        signals.push(pos);
    }
    return signals;
}

function generateSignals_CCI(closes, highs, lows) {
    var cci = calcCCI(closes, highs, lows, 14);
    var signals = [];
    var pos = 0;
    for (var i = 0; i < closes.length; i++) {
        if (i > 0 && cci[i] != null && cci[i - 1] != null) {
            if (cci[i] > -100 && cci[i - 1] <= -100) pos = 1;
            if (cci[i] < 100 && cci[i - 1] >= 100) pos = 0;
        }
        signals.push(pos);
    }
    return signals;
}

function generateSignals_MA(closes, shortP, longP) {
    var maShort = calcMA(closes, shortP);
    var maLong = calcMA(closes, longP);
    var signals = [];
    var pos = 0;
    for (var i = 0; i < closes.length; i++) {
        if (i > 0 && maShort[i] != null && maLong[i] != null && maShort[i - 1] != null && maLong[i - 1] != null) {
            if (maShort[i] > maLong[i] && maShort[i - 1] <= maLong[i - 1]) pos = 1;
            if (maShort[i] < maLong[i] && maShort[i - 1] >= maLong[i - 1]) pos = 0;
        }
        signals.push(pos);
    }
    return signals;
}

function generateSignals_Percentile(data, buyThresh, sellThresh, window) {
    var signals = [];
    var pos = 0;
    for (var i = 0; i < data.length; i++) {
        if (data[i] === null || i < window - 1) {
            signals.push(pos);
            continue;
        }
        // 计算滚动百分位
        var vals = [];
        for (var j = 0; j < window; j++) {
            if (data[i - j] !== null) vals.push(data[i - j]);
        }
        if (vals.length < window * 0.3) { signals.push(pos); continue; }
        var count = 0;
        for (var k = 0; k < vals.length; k++) { if (vals[k] <= data[i]) count++; }
        var pct = count / vals.length * 100;

        if (pct < buyThresh) pos = 1;
        if (pct > sellThresh) pos = 0;
        signals.push(pos);
    }
    return signals;
}

// ── 核心回测 ──
function runBacktest() {
    if (!stockData || !stockData.ohlc || stockData.ohlc.length < 30) {
        alert('请先查询股票数据（至少30个交易日）');
        return;
    }

    var factor = document.getElementById('bt-factor').value;
    var commission = parseFloat(document.getElementById('bt-commission').value) / 100 || 0;
    var dates = stockData.dates;
    var ohlc = stockData.ohlc;
    var closes = ohlc.map(function(d) { return d[3]; });
    var highs = ohlc.map(function(d) { return d[1]; });
    var lows = ohlc.map(function(d) { return d[2]; });
    var opens = ohlc.map(function(d) { return d[0]; });

    // 生成信号
    var signals;
    if (factor === 'macd') {
        signals = generateSignals_MACD(closes);
    } else if (factor === 'kdj') {
        signals = generateSignals_KDJ(closes, highs, lows);
    } else if (factor === 'cci') {
        signals = generateSignals_CCI(closes, highs, lows);
    } else if (factor === 'ma') {
        var shortP = parseInt(document.getElementById('bt-ma-short').value) || 5;
        var longP = parseInt(document.getElementById('bt-ma-long').value) || 20;
        if (shortP >= longP) { alert('短周期必须小于长周期'); return; }
        signals = generateSignals_MA(closes, shortP, longP);
    } else if (factor === 'ma_opt') {
        // 均线最优搜索：遍历所有组合
        var maOptResults = runMAOptSearch(closes, opens, dates, commission);
        renderMAOptTable(maOptResults);
        if (maOptResults.length > 0) {
            signals = generateSignals_MA(closes, maOptResults[0].shortP, maOptResults[0].longP);
        } else {
            alert('未找到有效均线组合'); return;
        }
    } else if (factor === 'ai_opt') {
        // AI 智能寻优：调用后端 AI 模型
        runAIStrategy(closes, highs, lows, opens, dates, commission);
        return; // 异步执行，直接返回
    } else if (factor === 'pe' || factor === 'pb' || factor === 'ps') {
        if (!stockData.basic_history || !stockData.basic_history[factor] || stockData.basic_history[factor].length === 0) {
            alert('缺少 ' + factor.toUpperCase() + ' 历史数据，无法进行百分位回测');
            return;
        }
        var buyThresh = parseFloat(document.getElementById('bt-pct-buy').value) || 30;
        var sellThresh = parseFloat(document.getElementById('bt-pct-sell').value) || 70;
        var window = parseInt(document.getElementById('bt-pct-window').value) || 252;

        // 将 basic_history 对齐到 K 线日期
        var bhDates = stockData.basic_history.dates;
        var bhValues = stockData.basic_history[factor];
        var bhMap = {};
        for (var i = 0; i < bhDates.length; i++) { bhMap[bhDates[i]] = bhValues[i]; }
        var alignedData = [];
        var lastVal = null;
        for (var i = 0; i < dates.length; i++) {
            if (bhMap[dates[i]] !== undefined && bhMap[dates[i]] !== null) {
                lastVal = bhMap[dates[i]];
            }
            alignedData.push(lastVal);
        }
        signals = generateSignals_Percentile(alignedData, buyThresh, sellThresh, window);
    }

    // 计算收益 — 信号在当天收盘后产生，次日开盘价执行
    // 持仓期间收益 = 当日收盘价/当日开盘价 的变化（日内）
    // 建仓日: 次日开盘买入，持仓收益从次日开盘到次日收盘
    // 平仓日: 次日开盘卖出
    var strategyReturns = [];
    var benchmarkReturns = [];
    var strategyNAV = [1];
    var benchmarkNAV = [1];

    // 实际持仓状态：信号延迟1天执行
    var actualPos = [];
    actualPos.push(0); // 第0天无法执行
    for (var i = 1; i < closes.length; i++) {
        actualPos.push(signals[i - 1]); // 昨天的信号今天执行
    }

    for (var i = 1; i < closes.length; i++) {
        var dailyRet = closes[i] / closes[i - 1] - 1;
        // 交易成本：仓位变化时扣除（基于今天开盘价执行）
        var cost = (actualPos[i] !== actualPos[i - 1]) ? commission : 0;
        var stratRet = actualPos[i] * dailyRet - cost;
        strategyReturns.push(stratRet);
        benchmarkReturns.push(dailyRet);
        strategyNAV.push(strategyNAV[strategyNAV.length - 1] * (1 + stratRet));
        benchmarkNAV.push(benchmarkNAV[benchmarkNAV.length - 1] * (1 + dailyRet));
    }

    // 提取交易记录（基于次日开盘价）
    var trades = [];
    var buySignals = []; // 用于图表标注
    var sellSignals = [];
    var openDate = null, openPrice = null, openIdx = null;
    for (var i = 1; i < actualPos.length; i++) {
        if (actualPos[i] === 1 && actualPos[i - 1] === 0) {
            // 今天开盘买入（昨天信号触发）
            openDate = dates[i]; openPrice = opens[i]; openIdx = i;
            buySignals.push({ dateIdx: i, price: opens[i] });
        }
        if (actualPos[i] === 0 && actualPos[i - 1] === 1 && openDate !== null) {
            // 今天开盘卖出
            sellSignals.push({ dateIdx: i, price: opens[i] });
            trades.push({
                buyDate: openDate, buyPrice: openPrice,
                sellDate: dates[i], sellPrice: opens[i],
                holdDays: i - openIdx,
                returnPct: ((opens[i] / openPrice - 1) * 100).toFixed(2)
            });
            openDate = null;
        }
    }
    if (openDate !== null) {
        var lastIdx = closes.length - 1;
        trades.push({
            buyDate: openDate, buyPrice: openPrice,
            sellDate: dates[lastIdx] + '(持仓中)', sellPrice: closes[lastIdx],
            holdDays: lastIdx - openIdx,
            returnPct: ((closes[lastIdx] / openPrice - 1) * 100).toFixed(2)
        });
    }

    // 计算绩效指标
    var metrics = calcBacktestMetrics(strategyReturns, benchmarkReturns, strategyNAV, benchmarkNAV, trades);

    // 渲染结果
    renderBacktestStats(metrics);
    renderBacktestChart(dates, strategyNAV, benchmarkNAV, buySignals, sellSignals);
    renderTradesTable(trades);
    // 非均线最优搜索时隐藏搜索结果表
    if (factor !== 'ma_opt') {
        document.getElementById('bt-ma-opt-section').style.display = 'none';
    }
}

// ── 绩效指标计算 ──
function calcBacktestMetrics(stratReturns, benchReturns, stratNAV, benchNAV, trades) {
    var n = stratReturns.length;
    if (n === 0) return {};

    var finalStrat = stratNAV[stratNAV.length - 1];
    var finalBench = benchNAV[benchNAV.length - 1];
    var years = n / 252;

    // 年化收益
    var annualReturn = Math.pow(finalStrat, 1 / years) - 1;
    var annualBench = Math.pow(finalBench, 1 / years) - 1;

    // 年化波动率
    var mean = 0;
    for (var i = 0; i < n; i++) mean += stratReturns[i];
    mean /= n;
    var variance = 0;
    for (var i = 0; i < n; i++) variance += Math.pow(stratReturns[i] - mean, 2);
    var volatility = Math.sqrt(variance / n) * Math.sqrt(252);

    // 夏普比率
    var sharpe = volatility > 0 ? annualReturn / volatility : 0;

    // 最大回撤
    var peak = stratNAV[0];
    var maxDrawdown = 0;
    for (var i = 1; i < stratNAV.length; i++) {
        if (stratNAV[i] > peak) peak = stratNAV[i];
        var dd = (peak - stratNAV[i]) / peak;
        if (dd > maxDrawdown) maxDrawdown = dd;
    }

    // 胜率
    var winCount = 0;
    for (var i = 0; i < trades.length; i++) {
        if (parseFloat(trades[i].returnPct) > 0) winCount++;
    }
    var winRate = trades.length > 0 ? winCount / trades.length : 0;

    // 平均持仓天数
    var totalDays = 0;
    for (var i = 0; i < trades.length; i++) totalDays += trades[i].holdDays;
    var avgHoldDays = trades.length > 0 ? (totalDays / trades.length).toFixed(0) : 0;

    return {
        cumReturn: ((finalStrat - 1) * 100).toFixed(2),
        cumBench: ((finalBench - 1) * 100).toFixed(2),
        annualReturn: (annualReturn * 100).toFixed(2),
        annualBench: (annualBench * 100).toFixed(2),
        volatility: (volatility * 100).toFixed(2),
        sharpe: sharpe.toFixed(2),
        maxDrawdown: (maxDrawdown * 100).toFixed(2),
        winRate: (winRate * 100).toFixed(1),
        totalTrades: trades.length,
        avgHoldDays: avgHoldDays
    };
}

// ── 渲染绩效统计 ──
function renderBacktestStats(m) {
    var el = document.getElementById('bt-stats');
    el.style.display = 'grid';
    el.innerHTML =
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">累积收益</div>' +
            '<div class="bt-stat-value ' + (parseFloat(m.cumReturn) >= 0 ? 'positive' : 'negative') + '">' + m.cumReturn + '%</div>' +
            '<div class="bt-stat-sub">基准: ' + m.cumBench + '%</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">年化收益</div>' +
            '<div class="bt-stat-value ' + (parseFloat(m.annualReturn) >= 0 ? 'positive' : 'negative') + '">' + m.annualReturn + '%</div>' +
            '<div class="bt-stat-sub">基准: ' + m.annualBench + '%</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">夏普比率</div>' +
            '<div class="bt-stat-value">' + m.sharpe + '</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">最大回撤</div>' +
            '<div class="bt-stat-value negative">-' + m.maxDrawdown + '%</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">年化波动率</div>' +
            '<div class="bt-stat-value">' + m.volatility + '%</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">胜率</div>' +
            '<div class="bt-stat-value">' + m.winRate + '%</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">总交易次数</div>' +
            '<div class="bt-stat-value">' + m.totalTrades + '</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">平均持仓天数</div>' +
            '<div class="bt-stat-value">' + m.avgHoldDays + '</div>' +
        '</div>';
}

// ── 渲染净值曲线 ──
function renderBacktestChart(dates, stratNAV, benchNAV, buySignals, sellSignals) {
    var container = document.getElementById('bt-chart');
    container.style.display = 'block';
    if (backtestChart) backtestChart.dispose();
    backtestChart = echarts.init(container, getEchartsTheme());

    var chartDates = dates.slice(0, stratNAV.length);
    var stratData = stratNAV.map(function(v) { return parseFloat(v.toFixed(4)); });
    var benchData = benchNAV.map(function(v) { return parseFloat(v.toFixed(4)); });

    // 构建买卖信号标注
    var buyMarks = (buySignals || []).map(function(s) {
        return {
            coord: [s.dateIdx, stratData[s.dateIdx] || 1],
            symbol: 'triangle', symbolSize: 12, symbolRotate: 0,
            itemStyle: { color: '#ef5350' },
            label: { show: false }
        };
    });
    var sellMarks = (sellSignals || []).map(function(s) {
        return {
            coord: [s.dateIdx, stratData[s.dateIdx] || 1],
            symbol: 'triangle', symbolSize: 12, symbolRotate: 180,
            itemStyle: { color: '#26a69a' },
            label: { show: false }
        };
    });

    var option = {
        animation: false,
        tooltip: { trigger: 'axis' },
        legend: { data: ['策略净值', '基准净值(买入持有)'], top: 5 },
        grid: { left: '8%', right: '4%', top: '14%', bottom: '15%' },
        xAxis: { type: 'category', data: chartDates, axisLabel: {fontSize: 11} },
        yAxis: { scale: true, splitLine: {lineStyle: {opacity: 0.3}} },
        dataZoom: [
            { type: 'inside', start: 0, end: 100 },
            { type: 'slider', bottom: 2, height: 18 }
        ],
        series: [
            {
                name: '策略净值', type: 'line', data: stratData,
                symbol: 'none', lineStyle: {width: 2, color: '#1a56db'},
                areaStyle: { color: 'rgba(26,86,219,0.06)' },
                markPoint: { data: buyMarks.concat(sellMarks) }
            },
            {
                name: '基准净值(买入持有)', type: 'line', data: benchData,
                symbol: 'none', lineStyle: {width: 1.5, color: '#9b59b6', type: 'dashed'}
            }
        ]
    };

    backtestChart.setOption(option);
    window.addEventListener('resize', function() { if (backtestChart) backtestChart.resize(); });
}

// ── AI 智能寻优策略 ──

// 快速回测：给定信号数组，返回绩效指标
function quickBacktest(signals, closes, commission) {
    var actualPos = [0];
    for (var i = 1; i < closes.length; i++) actualPos.push(signals[i - 1]);

    var nav = 1, peak = 1, maxDD = 0;
    var returns = [];
    var tradeCount = 0;

    for (var i = 1; i < closes.length; i++) {
        var ret = closes[i] / closes[i - 1] - 1;
        var cost = (actualPos[i] !== actualPos[i - 1]) ? commission : 0;
        if (actualPos[i] !== actualPos[i - 1]) tradeCount++;
        var stratRet = actualPos[i] * ret - cost;
        returns.push(stratRet);
        nav *= (1 + stratRet);
        if (nav > peak) peak = nav;
        var dd = (peak - nav) / peak;
        if (dd > maxDD) maxDD = dd;
    }

    var years = returns.length / 252;
    var annRet = years > 0 ? Math.pow(Math.max(nav, 0.001), 1 / years) - 1 : 0;
    var mean = 0;
    for (var i = 0; i < returns.length; i++) mean += returns[i];
    mean /= (returns.length || 1);
    var variance = 0;
    for (var i = 0; i < returns.length; i++) variance += Math.pow(returns[i] - mean, 2);
    var vol = Math.sqrt(variance / (returns.length || 1)) * Math.sqrt(252);
    var sharpe = vol > 0 ? annRet / vol : 0;

    return {
        cumReturn: ((nav - 1) * 100).toFixed(2),
        annualReturn: (annRet * 100).toFixed(2),
        sharpe: sharpe.toFixed(3),
        maxDrawdown: (maxDD * 100).toFixed(2),
        trades: Math.floor(tradeCount / 2)
    };
}

// 对齐估值数据到K线日期
function alignBasicHistory(factorKey, dates) {
    var bh = stockData ? stockData.basic_history : null;
    if (!bh || !bh[factorKey] || !bh.dates) return null;
    var bhMap = {};
    for (var j = 0; j < bh.dates.length; j++) { bhMap[bh.dates[j]] = bh[factorKey][j]; }
    var aligned = [];
    var lastVal = null;
    for (var j = 0; j < dates.length; j++) {
        if (bhMap[dates[j]] !== undefined && bhMap[dates[j]] !== null) {
            lastVal = bhMap[dates[j]];
        }
        aligned.push(lastVal);
    }
    return aligned;
}

// 暴力搜索所有策略并返回绩效
function runAllStrategies(closes, highs, lows, dates, commission) {
    var strategies = [];

    // 1. 技术因子
    var techFactors = [
        { id: 'macd', name: 'MACD金叉/红柱缩短', fn: function() { return generateSignals_MACD(closes); } },
        { id: 'kdj', name: 'KDJ金叉/死叉', fn: function() { return generateSignals_KDJ(closes, highs, lows); } },
        { id: 'cci', name: 'CCI超买超卖(-100/+100)', fn: function() { return generateSignals_CCI(closes, highs, lows); } }
    ];

    // 2. 均线组合
    var maCombos = [
        [5, 20], [5, 30], [5, 60], [10, 20], [10, 30], [10, 60],
        [20, 60], [20, 120], [30, 60], [30, 120], [60, 120]
    ];
    for (var mi = 0; mi < maCombos.length; mi++) {
        var sp = maCombos[mi][0], lp = maCombos[mi][1];
        (function(sp, lp) {
            techFactors.push({
                id: 'ma_' + sp + '_' + lp,
                name: 'MA' + sp + '/MA' + lp + '金叉死叉',
                fn: function() { return generateSignals_MA(closes, sp, lp); }
            });
        })(sp, lp);
    }

    // 3. 估值百分位（仅日线有 basic_history 时）
    var valFactors = ['pe', 'pb', 'ps'];
    var valThresholds = [[20, 80], [30, 70]];
    for (var vi = 0; vi < valFactors.length; vi++) {
        var vKey = valFactors[vi];
        var aligned = alignBasicHistory(vKey, dates);
        if (!aligned) continue;
        for (var ti = 0; ti < valThresholds.length; ti++) {
            var buyTh = valThresholds[ti][0], sellTh = valThresholds[ti][1];
            (function(vKey, aligned, buyTh, sellTh) {
                techFactors.push({
                    id: vKey + '_pct_' + buyTh + '_' + sellTh,
                    name: vKey.toUpperCase() + '百分位<' + buyTh + '买/' + '>' + sellTh + '卖',
                    fn: function() { return generateSignals_Percentile(aligned, buyTh, sellTh, 252); }
                });
            })(vKey, aligned, buyTh, sellTh);
        }
    }

    // 计算每个策略的绩效
    for (var i = 0; i < techFactors.length; i++) {
        var f = techFactors[i];
        var signals = f.fn();
        var perf = quickBacktest(signals, closes, commission);
        strategies.push({
            id: f.id,
            name: f.name,
            signals: signals,
            cumReturn: perf.cumReturn,
            annualReturn: perf.annualReturn,
            sharpe: perf.sharpe,
            maxDrawdown: perf.maxDrawdown,
            trades: perf.trades
        });
    }

    // 按夏普排序
    strategies.sort(function(a, b) { return parseFloat(b.sharpe) - parseFloat(a.sharpe); });
    return strategies;
}

// 组合多个策略的信号
function combineSignals(signalArrays, buyLogic, sellLogic) {
    if (signalArrays.length === 0) return [];
    var n = signalArrays[0].length;

    // 提取各策略的买入/卖出事件
    // 每个策略的 signal=1 表示持仓，0 表示空仓
    // 买入事件：signal 从 0 变为 1
    // 卖出事件：signal 从 1 变为 0
    var signals = [];
    var pos = 0;

    for (var i = 0; i < n; i++) {
        if (i === 0) { signals.push(0); continue; }

        // 计算各策略在此刻的买入/卖出信号
        var buyVotes = 0, sellVotes = 0, totalStrategies = signalArrays.length;

        for (var s = 0; s < totalStrategies; s++) {
            // 该策略此刻认为应该持仓
            if (signalArrays[s][i] === 1) buyVotes++;
            // 该策略此刻认为应该空仓
            if (signalArrays[s][i] === 0) sellVotes++;
        }

        if (pos === 0) {
            // 检查买入条件
            var shouldBuy = buyLogic === 'all'
                ? (buyVotes === totalStrategies)
                : (buyVotes > 0);
            if (shouldBuy) pos = 1;
        } else {
            // 检查卖出条件
            var shouldSell = sellLogic === 'all'
                ? (sellVotes === totalStrategies)
                : (sellVotes > 0);
            if (shouldSell) pos = 0;
        }

        signals.push(pos);
    }
    return signals;
}

function runAIStrategy(closes, highs, lows, opens, dates, commission) {
    var descEl = document.getElementById('bt-signal-desc');
    descEl.innerHTML = '<div class="signal-desc-title">AI 智能寻优 — 第1步：暴力搜索全部策略...</div>' +
        '<div class="signal-desc-row" style="color:var(--text-secondary,#64748b);">正在计算所有单因子策略的回测绩效...</div>';

    // Step 1: 暴力搜索全部策略
    var allStrategies = runAllStrategies(closes, highs, lows, dates, commission);

    if (allStrategies.length === 0) {
        descEl.innerHTML = '<div class="signal-desc-title">AI 寻优失败</div>' +
            '<div class="signal-desc-row" style="color:#ef5350;">无法计算任何策略的绩效。</div>';
        return;
    }

    // Step 2: 构建增强摘要发送给 AI
    var n = closes.length;
    var lastClose = closes[n - 1];
    var maxClose = Math.max.apply(null, closes);
    var minClose = Math.min.apply(null, closes);

    var basicInfo = '';
    if (stockData.basic) {
        var b = stockData.basic;
        basicInfo = 'PE=' + (b.pe || 'N/A') + ', PB=' + (b.pb || 'N/A') + ', PS=' + (b.ps || 'N/A');
    }

    var stockSummary = '股票: ' + document.getElementById('stock-code').value +
        ', 区间: ' + dates[0] + '~' + dates[n - 1] + ' (' + n + '日)' +
        ', 最新价: ' + lastClose.toFixed(2) + ', 最高: ' + maxClose.toFixed(2) + ', 最低: ' + minClose.toFixed(2) +
        (basicInfo ? ', ' + basicInfo : '');

    // 构建策略绩效表
    var strategyTable = '';
    var strategyIds = [];
    for (var i = 0; i < allStrategies.length; i++) {
        var s = allStrategies[i];
        strategyIds.push(s.id);
        strategyTable += s.id + ' | ' + s.name + ' | 累积' + s.cumReturn + '% | 年化' + s.annualReturn + '% | 夏普' + s.sharpe + ' | 回撤' + s.maxDrawdown + '% | ' + s.trades + '笔\n';
    }

    descEl.innerHTML = '<div class="signal-desc-title">AI 智能寻优 — 第2步：AI 分析策略组合...</div>' +
        '<div class="signal-desc-row" style="color:var(--text-secondary,#64748b);">已计算 ' + allStrategies.length + ' 个策略，正在调用 AI 选择最优组合...</div>';

    // Step 3: 调用 AI
    fetch('/api/stock/ai_strategy', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            stock_summary: stockSummary,
            strategy_results: strategyTable,
            strategy_ids: strategyIds
        })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        var reply = data.reply;

        // 尝试从回复中提取 JSON
        var jsonMatch = reply.match(/\{[\s\S]*\}/);
        var strategy = null;
        if (jsonMatch) {
            try { strategy = JSON.parse(jsonMatch[0]); } catch(e) { strategy = null; }
        }

        // Step 4: 解析 AI 策略并执行
        var selectedSignals = [];
        var selectedNames = [];
        var buyLogic = 'any', sellLogic = 'any';
        var usedFallback = false;

        if (strategy && strategy.buy_strategies && strategy.buy_strategies.length > 0) {
            buyLogic = strategy.buy_logic || 'any';
            sellLogic = strategy.sell_logic || 'any';

            // 查找 AI 选择的策略
            var buyStrats = strategy.buy_strategies;
            for (var i = 0; i < buyStrats.length; i++) {
                for (var j = 0; j < allStrategies.length; j++) {
                    if (allStrategies[j].id === buyStrats[i]) {
                        selectedSignals.push(allStrategies[j].signals);
                        selectedNames.push(allStrategies[j].name);
                        break;
                    }
                }
            }
        }

        // 如果 AI 返回无法解析或没有有效策略，用最优单因子保底
        if (selectedSignals.length === 0) {
            usedFallback = true;
            selectedSignals.push(allStrategies[0].signals);
            selectedNames.push(allStrategies[0].name);
            buyLogic = 'any';
            sellLogic = 'any';
        }

        // 组合信号
        var combinedSignals;
        if (selectedSignals.length === 1) {
            combinedSignals = selectedSignals[0];
        } else {
            combinedSignals = combineSignals(selectedSignals, buyLogic, sellLogic);
        }

        // 如果组合后信号全为 0，回退到最优单因子
        var hasSignal = false;
        for (var i = 0; i < combinedSignals.length; i++) {
            if (combinedSignals[i] === 1) { hasSignal = true; break; }
        }
        if (!hasSignal && !usedFallback) {
            usedFallback = true;
            combinedSignals = allStrategies[0].signals;
            selectedNames = [allStrategies[0].name];
            buyLogic = 'any';
            sellLogic = 'any';
        }

        // 显示策略说明
        var strategyName = (strategy && strategy.strategy_name) ? strategy.strategy_name : '最优策略';
        var strategyDesc = (strategy && strategy.description) ? strategy.description : '';
        var logicText = selectedSignals.length > 1
            ? '买入逻辑: ' + (buyLogic === 'all' ? '全部满足' : '任一满足') + ', 卖出逻辑: ' + (sellLogic === 'all' ? '全部满足' : '任一满足')
            : '单因子策略';

        descEl.innerHTML =
            '<div class="signal-desc-title">AI 策略: ' + strategyName + (usedFallback ? ' (保底: 夏普最优单因子)' : '') + '</div>' +
            '<div class="signal-desc-row">' + strategyDesc + '</div>' +
            '<div class="signal-desc-row"><span class="signal-buy-tag">选用因子</span> ' + selectedNames.join(' + ') + '</div>' +
            '<div class="signal-desc-row" style="color:var(--text-secondary,#64748b);font-size:12px;">' + logicText + '。次日开盘价执行。共搜索 ' + allStrategies.length + ' 个策略。</div>';

        // 执行回测
        executeBacktestWithSignals(combinedSignals, closes, opens, dates, commission);
    })
    .catch(function(err) {
        // AI 请求失败，直接用最优单因子保底
        var best = allStrategies[0];
        descEl.innerHTML =
            '<div class="signal-desc-title">AI 请求失败，使用最优策略保底: ' + best.name + '</div>' +
            '<div class="signal-desc-row" style="color:var(--text-secondary,#64748b);">夏普比率: ' + best.sharpe + ', 年化: ' + best.annualReturn + '%</div>';
        executeBacktestWithSignals(best.signals, closes, opens, dates, commission);
    });
}

function executeBacktestWithSignals(signals, closes, opens, dates, commission) {
    var strategyReturns = [];
    var benchmarkReturns = [];
    var strategyNAV = [1];
    var benchmarkNAV = [1];
    var actualPos = [0];
    for (var i = 1; i < closes.length; i++) actualPos.push(signals[i - 1]);

    var buySignals = [];
    var sellSignals = [];

    for (var i = 1; i < closes.length; i++) {
        var dailyRet = closes[i] / closes[i - 1] - 1;
        var cost = (actualPos[i] !== actualPos[i - 1]) ? commission : 0;
        var stratRet = actualPos[i] * dailyRet - cost;
        strategyReturns.push(stratRet);
        benchmarkReturns.push(dailyRet);
        strategyNAV.push(strategyNAV[strategyNAV.length - 1] * (1 + stratRet));
        benchmarkNAV.push(benchmarkNAV[benchmarkNAV.length - 1] * (1 + dailyRet));

        if (actualPos[i] === 1 && actualPos[i - 1] === 0) buySignals.push({ dateIdx: i, price: opens[i] });
        if (actualPos[i] === 0 && actualPos[i - 1] === 1) sellSignals.push({ dateIdx: i, price: opens[i] });
    }

    var trades = [];
    var openDate = null, openPrice = null, openIdx = null;
    for (var i = 1; i < actualPos.length; i++) {
        if (actualPos[i] === 1 && actualPos[i - 1] === 0) {
            openDate = dates[i]; openPrice = opens[i]; openIdx = i;
        }
        if (actualPos[i] === 0 && actualPos[i - 1] === 1 && openDate !== null) {
            trades.push({
                buyDate: openDate, buyPrice: openPrice,
                sellDate: dates[i], sellPrice: opens[i],
                holdDays: i - openIdx,
                returnPct: ((opens[i] / openPrice - 1) * 100).toFixed(2)
            });
            openDate = null;
        }
    }
    if (openDate !== null) {
        var lastIdx = closes.length - 1;
        trades.push({
            buyDate: openDate, buyPrice: openPrice,
            sellDate: dates[lastIdx] + '(持仓中)', sellPrice: closes[lastIdx],
            holdDays: lastIdx - openIdx,
            returnPct: ((closes[lastIdx] / openPrice - 1) * 100).toFixed(2)
        });
    }

    var metrics = calcBacktestMetrics(strategyReturns, benchmarkReturns, strategyNAV, benchmarkNAV, trades);
    renderBacktestStats(metrics);
    renderBacktestChart(dates, strategyNAV, benchmarkNAV, buySignals, sellSignals);
    renderTradesTable(trades);
    document.getElementById('bt-ma-opt-section').style.display = 'none';
}

// ── 均线最优搜索 ──
function runMAOptSearch(closes, opens, dates, commission) {
    var shortRange = [3, 5, 7, 10, 13, 15, 20, 25, 30];
    var longRange = [10, 15, 20, 25, 30, 40, 50, 60, 80, 100, 120];
    var results = [];

    for (var si = 0; si < shortRange.length; si++) {
        for (var li = 0; li < longRange.length; li++) {
            var sp = shortRange[si];
            var lp = longRange[li];
            if (sp >= lp) continue;

            var signals = generateSignals_MA(closes, sp, lp);

            // 快速计算绩效（简化版）
            var actualPos = [0];
            for (var i = 1; i < closes.length; i++) actualPos.push(signals[i - 1]);

            var nav = 1;
            var peak = 1;
            var maxDD = 0;
            var returns = [];
            var tradeCount = 0;

            for (var i = 1; i < closes.length; i++) {
                var ret = closes[i] / closes[i - 1] - 1;
                var cost = (actualPos[i] !== actualPos[i - 1]) ? commission : 0;
                if (actualPos[i] !== actualPos[i - 1]) tradeCount++;
                var stratRet = actualPos[i] * ret - cost;
                returns.push(stratRet);
                nav *= (1 + stratRet);
                if (nav > peak) peak = nav;
                var dd = (peak - nav) / peak;
                if (dd > maxDD) maxDD = dd;
            }

            var years = returns.length / 252;
            var annRet = years > 0 ? Math.pow(nav, 1 / years) - 1 : 0;
            var mean = 0;
            for (var i = 0; i < returns.length; i++) mean += returns[i];
            mean /= returns.length;
            var variance = 0;
            for (var i = 0; i < returns.length; i++) variance += Math.pow(returns[i] - mean, 2);
            var vol = Math.sqrt(variance / returns.length) * Math.sqrt(252);
            var sharpe = vol > 0 ? annRet / vol : 0;

            results.push({
                shortP: sp, longP: lp,
                cumReturn: ((nav - 1) * 100).toFixed(2),
                annualReturn: (annRet * 100).toFixed(2),
                sharpe: sharpe.toFixed(2),
                maxDrawdown: (maxDD * 100).toFixed(2),
                trades: Math.floor(tradeCount / 2)
            });
        }
    }

    // 按夏普比率降序排序
    results.sort(function(a, b) { return parseFloat(b.sharpe) - parseFloat(a.sharpe); });
    return results.slice(0, 10);
}

function renderMAOptTable(results) {
    var section = document.getElementById('bt-ma-opt-section');
    var table = document.getElementById('bt-ma-opt-table');
    if (results.length === 0) { section.style.display = 'none'; return; }
    section.style.display = 'block';

    var html = '<thead><tr><th>排名</th><th>短周期</th><th>长周期</th><th>累积收益</th><th>年化收益</th><th>夏普比率</th><th>最大回撤</th><th>交易次数</th></tr></thead><tbody>';
    for (var i = 0; i < results.length; i++) {
        var r = results[i];
        var retClass = parseFloat(r.cumReturn) >= 0 ? 'positive' : 'negative';
        html += '<tr' + (i === 0 ? ' style="background:rgba(26,86,219,0.06);font-weight:600;"' : '') + '>' +
            '<td>' + (i + 1) + '</td>' +
            '<td>MA' + r.shortP + '</td><td>MA' + r.longP + '</td>' +
            '<td class="' + retClass + '">' + r.cumReturn + '%</td>' +
            '<td class="' + retClass + '">' + r.annualReturn + '%</td>' +
            '<td>' + r.sharpe + '</td>' +
            '<td class="negative">-' + r.maxDrawdown + '%</td>' +
            '<td>' + r.trades + '</td></tr>';
    }
    html += '</tbody>';
    table.innerHTML = html;
}

// ── 渲染交易明细表 ──
function renderTradesTable(trades) {
    var section = document.getElementById('bt-trades-section');
    var table = document.getElementById('bt-trades-table');
    if (trades.length === 0) {
        section.style.display = 'none';
        return;
    }
    section.style.display = 'block';

    var html = '<thead><tr><th>序号</th><th>买入日期</th><th>买入价</th><th>卖出日期</th><th>卖出价</th><th>持仓天数</th><th>收益率</th></tr></thead><tbody>';
    for (var i = 0; i < trades.length; i++) {
        var t = trades[i];
        var retClass = parseFloat(t.returnPct) >= 0 ? 'positive' : 'negative';
        html += '<tr>' +
            '<td>' + (i + 1) + '</td>' +
            '<td>' + t.buyDate + '</td>' +
            '<td>' + t.buyPrice.toFixed(2) + '</td>' +
            '<td>' + t.sellDate + '</td>' +
            '<td>' + t.sellPrice.toFixed(2) + '</td>' +
            '<td>' + t.holdDays + '</td>' +
            '<td class="' + retClass + '">' + (parseFloat(t.returnPct) >= 0 ? '+' : '') + t.returnPct + '%</td>' +
            '</tr>';
    }
    html += '</tbody>';
    table.innerHTML = html;
}

// ═══════════════ 筹码分布与胜率分析 ═══════════════

function loadCyqData() {
    var tsCode = document.getElementById('stock-code').value.trim();
    var startDate = document.getElementById('stock-start-date').value.replace(/-/g, '');
    if (!tsCode) return;

    var btn = document.getElementById('btn-cyq-load');
    btn.disabled = true;
    btn.textContent = '加载中...';

    fetchCyq(tsCode, startDate)
    .then(function(data) {
        cyqData = data;
        btn.disabled = false;
        btn.textContent = '加载筹码数据';

        if (!cyqData.dates || cyqData.dates.length === 0) {
            setCyqDataStatus('warning', '未获取到筹码数据（可能是接口积分不足或网络波动），请稍后重试。');
            alert('未获取到筹码数据，请确认积分是否足够或股票代码是否正确');
            return;
        }

        setCyqDataStatus('ok');
        renderCyqDistChart();
        renderCyqTimelineChart();
        document.getElementById('cyq-backtest-section').style.display = 'block';
    })
    .catch(function(err) {
        btn.disabled = false;
        btn.textContent = '加载筹码数据';
        setCyqDataStatus('error', '筹码数据加载失败：' + String(err || 'unknown'));
        alert('筹码数据加载失败: ' + err);
    });
}

// ── 横向筹码分布图（最新一天） ──
function renderCyqDistChart() {
    var container = document.getElementById('cyq-dist-chart');
    container.style.display = 'block';
    if (cyqDistChart) cyqDistChart.dispose();
    cyqDistChart = echarts.init(container, getEchartsTheme());

    var n = cyqData.dates.length;
    var latest = {
        date: cyqData.dates[n - 1],
        cost_5: cyqData.cost_5pct[n - 1],
        cost_15: cyqData.cost_15pct[n - 1],
        cost_50: cyqData.cost_50pct[n - 1],
        cost_85: cyqData.cost_85pct[n - 1],
        cost_95: cyqData.cost_95pct[n - 1],
        avg: cyqData.weight_avg[n - 1],
        wr: cyqData.winner_rate[n - 1]
    };
    var currentPrice = cyqData.current_price;

    // 构建价格区间（基于 cyq_perf 分位成本）：
    // 注意：cyq_perf 仅提供分位点，不是逐价位明细，因此这里用“相对密度推断”展示形态。
    var zones = [
        { label: '0~5%分位', low: null, high: latest.cost_5, pct: 5 },
        { label: '5~15%分位', low: latest.cost_5, high: latest.cost_15, pct: 10 },
        { label: '15~50%分位', low: latest.cost_15, high: latest.cost_50, pct: 35 },
        { label: '50~85%分位', low: latest.cost_50, high: latest.cost_85, pct: 35 },
        { label: '85~95%分位', low: latest.cost_85, high: latest.cost_95, pct: 10 },
        { label: '95~100%分位', low: latest.cost_95, high: null, pct: 5 }
    ];

    // 计算区间宽度与“相对密度”：密度 = 区间质量 / 区间宽度
    var eps = 1e-6;
    var widths = new Array(zones.length).fill(0);
    var masses = zones.map(function(z) { return Number(z.pct || 0); });
    for (var wi = 0; wi < zones.length; wi++) {
        var zw = zones[wi];
        if (zw.low !== null && zw.high !== null) {
            widths[wi] = Math.max(Number(zw.high) - Number(zw.low), eps);
        }
    }
    var fallbackWidth = 0;
    for (var fw = 0; fw < widths.length; fw++) {
        if (widths[fw] > eps) { fallbackWidth = widths[fw]; break; }
    }
    if (!(fallbackWidth > eps)) fallbackWidth = 0.1;
    // 两端尾部用邻近区间宽度近似
    if (!(widths[0] > eps)) widths[0] = widths[1] > eps ? widths[1] : fallbackWidth;
    if (!(widths[widths.length - 1] > eps)) {
        var li = widths.length - 2;
        widths[widths.length - 1] = widths[li] > eps ? widths[li] : fallbackWidth;
    }
    // 兜底
    for (var wj = 0; wj < widths.length; wj++) {
        if (!(widths[wj] > eps)) widths[wj] = fallbackWidth;
    }

    var rawDensity = masses.map(function(m, i) { return m / Math.max(widths[i], eps); });
    var densitySum = rawDensity.reduce(function(a, b) { return a + b; }, 0);
    var densityPct = rawDensity.map(function(d) {
        if (!(densitySum > eps)) return 0;
        return d * 100 / densitySum;
    });

    var categories = [];
    var values = [];         // 相对密度占比
    var massValues = [];     // 原始分位质量（固定 5/10/35/35/10/5）
    var colors = [];

    for (var i = 0; i < zones.length; i++) {
        var z = zones[i];
        var priceLabel = '';
        if (z.low !== null && z.high !== null) {
            priceLabel = z.low.toFixed(2) + ' ~ ' + z.high.toFixed(2);
        } else if (z.low === null) {
            priceLabel = '< ' + z.high.toFixed(2);
        } else {
            priceLabel = '> ' + z.low.toFixed(2);
        }
        categories.push(priceLabel + '\n' + z.label);
        values.push(Number(densityPct[i] || 0).toFixed(2));
        massValues.push(Number(masses[i] || 0).toFixed(2));

        // 判断颜色：当前价以上为套牢盘(绿)，以下为获利盘(红)
        var midPrice = (z.low !== null && z.high !== null) ? (z.low + z.high) / 2 :
                       (z.low !== null ? z.low : z.high);
        if (currentPrice !== null && midPrice <= currentPrice) {
            colors.push('#ef5350'); // 红色-获利盘
        } else {
            colors.push('#26a69a'); // 绿色-套牢盘
        }
    }

    var option = {
        animation: false,
        title: {
            text: '筹码分布（分位推断，' + latest.date + '）',
            subtext: '当前价: ' + (currentPrice ? currentPrice.toFixed(2) : 'N/A') +
                     '  |  加权成本: ' + latest.avg.toFixed(2) +
                     '  |  胜率: ' + latest.wr.toFixed(2) + '%' +
                     '  |  说明: 由分位成本推断相对密度',
            left: 'center',
            textStyle: { fontSize: 16 },
            subtextStyle: { fontSize: 13, color: '#555' }
        },
        tooltip: {
            trigger: 'axis',
            axisPointer: { type: 'shadow' },
            formatter: function(params) {
                var p = params[0];
                var idx = p.dataIndex;
                var w = widths[idx] || 0;
                var m = massValues[idx] || 0;
                return p.name.replace('\n', '<br/>') +
                    '<br/>相对密度占比: <b>' + Number(p.value || 0).toFixed(2) + '%</b>' +
                    '<br/>分位质量: ' + Number(m).toFixed(2) + '%' +
                    '<br/>区间宽度: ' + Number(w).toFixed(2);
            }
        },
        grid: { left: '18%', right: '8%', top: '18%', bottom: '8%' },
        xAxis: {
            type: 'value',
            name: '相对筹码密度(%)',
            axisLabel: { formatter: '{value}%' }
        },
        yAxis: {
            type: 'category',
            data: categories,
            axisLabel: { fontSize: 11, interval: 0 }
        },
        series: [{
            type: 'bar',
            data: values.map(function(v, idx) {
                return {
                    value: v,
                    itemStyle: { color: colors[idx], borderRadius: [0, 4, 4, 0] }
                };
            }),
            barWidth: '55%',
            label: {
                show: true,
                position: 'right',
                formatter: function(p) {
                    return Number(p.value || 0).toFixed(2) + '%';
                },
                fontSize: 12
            }
        }]
    };

    // 标记线：当前价和加权平均成本
    if (currentPrice !== null) {
        // 在Y轴上找到对应位置（通过价格映射到分位区间）
        // 因为是category Y轴，用markLine在图上标注
    }

    cyqDistChart.setOption(option);
    window.addEventListener('resize', function() { if (cyqDistChart) cyqDistChart.resize(); });
}

// ── 时序图：三子图（分位线 / 股价与成本 / 胜率） ──
function renderCyqTimelineChart() {
    var container = document.getElementById('cyq-timeline-chart');
    container.style.display = 'block';
    if (cyqTimelineChart) cyqTimelineChart.dispose();
    cyqTimelineChart = echarts.init(container, getEchartsTheme());

    var dates = cyqData.dates;

    // 获取收盘价序列（与筹码日期对齐）
    var closePrices = [];
    if (stockData && stockData.dates && stockData.ohlc) {
        var closeMap = {};
        for (var i = 0; i < stockData.dates.length; i++) {
            closeMap[stockData.dates[i]] = stockData.ohlc[i][3];
        }
        for (var i = 0; i < dates.length; i++) {
            closePrices.push(closeMap[dates[i]] || null);
        }
    }

    var option = {
        animation: false,
        title: [
            { text: '筹码成本分位走势', left: 'center', top: 0, textStyle: { fontSize: 14 } },
            { text: '股价与加权成本', left: 'center', top: '36%', textStyle: { fontSize: 14 } },
            { text: '胜率走势', left: 'center', top: '68%', textStyle: { fontSize: 14 } }
        ],
        tooltip: {
            trigger: 'axis',
            formatter: function(params) {
                if (!params || params.length === 0) return '';
                var tip = params[0].axisValue + '<br/>';
                for (var i = 0; i < params.length; i++) {
                    var p = params[i];
                    if (p.value !== undefined && p.value !== null) {
                        tip += p.marker + p.seriesName + ': <b>' + (typeof p.value === 'number' ? p.value.toFixed(2) : p.value) + '</b><br/>';
                    }
                }
                return tip;
            }
        },
        legend: [
            { data: ['95%分位', '85%分位', '50%分位(中位)', '15%分位', '5%分位'], top: 18, textStyle: { fontSize: 11 }, icon: 'path://M0,4L30,4', itemWidth: 22, itemHeight: 3, itemGap: 16 },
            { data: ['收盘价', '加权成本'], top: '38%', left: 'right', right: '10%', textStyle: { fontSize: 11 }, icon: 'path://M0,4L30,4', itemWidth: 22, itemHeight: 3, itemGap: 16 },
            { data: ['胜率(%)'], top: '70%', left: 'right', right: '10%', textStyle: { fontSize: 11 }, icon: 'path://M0,4L30,4', itemWidth: 22, itemHeight: 3 }
        ],
        axisPointer: { link: [{ xAxisIndex: 'all' }] },
        dataZoom: [
            { type: 'inside', xAxisIndex: [0, 1, 2], start: 50, end: 100 },
            { type: 'slider', xAxisIndex: [0, 1, 2], bottom: 5, height: 18 }
        ],
        grid: [
            { left: '8%', right: '8%', top: '6%', height: '26%' },
            { left: '8%', right: '8%', top: '40%', height: '22%' },
            { left: '8%', right: '8%', top: '72%', height: '18%' }
        ],
        xAxis: [
            { type: 'category', data: dates, gridIndex: 0, axisLabel: { show: false } },
            { type: 'category', data: dates, gridIndex: 1, axisLabel: { show: false } },
            { type: 'category', data: dates, gridIndex: 2, axisLabel: { fontSize: 10 } }
        ],
        yAxis: [
            { type: 'value', gridIndex: 0, name: '成本价', scale: true, splitLine: { lineStyle: { opacity: 0.3 } } },
            { type: 'value', gridIndex: 1, name: '价格', scale: true, splitLine: { lineStyle: { opacity: 0.3 } } },
            { type: 'value', gridIndex: 2, name: '胜率(%)', min: 0, max: 100, splitLine: { lineStyle: { opacity: 0.3 } } }
        ],
        series: [
            // ── 子图1：筹码成本分位线（纯线条，无填充） ──
            {
                name: '95%分位', type: 'line', xAxisIndex: 0, yAxisIndex: 0,
                data: cyqData.cost_95pct, symbol: 'none',
                color: '#d32f2f', itemStyle: { color: '#d32f2f' },
                lineStyle: { width: 2 }
            },
            {
                name: '85%分位', type: 'line', xAxisIndex: 0, yAxisIndex: 0,
                data: cyqData.cost_85pct, symbol: 'none',
                color: '#f57c00', itemStyle: { color: '#f57c00' },
                lineStyle: { width: 1.5 }
            },
            {
                name: '50%分位(中位)', type: 'line', xAxisIndex: 0, yAxisIndex: 0,
                data: cyqData.cost_50pct, symbol: 'none',
                color: '#6a1b9a', itemStyle: { color: '#6a1b9a' },
                lineStyle: { width: 2.5 }
            },
            {
                name: '15%分位', type: 'line', xAxisIndex: 0, yAxisIndex: 0,
                data: cyqData.cost_15pct, symbol: 'none',
                color: '#2e7d32', itemStyle: { color: '#2e7d32' },
                lineStyle: { width: 1.5 }
            },
            {
                name: '5%分位', type: 'line', xAxisIndex: 0, yAxisIndex: 0,
                data: cyqData.cost_5pct, symbol: 'none',
                color: '#1565c0', itemStyle: { color: '#1565c0' },
                lineStyle: { width: 2 }
            },
            // ── 子图2：收盘价 + 加权成本 ──
            {
                name: '收盘价', type: 'line', xAxisIndex: 1, yAxisIndex: 1,
                data: closePrices.length > 0 ? closePrices : [],
                symbol: 'none',
                color: '#1a56db', itemStyle: { color: '#1a56db' },
                lineStyle: { width: 2 }
            },
            {
                name: '加权成本', type: 'line', xAxisIndex: 1, yAxisIndex: 1,
                data: cyqData.weight_avg, symbol: 'none',
                color: '#ab47bc', itemStyle: { color: '#ab47bc' },
                lineStyle: { width: 2, type: 'dashed' }
            },
            // ── 子图3：胜率 ──
            {
                name: '胜率(%)', type: 'line', xAxisIndex: 2, yAxisIndex: 2,
                data: cyqData.winner_rate, symbol: 'none',
                color: '#e91e63', itemStyle: { color: '#e91e63' },
                lineStyle: { width: 1.5 },
                areaStyle: { color: 'rgba(233,30,99,0.06)' },
                markLine: {
                    silent: true,
                    lineStyle: { type: 'dashed', opacity: 0.5 },
                    data: [
                        { yAxis: 20, label: { formatter: '20%', fontSize: 10 }, lineStyle: { color: '#26a69a' } },
                        { yAxis: 80, label: { formatter: '80%', fontSize: 10 }, lineStyle: { color: '#ef5350' } }
                    ]
                }
            }
        ]
    };

    cyqTimelineChart.setOption(option);
    window.addEventListener('resize', function() { if (cyqTimelineChart) cyqTimelineChart.resize(); });
}

// ── 胜率回测：生成信号 ──
function generateSignals_WinnerRate(winnerRates, closes, buyThresh, sellThresh) {
    var signals = [];
    var pos = 0;
    for (var i = 0; i < closes.length; i++) {
        if (winnerRates[i] !== null) {
            if (winnerRates[i] <= buyThresh) pos = 1;
            if (winnerRates[i] >= sellThresh) pos = 0;
        }
        signals.push(pos);
    }
    return signals;
}

// ── 自动搜索最优胜率阈值 ──
function runCyqAutoSearch() {
    if (!cyqData || cyqData.dates.length < 30) {
        alert('请先加载筹码数据');
        return;
    }

    var btn = document.getElementById('btn-cyq-auto');
    btn.disabled = true;
    btn.textContent = '搜索中...';

    // 对齐筹码数据到K线日期
    var aligned = alignCyqToKline();
    if (!aligned) {
        btn.disabled = false;
        btn.textContent = '自动搜索最优参数';
        alert('无法对齐筹码与K线数据');
        return;
    }

    var closes = stockData.ohlc.map(function(d) { return d[3]; });
    var commission = parseFloat(document.getElementById('cyq-commission').value) / 100 || 0;

    var results = [];
    var buyRange = [5, 8, 10, 12, 15, 18, 20, 25, 30];
    var sellRange = [60, 65, 70, 75, 80, 85, 90, 95];

    for (var bi = 0; bi < buyRange.length; bi++) {
        for (var si = 0; si < sellRange.length; si++) {
            var bTh = buyRange[bi];
            var sTh = sellRange[si];
            if (bTh >= sTh) continue;

            var signals = generateSignals_WinnerRate(aligned.winnerRates, closes, bTh, sTh);
            var perf = quickBacktest(signals, closes, commission);

            results.push({
                buyThresh: bTh,
                sellThresh: sTh,
                cumReturn: perf.cumReturn,
                annualReturn: perf.annualReturn,
                sharpe: perf.sharpe,
                maxDrawdown: perf.maxDrawdown,
                trades: perf.trades
            });
        }
    }

    results.sort(function(a, b) { return parseFloat(b.sharpe) - parseFloat(a.sharpe); });
    var top5 = results.slice(0, 5);

    // 渲染表格
    var section = document.getElementById('cyq-auto-result');
    var table = document.getElementById('cyq-auto-table');
    section.style.display = 'block';

    var html = '<thead><tr><th>排名</th><th>买入阈值</th><th>卖出阈值</th><th>累积收益</th><th>年化收益</th><th>夏普比率</th><th>最大回撤</th><th>交易次数</th><th>操作</th></tr></thead><tbody>';
    for (var i = 0; i < top5.length; i++) {
        var r = top5[i];
        var retClass = parseFloat(r.cumReturn) >= 0 ? 'positive' : 'negative';
        html += '<tr' + (i === 0 ? ' style="background:rgba(26,86,219,0.06);font-weight:600;"' : '') + '>' +
            '<td>' + (i + 1) + '</td>' +
            '<td>' + r.buyThresh + '%</td><td>' + r.sellThresh + '%</td>' +
            '<td class="' + retClass + '">' + r.cumReturn + '%</td>' +
            '<td class="' + retClass + '">' + r.annualReturn + '%</td>' +
            '<td>' + r.sharpe + '</td>' +
            '<td class="negative">-' + r.maxDrawdown + '%</td>' +
            '<td>' + r.trades + '</td>' +
            '<td><button class="btn btn-sm" onclick="applyCyqParams(' + r.buyThresh + ',' + r.sellThresh + ')">应用</button></td></tr>';
    }
    html += '</tbody>';
    table.innerHTML = html;

    // 自动用最优参数执行回测
    if (top5.length > 0) {
        document.getElementById('cyq-buy-thresh').value = top5[0].buyThresh;
        document.getElementById('cyq-sell-thresh').value = top5[0].sellThresh;
        runCyqBacktest();
    }

    btn.disabled = false;
    btn.textContent = '自动搜索最优参数';
}

function applyCyqParams(buyTh, sellTh) {
    document.getElementById('cyq-buy-thresh').value = buyTh;
    document.getElementById('cyq-sell-thresh').value = sellTh;
    runCyqBacktest();
}

// ── 对齐筹码数据到K线日期 ──
function alignCyqToKline() {
    if (!stockData || !stockData.dates || !cyqData || !cyqData.dates) return null;

    var wrMap = {};
    for (var i = 0; i < cyqData.dates.length; i++) {
        wrMap[cyqData.dates[i]] = cyqData.winner_rate[i];
    }

    var winnerRates = [];
    var lastVal = null;
    for (var i = 0; i < stockData.dates.length; i++) {
        if (wrMap[stockData.dates[i]] !== undefined && wrMap[stockData.dates[i]] !== null) {
            lastVal = wrMap[stockData.dates[i]];
        }
        winnerRates.push(lastVal);
    }

    return { winnerRates: winnerRates };
}

// ── 筹码胜率回测主函数 ──
function runCyqBacktest() {
    if (!cyqData || cyqData.dates.length < 30 || !stockData || stockData.ohlc.length < 30) {
        alert('请先加载筹码数据和K线数据');
        return;
    }

    var buyThresh = parseFloat(document.getElementById('cyq-buy-thresh').value) || 10;
    var sellThresh = parseFloat(document.getElementById('cyq-sell-thresh').value) || 80;
    var commission = parseFloat(document.getElementById('cyq-commission').value) / 100 || 0;

    var aligned = alignCyqToKline();
    if (!aligned) { alert('对齐数据失败'); return; }

    var dates = stockData.dates;
    var closes = stockData.ohlc.map(function(d) { return d[3]; });
    var opens = stockData.ohlc.map(function(d) { return d[0]; });

    var signals = generateSignals_WinnerRate(aligned.winnerRates, closes, buyThresh, sellThresh);

    // 计算回测（复用现有逻辑）
    var strategyReturns = [];
    var benchmarkReturns = [];
    var strategyNAV = [1];
    var benchmarkNAV = [1];
    var actualPos = [0];
    for (var i = 1; i < closes.length; i++) actualPos.push(signals[i - 1]);

    var buySignals = [];
    var sellSignals = [];

    for (var i = 1; i < closes.length; i++) {
        var dailyRet = closes[i] / closes[i - 1] - 1;
        var cost = (actualPos[i] !== actualPos[i - 1]) ? commission : 0;
        var stratRet = actualPos[i] * dailyRet - cost;
        strategyReturns.push(stratRet);
        benchmarkReturns.push(dailyRet);
        strategyNAV.push(strategyNAV[strategyNAV.length - 1] * (1 + stratRet));
        benchmarkNAV.push(benchmarkNAV[benchmarkNAV.length - 1] * (1 + dailyRet));

        if (actualPos[i] === 1 && actualPos[i - 1] === 0) buySignals.push({ dateIdx: i, price: opens[i] });
        if (actualPos[i] === 0 && actualPos[i - 1] === 1) sellSignals.push({ dateIdx: i, price: opens[i] });
    }

    // 交易记录
    var trades = [];
    var openDate = null, openPrice = null, openIdx = null;
    for (var i = 1; i < actualPos.length; i++) {
        if (actualPos[i] === 1 && actualPos[i - 1] === 0) {
            openDate = dates[i]; openPrice = opens[i]; openIdx = i;
        }
        if (actualPos[i] === 0 && actualPos[i - 1] === 1 && openDate !== null) {
            trades.push({
                buyDate: openDate, buyPrice: openPrice,
                sellDate: dates[i], sellPrice: opens[i],
                holdDays: i - openIdx,
                returnPct: ((opens[i] / openPrice - 1) * 100).toFixed(2)
            });
            openDate = null;
        }
    }
    if (openDate !== null) {
        var lastIdx = closes.length - 1;
        trades.push({
            buyDate: openDate, buyPrice: openPrice,
            sellDate: dates[lastIdx] + '(持仓中)', sellPrice: closes[lastIdx],
            holdDays: lastIdx - openIdx,
            returnPct: ((closes[lastIdx] / openPrice - 1) * 100).toFixed(2)
        });
    }

    var metrics = calcBacktestMetrics(strategyReturns, benchmarkReturns, strategyNAV, benchmarkNAV, trades);

    // 渲染绩效统计（复用样式，用独立容器）
    var statsEl = document.getElementById('cyq-bt-stats');
    statsEl.style.display = 'grid';
    statsEl.innerHTML =
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">累积收益</div>' +
            '<div class="bt-stat-value ' + (parseFloat(metrics.cumReturn) >= 0 ? 'positive' : 'negative') + '">' + metrics.cumReturn + '%</div>' +
            '<div class="bt-stat-sub">基准: ' + metrics.cumBench + '%</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">年化收益</div>' +
            '<div class="bt-stat-value ' + (parseFloat(metrics.annualReturn) >= 0 ? 'positive' : 'negative') + '">' + metrics.annualReturn + '%</div>' +
            '<div class="bt-stat-sub">基准: ' + metrics.annualBench + '%</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">夏普比率</div>' +
            '<div class="bt-stat-value">' + metrics.sharpe + '</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">最大回撤</div>' +
            '<div class="bt-stat-value negative">-' + metrics.maxDrawdown + '%</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">年化波动率</div>' +
            '<div class="bt-stat-value">' + metrics.volatility + '%</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">交易胜率</div>' +
            '<div class="bt-stat-value">' + metrics.winRate + '%</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">总交易次数</div>' +
            '<div class="bt-stat-value">' + metrics.totalTrades + '</div>' +
        '</div>' +
        '<div class="bt-stat-card">' +
            '<div class="bt-stat-label">策略说明</div>' +
            '<div class="bt-stat-value" style="font-size:12px;">胜率<' + buyThresh + '%买 >' + sellThresh + '%卖</div>' +
        '</div>';

    // 渲染净值曲线
    renderCyqBtChart(dates, strategyNAV, benchmarkNAV, buySignals, sellSignals);

    // 渲染交易明细
    renderCyqTradesTable(trades);
}

function renderCyqBtChart(dates, stratNAV, benchNAV, buySignals, sellSignals) {
    var container = document.getElementById('cyq-bt-chart');
    container.style.display = 'block';
    if (cyqBtChart) cyqBtChart.dispose();
    cyqBtChart = echarts.init(container, getEchartsTheme());

    var chartDates = dates.slice(0, stratNAV.length);
    var stratData = stratNAV.map(function(v) { return parseFloat(v.toFixed(4)); });
    var benchData = benchNAV.map(function(v) { return parseFloat(v.toFixed(4)); });

    var buyMarks = (buySignals || []).map(function(s) {
        return {
            coord: [s.dateIdx, stratData[s.dateIdx] || 1],
            symbol: 'triangle', symbolSize: 10, symbolRotate: 0,
            itemStyle: { color: '#ef5350' },
            label: { show: false }
        };
    });
    var sellMarks = (sellSignals || []).map(function(s) {
        return {
            coord: [s.dateIdx, stratData[s.dateIdx] || 1],
            symbol: 'triangle', symbolSize: 10, symbolRotate: 180,
            itemStyle: { color: '#26a69a' },
            label: { show: false }
        };
    });

    var option = {
        animation: false,
        title: { text: '胜率策略 vs 买入持有', left: 'center', textStyle: { fontSize: 14 } },
        tooltip: { trigger: 'axis' },
        legend: { data: ['策略净值', '基准净值(买入持有)'], top: 25 },
        grid: { left: '8%', right: '4%', top: '18%', bottom: '15%' },
        xAxis: { type: 'category', data: chartDates, axisLabel: { fontSize: 10 } },
        yAxis: { scale: true, splitLine: { lineStyle: { opacity: 0.3 } } },
        dataZoom: [
            { type: 'inside', start: 0, end: 100 },
            { type: 'slider', bottom: 2, height: 18 }
        ],
        series: [
            {
                name: '策略净值', type: 'line', data: stratData,
                symbol: 'none', lineStyle: { width: 2, color: '#1a56db' },
                areaStyle: { color: 'rgba(26,86,219,0.06)' },
                markPoint: { data: buyMarks.concat(sellMarks) }
            },
            {
                name: '基准净值(买入持有)', type: 'line', data: benchData,
                symbol: 'none', lineStyle: { width: 1.5, color: '#9b59b6', type: 'dashed' }
            }
        ]
    };

    cyqBtChart.setOption(option);
    window.addEventListener('resize', function() { if (cyqBtChart) cyqBtChart.resize(); });
}

function renderCyqTradesTable(trades) {
    var section = document.getElementById('cyq-bt-trades-section');
    var table = document.getElementById('cyq-bt-trades-table');
    if (trades.length === 0) { section.style.display = 'none'; return; }
    section.style.display = 'block';

    var html = '<thead><tr><th>序号</th><th>买入日期</th><th>买入价</th><th>卖出日期</th><th>卖出价</th><th>持仓天数</th><th>收益率</th></tr></thead><tbody>';
    for (var i = 0; i < trades.length; i++) {
        var t = trades[i];
        var retClass = parseFloat(t.returnPct) >= 0 ? 'positive' : 'negative';
        html += '<tr>' +
            '<td>' + (i + 1) + '</td>' +
            '<td>' + t.buyDate + '</td>' +
            '<td>' + t.buyPrice.toFixed(2) + '</td>' +
            '<td>' + t.sellDate + '</td>' +
            '<td>' + t.sellPrice.toFixed(2) + '</td>' +
            '<td>' + t.holdDays + '</td>' +
            '<td class="' + retClass + '">' + (parseFloat(t.returnPct) >= 0 ? '+' : '') + t.returnPct + '%</td>' +
            '</tr>';
    }
    html += '</tbody>';
    table.innerHTML = html;
}

// ══════════════ K线结构相似预测 ══════════════

function _parsePatternHorizons(text) {
    var s = String(text || '').trim();
    if (!s) return [5, 10, 20];
    var arr = s.split(',').map(function(x) { return parseInt(String(x).trim(), 10); })
        .filter(function(x) { return Number.isFinite(x) && x > 0 && x <= 260; });
    var uniq = [];
    var seen = {};
    for (var i = 0; i < arr.length; i++) {
        if (!seen[arr[i]]) {
            seen[arr[i]] = true;
            uniq.push(arr[i]);
        }
    }
    return uniq.length ? uniq : [5, 10, 20];
}

function _fmtPct(v) {
    if (v === null || v === undefined || isNaN(v)) return '--';
    var n = parseFloat(v);
    return (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
}

function clearPatternCharts() {
    if (!patternCharts || !patternCharts.length) return;
    for (var i = 0; i < patternCharts.length; i++) {
        try { patternCharts[i].dispose(); } catch (e) {}
    }
    patternCharts = [];
}

function ensurePatternResizeBinding() {
    if (patternResizeBound) return;
    patternResizeBound = true;
    window.addEventListener('resize', function() {
        for (var i = 0; i < patternCharts.length; i++) {
            try { if (patternCharts[i]) patternCharts[i].resize(); } catch (e) {}
        }
    });
}

function renderPatternSegmentChart(containerId, chartData, title, subtitle) {
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

    var chart = echarts.init(el, getEchartsTheme());
    patternCharts.push(chart);

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
                data: (macd.hist || []).map(function(v) {
                    return { value: v, itemStyle: { color: (v >= 0 ? '#ef5350' : '#26a69a') } };
                })
            },
            { name: 'K', type: 'line', xAxisIndex: 3, yAxisIndex: 3, data: kdj.k || [], symbol: 'none', lineStyle: { width: 1, color: '#1d4ed8' } },
            { name: 'D', type: 'line', xAxisIndex: 3, yAxisIndex: 3, data: kdj.d || [], symbol: 'none', lineStyle: { width: 1, color: '#f59e0b' } },
            { name: 'J', type: 'line', xAxisIndex: 3, yAxisIndex: 3, data: kdj.j || [], symbol: 'none', lineStyle: { width: 1, color: '#8b5cf6' } }
        ]
    };

    chart.setOption(option);
}

function renderPatternSummary(prediction, freqUnit) {
    var box = document.getElementById('pattern-summary');
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
        var main = parseFloat(p.weighted_mean_pct || 0);
        var mainCls = main >= 0 ? 'positive' : 'negative';
        html += '<div class="pattern-stat-card">' +
            '<div class="pattern-stat-title">未来 ' + k + freqUnit + '（相似度加权）</div>' +
            '<div class="pattern-stat-main ' + mainCls + '">' + _fmtPct(main) + '</div>' +
            '<div class="pattern-stat-sub">' +
            '中位数: ' + _fmtPct(p.median_pct) + ' ｜ 胜率: ' + (p.up_ratio_pct === undefined ? '--' : p.up_ratio_pct + '%') + '<br>' +
            '区间: ' + _fmtPct(p.min_pct) + ' ~ ' + _fmtPct(p.max_pct) +
            '</div>' +
            '</div>';
    }
    box.innerHTML = html;
    box.style.display = 'grid';
}

function renderPatternTable(matches, horizons, freqUnit) {
    var wrap = document.getElementById('pattern-table-wrap');
    var table = document.getElementById('pattern-table');
    if (!wrap || !table) return;
    if (!matches || !matches.length) {
        wrap.style.display = 'none';
        return;
    }
    var hs = (horizons || []).slice().sort(function(a, b) { return a - b; });
    var head = '<thead><tr><th>排名</th><th>历史阶段</th><th>相似度</th>';
    for (var i = 0; i < hs.length; i++) {
        head += '<th>未来' + hs[i] + freqUnit + '</th>';
    }
    head += '</tr></thead><tbody>';

    var body = '';
    for (var r = 0; r < matches.length; r++) {
        var m = matches[r];
        body += '<tr>' +
            '<td>' + (m.rank || (r + 1)) + '</td>' +
            '<td>' + (m.start_date || '--') + ' ~ ' + (m.end_date || '--') + '</td>' +
            '<td>' + (m.similarity === undefined ? '--' : Number(m.similarity).toFixed(4)) + '</td>';
        for (var j = 0; j < hs.length; j++) {
            var key = String(hs[j]);
            var v = m.future_returns ? m.future_returns[key] : null;
            var cls = (v === null || v === undefined || isNaN(v)) ? '' : (parseFloat(v) >= 0 ? 'positive' : 'negative');
            body += '<td class="' + cls + '">' + _fmtPct(v) + '</td>';
        }
        body += '</tr>';
    }
    body += '</tbody>';
    table.innerHTML = head + body;
    wrap.style.display = 'block';
}

function renderPatternMatches(matches, horizons, freqUnit) {
    var grid = document.getElementById('pattern-matches-grid');
    if (!grid) return;
    if (!matches || !matches.length) {
        grid.innerHTML = '';
        return;
    }
    var hs = (horizons || []).slice().sort(function(a, b) { return a - b; });
    var html = '';
    for (var i = 0; i < matches.length; i++) {
        var m = matches[i];
        var retTags = '';
        for (var j = 0; j < hs.length; j++) {
            var key = String(hs[j]);
            var v = m.future_returns ? m.future_returns[key] : null;
            var cls = (v === null || v === undefined || isNaN(v)) ? '' : (parseFloat(v) >= 0 ? 'positive' : 'negative');
            retTags += '<span class="pattern-ret-tag ' + cls + '">' + hs[j] + freqUnit + ': ' + _fmtPct(v) + '</span>';
        }
        html += '<div class="pattern-match-card">' +
            '<div class="pattern-match-head">' +
            '<div class="pattern-match-title">相似阶段 #' + (m.rank || (i + 1)) + '：' + (m.start_date || '--') + ' ~ ' + (m.end_date || '--') + '</div>' +
            '<div class="pattern-sim-tag">相似度 ' + (m.similarity === undefined ? '--' : Number(m.similarity).toFixed(4)) + '</div>' +
            '</div>' +
            '<div class="pattern-ret-tags">' + retTags + '</div>' +
            '<div id="pattern-match-chart-' + i + '" class="stock-chart-container pattern-chart-box"></div>' +
            '</div>';
    }
    grid.innerHTML = html;

    for (var k = 0; k < matches.length; k++) {
        var item = matches[k] || {};
        renderPatternSegmentChart(
            'pattern-match-chart-' + k,
            item.chart || {},
            '相似阶段 #' + (item.rank || (k + 1)),
            (item.start_date || '--') + ' ~ ' + (item.end_date || '--')
        );
    }
}

function loadPatternMatch() {
    var tsCode = document.getElementById('stock-code').value.trim();
    var startDate = document.getElementById('stock-start-date').value.replace(/-/g, '');
    if (!tsCode) return;

    var windowN = parseInt(document.getElementById('pattern-window').value, 10);
    var topK = parseInt(document.getElementById('pattern-topk').value, 10);
    var horizons = _parsePatternHorizons(document.getElementById('pattern-horizons').value);

    if (!Number.isFinite(windowN) || windowN < 12) windowN = 40;
    if (!Number.isFinite(topK) || topK < 1) topK = 5;
    topK = Math.min(10, topK);

    var btn = document.getElementById('btn-pattern-run');
    btn.disabled = true;
    btn.textContent = '匹配中...';
    setPatternStatus('ok', '正在进行历史结构匹配，请稍候...');

    fetchPatternMatch(tsCode, currentFreq, '19900101', windowN, topK, horizons)
    .then(function(data) {
        btn.disabled = false;
        btn.textContent = '开始匹配';
        patternData = data || {};

        if (!data || data.ok === false) {
            setPatternStatus('warning', '匹配失败：' + (data && data.message ? data.message : 'unknown'));
            clearPatternCharts();
            return;
        }

        clearPatternCharts();
        ensurePatternResizeBinding();

        var msg = '匹配完成：频率=' + (data.freq_label || currentFreq) +
            ' ｜ 窗口=' + (data.window || windowN) +
            ' ｜ 候选样本=' + (data.candidate_count || 0) +
            ' ｜ 数据源=' + _sourceLabel(data.data_source) +
            ' ｜ 最新日期=' + _fmtYmd(data.latest_trade_date || '') +
            ' ｜ 更新时间=' + (data.generated_at || '--');
        setPatternStatus('ok', msg);

        renderPatternSummary(data.prediction || {}, data.freq_unit || '天');
        renderPatternTable(data.matches || [], data.horizons || horizons, data.freq_unit || '天');

        var targetWrap = document.getElementById('pattern-target-wrap');
        if (targetWrap) targetWrap.style.display = 'block';
        renderPatternSegmentChart(
            'pattern-target-chart',
            ((data.target || {}).chart) || {},
            '当前结构（目标片段）',
            ((data.target || {}).start_date || '--') + ' ~ ' + ((data.target || {}).end_date || '--')
        );

        renderPatternMatches(data.matches || [], data.horizons || horizons, data.freq_unit || '天');
    })
    .catch(function(err) {
        btn.disabled = false;
        btn.textContent = '开始匹配';
        clearPatternCharts();
        setPatternStatus('error', '匹配请求失败：' + String(err || 'unknown'));
    });
}

// ══════════════ 新闻资讯模块 ══════════════

function loadNewsData() {
    var tsCode = document.getElementById('stock-code').value.trim();
    if (!tsCode) return;

    var btn = document.getElementById('btn-news-load');
    var loading = document.getElementById('news-loading');
    btn.disabled = true;
    btn.style.display = 'none';
    loading.style.display = 'inline';

    fetch('/api/stock/news', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ts_code: tsCode})
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        newsData = data;
        loading.style.display = 'none';

        if (data.error) {
            btn.style.display = 'inline-block';
            btn.disabled = false;
            alert('新闻加载失败: ' + data.error);
            return;
        }

        document.getElementById('news-panels').style.display = 'block';
        renderNewsPanel('industry_policy', data.industry_policy || []);
        renderNewsPanel('company_announcements', data.company_announcements || []);
        renderNewsPanel('company_news', data.company_news || []);
        renderNewsPanel('world_events', data.world_events || []);

        btn.textContent = '刷新新闻';
        btn.style.display = 'inline-block';
        btn.disabled = false;
    })
    .catch(function(err) {
        loading.style.display = 'none';
        btn.style.display = 'inline-block';
        btn.disabled = false;
        alert('新闻加载失败: ' + err);
    });
}

function renderNewsPanel(category, items) {
    var body = document.getElementById('news-body-' + category);
    if (!body) return;

    if (!items || items.length === 0) {
        body.innerHTML = '<p style="color:var(--text-secondary,#64748b);font-size:13px;padding:8px 0;">暂无相关新闻</p>';
        return;
    }

    var html = '';
    items.forEach(function(item) {
        var impactClass = item.impact ? ' impact' : '';
        var hasUrl = item.url && item.url.trim() !== '';
        html += '<div class="news-item' + impactClass + '">';
        html += '<div class="news-item-header" onclick="toggleNewsItem(this.parentNode)">';
        html += '<div class="news-item-title">' + _escapeHtml(item.title || '');
        if (item.impact) html += ' <span style="font-size:11px;color:#dc2626;">●</span>';
        html += '</div>';
        html += '<div class="news-item-date">' + _escapeHtml(item.date || '日期未知');
        if (hasUrl) html += ' <a href="' + _escapeHtml(item.url) + '" target="_blank" rel="noopener" onclick="event.stopPropagation();" style="margin-left:8px;color:#1a56db;font-size:12px;">搜索原文 ↗</a>';
        html += '</div>';
        html += '</div>';
        html += '<div class="news-item-summary">' + _escapeHtml(item.summary || '') + '</div>';
        html += '</div>';
    });
    body.innerHTML = html;
}

function toggleNewsCard(category) {
    var card = document.getElementById('news-' + category);
    if (card) card.classList.toggle('collapsed');
}

function toggleNewsItem(el) {
    el.classList.toggle('expanded');
}

function _escapeHtml(text) {
    var div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

window.addEventListener('beforeunload', function() {
    if (stockAutoRefreshTimer) {
        clearInterval(stockAutoRefreshTimer);
        stockAutoRefreshTimer = null;
    }
    clearPatternCharts();
});

// ── 页面加载时检查 URL 参数，支持从大师选股等页面跳转 ──
(function() {
    var urlParams = new URLSearchParams(window.location.search);
    var code = urlParams.get('code');
    if (code) {
        document.getElementById('stock-code').value = code;
        loadStockData();
    }
})();
