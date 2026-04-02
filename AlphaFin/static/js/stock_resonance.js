/* 多周期共振系统 */

var resonanceKlineChart = null;
var resonanceMacdTimelineChart = null;
var resonanceKdjTimelineChart = null;
var resonanceWallCharts = [];
var resonancePeriodsCache = [];
var resonanceWallMode = 'short';  // short | long | all

function queryResonance() {
    var tsCode = (document.getElementById('res-ts-code').value || '').trim().toUpperCase();
    var assetType = (document.getElementById('res-asset-type').value || 'stock').trim();
    var startDate = (document.getElementById('res-start-date').value || '2020-01-01').replace(/-/g, '');
    var btn = document.getElementById('res-query-btn');

    if (!tsCode) {
        alert('请输入股票/指数代码');
        return;
    }

    btn.disabled = true;
    btn.textContent = '计算中...';
    setResState('正在拉取多周期数据并计算 MACD/KDJ ...', 'loading');

    fetch('/api/stock/resonance', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            ts_code: tsCode,
            asset_type: assetType,
            start_date: startDate
        })
    }).then(function(r) {
        return r.json();
    }).then(function(payload) {
        renderResonance(payload || {});
        if (payload && payload.ok) {
            setResState('分析完成', 'ok');
        } else {
            setResState('未获取到有效数据：' + (payload.message || '请稍后重试'), 'error');
        }
    }).catch(function(err) {
        setResState('请求失败：' + String(err || 'unknown error'), 'error');
    }).finally(function() {
        btn.disabled = false;
        btn.textContent = '生成共振分析';
    });
}

function setResState(text, level) {
    var el = document.getElementById('res-state');
    if (!el) return;
    el.textContent = '状态：' + String(text || '--');
    el.classList.remove('state-idle', 'state-loading', 'state-ok', 'state-error');
    el.classList.add('state-' + (level || 'idle'));
}

function renderResonance(payload) {
    var summary = payload.summary || {};
    var periods = payload.periods || [];
    resonancePeriodsCache = periods;
    renderWarningLight(summary);
    renderSummary(summary);
    renderTables(periods);
    renderFutureTimeline(periods);
    renderIndicatorWall(resonancePeriodsCache);
    renderKline(payload.kline || {}, payload.ts_code || '');

    var assumption = document.getElementById('res-assumption');
    if (assumption) {
        assumption.textContent = payload.assumption || '预测假设：未来价格维持当前收盘价不变，仅用于观察指标演化路径，不构成投资建议。';
    }
    var meta = document.getElementById('res-kline-meta');
    if (meta) {
        var t = payload.generated_at || '--';
        var latestTrade = payload.latest_trade_date || '--';
        var win = (payload.minute_window_trade_dates || []).join(' ~ ');
        var fallbackText = payload.used_latest_trade_day_fallback ? '（休市回退口径）' : '';
        meta.textContent = '生成时间：' + t + ' ｜ 分钟窗口：' + (win || '--') + ' ｜ 最新交易日：' + latestTrade + fallbackText;
    }
}

function setWallMode(mode) {
    resonanceWallMode = (mode === 'all' || mode === 'long') ? mode : 'short';
    syncWallModeControls();
    renderIndicatorWall(resonancePeriodsCache || []);
}

function syncWallModeControls() {
    var bShort = document.getElementById('res-wall-mode-short');
    var bLong = document.getElementById('res-wall-mode-long');
    var bAll = document.getElementById('res-wall-mode-all');
    var meta = document.getElementById('res-wall-meta');
    if (bShort) {
        bShort.classList.remove('btn-primary', 'btn-outline');
        bShort.classList.add(resonanceWallMode === 'short' ? 'btn-primary' : 'btn-outline');
    }
    if (bLong) {
        bLong.classList.remove('btn-primary', 'btn-outline');
        bLong.classList.add(resonanceWallMode === 'long' ? 'btn-primary' : 'btn-outline');
    }
    if (bAll) {
        bAll.classList.remove('btn-primary', 'btn-outline');
        bAll.classList.add(resonanceWallMode === 'all' ? 'btn-primary' : 'btn-outline');
    }
    if (meta) {
        if (resonanceWallMode === 'all') {
            meta.textContent = '当前显示：全部周期（含日/周/月）';
        } else if (resonanceWallMode === 'long') {
            meta.textContent = '当前显示：日线/周线/月线';
        } else {
            meta.textContent = '当前显示：5m/15m/30m/60m';
        }
    }
}

function renderSummary(summary) {
    var box = document.getElementById('res-summary-grid');
    if (!box) return;

    var dual = !!summary.short_dual_all_golden;
    var macdOk = !!summary.short_macd_all_golden;
    var kdjOk = !!summary.short_kdj_all_golden;
    var risks = summary.week_month_death_risk || [];

    var riskText = '无近端周/月死叉风险';
    if (risks.length) {
        riskText = risks.map(function(r) {
            var ind = (r.indicator || '').toUpperCase();
            var day = fmtDay(r.days_to_death);
            return (r.period || '') + ' ' + ind + '≈' + day;
        }).join(' ｜ ');
    }

    box.innerHTML = '' +
        '<article class="resonance-summary-card tone-' + (dual ? 'up' : 'mid') + '">' +
            '<h3>短周期双指标共振</h3>' +
            '<p class="value">' + (dual ? '已形成' : '未形成') + '</p>' +
            '<p>' + (summary.bias || '--') + '</p>' +
        '</article>' +
        '<article class="resonance-summary-card tone-' + (macdOk ? 'up' : 'down') + '">' +
            '<h3>短周期 MACD</h3>' +
            '<p class="value">' + (macdOk ? '全金叉' : '未齐') + '</p>' +
            '<p>检查 5m/15m/30m/60m/日线</p>' +
        '</article>' +
        '<article class="resonance-summary-card tone-' + (kdjOk ? 'up' : 'down') + '">' +
            '<h3>短周期 KDJ</h3>' +
            '<p class="value">' + (kdjOk ? '全金叉' : '未齐') + '</p>' +
            '<p>检查 5m/15m/30m/60m/日线</p>' +
        '</article>' +
        '<article class="resonance-summary-card tone-' + (risks.length ? 'down' : 'up') + '">' +
            '<h3>周/月反向风险</h3>' +
            '<p class="value">' + (risks.length ? '需警惕' : '较低') + '</p>' +
            '<p>' + escapeHtml(riskText) + '</p>' +
        '</article>';
}

function renderWarningLight(summary) {
    var box = document.getElementById('res-warning-box');
    if (!box) return;

    var shortDual = !!summary.short_dual_all_golden;
    var risks = summary.week_month_death_risk || [];

    box.classList.remove('warning-idle', 'warning-safe', 'warning-watch', 'warning-risk');

    if (shortDual && risks.length) {
        box.classList.add('warning-risk');
        box.innerHTML = '' +
            '<div class="light"></div>' +
            '<div class="content">' +
            '<h3>短多长空预警</h3>' +
            '<p>短周期已共振偏多，但周/月出现潜在死叉风险：' + escapeHtml(risks.map(function(r) {
                return (r.period || '') + ' ' + String(r.indicator || '').toUpperCase() + '≈' + fmtDay(r.days_to_death);
            }).join(' ｜ ')) + '</p>' +
            '</div>';
        return;
    }

    if (shortDual && !risks.length) {
        box.classList.add('warning-safe');
        box.innerHTML = '' +
            '<div class="light"></div>' +
            '<div class="content">' +
            '<h3>共振通过</h3>' +
            '<p>短周期双指标同向共振，且周/月未见近端死叉风险，可作为重点跟踪结构。</p>' +
            '</div>';
        return;
    }

    if (!shortDual && risks.length) {
        box.classList.add('warning-watch');
        box.innerHTML = '' +
            '<div class="light"></div>' +
            '<div class="content">' +
            '<h3>观望优先</h3>' +
            '<p>短周期尚未完成共振，且周/月存在潜在死叉压力，建议降低追涨冲动。</p>' +
            '</div>';
        return;
    }

    box.classList.add('warning-idle');
    box.innerHTML = '' +
        '<div class="light"></div>' +
        '<div class="content">' +
        '<h3>结构中性</h3>' +
        '<p>当前未形成完整共振，建议继续观察短中长周期是否逐步一致。</p>' +
        '</div>';
}

function renderTables(periods) {
    var macdBody = document.getElementById('res-macd-body');
    var kdjBody = document.getElementById('res-kdj-body');
    if (!macdBody || !kdjBody) return;

    if (!periods || !periods.length) {
        macdBody.innerHTML = '<tr><td colspan="7" class="res-empty-cell">暂无数据</td></tr>';
        kdjBody.innerHTML = '<tr><td colspan="7" class="res-empty-cell">暂无数据</td></tr>';
        return;
    }

    var macdRows = [];
    var kdjRows = [];

    periods.forEach(function(item) {
        var label = item.label || item.period || '--';
        if (!item.has_data) {
            var msg = item.message || '暂无数据';
            macdRows.push('<tr><td>' + escapeHtml(label) + '</td><td colspan="6" class="res-empty-cell">' + escapeHtml(msg) + '</td></tr>');
            kdjRows.push('<tr><td>' + escapeHtml(label) + '</td><td colspan="6" class="res-empty-cell">' + escapeHtml(msg) + '</td></tr>');
            return;
        }

        var macd = item.macd || {};
        var kdj = item.kdj || {};
        var bars = item.bars != null ? String(item.bars) : '--';
        var timeText = item.latest_time ? ('<div class="res-time">' + escapeHtml(item.latest_time) + '</div>') : '';

        macdRows.push('' +
            '<tr>' +
                '<td>' + escapeHtml(label) + timeText + '</td>' +
                '<td>DIF ' + fmtNum(macd.dif, 4) + '<br>DEA ' + fmtNum(macd.dea, 4) + '<br>HIST ' + fmtNum(macd.hist, 4) + '</td>' +
                '<td>' + relationBadge(macd.relation) + '</td>' +
                '<td>' + crossText(macd.cross_event) + '</td>' +
                '<td>' + fmtDay(macd.days_to_golden) + '</td>' +
                '<td>' + fmtDay(macd.days_to_death) + '</td>' +
                '<td>' + bars + '</td>' +
            '</tr>'
        );

        kdjRows.push('' +
            '<tr>' +
                '<td>' + escapeHtml(label) + timeText + '</td>' +
                '<td>K ' + fmtNum(kdj.k, 2) + '<br>D ' + fmtNum(kdj.d, 2) + '<br>J ' + fmtNum(kdj.j, 2) + '</td>' +
                '<td>' + relationBadge(kdj.relation) + '</td>' +
                '<td>' + crossText(kdj.cross_event) + '</td>' +
                '<td>' + fmtDay(kdj.days_to_golden) + '</td>' +
                '<td>' + fmtDay(kdj.days_to_death) + '</td>' +
                '<td>' + bars + '</td>' +
            '</tr>'
        );
    });

    macdBody.innerHTML = macdRows.join('');
    kdjBody.innerHTML = kdjRows.join('');
}

function relationBadge(rel) {
    var x = String(rel || 'neutral').toLowerCase();
    if (x === 'golden') return '<span class="res-badge b-golden">金叉区间</span>';
    if (x === 'death') return '<span class="res-badge b-death">死叉区间</span>';
    return '<span class="res-badge b-neutral">粘合</span>';
}

function crossText(ev) {
    var x = String(ev || 'none').toLowerCase();
    if (x === 'golden') return '<span class="res-cross c-golden">刚金叉</span>';
    if (x === 'death') return '<span class="res-cross c-death">刚死叉</span>';
    return '<span class="res-cross c-none">无</span>';
}

function fmtNum(v, digits) {
    if (v === null || v === undefined || v === '') return '--';
    var n = Number(v);
    if (Number.isNaN(n)) return '--';
    return n.toFixed(digits || 2);
}

function fmtDay(v) {
    if (v === null || v === undefined || v === '') return '--';
    var n = Number(v);
    if (Number.isNaN(n)) return '--';
    return n.toFixed(1) + '天';
}

function renderFutureTimeline(periods) {
    var macdEl = document.getElementById('res-macd-timeline');
    var kdjEl = document.getElementById('res-kdj-timeline');
    if (!macdEl || !kdjEl || typeof echarts === 'undefined') return;

    if (resonanceMacdTimelineChart) resonanceMacdTimelineChart.dispose();
    if (resonanceKdjTimelineChart) resonanceKdjTimelineChart.dispose();

    resonanceMacdTimelineChart = echarts.init(macdEl, getChartTheme());
    resonanceKdjTimelineChart = echarts.init(kdjEl, getChartTheme());

    resonanceMacdTimelineChart.setOption(buildTimelineOption(periods, 'macd', 'MACD'));
    resonanceKdjTimelineChart.setOption(buildTimelineOption(periods, 'kdj', 'KDJ'));
}

function renderIndicatorWall(periods) {
    var wall = document.getElementById('res-indicator-wall');
    if (!wall) return;

    resonanceWallCharts.forEach(function(c) {
        try { c.dispose(); } catch (e) {}
    });
    resonanceWallCharts = [];

    var shortOrder = ['5MIN', '15MIN', '30MIN', '60MIN'];
    var longOrder = ['D', 'W', 'M'];
    var allOrder = ['5MIN', '15MIN', '30MIN', '60MIN', 'D', 'W', 'M'];
    var targetOrder = shortOrder;
    if (resonanceWallMode === 'long') targetOrder = longOrder;
    if (resonanceWallMode === 'all') targetOrder = allOrder;
    var orderMap = {};
    targetOrder.forEach(function(k, i) { orderMap[k] = i; });

    var byPeriod = {};
    (periods || []).forEach(function(p) {
        byPeriod[String(p.period || '')] = p;
    });

    var rows = targetOrder.map(function(key) {
        var p = byPeriod[key] || {period: key, label: key, has_data: false, message: '该周期未返回数据'};
        var idPart = String(p.period || '').replace(/[^A-Za-z0-9_-]/g, '_');
        return '' +
            '<article class="res-wall-row">' +
                '<div class="res-wall-head">' + escapeHtml(p.label || p.period || '--') + '</div>' +
                (
                    p.has_data && p.chart && p.chart.times && p.chart.times.length
                    ? (
                        '<div class="res-wall-grid">' +
                            '<div class="res-mini-chart" id="res-macd-chart-' + idPart + '"></div>' +
                            '<div class="res-mini-chart" id="res-kdj-chart-' + idPart + '"></div>' +
                        '</div>'
                    )
                    : (
                        '<div class="res-mini-placeholder">' +
                            '暂无' + escapeHtml(p.label || p.period || '--') + '数据：' + escapeHtml(p.message || '接口未返回') +
                        '</div>'
                    )
                ) +
            '</article>';
    }).join('');
    wall.innerHTML = rows;

    targetOrder.forEach(function(key) {
        var p = byPeriod[key];
        if (!p || !p.has_data || !p.chart || !p.chart.times || !p.chart.times.length) return;
        var idPart = String(p.period || '').replace(/[^A-Za-z0-9_-]/g, '_');
        var macdEl = document.getElementById('res-macd-chart-' + idPart);
        var kdjEl = document.getElementById('res-kdj-chart-' + idPart);
        if (!macdEl || !kdjEl) return;
        var macdChart = echarts.init(macdEl, getChartTheme());
        var kdjChart = echarts.init(kdjEl, getChartTheme());
        macdChart.setOption(buildMiniMacdOption(p));
        kdjChart.setOption(buildMiniKdjOption(p));
        resonanceWallCharts.push(macdChart);
        resonanceWallCharts.push(kdjChart);
    });
}

function buildMiniMacdOption(item) {
    var chart = item.chart || {};
    var times = chart.times || [];
    var m = chart.macd || {};
    var dif = m.dif || [];
    var dea = m.dea || [];
    var hist = m.hist || [];
    return {
        animation: false,
        title: {
            text: 'MACD · ' + (item.label || item.period || ''),
            left: 6,
            top: 2,
            textStyle: {fontSize: 12}
        },
        legend: {
            data: ['DIF', 'DEA', 'HIST'],
            right: 6,
            top: 2,
            textStyle: {fontSize: 10}
        },
        grid: {left: 44, right: 10, top: 26, bottom: 20},
        tooltip: {trigger: 'axis', axisPointer: {type: 'cross'}},
        xAxis: {type: 'category', data: times, axisLabel: {show: false}},
        yAxis: {type: 'value', splitLine: {lineStyle: {opacity: 0.25}}},
        series: [
            {name: 'DIF', type: 'line', data: dif, symbol: 'none', smooth: true, lineStyle: {width: 1.2, color: '#2563eb'}},
            {name: 'DEA', type: 'line', data: dea, symbol: 'none', smooth: true, lineStyle: {width: 1.2, color: '#f59e0b'}},
            {name: 'HIST', type: 'bar', data: hist.map(function(v) {
                return {
                    value: v,
                    itemStyle: {color: (v != null && v >= 0) ? '#16a34a' : '#dc2626'}
                };
            })}
        ]
    };
}

function buildMiniKdjOption(item) {
    var chart = item.chart || {};
    var times = chart.times || [];
    var k = ((chart.kdj || {}).k) || [];
    var d = ((chart.kdj || {}).d) || [];
    var j = ((chart.kdj || {}).j) || [];
    return {
        animation: false,
        title: {
            text: 'KDJ · ' + (item.label || item.period || ''),
            left: 6,
            top: 2,
            textStyle: {fontSize: 12}
        },
        legend: {
            data: ['K', 'D', 'J'],
            right: 6,
            top: 2,
            textStyle: {fontSize: 10}
        },
        grid: {left: 44, right: 10, top: 26, bottom: 20},
        tooltip: {trigger: 'axis', axisPointer: {type: 'cross'}},
        xAxis: {type: 'category', data: times, axisLabel: {show: false}},
        yAxis: {
            type: 'value',
            min: function(v) { return Math.floor((v.min - 5) / 10) * 10; },
            max: function(v) { return Math.ceil((v.max + 5) / 10) * 10; },
            splitLine: {lineStyle: {opacity: 0.25}}
        },
        series: [
            {name: 'K', type: 'line', data: k, symbol: 'none', smooth: true, lineStyle: {width: 1.2, color: '#2563eb'}},
            {name: 'D', type: 'line', data: d, symbol: 'none', smooth: true, lineStyle: {width: 1.2, color: '#f59e0b'}},
            {name: 'J', type: 'line', data: j, symbol: 'none', smooth: true, lineStyle: {width: 1.2, color: '#8b5cf6'}}
        ]
    };
}

function buildTimelineOption(periods, indicatorKey, title) {
    var order = ['5MIN', '15MIN', '30MIN', '60MIN', 'D', 'W', 'M'];
    var labelMap = {};
    (periods || []).forEach(function(p) {
        labelMap[String(p.period || '')] = p.label || p.period || '';
    });
    var yCategories = order.map(function(k) { return labelMap[k] || k; });

    var byPeriod = {};
    (periods || []).forEach(function(p) { byPeriod[String(p.period || '')] = p; });

    var statePoints = [];
    var goldenPoints = [];
    var deathPoints = [];
    var maxDay = 5;

    order.forEach(function(k, idx) {
        var item = byPeriod[k];
        if (!item || !item.has_data) return;
        var ind = item[indicatorKey] || {};
        var rel = String(ind.relation || 'neutral').toLowerCase();
        var g = toNumber(ind.days_to_golden);
        var d = toNumber(ind.days_to_death);

        statePoints.push({
            value: [0, idx],
            relation: rel,
            periodLabel: item.label || k
        });

        if (g !== null) {
            goldenPoints.push({
                value: [g, idx],
                periodLabel: item.label || k,
                text: fmtDay(g)
            });
            if (g > maxDay) maxDay = g;
        }
        if (d !== null) {
            deathPoints.push({
                value: [d, idx],
                periodLabel: item.label || k,
                text: fmtDay(d)
            });
            if (d > maxDay) maxDay = d;
        }
    });

    maxDay = Math.min(Math.max(5, Math.ceil(maxDay + 2)), 260);

    return {
        animation: false,
        grid: {left: 74, right: 24, top: 38, bottom: 28},
        legend: {
            top: 4,
            data: ['当前状态', '预计金叉', '预计死叉'],
            textStyle: {fontSize: 11}
        },
        tooltip: {
            trigger: 'item',
            formatter: function(p) {
                var s = p.seriesName || '';
                var period = (p.data && p.data.periodLabel) ? p.data.periodLabel : '--';
                if (s === '当前状态') {
                    var rel = (p.data && p.data.relation) ? p.data.relation : 'neutral';
                    var relText = rel === 'golden' ? '金叉区间' : (rel === 'death' ? '死叉区间' : '粘合');
                    return period + '<br>' + title + ' 当前：' + relText;
                }
                var x = (p.data && p.data.value && p.data.value[0] != null) ? Number(p.data.value[0]).toFixed(1) + '天' : '--';
                return period + '<br>' + s + '：' + x;
            }
        },
        xAxis: {
            type: 'value',
            min: 0,
            max: maxDay,
            name: '预计天数',
            nameGap: 12,
            splitLine: {lineStyle: {opacity: 0.25}}
        },
        yAxis: {
            type: 'category',
            inverse: true,
            data: yCategories,
            axisLabel: {fontSize: 11}
        },
        series: [
            {
                name: '当前状态',
                type: 'scatter',
                symbol: 'circle',
                symbolSize: 12,
                data: statePoints,
                itemStyle: {
                    color: function(x) {
                        var rel = x.data && x.data.relation;
                        if (rel === 'golden') return '#16a34a';
                        if (rel === 'death') return '#dc2626';
                        return '#64748b';
                    }
                }
            },
            {
                name: '预计金叉',
                type: 'scatter',
                symbol: 'diamond',
                symbolSize: 14,
                data: goldenPoints,
                itemStyle: {color: '#22c55e'},
                label: {
                    show: true,
                    position: 'right',
                    formatter: function(p) { return p.data && p.data.text ? p.data.text : ''; },
                    fontSize: 10
                }
            },
            {
                name: '预计死叉',
                type: 'scatter',
                symbol: 'triangle',
                symbolSize: 14,
                data: deathPoints,
                itemStyle: {color: '#ef4444'},
                label: {
                    show: true,
                    position: 'right',
                    formatter: function(p) { return p.data && p.data.text ? p.data.text : ''; },
                    fontSize: 10
                }
            }
        ]
    };
}

function toNumber(v) {
    if (v === null || v === undefined || v === '') return null;
    var n = Number(v);
    if (Number.isNaN(n)) return null;
    return n;
}

function getChartTheme() {
    var root = document.documentElement;
    var theme = (root && root.getAttribute('data-theme')) || '';
    return theme === 'dark' ? 'dark' : null;
}

function renderKline(kline, code) {
    var el = document.getElementById('res-kline-chart');
    if (!el) return;
    if (resonanceKlineChart) {
        resonanceKlineChart.dispose();
    }
    resonanceKlineChart = echarts.init(el, getChartTheme());

    var dates = (kline && kline.dates) ? kline.dates : [];
    var ohlc = (kline && kline.ohlc) ? kline.ohlc : [];
    var vols = (kline && kline.volumes) ? kline.volumes : [];
    if (!dates.length || !ohlc.length) {
        resonanceKlineChart.setOption({
            title: {text: '暂无日线K线数据', left: 'center', top: 'middle', textStyle: {fontSize: 15}}
        });
        return;
    }

    var echartsOhlc = ohlc.map(function(x) { return [x[0], x[3], x[2], x[1]]; });
    var closes = ohlc.map(function(x) { return x[3]; });
    var ma5 = calcMA(closes, 5);
    var ma10 = calcMA(closes, 10);
    var ma20 = calcMA(closes, 20);
    var volBar = vols.map(function(v, i) {
        var up = ohlc[i][3] >= ohlc[i][0];
        return {
            value: v,
            itemStyle: {color: up ? '#ef5350' : '#26a69a'}
        };
    });

    var startPct = Math.max(0, 100 - 3000 / Math.max(120, dates.length) * 100);
    resonanceKlineChart.setOption({
        animation: false,
        title: {
            text: (code || '') + ' 日线结构',
            left: 8,
            top: 2,
            textStyle: {fontSize: 13}
        },
        legend: {
            data: ['K线', 'MA5', 'MA10', 'MA20', '成交量'],
            right: 8,
            top: 2,
            textStyle: {fontSize: 11}
        },
        tooltip: {trigger: 'axis', axisPointer: {type: 'cross'}},
        axisPointer: {link: [{xAxisIndex: 'all'}]},
        grid: [
            {left: '8%', right: '4%', top: '10%', height: '58%'},
            {left: '8%', right: '4%', top: '72%', height: '18%'}
        ],
        xAxis: [
            {type: 'category', data: dates, scale: true, boundaryGap: false, axisLine: {onZero: false}, axisLabel: {show: false}, min: 'dataMin', max: 'dataMax'},
            {type: 'category', data: dates, gridIndex: 1, axisLabel: {fontSize: 11}, axisLine: {onZero: false}, axisTick: {show: false}}
        ],
        yAxis: [
            {scale: true, splitLine: {lineStyle: {opacity: 0.3}}},
            {gridIndex: 1, scale: true, splitLine: {lineStyle: {opacity: 0.25}}, axisLabel: {formatter: function(v) { return (v / 100).toFixed(0); }}}
        ],
        dataZoom: [
            {type: 'inside', xAxisIndex: [0, 1], start: startPct, end: 100},
            {type: 'slider', xAxisIndex: [0, 1], start: startPct, end: 100, height: 16, bottom: 2}
        ],
        series: [
            {name: 'K线', type: 'candlestick', data: echartsOhlc, itemStyle: {color: '#ef5350', color0: '#26a69a', borderColor: '#ef5350', borderColor0: '#26a69a'}},
            {name: 'MA5', type: 'line', data: ma5, symbol: 'none', smooth: true, lineStyle: {width: 1, color: '#f59e0b'}},
            {name: 'MA10', type: 'line', data: ma10, symbol: 'none', smooth: true, lineStyle: {width: 1, color: '#3b82f6'}},
            {name: 'MA20', type: 'line', data: ma20, symbol: 'none', smooth: true, lineStyle: {width: 1, color: '#8b5cf6'}},
            {name: '成交量', type: 'bar', xAxisIndex: 1, yAxisIndex: 1, data: volBar}
        ]
    });
}

function calcMA(data, period) {
    var out = [];
    for (var i = 0; i < data.length; i++) {
        if (i < period - 1) {
            out.push(null);
            continue;
        }
        var sum = 0;
        for (var j = 0; j < period; j++) sum += data[i - j];
        out.push(sum / period);
    }
    return out;
}

function escapeHtml(text) {
    var div = document.createElement('div');
    div.textContent = String(text || '');
    return div.innerHTML;
}

syncWallModeControls();

window.addEventListener('resize', function() {
    if (resonanceKlineChart) resonanceKlineChart.resize();
    if (resonanceMacdTimelineChart) resonanceMacdTimelineChart.resize();
    if (resonanceKdjTimelineChart) resonanceKdjTimelineChart.resize();
    resonanceWallCharts.forEach(function(c) {
        if (c && c.resize) c.resize();
    });
});
