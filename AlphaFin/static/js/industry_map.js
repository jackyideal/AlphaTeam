/* 行业时空图谱 */

var industryMapChart = null;
var lastIndustryPayload = null;
var industryImmersive = false;
var industryXWindowTouched = false;

function getIndustryChartTheme() {
    if (typeof getChartTheme === 'function') {
        return getChartTheme();
    }
    return null;
}

function loadIndustryMap() {
    var level = (document.getElementById('im-level').value || 'L1').trim();
    var startDate = (document.getElementById('im-start-date').value || '2024-09-24').replace(/-/g, '');
    var endDate = (document.getElementById('im-end-date').value || '').replace(/-/g, '');
    var xMetric = (document.getElementById('im-x-metric').value || 'pct_change').trim();
    var xStartDate = (document.getElementById('im-x-start-date').value || '').replace(/-/g, '');
    var xEndDate = (document.getElementById('im-x-end-date').value || '').replace(/-/g, '');
    if (xMetric === 'x_interval_return') {
        if (!xStartDate) xStartDate = startDate;
        if (!xEndDate) xEndDate = endDate || startDate;
    }
    var bubbleMetric = (document.getElementById('im-bubble-metric').value || 'amount').trim();
    var targetCode = (document.getElementById('im-target-code').value || '600425.SH').trim().toUpperCase();
    var btn = document.getElementById('im-query-btn');

    btn.disabled = true;
    btn.textContent = '加载中...';
    setIndustryMapStatus('正在拉取行业数据并构建图谱...', 'loading');

    fetch('/api/stock/industry_map', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            level: level,
            start_date: startDate,
            end_date: endDate,
            x_metric: xMetric,
            x_start_date: xStartDate,
            x_end_date: xEndDate,
            bubble_metric: bubbleMetric,
            target_code: targetCode
        })
    }).then(function (resp) {
        return resp.json();
    }).then(function (payload) {
        lastIndustryPayload = payload || {};
        if (!payload || !payload.ok) {
            setIndustryMapStatus('数据获取失败：' + escapeHtml((payload && payload.message) || '未知错误'), 'error');
            renderIndustryEmpty();
            return;
        }
        renderIndustryMap(payload);
        setIndustryMapStatus('图谱生成完成' + (payload.cache_hit ? '（命中缓存）' : ''), 'ok');
    }).catch(function (err) {
        setIndustryMapStatus('请求失败：' + String(err || 'unknown error'), 'error');
        renderIndustryEmpty();
    }).finally(function () {
        btn.disabled = false;
        btn.textContent = '生成图谱';
    });
}

function onXMetricChanged() {
    var metric = (document.getElementById('im-x-metric').value || '').trim();
    var wrapStart = document.getElementById('im-x-start-wrap');
    var wrapEnd = document.getElementById('im-x-end-wrap');
    var xStartInput = document.getElementById('im-x-start-date');
    var xEndInput = document.getElementById('im-x-end-date');
    var startInput = document.getElementById('im-start-date');
    var endInput = document.getElementById('im-end-date');
    var show = metric === 'x_interval_return';
    if (wrapStart) wrapStart.style.display = show ? 'flex' : 'none';
    if (wrapEnd) wrapEnd.style.display = show ? 'flex' : 'none';
    if (show && !industryXWindowTouched) {
        if (xStartInput && startInput && !xStartInput.value) xStartInput.value = startInput.value || '';
        if (xEndInput && endInput && !xEndInput.value) xEndInput.value = endInput.value || '';
    }
}

function toggleIndustryImmersive(forceState) {
    var page = document.querySelector('.industry-map-page');
    var btn = document.getElementById('im-immersive-btn');
    if (!page || !btn) return;

    var enable = (typeof forceState === 'boolean') ? forceState : !page.classList.contains('immersive-mode');
    industryImmersive = !!enable;
    page.classList.toggle('immersive-mode', industryImmersive);
    btn.textContent = industryImmersive ? '退出巨屏' : '巨屏模式';
    btn.title = industryImmersive ? 'Esc 也可退出巨屏模式' : '隐藏右侧栏与表格，图谱全宽沉浸显示';

    if (industryImmersive) {
        try {
            page.scrollIntoView({behavior: 'smooth', block: 'start'});
        } catch (e) {
        }
    }

    setTimeout(function () {
        if (industryMapChart) {
            industryMapChart.resize();
        }
    }, 120);
}

function renderIndustryMap(payload) {
    renderIndustryKPI(payload);
    renderIndustryChart(payload);
    renderTargetCard(payload.target || null, payload);
    renderSimilarList(payload.similar_industries || []);
    renderIndustryTable(payload.points || []);
}

function renderIndustryEmpty() {
    var body = document.getElementById('im-table-body');
    if (body) {
        body.innerHTML = '<tr><td colspan="9" class="res-empty-cell">暂无数据</td></tr>';
    }
    var target = document.getElementById('im-target-card');
    if (target) target.innerHTML = '<p class="muted">暂无定位数据</p>';
    var similar = document.getElementById('im-similar-list');
    if (similar) similar.innerHTML = '<p class="muted">暂无相似行业</p>';
    var kpiCount = document.getElementById('im-kpi-count');
    if (kpiCount) kpiCount.textContent = '--';
    var kpiWindow = document.getElementById('im-kpi-window');
    if (kpiWindow) kpiWindow.textContent = '--';
    var kpiMedian = document.getElementById('im-kpi-median');
    if (kpiMedian) kpiMedian.textContent = 'X: -- / Y: --';
    var detail = document.getElementById('im-window-detail');
    if (detail) detail.textContent = '等待查询...';
}

function setIndustryMapStatus(text, level) {
    var el = document.getElementById('im-status');
    if (!el) return;
    el.textContent = '状态：' + String(text || '--');
    el.classList.remove('state-idle', 'state-loading', 'state-ok', 'state-error');
    el.classList.add('state-' + (level || 'idle'));
}

function renderIndustryKPI(payload) {
    var count = document.getElementById('im-kpi-count');
    var windowText = document.getElementById('im-kpi-window');
    var medianText = document.getElementById('im-kpi-median');
    var detail = document.getElementById('im-window-detail');
    var win = payload.window || {};
    var stats = payload.stats || {};

    if (count) count.textContent = String(payload.industry_count || 0);
    if (windowText) {
        windowText.textContent = (fmtDate(win.start_trade_date) + ' ~ ' + fmtDate(win.end_trade_date));
    }
    if (medianText) {
        medianText.textContent = 'X: ' + fmtNum(stats.x_median, 2) + ' / Y: ' + fmtNum(stats.y_median, 2) + '%';
    }
    if (detail) {
        var metricMeta = payload.metric_meta || {};
        var xMeta = metricMeta[payload.x_metric] || {label: payload.x_metric || 'X轴'};
        detail.textContent = '采样窗口：' + fmtDate(win.start_trade_date) + ' ~ ' + fmtDate(win.end_trade_date) +
            ' ｜ 坐标轴：X=' + escapeHtml(xMeta.label || 'X轴') + '，Y=区间累计涨幅(%)' +
            ' ｜ 数据源：' + escapeHtml(payload.universe_source || '--');
        if (payload.x_metric === 'x_interval_return' && payload.x_window) {
            detail.textContent += ' ｜ X区间：' +
                fmtDate(payload.x_window.start_trade_date) + ' ~ ' + fmtDate(payload.x_window.end_trade_date);
        }
    }
}

function renderIndustryChart(payload) {
    var el = document.getElementById('industry-map-chart');
    if (!el || typeof echarts === 'undefined') return;

    if (industryMapChart) {
        try {
            industryMapChart.dispose();
        } catch (e) {
        }
    }
    industryMapChart = echarts.init(el, getIndustryChartTheme());

    var points = payload.points || [];
    var industries = points.filter(function (p) {
        return !p.is_target && isFiniteNumber(p.x_value) && isFiniteNumber(p.cum_return);
    });
    var target = null;
    for (var i = 0; i < points.length; i++) {
        if (points[i].is_target) {
            target = points[i];
            break;
        }
    }

    var yValues = industries.map(function (p) {
        return Number(p.cum_return);
    });
    var xValues = industries.map(function (p) {
        return Number(p.x_value);
    });
    if (target && isFiniteNumber(target.x_value)) {
        xValues.push(Number(target.x_value));
    }
    if (target && isFiniteNumber(target.cum_return)) {
        yValues.push(Number(target.cum_return));
    }

    var xMin = xValues.length ? Math.min.apply(null, xValues) : -5;
    var xMax = xValues.length ? Math.max.apply(null, xValues) : 5;
    var yMin = yValues.length ? Math.min.apply(null, yValues) : -5;
    var yMax = yValues.length ? Math.max.apply(null, yValues) : 5;

    if (xMin === xMax) {
        xMin -= 1;
        xMax += 1;
    }
    if (yMin === yMax) {
        yMin -= 1;
        yMax += 1;
    }

    var xPad = (xMax - xMin) * 0.08;
    var yPad = (yMax - yMin) * 0.08;
    var xAxisMin = xMin - xPad;
    var xAxisMax = xMax + xPad;
    var yAxisMin = yMin - yPad;
    var yAxisMax = yMax + yPad;
    var xMid = isFiniteNumber(payload.stats && payload.stats.x_median) ? Number(payload.stats.x_median) : null;
    var yMid = isFiniteNumber(payload.stats && payload.stats.y_median) ? Number(payload.stats.y_median) : null;

    var metricMeta = payload.metric_meta || {};
    var xMeta = metricMeta[payload.x_metric] || {label: payload.x_metric || 'X'};
    var bubbleMeta = metricMeta[payload.bubble_metric] || {label: payload.bubble_metric || '气泡'};
    var xIsPct = (xMeta.fmt === 'pct');

    var labelMap = {};
    industries.slice().sort(function (a, b) {
        return Number(b.cum_return || -99999) - Number(a.cum_return || -99999);
    }).slice(0, 10).forEach(function (p) {
        labelMap[String(p.ts_code || '')] = true;
    });

    var industrySeries = industries.map(function (p) {
        return {
            name: p.name || p.ts_code,
            value: [Number(p.x_value), Number(p.cum_return), Number(p.bubble_size || 20)],
            raw: p,
            showLabel: !!labelMap[String(p.ts_code || '')]
        };
    });

    var series = [{
        name: '行业',
        type: 'scatter',
        data: industrySeries,
        symbolSize: function (val) {
            return Math.max(12, Math.min(62, Number(val[2] || 20)));
        },
        emphasis: {
            focus: 'series',
            scale: true,
            itemStyle: {
                borderWidth: 2.5,
                shadowBlur: 20,
                shadowColor: 'rgba(15, 23, 42, 0.28)'
            },
            label: {
                show: true,
                formatter: function (p) {
                    return p.name;
                },
                color: '#0f172a',
                fontSize: 12,
                fontWeight: 700,
                backgroundColor: 'rgba(255,255,255,0.86)',
                borderRadius: 8,
                padding: [3, 6]
            }
        },
        label: {
            show: true,
            position: 'top',
            distance: 6,
            formatter: function (p) {
                var r = (p.data && p.data.raw) || {};
                return p.data && p.data.showLabel ? (r.name || '') : '';
            },
            color: '#334155',
            fontSize: 11
        },
        itemStyle: {
            opacity: 0.9,
            borderColor: '#ffffff',
            borderWidth: 1.25,
            shadowBlur: 10,
            shadowColor: 'rgba(30, 41, 59, 0.14)'
        },
        markLine: {
            silent: true,
            symbol: ['none', 'none'],
            label: {
                show: true,
                formatter: '{b}',
                color: '#475569',
                fontSize: 11
            },
            lineStyle: {type: 'dashed', color: '#94a3b8', width: 1.2}
        }
    }];

    if (xMid !== null && yMid !== null) {
        series[0].markArea = {
            silent: true,
            itemStyle: {opacity: 0.08},
            data: [
                [{xAxis: xAxisMin, yAxis: yMid}, {xAxis: xMid, yAxis: yAxisMax, itemStyle: {color: '#38bdf8'}}],
                [{xAxis: xMid, yAxis: yMid}, {xAxis: xAxisMax, yAxis: yAxisMax, itemStyle: {color: '#22c55e'}}],
                [{xAxis: xAxisMin, yAxis: yAxisMin}, {xAxis: xMid, yAxis: yMid, itemStyle: {color: '#f59e0b'}}],
                [{xAxis: xMid, yAxis: yAxisMin}, {xAxis: xAxisMax, yAxis: yMid, itemStyle: {color: '#fb7185'}}]
            ]
        };
    }

    var mlData = [];
    if (xMid !== null) {
        mlData.push({xAxis: xMid, name: 'X中位数'});
    }
    if (yMid !== null) {
        mlData.push({yAxis: yMid, name: 'Y中位数'});
    }
    series[0].markLine.data = mlData;

    if (target && isFiniteNumber(target.x_value) && isFiniteNumber(target.cum_return)) {
        series.push({
            name: target.name || '目标个股',
            type: 'scatter',
            data: [{
                name: target.name || target.ts_code,
                value: [Number(target.x_value), Number(target.cum_return), Number(target.bubble_size || 42)],
                raw: target
            }],
            symbol: 'pin',
            symbolSize: function (val) {
                return Math.max(32, Number(val[2] || 46));
            },
            z: 5,
            itemStyle: {
                color: '#dc2626',
                borderColor: '#fff7ed',
                borderWidth: 3,
                shadowColor: 'rgba(220,38,38,0.36)',
                shadowBlur: 18
            },
            label: {
                show: true,
                position: 'top',
                formatter: function (p) {
                    return p.name;
                },
                color: '#991b1b',
                fontWeight: 800,
                fontSize: 12
            }
        });

        series.push({
            name: '目标脉冲',
            type: 'effectScatter',
            z: 6,
            rippleEffect: {
                scale: 5.5,
                brushType: 'stroke'
            },
            symbolSize: 14,
            itemStyle: {
                color: '#ef4444'
            },
            tooltip: {show: false},
            data: [{
                value: [Number(target.x_value), Number(target.cum_return)]
            }]
        });
    }

    industryMapChart.setOption({
        animationDuration: 900,
        animationEasing: 'cubicOut',
        grid: {left: 84, right: 48, top: 86, bottom: 86},
        legend: {
            top: 14,
            textStyle: {fontSize: 12, color: '#475569'},
            data: series.filter(function (s) {
                return String(s.name || '') !== '目标脉冲';
            }).map(function (s) {
                return s.name;
            })
        },
        graphic: (xMid !== null && yMid !== null) ? [
            {
                type: 'text',
                left: '16%',
                top: 80,
                style: {text: '低X / 高Y', fill: 'rgba(30,64,175,0.52)', font: '600 12px sans-serif'}
            },
            {
                type: 'text',
                right: '19%',
                top: 80,
                style: {text: '高X / 高Y', fill: 'rgba(22,101,52,0.52)', font: '600 12px sans-serif'}
            },
            {
                type: 'text',
                left: '16%',
                bottom: 48,
                style: {text: '低X / 低Y', fill: 'rgba(146,64,14,0.52)', font: '600 12px sans-serif'}
            },
            {
                type: 'text',
                right: '19%',
                bottom: 48,
                style: {text: '高X / 低Y', fill: 'rgba(157,23,77,0.5)', font: '600 12px sans-serif'}
            }
        ] : [],
        tooltip: {
            trigger: 'item',
            borderWidth: 0,
            backgroundColor: 'rgba(15,23,42,0.94)',
            textStyle: {color: '#e2e8f0', fontSize: 12},
            formatter: function (params) {
                var r = (params.data && params.data.raw) || {};
                var rows = [
                    '<div style="font-weight:700;margin-bottom:6px;">' + escapeHtml((r.name || '--') + '  ' + (r.ts_code || '')) + '</div>',
                    '<div>区间累计涨幅：<b>' + fmtNum(r.cum_return, 2) + '%</b></div>',
                    '<div>' + escapeHtml(xMeta.label || 'X轴') + '：<b>' + fmtNum(r.x_value, 2) + (xIsPct ? '%' : '') + '</b></div>',
                    '<div>' + escapeHtml(bubbleMeta.label || '气泡') + '：<b>' + fmtLarge(r.bubble_value) + '</b></div>',
                    '<div>最近交易日涨跌幅：' + fmtNum(r.pct_change, 2) + '%</div>',
                    '<div>PE / PB：' + fmtNum(r.pe, 2) + ' / ' + fmtNum(r.pb, 2) + '</div>',
                    '<div>总市值 / 流通市值：' + fmtLarge(r.total_mv) + ' / ' + fmtLarge(r.float_mv) + '</div>'
                ];
                if (r.close !== null && r.close !== undefined) {
                    rows.push('<div>收盘价：' + fmtNum(r.close, 2) + '</div>');
                }
                if (r.trade_date) {
                    rows.push('<div style="color:#94a3b8;margin-top:5px;">日期：' + fmtDate(r.trade_date) + '</div>');
                }
                return rows.join('');
            }
        },
        dataZoom: [
            {type: 'inside', xAxisIndex: 0, filterMode: 'none'},
            {type: 'inside', yAxisIndex: 0, filterMode: 'none'},
            {type: 'slider', xAxisIndex: 0, height: 18, bottom: 18, borderColor: 'rgba(148,163,184,0.3)'}
        ],
        xAxis: {
            name: xMeta.label || 'X轴',
            nameLocation: 'center',
            nameGap: 30,
            type: 'value',
            min: xAxisMin,
            max: xAxisMax,
            nameTextStyle: {color: '#0f172a', fontSize: 13, fontWeight: 700},
            axisLine: {lineStyle: {color: '#334155', width: 1.8}},
            axisTick: {show: true, lineStyle: {color: '#64748b'}},
            axisLabel: {
                color: '#334155',
                formatter: function (v) {
                    if (xIsPct) return v.toFixed(1) + '%';
                    return shortNumber(v);
                }
            },
            splitLine: {lineStyle: {color: 'rgba(100,116,139,0.28)', width: 1}}
        },
        yAxis: {
            name: '区间累计涨幅(%)',
            nameLocation: 'center',
            nameGap: 56,
            type: 'value',
            min: yAxisMin,
            max: yAxisMax,
            nameTextStyle: {color: '#0f172a', fontSize: 13, fontWeight: 700},
            axisLine: {lineStyle: {color: '#334155', width: 1.8}},
            axisTick: {show: true, lineStyle: {color: '#64748b'}},
            axisLabel: {formatter: '{value}%', color: '#334155'},
            splitLine: {lineStyle: {color: 'rgba(100,116,139,0.28)', width: 1}}
        },
        visualMap: {
            show: false,
            dimension: 1,
            seriesIndex: 0,
            min: yAxisMin,
            max: yAxisMax,
            inRange: {
                color: ['#1e40af', '#0ea5e9', '#22c55e', '#f59e0b', '#dc2626']
            }
        },
        series: series
    });

    window.addEventListener('resize', function () {
        if (industryMapChart) industryMapChart.resize();
    });
}

function renderTargetCard(target, payload) {
    var box = document.getElementById('im-target-card');
    if (!box) return;
    if (!target) {
        box.innerHTML = '<p class="muted">暂无个股定位数据</p>';
        return;
    }

    var xMetric = payload && payload.x_metric;
    var meta = (payload && payload.metric_meta && payload.metric_meta[xMetric]) || {label: 'X轴'};
    var xSuffix = meta.fmt === 'pct' ? '%' : '';
    var biasClass = Number(target.cum_return || 0) >= 0 ? 'up' : 'down';
    var xMedian = payload && payload.stats ? Number(payload.stats.x_median) : NaN;
    var targetX = Number(target.x_value);
    var hasTargetX = Number.isFinite(targetX);
    var hasMedian = Number.isFinite(xMedian);

    var industryXValues = ((payload && payload.points) || []).filter(function (p) {
        return !p.is_target && isFiniteNumber(p.x_value);
    }).map(function (p) {
        return Number(p.x_value);
    });

    var xRankText = '--';
    if (hasTargetX && industryXValues.length) {
        var higherCount = industryXValues.filter(function (v) {
            return v > targetX;
        }).length;
        var rank = higherCount + 1;
        var total = industryXValues.length + 1; // include target itself
        xRankText = rank + ' / ' + total + '（高→低）';
    }

    var xDeltaText = '--';
    if (hasTargetX && hasMedian) {
        var delta = targetX - xMedian;
        var sign = delta > 0 ? '+' : '';
        xDeltaText = sign + fmtNum(delta, 2) + xSuffix;
    }

    box.innerHTML = '' +
        '<div class="target-name">' + escapeHtml((target.name || '--') + ' (' + (target.ts_code || '--') + ')') + '</div>' +
        '<div class="target-tagline">定位日期：' + escapeHtml(fmtDate(target.trade_date || '')) + '</div>' +
        '<div class="target-metric-row">' +
            '<div><span>区间累计涨幅</span><b class="' + biasClass + '">' + fmtNum(target.cum_return, 2) + '%</b></div>' +
            '<div><span>当日涨跌幅</span><b>' + fmtNum(target.pct_change, 2) + '%</b></div>' +
        '</div>' +
        '<div class="target-metric-row">' +
            '<div><span>' + escapeHtml(meta.label || 'X轴') + '</span><b>' + fmtNum(target.x_value, 2) + xSuffix + '</b></div>' +
            '<div><span>收盘价</span><b>' + fmtNum(target.close, 2) + '</b></div>' +
        '</div>' +
        '<div class="target-metric-row">' +
            '<div><span>X轴排名</span><b>' + xRankText + '</b></div>' +
            '<div><span>X轴中位数</span><b>' + (hasMedian ? (fmtNum(xMedian, 2) + xSuffix) : '--') + '</b></div>' +
        '</div>' +
        '<div class="target-metric-row">' +
            '<div><span>X轴相对中位数</span><b>' + xDeltaText + '</b></div>' +
            '<div><span>对照个股X值</span><b>' + fmtNum(target.x_value, 2) + xSuffix + '</b></div>' +
        '</div>' +
        '<div class="target-metric-row">' +
            '<div><span>PE / PB</span><b>' + fmtNum(target.pe, 2) + ' / ' + fmtNum(target.pb, 2) + '</b></div>' +
            '<div><span>总市值</span><b>' + fmtLarge(target.total_mv) + '</b></div>' +
        '</div>';
}

function renderSimilarList(items) {
    var box = document.getElementById('im-similar-list');
    if (!box) return;
    if (!items || !items.length) {
        box.innerHTML = '<p class="muted">暂无可比行业（可尝试切换层级或日期区间）</p>';
        return;
    }
    var html = ['<ol>'];
    items.forEach(function (it) {
        html.push(
            '<li>' +
                '<div class="sim-head">' +
                    '<strong>' + escapeHtml((it.name || '--') + ' ' + (it.ts_code || '')) + '</strong>' +
                    '<span>相似度 ' + fmtNum(it.score, 2) + '</span>' +
                '</div>' +
                '<div class="sim-sub">距离: ' + fmtNum(it.distance, 4) +
                    ' ｜ 依据: ' + escapeHtml((it.used_feature_labels || []).join('、')) + '</div>' +
            '</li>'
        );
    });
    html.push('</ol>');
    box.innerHTML = html.join('');
}

function renderIndustryTable(points) {
    var body = document.getElementById('im-table-body');
    if (!body) return;

    var industries = (points || []).filter(function (p) {
        return !p.is_target;
    });
    industries.sort(function (a, b) {
        var av = Number(a.cum_return || -99999);
        var bv = Number(b.cum_return || -99999);
        return bv - av;
    });

    var target = null;
    for (var i = 0; i < (points || []).length; i++) {
        if (points[i].is_target) {
            target = points[i];
            break;
        }
    }

    var rows = [];
    if (target) {
        rows.push(buildTableRow(target, true));
    }
    var maxRows = Math.min(industries.length, 200);
    for (var j = 0; j < maxRows; j++) {
        rows.push(buildTableRow(industries[j], false));
    }
    if (!rows.length) {
        rows.push('<tr><td colspan="9" class="res-empty-cell">暂无数据</td></tr>');
    }
    body.innerHTML = rows.join('');
}

function buildTableRow(row, isTarget) {
    var cls = isTarget ? ' class="target-row"' : '';
    var name = escapeHtml(row.name || '--');
    var code = escapeHtml(row.ts_code || '--');
    return '' +
        '<tr' + cls + '>' +
            '<td>' + (isTarget ? '⭐ ' : '') + name + '</td>' +
            '<td>' + code + '</td>' +
            '<td>' + fmtNum(row.cum_return, 2) + '</td>' +
            '<td>' + fmtNum(row.pct_change, 2) + '</td>' +
            '<td>' + fmtNum(row.pe, 2) + '</td>' +
            '<td>' + fmtNum(row.pb, 2) + '</td>' +
            '<td>' + fmtLarge(row.total_mv) + '</td>' +
            '<td>' + fmtLarge(row.float_mv) + '</td>' +
            '<td>' + fmtLarge(row.amount) + '</td>' +
        '</tr>';
}

function fmtNum(v, digits) {
    if (v === null || v === undefined || v === '') return '--';
    var n = Number(v);
    if (!Number.isFinite(n)) return '--';
    return n.toFixed(digits == null ? 2 : digits);
}

function fmtLarge(v) {
    if (v === null || v === undefined || v === '') return '--';
    var n = Number(v);
    if (!Number.isFinite(n)) return '--';
    var abs = Math.abs(n);
    if (abs >= 100000000) return (n / 100000000).toFixed(2) + '亿';
    if (abs >= 10000) return (n / 10000).toFixed(2) + '万';
    return n.toFixed(2);
}

function shortNumber(v) {
    var n = Number(v);
    if (!Number.isFinite(n)) return '--';
    var abs = Math.abs(n);
    if (abs >= 100000000) return (n / 100000000).toFixed(1) + '亿';
    if (abs >= 10000) return (n / 10000).toFixed(1) + '万';
    return n.toFixed(1);
}

function fmtDate(v) {
    var s = String(v || '');
    if (s.length === 8) return s.slice(0, 4) + '-' + s.slice(4, 6) + '-' + s.slice(6, 8);
    return s || '--';
}

function isFiniteNumber(v) {
    var n = Number(v);
    return Number.isFinite(n);
}

function escapeHtml(str) {
    return String(str || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

document.addEventListener('DOMContentLoaded', function () {
    var startInput = document.getElementById('im-start-date');
    var endInput = document.getElementById('im-end-date');
    var xStartInput = document.getElementById('im-x-start-date');
    var xEndInput = document.getElementById('im-x-end-date');
    if (endInput && !endInput.value) {
        var now = new Date();
        var month = String(now.getMonth() + 1).padStart(2, '0');
        var day = String(now.getDate()).padStart(2, '0');
        endInput.value = now.getFullYear() + '-' + month + '-' + day;
    }
    if (xStartInput && !xStartInput.value && startInput) xStartInput.value = startInput.value || '';
    if (xEndInput && !xEndInput.value && endInput) xEndInput.value = endInput.value || '';
    if (xStartInput) {
        xStartInput.addEventListener('input', function () { industryXWindowTouched = true; });
        xStartInput.addEventListener('change', function () { industryXWindowTouched = true; });
    }
    if (xEndInput) {
        xEndInput.addEventListener('input', function () { industryXWindowTouched = true; });
        xEndInput.addEventListener('change', function () { industryXWindowTouched = true; });
    }
    onXMetricChanged();
    document.addEventListener('keydown', function (e) {
        if (e && e.key === 'Escape' && industryImmersive) {
            toggleIndustryImmersive(false);
        }
    });
    loadIndustryMap();
});
