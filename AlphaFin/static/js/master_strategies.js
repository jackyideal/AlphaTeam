/* ═══════════════ 大师选股策略 — 前端逻辑 ═══════════════ */

var masterDataLoaded = false;
var strategies = [];
var activeStrategyId = null;
var strategyResults = {};  // 缓存各策略结果

// ── 第一阶段：加载全市场数据 ──
function loadMasterData() {
    var btn = document.getElementById('btn-load-data');
    var statusEl = document.getElementById('load-status');
    btn.disabled = true;
    btn.textContent = '加载中...';
    statusEl.textContent = '';

    // 显示进度条
    var progressSection = document.getElementById('load-progress');
    progressSection.style.display = 'block';
    document.getElementById('progress-bar').style.width = '0%';
    document.getElementById('progress-text').textContent = '正在初始化...';

    fetch('/api/master/load', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: '{}'
    })
    .then(function(res) { return res.json(); })
    .then(function(data) {
        listenLoadProgress(data.task_id);
    })
    .catch(function(err) {
        document.getElementById('progress-text').textContent = '请求失败: ' + err;
        btn.disabled = false;
        btn.textContent = '加载全市场数据';
    });
}

function listenLoadProgress(taskId) {
    var evtSource = new EventSource('/api/progress/' + taskId);
    var progressBar = document.getElementById('progress-bar');
    var progressText = document.getElementById('progress-text');
    var loadStartTime = Date.now();
    var lastStep = 0;

    evtSource.onmessage = function(event) {
        var info = JSON.parse(event.data);
        var step = info.step || 0;
        var total = info.total || 6;
        var percent = total > 0 ? Math.round((step / total) * 100) : 0;
        progressBar.style.width = percent + '%';

        // 计算预估剩余时间
        var timeStr = '';
        if (step > 0 && step < total) {
            var elapsed = (Date.now() - loadStartTime) / 1000;
            var perStep = elapsed / step;
            var remaining = Math.round(perStep * (total - step));
            if (remaining >= 60) {
                timeStr = ' · 预计剩余 ' + Math.floor(remaining / 60) + '分' + (remaining % 60) + '秒';
            } else {
                timeStr = ' · 预计剩余 ' + remaining + '秒';
            }
        }

        progressText.textContent = info.message + ' (' + step + '/' + total + ')' + timeStr;
        lastStep = step;

        if (info.done) {
            evtSource.close();
            progressBar.style.width = '100%';
            var totalElapsed = Math.round((Date.now() - loadStartTime) / 1000);
            progressText.textContent = info.message + '（共耗时 ' + totalElapsed + '秒）';

            var btn = document.getElementById('btn-load-data');
            btn.textContent = '重新加载数据';
            btn.disabled = false;
            document.getElementById('load-status').textContent = '数据就绪';

            masterDataLoaded = true;
            loadStrategiesInfo();
        }
    };

    evtSource.onerror = function() {
        evtSource.close();
        var btn = document.getElementById('btn-load-data');
        btn.disabled = false;
        btn.textContent = '加载全市场数据';
    };
}

// ── 加载策略列表并渲染卡片 ──
function loadStrategiesInfo() {
    fetch('/api/master/strategies')
    .then(function(res) { return res.json(); })
    .then(function(data) {
        strategies = data;
        renderStrategyCards(data);
    });
}

function renderStrategyCards(strategyList) {
    var grid = document.getElementById('strategy-grid');
    grid.innerHTML = '';

    for (var i = 0; i < strategyList.length; i++) {
        var s = strategyList[i];
        var card = document.createElement('div');
        card.className = 'strategy-card';
        card.setAttribute('data-id', s.id);
        card.onclick = (function(sid) {
            return function() { runStrategy(sid); };
        })(s.id);

        card.innerHTML =
            '<div class="card-name">' + (i + 1) + '. ' + s.name + '</div>' +
            '<div class="card-desc">' + s.description + '</div>' +
            '<div class="card-count" id="count-' + s.id + '"></div>' +
            '<div class="card-spinner">计算中...</div>';

        grid.appendChild(card);
    }

    document.getElementById('strategy-cards').style.display = 'block';
    document.getElementById('backtest-section').style.display = 'block';
}

// ── 第二阶段：运行单个策略 ──
function runStrategy(strategyId) {
    if (!masterDataLoaded) {
        alert('请先加载数据');
        return;
    }

    // 如果有缓存结果，直接显示
    if (strategyResults[strategyId]) {
        activeStrategyId = strategyId;
        highlightCard(strategyId);
        displayResult(strategyResults[strategyId]);
        return;
    }

    // 标记为计算中
    var cards = document.querySelectorAll('.strategy-card');
    for (var i = 0; i < cards.length; i++) {
        if (cards[i].getAttribute('data-id') === strategyId) {
            cards[i].classList.add('computing');
        }
    }

    activeStrategyId = strategyId;
    highlightCard(strategyId);

    fetch('/api/master/run', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({strategy_id: strategyId})
    })
    .then(function(res) { return res.json(); })
    .then(function(result) {
        // 取消计算状态
        var cards = document.querySelectorAll('.strategy-card');
        for (var i = 0; i < cards.length; i++) {
            if (cards[i].getAttribute('data-id') === strategyId) {
                cards[i].classList.remove('computing');
                cards[i].classList.add('computed');
            }
        }

        // 更新卡片上的数量
        var countEl = document.getElementById('count-' + strategyId);
        if (countEl) {
            countEl.textContent = '筛出 ' + result.count + ' 只股票';
        }

        // 缓存结果
        strategyResults[strategyId] = result;

        // 如果当前还是这个策略，显示结果
        if (activeStrategyId === strategyId) {
            displayResult(result);
        }
    })
    .catch(function(err) {
        var cards = document.querySelectorAll('.strategy-card');
        for (var i = 0; i < cards.length; i++) {
            if (cards[i].getAttribute('data-id') === strategyId) {
                cards[i].classList.remove('computing');
            }
        }
        alert('策略执行失败: ' + err);
    });
}

function highlightCard(strategyId) {
    var cards = document.querySelectorAll('.strategy-card');
    for (var i = 0; i < cards.length; i++) {
        if (cards[i].getAttribute('data-id') === strategyId) {
            cards[i].classList.add('active');
        } else {
            cards[i].classList.remove('active');
        }
    }
}

// ── 渲染结果表格 ──
function displayResult(result) {
    var section = document.getElementById('result-section');

    if (result.error) {
        section.style.display = 'block';
        document.getElementById('result-title').textContent = result.strategy_name || '策略结果';
        document.getElementById('result-count').textContent = '执行失败: ' + result.error;
        document.getElementById('result-table').innerHTML = '';
        return;
    }

    var strategyName = result.strategy_name || '策略结果';
    document.getElementById('result-title').textContent = strategyName;
    document.getElementById('result-count').textContent =
        result.count > 0 ? '共筛出 ' + result.count + ' 只股票' : '未筛选到符合条件的股票';

    var table = document.getElementById('result-table');
    table.innerHTML = '';

    if (!result.columns || result.columns.length === 0 || !result.data || result.data.length === 0) {
        table.innerHTML = '<tr><td style="padding:40px; text-align:center; color:#999; font-size:16px;">当前无符合条件的股票</td></tr>';
        section.style.display = 'block';
        return;
    }

    // 表头
    var thead = '<thead><tr><th>#</th>';
    for (var i = 0; i < result.columns.length; i++) {
        thead += '<th>' + result.columns[i] + '</th>';
    }
    thead += '</tr></thead>';

    // 表体
    var tbody = '<tbody>';
    for (var r = 0; r < result.data.length; r++) {
        var row = result.data[r];
        tbody += '<tr>';
        tbody += '<td>' + (r + 1) + '</td>';
        for (var c = 0; c < result.columns.length; c++) {
            var col = result.columns[c];
            var val = row[col];

            if (col === '代码' && val) {
                // 股票代码做成可点击链接
                tbody += '<td><a class="stock-link" href="/stock?code=' + val + '" title="查看个股分析">' + val + '</a></td>';
            } else if (val === null || val === undefined) {
                tbody += '<td>-</td>';
            } else if (typeof val === 'number') {
                // 数值列着色
                var cls = '';
                if (col.indexOf('增长') >= 0 || col.indexOf('ROE') >= 0 || col.indexOf('ROIC') >= 0 || col.indexOf('EP') >= 0) {
                    cls = val > 0 ? 'positive' : (val < 0 ? 'negative' : '');
                }
                tbody += '<td class="' + cls + '">' + val.toFixed(2) + '</td>';
            } else {
                tbody += '<td>' + val + '</td>';
            }
        }
        tbody += '</tr>';
    }
    tbody += '</tbody>';

    table.innerHTML = thead + tbody;
    section.style.display = 'block';

    // 滚动到结果区
    section.scrollIntoView({behavior: 'smooth', block: 'start'});
}

// ── 历史回测 ──
var backtestRunning = false;

function runBacktest() {
    if (!masterDataLoaded) {
        alert('请先加载数据');
        return;
    }
    if (backtestRunning) return;

    backtestRunning = true;
    var btn = document.getElementById('btn-backtest');
    btn.disabled = true;
    btn.textContent = '回测中...';
    document.getElementById('backtest-status').textContent = '';

    var progressSection = document.getElementById('backtest-progress');
    progressSection.style.display = 'block';
    document.getElementById('backtest-progress-bar').style.width = '0%';
    document.getElementById('backtest-progress-text').textContent = '正在初始化...';

    fetch('/api/master/backtest', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: '{}'
    })
    .then(function(res) { return res.json(); })
    .then(function(data) {
        listenBacktestProgress(data.task_id);
    })
    .catch(function(err) {
        document.getElementById('backtest-progress-text').textContent = '请求失败: ' + err;
        btn.disabled = false;
        btn.textContent = '运行回测（3年季度调仓）';
        backtestRunning = false;
    });
}

function listenBacktestProgress(taskId) {
    var evtSource = new EventSource('/api/progress/' + taskId);
    var progressBar = document.getElementById('backtest-progress-bar');
    var progressText = document.getElementById('backtest-progress-text');
    var startTime = Date.now();

    evtSource.onmessage = function(event) {
        var info = JSON.parse(event.data);
        var step = info.step || 0;
        var total = info.total || 5;
        var percent = total > 0 ? Math.round((step / total) * 100) : 0;
        progressBar.style.width = percent + '%';

        var timeStr = '';
        if (step > 0 && step < total) {
            var elapsed = (Date.now() - startTime) / 1000;
            var perStep = elapsed / step;
            var remaining = Math.round(perStep * (total - step));
            if (remaining >= 60) {
                timeStr = ' · 预计剩余 ' + Math.floor(remaining / 60) + '分' + (remaining % 60) + '秒';
            } else {
                timeStr = ' · 预计剩余 ' + remaining + '秒';
            }
        }

        progressText.textContent = info.message + ' (' + step + '/' + total + ')' + timeStr;

        if (info.done) {
            evtSource.close();
            progressBar.style.width = '100%';
            var totalElapsed = Math.round((Date.now() - startTime) / 1000);
            progressText.textContent = info.message + '（共耗时 ' + totalElapsed + '秒）';

            var btn = document.getElementById('btn-backtest');
            btn.textContent = '重新回测';
            btn.disabled = false;
            backtestRunning = false;

            // 获取回测结果
            fetch('/api/master/backtest_result')
            .then(function(res) { return res.json(); })
            .then(function(result) {
                if (result.error) {
                    document.getElementById('backtest-status').textContent = '回测失败: ' + result.error;
                } else {
                    renderBacktestChart(result);
                }
            });
        }
    };

    evtSource.onerror = function() {
        evtSource.close();
        var btn = document.getElementById('btn-backtest');
        btn.disabled = false;
        btn.textContent = '运行回测（3年季度调仓）';
        backtestRunning = false;
    };
}

function renderBacktestChart(data) {
    var chartDom = document.getElementById('backtest-chart');
    chartDom.style.display = 'block';

    // 处理暗色主题
    var isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    var chart = echarts.init(chartDom, isDark ? 'dark' : null);

    var colors = [
        '#1a6ddb', '#e6550d', '#31a354', '#756bb1', '#e7298a',
        '#66a61e', '#e7ba52', '#a6761d', '#666666'
    ];

    var series = [];
    var legendData = [];
    var colorIdx = 0;

    // 各策略折线
    var keys = Object.keys(data.strategies);
    for (var i = 0; i < keys.length; i++) {
        var key = keys[i];
        var s = data.strategies[key];
        legendData.push(s.name);
        series.push({
            name: s.name,
            type: 'line',
            data: s.values,
            smooth: false,
            symbol: 'none',
            lineStyle: { width: 2, color: colors[colorIdx % colors.length] },
            itemStyle: { color: colors[colorIdx % colors.length] }
        });
        colorIdx++;
    }

    // 基准线（虚线）
    legendData.push(data.benchmark.name);
    series.push({
        name: data.benchmark.name,
        type: 'line',
        data: data.benchmark.values,
        smooth: false,
        symbol: 'none',
        lineStyle: { width: 2, type: 'dashed', color: '#999' },
        itemStyle: { color: '#999' }
    });

    var option = {
        title: {
            text: '策略历史回测 — 累计净值曲线',
            left: 'center',
            textStyle: { fontSize: 16 }
        },
        tooltip: {
            trigger: 'axis',
            formatter: function(params) {
                var html = '<b>' + params[0].axisValue + '</b><br/>';
                for (var i = 0; i < params.length; i++) {
                    var p = params[i];
                    html += '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:' +
                        p.color + ';margin-right:6px;"></span>' +
                        p.seriesName + ': <b>' + p.value.toFixed(4) + '</b>';
                    if (p.value >= 1) {
                        html += ' <span style="color:#e74c3c;">(+' + ((p.value - 1) * 100).toFixed(1) + '%)</span>';
                    } else {
                        html += ' <span style="color:#27ae60;">(' + ((p.value - 1) * 100).toFixed(1) + '%)</span>';
                    }
                    html += '<br/>';
                }
                return html;
            }
        },
        legend: {
            data: legendData,
            top: 40,
            type: 'scroll',
            textStyle: { fontSize: 12 }
        },
        grid: {
            left: 60,
            right: 30,
            top: 90,
            bottom: 60
        },
        xAxis: {
            type: 'category',
            data: data.dates,
            axisLabel: {
                rotate: 30,
                fontSize: 10,
                interval: Math.floor(data.dates.length / 10)
            }
        },
        yAxis: {
            type: 'value',
            name: '净值',
            axisLabel: {
                formatter: function(v) { return v.toFixed(2); }
            },
            splitLine: { lineStyle: { type: 'dashed' } }
        },
        dataZoom: [
            { type: 'inside', start: 0, end: 100 },
            { type: 'slider', start: 0, end: 100, height: 20, bottom: 10 }
        ],
        series: series
    };

    chart.setOption(option);

    // 响应窗口调整
    window.addEventListener('resize', function() { chart.resize(); });

    // 滚动到图表区
    chartDom.scrollIntoView({behavior: 'smooth', block: 'start'});
}
