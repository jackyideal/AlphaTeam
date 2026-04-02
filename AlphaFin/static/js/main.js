/* ═══════════════ AlphaFin 前端逻辑 ═══════════════ */

// ── 指标计算 ──
function computeIndicator() {
    const btn = document.getElementById('btn-compute');
    btn.disabled = true;
    btn.textContent = '计算中...';

    // 收集参数
    const params = {};
    const queryInput = document.getElementById('query_input');
    const tsCodeInput = document.getElementById('ts_code');
    const startDateSelect = document.getElementById('start_date');
    if (queryInput) params.query = queryInput.value.trim();
    if (tsCodeInput) params.ts_code = tsCodeInput.value.trim();
    if (startDateSelect) params.start_date = startDateSelect.value;

    // 清空旧图表
    document.getElementById('charts-section').innerHTML = '';
    document.getElementById('chat-section').style.display = 'none';

    // 显示进度条
    const progressSection = document.getElementById('progress-section');
    progressSection.style.display = 'block';
    document.getElementById('progress-bar').style.width = '0%';
    document.getElementById('progress-text').textContent = '正在初始化...';

    // 发起计算请求
    fetch(`/api/compute/${INDICATOR_ID}`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(params)
    })
    .then(res => res.json())
    .then(data => {
        currentTaskId = data.task_id;
        listenProgress(data.task_id);
    })
    .catch(err => {
        document.getElementById('progress-text').textContent = '请求失败: ' + err;
        btn.disabled = false;
        btn.textContent = '开始计算';
    });
}

// ── SSE 进度监听 ──
function listenProgress(taskId) {
    const evtSource = new EventSource(`/api/progress/${taskId}`);
    const progressBar = document.getElementById('progress-bar');
    const progressText = document.getElementById('progress-text');

    evtSource.onmessage = function(event) {
        const info = JSON.parse(event.data);
        const percent = info.total > 0 ? Math.round((info.step / info.total) * 100) : 0;
        progressBar.style.width = percent + '%';
        progressText.textContent = `${info.message} (${info.step}/${info.total})`;

        if (info.done) {
            evtSource.close();
            progressBar.style.width = '100%';

            if (info.chart_paths && info.chart_paths.length > 0) {
                progressText.textContent = `完成！共生成 ${info.chart_paths.length} 张图表`;
                displayCharts(info.chart_paths, info.chart_titles);
            } else {
                progressText.textContent = info.message || '完成（无图表生成）';
            }

            const btn = document.getElementById('btn-compute');
            btn.disabled = false;
            btn.textContent = '开始计算';
        }
    };

    evtSource.onerror = function() {
        evtSource.close();
        // 尝试获取最终结果
        fetch(`/api/charts/${taskId}`)
            .then(res => res.json())
            .then(data => {
                if (data.charts && data.charts.length > 0) {
                    displayCharts(data.charts, data.titles);
                }
            });

        const btn = document.getElementById('btn-compute');
        btn.disabled = false;
        btn.textContent = '开始计算';
    };
}

// ── 展示图表 ──
function getLogicfinCardClass(title, index) {
    if (INDICATOR_ID !== 'ind_27_logicfin') return '';
    const text = String(title || '');
    if (index === 0 || text.includes('逻辑链路')) return 'logicfin-card-hero logicfin-card-spotlight';
    if (text.includes('最终决策') || text.includes('买卖决策') || text.includes('策略构建') || text.includes('策略效果')) {
        return 'logicfin-card-hero logicfin-card-spotlight';
    }
    if (text.includes('日K线') || text.includes('策略效果')) return 'logicfin-card-visual';
    return 'logicfin-card-analysis';
}

function displayCharts(chartPaths, chartTitles) {
    currentChartPaths = chartPaths;
    const section = document.getElementById('charts-section');
    section.innerHTML = '';
    if (INDICATOR_ID === 'ind_27_logicfin') {
        section.classList.add('logicfin-grid');
    } else {
        section.classList.remove('logicfin-grid');
    }

    chartPaths.forEach((path, i) => {
        const title = chartTitles[i] || `图表 ${i + 1}`;
        const desc = (typeof META !== 'undefined' && META.chart_descriptions && META.chart_descriptions[i]) ? META.chart_descriptions[i] : '';
        const zoomAttr = INDICATOR_ID === 'ind_27_logicfin'
            ? `class="logicfin-chart-image" onclick="window.open('${path}', '_blank')" title="点击新窗口查看大图"`
            : '';
        const card = document.createElement('div');
        const logicfinClass = getLogicfinCardClass(title, i);
        card.className = `chart-card ${logicfinClass}`.trim();
        card.innerHTML = `
            <div class="chart-title">
                <span class="chart-number">${i + 1}/${chartPaths.length}</span>
                ${title}
                <label class="chart-checkbox">
                    <input type="checkbox" class="chart-select" data-index="${i}" checked>
                    发送给AI
                </label>
            </div>
            <img src="${path}" alt="${title}" loading="lazy" ${zoomAttr}>
            ${desc ? '<p class="chart-desc">' + desc + '</p>' : ''}
        `;
        section.appendChild(card);
    });

    // 显示聊天区域
    document.getElementById('chat-section').style.display = 'block';
    chatHistory = [];
    document.getElementById('chat-messages').innerHTML = '';
}

// ── Claude 对话 ──
function getSelectedChartPaths() {
    const checkboxes = document.querySelectorAll('.chart-select:checked');
    const paths = [];
    checkboxes.forEach(cb => {
        const idx = parseInt(cb.dataset.index);
        if (currentChartPaths[idx]) paths.push(currentChartPaths[idx]);
    });
    return paths;
}

function sendChat() {
    const input = document.getElementById('chat-input');
    const message = input.value.trim();
    if (!message) return;

    const selectedPaths = getSelectedChartPaths();
    input.value = '';

    // 显示用户消息
    appendChatMessage('user', message);

    // 显示加载
    const loadingId = appendChatMessage('assistant', '<span class="spinner"></span> 分析中...');

    fetch('/api/chat', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            chart_paths: selectedPaths,
            message: message,
            history: chatHistory
        })
    })
    .then(res => res.json())
    .then(data => {
        // 移除加载，显示回复
        document.getElementById(loadingId).innerHTML =
            `<div class="role">🤖 Qwen</div>${data.reply}`;
        chatHistory.push({role: 'user', content: message});
        chatHistory.push({role: 'assistant', content: data.reply});
    })
    .catch(err => {
        document.getElementById(loadingId).innerHTML =
            `<div class="role">🤖 Qwen</div>请求失败: ${err}`;
    });
}

function analyzeAllCharts() {
    const input = document.getElementById('chat-input');
    input.value = '请分析当前展示的所有图表，给出综合判断和交易建议。';
    // 全选所有图表
    document.querySelectorAll('.chart-select').forEach(cb => cb.checked = true);
    sendChat();
}

function appendChatMessage(role, content) {
    const container = document.getElementById('chat-messages');
    const id = 'msg-' + Date.now();
    const div = document.createElement('div');
    div.id = id;
    div.className = `chat-msg ${role}`;
    if (role === 'user') {
        div.innerHTML = `<div class="role">👤 你</div>${content}`;
    } else {
        div.innerHTML = content;
    }
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
    return id;
}

// ── 数据库更新 ──
function updateDatabase() {
    const modal = document.getElementById('update-modal');
    modal.style.display = 'flex';
    document.getElementById('update-options').style.display = 'block';
    document.getElementById('update-start-btn').style.display = 'inline-block';
    document.getElementById('update-progress-bar').style.width = '0%';
    document.getElementById('update-message').textContent = '';
    document.getElementById('update-close-btn').style.display = 'none';
}

function startUpdate() {
    const includeFina = document.getElementById('update-fina').checked;
    document.getElementById('update-options').style.display = 'none';
    document.getElementById('update-start-btn').style.display = 'none';
    document.getElementById('update-message').textContent = '准备中...';

    fetch('/api/update', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({include_fina: includeFina})
    })
    .then(res => res.json())
    .then(data => {
        const evtSource = new EventSource(`/api/progress/${data.task_id}`);
        evtSource.onmessage = function(event) {
            const info = JSON.parse(event.data);
            const percent = info.total > 0 ? Math.round((info.step / info.total) * 100) : 0;
            document.getElementById('update-progress-bar').style.width = percent + '%';
            document.getElementById('update-message').textContent = info.message;

            if (info.done) {
                evtSource.close();
                document.getElementById('update-progress-bar').style.width = '100%';
                document.getElementById('update-close-btn').style.display = 'inline-block';
            }
        };
    });
}

function closeUpdateModal() {
    document.getElementById('update-modal').style.display = 'none';
}

// ── 侧栏拖拽调整宽度 ──
(function() {
    var resizer = document.getElementById('sidebar-resizer');
    if (!resizer) return;
    var sidebar = document.getElementById('sidebar');
    var isResizing = false;

    resizer.addEventListener('mousedown', function(e) {
        isResizing = true;
        resizer.classList.add('active');
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
        e.preventDefault();
    });

    function updateSidebarFontSize(width) {
        // 字体大小随宽度线性变化：180px→13px, 260px→15px, 500px→20px
        var fontSize = Math.max(13, Math.min(20, 13 + (width - 180) * 7 / 320));
        var items = sidebar.querySelectorAll('.sidebar-item');
        for (var i = 0; i < items.length; i++) {
            items[i].style.fontSize = fontSize.toFixed(1) + 'px';
        }
        var titles = sidebar.querySelectorAll('.group-title');
        for (var j = 0; j < titles.length; j++) {
            titles[j].style.fontSize = Math.max(10, fontSize - 4).toFixed(1) + 'px';
        }
    }

    document.addEventListener('mousemove', function(e) {
        if (!isResizing) return;
        var newWidth = Math.max(180, Math.min(500, e.clientX));
        document.documentElement.style.setProperty('--sidebar-width', newWidth + 'px');
        updateSidebarFontSize(newWidth);
    });

    document.addEventListener('mouseup', function() {
        if (isResizing) {
            isResizing = false;
            resizer.classList.remove('active');
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
        }
    });
})();

// ── 侧栏分组折叠/展开（本地持久化） ──
(function() {
    var sidebar = document.getElementById('sidebar');
    if (!sidebar) return;

    var groups = Array.prototype.slice.call(sidebar.querySelectorAll('.sidebar-group[data-group-id]'));
    if (!groups.length) return;

    var storageKey = 'alphafin_sidebar_group_state_v1';
    var states = {};
    var hasPersistedState = false;

    try {
        var raw = localStorage.getItem(storageKey);
        hasPersistedState = raw !== null;
        states = JSON.parse(raw || '{}') || {};
    } catch (e) {
        states = {};
        hasPersistedState = false;
    }

    function persist() {
        try {
            localStorage.setItem(storageKey, JSON.stringify(states));
        } catch (e) {
            // ignore storage errors
        }
    }

    function setCollapsed(groupEl, collapsed) {
        groupEl.classList.toggle('collapsed', !!collapsed);
        var btn = groupEl.querySelector('.group-toggle');
        if (btn) {
            btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
        }
    }

    groups.forEach(function(groupEl) {
        var groupId = groupEl.getAttribute('data-group-id');
        var hasState = Object.prototype.hasOwnProperty.call(states, groupId);
        var isIndicatorGroup = groupId && groupId.indexOf('indicator-') === 0;
        var isCollapsed = hasState
            ? !!states[groupId]
            : (hasPersistedState ? false : !!isIndicatorGroup);
        setCollapsed(groupEl, isCollapsed);

        var btn = groupEl.querySelector('.group-toggle');
        if (!btn) return;
        btn.addEventListener('click', function() {
            var willCollapse = !groupEl.classList.contains('collapsed');
            setCollapsed(groupEl, willCollapse);
            states[groupId] = willCollapse;
            persist();
        });
    });

    // 保证当前激活页所在分组始终展开，避免刷新后找不到当前菜单项
    var activeItem = sidebar.querySelector('.sidebar-item.active');
    if (activeItem) {
        var activeGroup = activeItem.closest('.sidebar-group[data-group-id]');
        if (activeGroup && activeGroup.classList.contains('collapsed')) {
            var gid = activeGroup.getAttribute('data-group-id');
            setCollapsed(activeGroup, false);
            states[gid] = false;
            persist();
        }
    }
})();

// ── 实时时钟 ──
function updateClock() {
    const el = document.getElementById('header-clock');
    if (!el) return;
    const now = new Date();
    const h = String(now.getHours()).padStart(2, '0');
    const m = String(now.getMinutes()).padStart(2, '0');
    const s = String(now.getSeconds()).padStart(2, '0');
    el.textContent = h + ':' + m + ':' + s;
}
setInterval(updateClock, 1000);
updateClock();

// ── 统计计数器动画 ──
function animateCounters() {
    var counters = document.querySelectorAll('.stat-number[data-target]');
    counters.forEach(function(counter) {
        var target = parseInt(counter.dataset.target);
        var duration = 1500;
        var start = performance.now();
        function tick(now) {
            var elapsed = now - start;
            var progress = Math.min(elapsed / duration, 1);
            var eased = 1 - Math.pow(1 - progress, 3);
            counter.textContent = Math.round(target * eased);
            if (progress < 1) requestAnimationFrame(tick);
        }
        requestAnimationFrame(tick);
    });
}

// 滚动到可见时触发计数器动画
if (document.querySelector('.stats-bar')) {
    var observer = new IntersectionObserver(function(entries) {
        entries.forEach(function(entry) {
            if (entry.isIntersecting) {
                animateCounters();
                observer.disconnect();
            }
        });
    }, { threshold: 0.5 });
    observer.observe(document.querySelector('.stats-bar'));
}

// ── 主题切换 ──
function toggleTheme() {
    var html = document.documentElement;
    var btn = document.getElementById('theme-toggle');
    if (html.getAttribute('data-theme') === 'dark') {
        html.removeAttribute('data-theme');
        btn.textContent = '🌙';
        localStorage.setItem('theme', 'light');
    } else {
        html.setAttribute('data-theme', 'dark');
        btn.textContent = '☀️';
        localStorage.setItem('theme', 'dark');
    }
}

// 页面加载时恢复主题（默认深色）
(function() {
    var saved = localStorage.getItem('theme');
    if (saved !== 'light') {
        document.documentElement.setAttribute('data-theme', 'dark');
        var btn = document.getElementById('theme-toggle');
        if (btn) btn.textContent = '☀️';
    }
})();

// ── 低负载渲染模式（默认开启，改善滚动卡顿） ──
(function() {
    var key = 'alphafin_perf_mode';
    var saved = null;
    try {
        saved = localStorage.getItem(key);
        if (!saved) {
            saved = 'lite';
            localStorage.setItem(key, saved);
        }
    } catch (e) {
        saved = 'lite';
    }

    if (saved === 'lite' && document.body) {
        document.body.classList.add('perf-lite');
    }
})();

// ── 首页：AlphaTeam 入口弹窗 ──
var _teamEntryBusy = false;
var _teamEntryState = { running: false };

function _setTeamEntryUiState(state) {
    var text = document.getElementById('team-entry-modal-text');
    var actionBtn = document.getElementById('btn-team-entry-start');
    if (!text || !actionBtn) return;

    _teamEntryState = state || { running: false };

    if (_teamEntryBusy) {
        actionBtn.disabled = true;
        actionBtn.textContent = '处理中...';
        text.textContent = '正在处理模块状态，请稍候...';
        return;
    }

    actionBtn.disabled = false;
    if (_teamEntryState.running) {
        actionBtn.textContent = '直接进入';
        text.textContent = '模块已在后台运行，可直接进入 AlphaTeam。';
    } else {
        actionBtn.textContent = '启动并进入';
        text.textContent = '模块当前未启动，点击“启动并进入”后将拉起后台服务并进入团队页面。';
    }
}

function _queryTeamModuleStatus() {
    return fetch('/api/team/module/status')
        .then(function(res) { return res.json(); })
        .then(function(data) { return data || { running: false }; })
        .catch(function() { return { running: false, error: true }; });
}

function openTeamEntryGate(event) {
    if (event && typeof event.preventDefault === 'function') {
        event.preventDefault();
    }

    var modal = document.getElementById('team-entry-modal');
    if (!modal) return true;

    modal.style.display = 'flex';
    _teamEntryBusy = true;
    _setTeamEntryUiState(_teamEntryState);

    _queryTeamModuleStatus().then(function(state) {
        _teamEntryBusy = false;
        _setTeamEntryUiState(state);
    });
    return false;
}

function closeTeamEntryModal() {
    var modal = document.getElementById('team-entry-modal');
    if (!modal) return;
    modal.style.display = 'none';
}

function confirmTeamModuleStartAndEnter() {
    if (_teamEntryBusy) return;

    if (_teamEntryState.running) {
        window.location.href = '/team';
        return;
    }

    _teamEntryBusy = true;
    _setTeamEntryUiState(_teamEntryState);

    fetch('/api/team/module/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
    })
        .then(function(res) { return res.json(); })
        .then(function(data) {
            _teamEntryBusy = false;
            _setTeamEntryUiState(data || { running: false });
            window.location.href = '/team';
        })
        .catch(function() {
            _teamEntryBusy = false;
            _setTeamEntryUiState(_teamEntryState);
            alert('模块启动失败，请稍后重试');
        });
}

(function initTeamEntryModal() {
    var modal = document.getElementById('team-entry-modal');
    if (!modal) return;
    modal.addEventListener('click', function(e) {
        if (e.target === modal) {
            closeTeamEntryModal();
        }
    });
})();
