/**
 * AlphaFin 智能分析团队 - 页面逻辑 + SSE 监听
 */

// ──────────────── 全局状态 ────────────────
let activitySource = null;
let autoRunning = false;
let agentColors = {};
let pfSignalMap = {};
let activityFilter = 'all';
let activityBuffer = [];
let activityDateExpanded = {};
let currentReportData = null;
let currentActivityDownload = null;
let lastAskDownload = null;
let currentAskPollTimer = null;
let currentAskId = '';
let workflowSessionId = '';
let activityDetailSessionId = '';
let workflowSessionSnapshot = [];
let teamPromptConfigs = [];
let teamContextIds = [];
let teamContextFiles = [];
let askWorkflowConfig = null;
let askWorkflowPresets = {};
let lastAskMeta = null;
let askSessionMap = {};
let askChatSessions = [];
let askActiveChatId = '';
let askPendingMessageByTask = {};
let taskChatBindings = {};
let chatActivityMirrorSeen = {};
let toolAuditCatalog = null;
let toolAuditLoaded = false;
let latestBudgetSnapshot = null;
let latestTraceRuns = [];
let lastTraceRefreshTs = 0;
let activityRenderTimer = null;
let activityRenderDirty = false;
let latestOrchestratorState = null;
let latestPortfolioSchedulerState = null;
let latestAgentStatuses = [];
let latestAskSessionProgress = null;
let latestAskSessionOvertime = null;
let latestAskSessionTiming = null;
let latestPromptProfilesByAgent = {};
let teamModelConfig = null;
let latestMemoryCenter = null;
let overtimePromptRequestTsBySession = {};
let latestWaitingOvertimeSessions = [];
const ACTIVITY_MAX_BUFFER = 1000;
const ACTIVITY_MAX_VISIBLE = 200;
const ACTIVITY_RENDER_DEBOUNCE_MS = 220;
const STATUS_POLL_INTERVAL_MS = 5000;
const REPORT_POLL_INTERVAL_MS = 45000;
const TRACE_POLL_INTERVAL_MS = 45000;
const ASK_POLL_INTERVAL_MS = 1800;
const ASK_POLL_TIMEOUT_MS = 12 * 60 * 1000;
const ASK_SESSION_STORAGE_KEY = 'alphafin_ai_team_ask_sessions_v1';
const ASK_CHAT_STORAGE_KEY = 'alphafin_ai_team_ask_chat_sessions_v1';
const ASK_CHAT_ACTIVE_STORAGE_KEY = 'alphafin_ai_team_ask_chat_active_v1';
const ASK_MODE_STORAGE_KEY = 'alphafin_ai_team_ask_mode_v1';
const ASK_WEB_STORAGE_KEY = 'alphafin_ai_team_ask_web_enabled_v1';
const UNIFIED_TIME_LIMIT_STORAGE_KEY = 'alphafin_ai_team_unified_time_limit_minutes_v1';
const TEAM_MODE_TIMEOUT_PROMPT_STORAGE_KEY = 'alphafin_ai_team_team_timeout_prompt_seen_v1';
const TEAM_SCENE_STORAGE_KEY = 'alphafin_ai_team_scene_open_v1';
let runtimeAlerts = {};

function _errText(err) {
    var msg = String((err && err.message) || err || '').trim();
    if (!msg) return '未知错误';
    return msg.length > 220 ? (msg.slice(0, 220) + '...') : msg;
}

function setRuntimeAlert(key, message, level) {
    var k = String(key || '').trim();
    if (!k) return;
    var msg = String(message || '').trim();
    if (!msg) {
        delete runtimeAlerts[k];
        renderRuntimeAlert();
        return;
    }
    runtimeAlerts[k] = {
        message: msg,
        level: String(level || 'error'),
        ts: Date.now()
    };
    renderRuntimeAlert();
}

function clearRuntimeAlert(key) {
    var k = String(key || '').trim();
    if (!k) return;
    if (runtimeAlerts[k]) {
        delete runtimeAlerts[k];
        renderRuntimeAlert();
    }
}

function renderRuntimeAlert() {
    var el = document.getElementById('team-runtime-alert');
    if (!el) return;
    var rows = Object.keys(runtimeAlerts).map(function(k) {
        return runtimeAlerts[k];
    }).filter(function(row) {
        return !!(row && row.message);
    }).sort(function(a, b) {
        return Number(b.ts || 0) - Number(a.ts || 0);
    });
    if (!rows.length) {
        el.style.display = 'none';
        el.className = 'team-runtime-alert';
        el.textContent = '';
        return;
    }
    var top = rows[0];
    el.style.display = 'block';
    el.className = 'team-runtime-alert' + (top.level === 'error' ? ' error' : '');
    el.textContent = top.message;
}

function persistSceneVisibility(opened) {
    try {
        localStorage.setItem(TEAM_SCENE_STORAGE_KEY, opened ? '1' : '0');
    } catch (e) {}
}

function readSceneVisibility() {
    try {
        var raw = String(localStorage.getItem(TEAM_SCENE_STORAGE_KEY) || '1');
        return !(raw === '0' || raw === 'false' || raw === 'off');
    } catch (e) {
        return true;
    }
}

function syncSceneVisibilityButton() {
    var btn = document.getElementById('btn-toggle-scene');
    var shell = document.getElementById('team-scene-shell');
    if (!btn || !shell) return;
    var hidden = shell.classList.contains('scene-hidden') || shell.style.display === 'none';
    btn.textContent = hidden ? '打开3D' : '关闭3D';
    btn.classList.toggle('btn-active', !hidden);
}

function setSceneVisibility(opened, options) {
    var shell = document.getElementById('team-scene-shell');
    var body = document.getElementById('scene-shell-body');
    var collapsed = document.getElementById('scene-shell-collapsed');
    if (!shell || !body || !collapsed) return;
    var visible = !!opened;
    shell.classList.toggle('scene-hidden', !visible);
    shell.style.display = visible ? '' : 'none';
    body.style.display = visible ? '' : 'none';
    collapsed.style.display = 'none';
    try {
        if (typeof window.setTeamScenePaused === 'function') {
            window.setTeamScenePaused(!visible);
        }
    } catch (e) {}
    if (!(options && options.silentPersist)) {
        persistSceneVisibility(visible);
    }
    syncSceneVisibilityButton();
}

function toggleSceneVisibility() {
    var shell = document.getElementById('team-scene-shell');
    if (!shell) return;
    setSceneVisibility(shell.classList.contains('scene-hidden'));
}

function focusAskComposer() {
    var card = document.getElementById('workspace-chat-card');
    var input = document.getElementById('ask-input');
    if (card) {
        card.scrollIntoView({behavior: 'smooth', block: 'start'});
    }
    if (input) {
        setTimeout(function() { input.focus(); }, 60);
    }
}

function upsertWaitingOvertimeRow(row) {
    if (!row || typeof row !== 'object') return;
    var sid = String(
        row.session_id ||
        (row.session_overtime && row.session_overtime.session_id) ||
        ''
    ).trim();
    if (!sid) return;
    var overtime = row.session_overtime && row.session_overtime.active
        ? row.session_overtime
        : {
            active: true,
            waiting: true,
            session_id: sid,
        };
    var normalized = {
        session_id: sid,
        session_timing: row.session_timing || {active: false, session_id: sid},
        session_progress: row.session_progress || {active: false, session_id: sid},
        session_overtime: overtime,
    };
    var arr = Array.isArray(latestWaitingOvertimeSessions) ? latestWaitingOvertimeSessions : [];
    var next = [];
    var inserted = false;
    arr.forEach(function(item) {
        var rid = String(
            (item && item.session_id) ||
            (item && item.session_overtime && item.session_overtime.session_id) ||
            ''
        ).trim();
        if (!rid) return;
        if (rid === sid) {
            if (!inserted) {
                next.push(normalized);
                inserted = true;
            }
            return;
        }
        next.push(item);
    });
    if (!inserted) next.unshift(normalized);
    latestWaitingOvertimeSessions = next.slice(0, 12);
}

function removeWaitingOvertimeRow(sessionId) {
    var sid = String(sessionId || '').trim();
    if (!sid) return;
    latestWaitingOvertimeSessions = (latestWaitingOvertimeSessions || []).filter(function(item) {
        var rid = String(
            (item && item.session_id) ||
            (item && item.session_overtime && item.session_overtime.session_id) ||
            ''
        ).trim();
        return rid && rid !== sid;
    });
}

// 3D场景接口兜底（场景加载失败时不报错）
if (!window.updateSceneAgents) window.updateSceneAgents = function() {};
if (!window.onAgentActivity) window.onAgentActivity = function() {};

// 智能体图标映射
const AGENT_ICONS = {
    director: '👑', analyst: '📊', risk: '🛡️',
    intel: '🌐', quant: '📈', restructuring: '🏗️', auditor: '🔍'
};
const TEAM_AGENT_ORDER = ['director', 'analyst', 'risk', 'intel', 'quant', 'restructuring', 'auditor'];
const TEAM_AGENT_NAMES = {
    director: '决策总监',
    analyst: '投资分析师',
    risk: '风控官',
    intel: '市场情报员',
    quant: '量化策略师',
    restructuring: '资产重组专家',
    auditor: '反思审计员'
};

function sanitizeFilename(name) {
    var raw = String(name || 'alphafin_content').trim();
    raw = raw.replace(/[\\/:*?"<>|]/g, '_').replace(/\s+/g, '_');
    if (!raw) raw = 'alphafin_content';
    return raw.slice(0, 80);
}

function formatDurationClock(seconds) {
    var total = Math.max(0, Number(seconds) || 0);
    var h = Math.floor(total / 3600);
    var m = Math.floor((total % 3600) / 60);
    var s = Math.floor(total % 60);
    if (h > 0) return h + 'h ' + pad2(m) + 'm ' + pad2(s) + 's';
    return m + 'm ' + pad2(s) + 's';
}

function formatDeadlineBadge(timing) {
    if (!timing || !timing.active) return '';
    var remain = formatDurationClock(timing.remaining_seconds || 0);
    var total = formatDurationClock(timing.total_seconds || 0);
    var state = String(timing.state || 'running');
    var stateText = state === 'expired' ? '已到时限' : (state === 'converging' ? '收敛中' : '进行中');
    return stateText + ' · 剩余 ' + remain + ' / 总时限 ' + total;
}

function pickActiveSessionTiming() {
    if (latestAskSessionTiming && latestAskSessionTiming.active) {
        return {timing: latestAskSessionTiming, source: 'ask', label: '直连问答'};
    }
    var pf = latestPortfolioSchedulerState || {};
    var pfTiming = pf.session_timing || null;
    if (pfTiming && pfTiming.active && pf.current_session) {
        return {timing: pfTiming, source: 'portfolio', label: '投资执行'};
    }
    var orch = latestOrchestratorState || {};
    var orchTiming = orch.session_timing || null;
    if (orchTiming && orchTiming.active && orch.current_session) {
        var label = orch.current_session && String(orch.current_session).indexOf('chat_') === 0
            ? '同事闲聊'
            : (orch.current_session && String(orch.current_session).indexOf('idle_') === 0
                ? '闲时学习'
                : '智能分析任务');
        return {timing: orchTiming, source: 'orchestrator', label: label};
    }
    return null;
}

function renderSessionTimingBanner() {
    var el = document.getElementById('session-timing-banner');
    if (!el) return;
    var bundle = pickActiveSessionBundle();
    if (!bundle || !bundle.timing || !bundle.timing.active) {
        el.style.display = 'none';
        el.innerHTML = '';
        return;
    }
    var timing = bundle.timing;
    var overtime = bundle.overtime || null;
    var state = String(timing.state || 'running');
    var bg = state === 'expired'
        ? 'linear-gradient(135deg,rgba(254,242,242,0.98),rgba(255,255,255,0.98))'
        : (state === 'converging'
            ? 'linear-gradient(135deg,rgba(255,247,237,0.98),rgba(255,255,255,0.98))'
            : 'linear-gradient(135deg,rgba(239,246,255,0.98),rgba(255,255,255,0.98))');
    var border = state === 'expired' ? 'rgba(220,38,38,0.25)' : (state === 'converging' ? 'rgba(217,119,6,0.25)' : 'rgba(37,99,235,0.20)');
    var title = bundle.label + ' · ' + (timing.title || '当前任务');
    var waitingDecision = !!(
        (overtime && overtime.waiting) ||
        (bundle.progress && bundle.progress.active && String(bundle.progress.state || '') === 'waiting_user')
    );
    var stateText = waitingDecision
        ? '等待你的决定'
        : (state === 'expired' ? '已到时限' : (state === 'converging' ? '收敛输出中' : '正常推进'));
    el.style.display = 'block';
    el.style.margin = '10px 0 14px 0';
    el.style.padding = '12px 16px';
    el.style.borderRadius = '14px';
    el.style.border = '1px solid ' + border;
    el.style.background = bg;
    el.innerHTML =
        '<div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap;">' +
            '<div>' +
                '<div style="font-size:13px;font-weight:700;color:#0f172a;">' + escapeHtml(title) + '</div>' +
                '<div style="margin-top:4px;font-size:12px;color:#475569;">状态：' + escapeHtml(stateText) +
                ' · 已耗时 ' + escapeHtml(formatDurationClock(timing.elapsed_seconds || 0)) +
                ' · 剩余 ' + escapeHtml(formatDurationClock(timing.remaining_seconds || 0)) + '</div>' +
            '</div>' +
            '<div style="font-size:12px;font-weight:700;color:#1e293b;">任务必须在 ' + escapeHtml(formatUnixTime(timing.deadline_ts || 0)) + ' 前完成</div>' +
        '</div>';
}

function renderPromptProfileSummary(profile) {
    var p = (profile && typeof profile === 'object') ? profile : {};
    var modifiers = Array.isArray(p.modifiers) ? p.modifiers : [];
    if (!p.agent_name && !modifiers.length) {
        return '<div style="font-size:12px;color:#64748b;">当前未获取到运行时提示词画像</div>';
    }
    var chips = modifiers.map(function(item) {
        return '<span class="session-prompt-chip">' + escapeHtml(String(item || '')) + '</span>';
    }).join('');
    return '<div>' +
        '<div style="font-size:12px;font-weight:700;color:#0f172a;">运行时提示词画像</div>' +
        '<div style="margin-top:4px;font-size:12px;color:#475569;">' +
        '角色: ' + escapeHtml(String(p.agent_name || '-')) +
        ' · 工作流: ' + escapeHtml(String(p.workflow || '-')) +
        ' · 模式: ' + escapeHtml(getAskModeText(String(p.response_style || 'auto'))) +
        '</div>' +
        (chips ? ('<div class="session-prompt-chips">' + chips + '</div>') : '') +
        '</div>';
}

function findLatestAgentStatus(agentId) {
    var aid = String(agentId || '').trim();
    if (!aid) return null;
    for (var i = 0; i < latestAgentStatuses.length; i++) {
        var row = latestAgentStatuses[i] || {};
        if (String(row.agent_id || '') === aid) return row;
    }
    return null;
}

function cachePromptProfiles(rows) {
    var arr = Array.isArray(rows) ? rows : [];
    arr.forEach(function(row) {
        if (!row) return;
        var aid = String(row.agent_id || '').trim();
        var profile = row.prompt_profile;
        if (!aid || !profile || typeof profile !== 'object') return;
        var modifiers = Array.isArray(profile.modifiers) ? profile.modifiers : [];
        if (!profile.agent_name && !modifiers.length) return;
        latestPromptProfilesByAgent[aid] = Object.assign({}, profile);
    });
}

function renderPromptProfileChips(profile, limit) {
    var p = (profile && typeof profile === 'object') ? profile : {};
    var modifiers = Array.isArray(p.modifiers) ? p.modifiers : [];
    var maxCount = Math.max(1, Number(limit || 4));
    if (!modifiers.length) return '';
    return '<div class="session-prompt-chips">' +
        modifiers.slice(0, maxCount).map(function(item) {
            return '<span class="session-prompt-chip">' + escapeHtml(String(item || '')) + '</span>';
        }).join('') +
        '</div>';
}

function getPromptProfileForAgent(agentId) {
    var live = findLatestAgentStatus(agentId);
    if (live && live.prompt_profile && typeof live.prompt_profile === 'object') {
        var liveMods = Array.isArray(live.prompt_profile.modifiers) ? live.prompt_profile.modifiers : [];
        if (live.prompt_profile.agent_name || liveMods.length) {
            return Object.assign({}, live.prompt_profile);
        }
    }
    var cached = latestPromptProfilesByAgent[String(agentId || '').trim()];
    if (cached && typeof cached === 'object') return Object.assign({}, cached);
    return {};
}

function renderTeamPromptRuntime() {
    var el = document.getElementById('team-prompt-runtime');
    if (!el) return;
    var item = getCurrentTeamPromptItem();
    var selectedAgentId = item ? String(item.key || '') : '';
    var bundle = pickActiveSessionBundle();
    var actorId = bundle && bundle.progress ? String(bundle.progress.actor || '') : '';
    var selectedProfile = getPromptProfileForAgent(selectedAgentId);
    var selectedHtml = renderPromptProfileSummary(selectedProfile);
    var extraHtml = '';
    if (actorId && actorId !== selectedAgentId) {
        var actorProfile = getPromptProfileForAgent(actorId);
        var actorMods = Array.isArray(actorProfile.modifiers) ? actorProfile.modifiers : [];
        if (actorProfile.agent_name || actorMods.length) {
            extraHtml =
                '<div style="margin-top:10px;padding-top:10px;border-top:1px dashed rgba(148,163,184,0.28);">' +
                    '<div style="font-size:12px;font-weight:700;color:#9a3412;">当前主导链路</div>' +
                    '<div style="margin-top:4px;font-size:12px;color:#475569;">' +
                        escapeHtml(String(actorProfile.agent_name || actorId)) +
                        ' 正在推进当前任务' +
                    '</div>' +
                    renderPromptProfileChips(actorProfile, 6) +
                '</div>';
        }
    }
    el.innerHTML = selectedHtml + extraHtml;
}

function pickActiveSessionBundle() {
    var candidates = [];
    if (latestAskSessionTiming && latestAskSessionTiming.active) {
        candidates.push({
            source: 'ask',
            timing: latestAskSessionTiming,
            progress: latestAskSessionProgress && latestAskSessionProgress.active ? latestAskSessionProgress : null,
            overtime: latestAskSessionOvertime && latestAskSessionOvertime.active ? latestAskSessionOvertime : null,
            label: '直连问答'
        });
    }
    var pf = latestPortfolioSchedulerState || {};
    if (pf && pf.current_session && pf.session_timing && pf.session_timing.active) {
        candidates.push({
            source: 'portfolio',
            timing: pf.session_timing,
            progress: pf.session_progress && pf.session_progress.active ? pf.session_progress : null,
            overtime: pf.session_overtime && pf.session_overtime.active ? pf.session_overtime : null,
            label: '投资执行'
        });
    }
    var orch = latestOrchestratorState || {};
    if (orch && orch.current_session && orch.session_timing && orch.session_timing.active) {
        var label = orch.current_session && String(orch.current_session).indexOf('chat_') === 0
            ? '同事闲聊'
            : (orch.current_session && String(orch.current_session).indexOf('idle_') === 0
                ? '闲时学习'
                : '智能分析任务');
        candidates.push({
            source: 'orchestrator',
            timing: orch.session_timing,
            progress: orch.session_progress && orch.session_progress.active ? orch.session_progress : null,
            overtime: orch.session_overtime && orch.session_overtime.active ? orch.session_overtime : null,
            label: label
        });
    }
    var waitingRows = Array.isArray(latestWaitingOvertimeSessions) ? latestWaitingOvertimeSessions : [];
    if (waitingRows.length) {
        var exists = {};
        candidates.forEach(function(c) {
            var sid = String((c && c.timing && c.timing.session_id) || (c && c.overtime && c.overtime.session_id) || '');
            if (sid) exists[sid] = true;
        });
        waitingRows.forEach(function(row) {
            var overtime = row && row.session_overtime && row.session_overtime.active ? row.session_overtime : null;
            if (!overtime || !overtime.waiting) return;
            var sid = String((overtime && overtime.session_id) || (row && row.session_id) || '');
            if (!sid || exists[sid]) return;
            var progress = row && row.session_progress && row.session_progress.active ? row.session_progress : null;
            var timing = row && row.session_timing && row.session_timing.active ? row.session_timing : (row && row.session_timing ? row.session_timing : null);
            var workflow = String((overtime && overtime.workflow) || (progress && progress.workflow) || (timing && timing.workflow) || '').toLowerCase();
            var label = '超时任务';
            if (workflow === 'auto_research') label = '手动/自动研究';
            else if (workflow === 'user_ask') label = '直连问答';
            else if (workflow === 'portfolio_investment') label = '投资执行';
            else if (workflow === 'market_watch') label = '盘中盯盘';
            candidates.push({
                source: 'waiting_overtime',
                timing: timing,
                progress: progress,
                overtime: overtime,
                label: label
            });
            exists[sid] = true;
        });
    }
    if (!candidates.length) return null;

    function scoreBundle(bundle) {
        if (!bundle) return -1;
        var overtime = bundle.overtime || null;
        var progress = bundle.progress || null;
        var timing = bundle.timing || null;
        var waiting = !!(
            (overtime && overtime.active && overtime.waiting) ||
            (progress && progress.active && String(progress.state || '') === 'waiting_user')
        );
        var timingActive = !!(timing && timing.active);
        if (!timingActive && !waiting) return -1;
        var state = String((timing && timing.state) || 'running');
        var base = waiting ? 1000 : 0;
        if (!timingActive) base += 600;
        else if (state === 'expired') base += 500;
        else if (state === 'converging') base += 300;
        else base += 100;
        if (bundle.source === 'orchestrator') base += 30;
        else if (bundle.source === 'portfolio') base += 20;
        else if (bundle.source === 'ask') base += 10;
        else if (bundle.source === 'waiting_overtime') base += 40;
        var updated = Number(
            (progress && progress.updated_at) ||
            (timing && timing.started_at) ||
            (overtime && overtime.requested_at) ||
            0
        );
        return base + (updated / 10000000000);
    }

    candidates.sort(function(a, b) {
        return scoreBundle(b) - scoreBundle(a);
    });
    return candidates[0];
}

function renderSessionProgressBoard() {
    var el = document.getElementById('session-progress-board');
    if (!el) return;
    var bundle = pickActiveSessionBundle();
    var progress = bundle && bundle.progress ? bundle.progress : null;
    if (!bundle || !progress || !progress.active || !Array.isArray(progress.steps) || !progress.steps.length) {
        el.style.display = 'none';
        el.innerHTML = '';
        return;
    }
    var currentIndex = Math.max(0, Number(progress.current_index || 0));
    var state = String(progress.state || 'running');
    var progressRatio = progress.steps.length ? Math.min(1, Math.max(0, currentIndex / progress.steps.length)) : 0;
    var actorId = String(progress.actor || '').trim();
    var actorProfile = actorId ? getPromptProfileForAgent(actorId) : {};
    var actorName = String((actorProfile && actorProfile.agent_name) || (findLatestAgentStatus(actorId) || {}).name || actorId || '-');
    var stepsHtml = progress.steps.map(function(step, idx) {
        var stepIndex = idx + 1;
        var cls = 'session-progress-step';
        var stateText = '待执行';
        if (state === 'waiting_user' && stepIndex === currentIndex) {
            cls += ' waiting';
            stateText = '等待用户决策';
        } else if (stepIndex < currentIndex) {
            cls += ' done';
            stateText = '已完成';
        } else if (stepIndex === currentIndex) {
            cls += ' active';
            stateText = '进行中';
        }
        return '<div class="' + cls + '">' +
            '<div class="session-progress-index">步骤 ' + stepIndex + '</div>' +
            '<div class="session-progress-name">' + escapeHtml(String(step || ('步骤' + stepIndex))) + '</div>' +
            '<div class="session-progress-state">' + escapeHtml(stateText) + '</div>' +
            '</div>';
    }).join('');
    el.style.display = 'block';
    el.innerHTML =
        '<div class="session-progress-head">' +
            '<div>' +
                '<div class="session-progress-title">当前任务流程 · ' + escapeHtml(bundle.label || '任务') + '</div>' +
                '<div class="session-progress-sub">' +
                    escapeHtml(String(progress.title || (bundle.timing && bundle.timing.title) || '当前任务')) +
                    (bundle.timing && bundle.timing.active ? (' · ' + escapeHtml(formatDeadlineBadge(bundle.timing))) : '') +
                '</div>' +
            '</div>' +
        '</div>' +
        '<div style="margin:10px 0 12px 0;">' +
            '<div style="height:8px;border-radius:999px;background:rgba(226,232,240,0.9);overflow:hidden;">' +
                '<div style="height:100%;width:' + (progressRatio * 100).toFixed(1) + '%;border-radius:999px;background:linear-gradient(90deg,#fb7185,#f59e0b);transition:width .24s ease;"></div>' +
            '</div>' +
            '<div style="margin-top:6px;font-size:12px;color:#64748b;">已完成 ' + escapeHtml(String(Math.max(0, currentIndex - 1))) +
                ' / ' + escapeHtml(String(progress.steps.length)) + ' 步</div>' +
        '</div>' +
        '<div class="session-progress-steps">' + stepsHtml + '</div>' +
        '<div class="session-progress-detail">' +
            '<strong>当前阶段：</strong>' + escapeHtml(String(progress.current_step || '-')) +
            (actorId ? ('<br><strong>当前主导：</strong>' + escapeHtml(actorName)) : '') +
            (progress.detail ? ('<br><strong>说明：</strong>' + escapeHtml(String(progress.detail || ''))) : '') +
            renderPromptProfileChips(actorProfile, 5) +
        '</div>';
}

function resolveSessionOvertimeDecision(sessionId, decision, extendMinutes) {
    if (!sessionId) return Promise.reject(new Error('缺少 session_id'));
    return fetchJsonStrict('/api/team/session/' + encodeURIComponent(sessionId) + '/overtime', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            decision: decision,
            extend_minutes: extendMinutes || 5
        })
    }).then(function(data) {
        var sid = String(sessionId || '');
        overtimePromptRequestTsBySession[sid] = 0;
        var bundle = pickActiveSessionBundle();
        var timing = (data && data.session_timing && data.session_timing.active) ? data.session_timing : null;
        var progress = (data && data.session_progress && data.session_progress.active) ? data.session_progress : null;
        var overtime = (data && data.session_overtime && data.session_overtime.active) ? data.session_overtime : null;
        var updated = false;
        if ((bundle && bundle.source === 'ask' && String((bundle.timing && bundle.timing.session_id) || '') === sid) ||
            String((latestAskSessionTiming && latestAskSessionTiming.session_id) || '') === sid) {
            latestAskSessionTiming = timing;
            latestAskSessionProgress = progress;
            latestAskSessionOvertime = overtime;
            updated = true;
        }
        if (!updated && latestPortfolioSchedulerState && String(latestPortfolioSchedulerState.current_session || '') === sid) {
            latestPortfolioSchedulerState.session_timing = timing || {active: false, session_id: sid};
            latestPortfolioSchedulerState.session_progress = progress || {active: false, session_id: sid};
            latestPortfolioSchedulerState.session_overtime = overtime || {active: false, session_id: sid};
            updated = true;
        }
        if (!updated && latestOrchestratorState && String(latestOrchestratorState.current_session || '') === sid) {
            latestOrchestratorState.session_timing = timing || {active: false, session_id: sid};
            latestOrchestratorState.session_progress = progress || {active: false, session_id: sid};
            latestOrchestratorState.session_overtime = overtime || {active: false, session_id: sid};
            updated = true;
        }
        if (!updated) {
            latestAskSessionTiming = timing;
            latestAskSessionProgress = progress;
            latestAskSessionOvertime = overtime;
        }
        removeWaitingOvertimeRow(sid);
        renderSessionTimingBanner();
        renderSessionProgressBoard();
        renderSessionOvertimePanel();
        renderTeamPromptRuntime();
        return data;
    });
}

function requestSessionOvertimePrompt(sessionId, workflow, title, force) {
    var sid = String(sessionId || '').trim();
    if (!sid) return Promise.resolve(null);
    var now = Date.now();
    var lastTs = Number(overtimePromptRequestTsBySession[sid] || 0);
    if (!force && now - lastTs < 10000) {
        return Promise.resolve(null);
    }
    overtimePromptRequestTsBySession[sid] = now;
    return fetchJsonStrict('/api/team/session/' + encodeURIComponent(sid) + '/overtime/request', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            workflow: String(workflow || ''),
            title: String(title || ''),
            extend_minutes: 5,
            message: '任务已达到设定时限，请选择继续等待，或立即停止任务。'
        })
    }).then(function(data) {
        var timing = (data && data.session_timing && data.session_timing.active) ? data.session_timing : null;
        var progress = (data && data.session_progress && data.session_progress.active) ? data.session_progress : null;
        var overtime = (data && data.session_overtime && data.session_overtime.active) ? data.session_overtime : null;

        if (latestPortfolioSchedulerState && String(latestPortfolioSchedulerState.current_session || '') === sid) {
            latestPortfolioSchedulerState.session_timing = timing || {active: false, session_id: sid};
            latestPortfolioSchedulerState.session_progress = progress || {active: false, session_id: sid};
            latestPortfolioSchedulerState.session_overtime = overtime || {active: false, session_id: sid};
        } else if (latestOrchestratorState && String(latestOrchestratorState.current_session || '') === sid) {
            latestOrchestratorState.session_timing = timing || {active: false, session_id: sid};
            latestOrchestratorState.session_progress = progress || {active: false, session_id: sid};
            latestOrchestratorState.session_overtime = overtime || {active: false, session_id: sid};
        } else if (String((latestAskSessionTiming && latestAskSessionTiming.session_id) || '') === sid) {
            latestAskSessionTiming = timing;
            latestAskSessionProgress = progress;
            latestAskSessionOvertime = overtime;
        }
        if (overtime && overtime.active && overtime.waiting) {
            upsertWaitingOvertimeRow({
                session_id: sid,
                session_timing: timing || {active: false, session_id: sid},
                session_progress: progress || {active: false, session_id: sid},
                session_overtime: overtime
            });
        }

        renderSessionTimingBanner();
        renderSessionProgressBoard();
        renderSessionOvertimePanel();
        renderTeamPromptRuntime();
        return data;
    }).catch(function() {
        return null;
    });
}

function submitSessionOvertimeDecision(sessionId, decision, extendMinutes) {
    var panel = document.getElementById('session-overtime-panel');
    if (panel) {
        Array.prototype.forEach.call(panel.querySelectorAll('button'), function(btn) {
            btn.disabled = true;
        });
    }
    resolveSessionOvertimeDecision(sessionId, decision, extendMinutes)
        .then(function() {
            loadStatus();
        })
        .catch(function(err) {
            alert('提交超时决策失败：' + String((err && err.message) || err || '未知错误'));
            renderSessionOvertimePanel();
        });
}

function renderSessionOvertimePanel() {
    var el = document.getElementById('session-overtime-panel');
    if (!el) return;
    var bundle = pickActiveSessionBundle();
    var overtime = bundle && bundle.overtime ? bundle.overtime : null;
    var progress = bundle && bundle.progress ? bundle.progress : null;
    var timing = bundle && bundle.timing ? bundle.timing : null;
    var waitingByOvertime = !!(overtime && overtime.active && overtime.waiting);
    var waitingByProgress = !!(
        bundle &&
        progress &&
        progress.active &&
        String(progress.state || '') === 'waiting_user' &&
        timing &&
        timing.active
    );
    if (!bundle) {
        el.style.display = 'none';
        el.innerHTML = '';
        return;
    }
    var sessionId = String(
        (overtime && overtime.session_id) ||
        (timing && timing.session_id) ||
        (progress && progress.session_id) ||
        ''
    );
    var timingExpired = !!(
        timing &&
        timing.active &&
        (String(timing.state || '').toLowerCase() === 'expired' || !!timing.is_expired)
    );
    if (!waitingByOvertime && !waitingByProgress) {
        if (timingExpired && sessionId) {
            requestSessionOvertimePrompt(
                sessionId,
                String((progress && progress.workflow) || (timing && timing.workflow) || ''),
                String((progress && progress.title) || (timing && timing.title) || ''),
                false
            );
            el.style.display = 'block';
            el.innerHTML =
                '<div class="session-overtime-head">' +
                    '<div>' +
                        '<div class="session-overtime-title">任务已超时，正在唤起决策</div>' +
                        '<div class="session-overtime-sub">系统正在请求“继续等待 / 立即停止”决策面板，如果未出现可手动重试。</div>' +
                    '</div>' +
                '</div>' +
                '<div class="session-overtime-actions">' +
                    '<button class="btn btn-outline btn-sm" onclick="requestSessionOvertimePrompt(\'' + escapeHtml(sessionId) + '\', \'' +
                        escapeHtml(String((progress && progress.workflow) || (timing && timing.workflow) || '')) + '\', \'' +
                        escapeHtml(String((progress && progress.title) || (timing && timing.title) || '')) + '\', true)">手动重试弹出决策</button>' +
                '</div>';
            return;
        }
        el.style.display = 'none';
        el.innerHTML = '';
        return;
    }
    var extendMinutes = Math.max(
        1,
        Math.floor((Number((overtime && overtime.default_extend_seconds) || 300) || 300) / 60)
    );
    var message = String(
        (overtime && overtime.message) ||
        (progress && progress.detail) ||
        '当前任务超时，建议你决定继续等待还是立即停止。'
    );
    el.style.display = 'block';
    if (!sessionId) {
        el.innerHTML =
            '<div class="session-overtime-head">' +
                '<div>' +
                    '<div class="session-overtime-title">任务已达到时限，等待你的决定</div>' +
                    '<div class="session-overtime-sub">当前会话标识缺失，正在自动同步状态，请稍后重试。</div>' +
                '</div>' +
            '</div>' +
            '<div class="session-overtime-actions">' +
                '<button class="btn btn-outline btn-sm" onclick="loadStatus()">立即刷新状态</button>' +
            '</div>';
        return;
    }
    el.innerHTML =
        '<div class="session-overtime-head">' +
            '<div>' +
                '<div class="session-overtime-title">任务已达到时限，等待你的决定</div>' +
                '<div class="session-overtime-sub">' +
                    escapeHtml(message) +
                '</div>' +
            '</div>' +
        '</div>' +
        '<div class="session-overtime-actions">' +
            '<button class="btn btn-primary btn-sm" onclick="submitSessionOvertimeDecision(\'' + escapeHtml(sessionId) + '\', \'extend\', ' + extendMinutes + ')">继续等待' + extendMinutes + '分钟</button>' +
            '<button class="btn btn-outline btn-sm btn-report-delete" onclick="submitSessionOvertimeDecision(\'' + escapeHtml(sessionId) + '\', \'stop\', 0)">立即停止任务</button>' +
        '</div>';
}

function downloadTextFile(filename, content, mimeType) {
    var blob = new Blob([content || ''], {type: mimeType || 'text/plain;charset=utf-8'});
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(function() { URL.revokeObjectURL(url); }, 3000);
}

function isWorkbenchOpen() {
    var root = document.getElementById('team-root');
    if (!root) return false;
    return !root.classList.contains('immersive-mode');
}

function syncAskPanelButton() {
    var btn = document.getElementById('btn-toggle-ask-panel');
    if (!btn) return;
    btn.textContent = '🗨 直连问答';
    btn.classList.add('btn-active');
}

function setWorkbenchOpen(opened) {
    var root = document.getElementById('team-root');
    var btn = document.getElementById('btn-toggle-workbench');
    if (!root) return;
    root.classList.toggle('immersive-mode', !opened);
    if (btn) {
        btn.textContent = opened ? '沉浸场景' : '返回工作台';
    }
    syncAskPanelButton();
}

function toggleWorkbench() {
    setWorkbenchOpen(!isWorkbenchOpen());
}

function setAskPanelOpen(opened) {
    var root = document.getElementById('team-root');
    if (!root) return;
    root.classList.toggle('ask-open', !!opened);
    syncAskPanelButton();
}

function toggleAskPanel() {
    if (!isWorkbenchOpen()) {
        setWorkbenchOpen(true);
    }
    setAskPanelOpen(true);
    focusAskComposer();
}

// ──────────────── 系统提示词管理 ────────────────
function setTeamPromptStatus(text, level) {
    var el = document.getElementById('team-prompt-status');
    if (!el) return;
    el.textContent = text || '';
    el.classList.remove('ok', 'error', 'loading');
    if (level === 'ok') el.classList.add('ok');
    else if (level === 'error') el.classList.add('error');
    else if (level === 'loading') el.classList.add('loading');
}

function setTeamModelStatus(text, level) {
    var el = document.getElementById('team-model-status');
    if (!el) return;
    el.textContent = text || '';
    el.classList.remove('ok', 'error', 'loading');
    if (level === 'ok') el.classList.add('ok');
    else if (level === 'error') el.classList.add('error');
    else if (level === 'loading') el.classList.add('loading');
}

function _teamModelAllowed() {
    var rows = teamModelConfig && Array.isArray(teamModelConfig.allowed_models)
        ? teamModelConfig.allowed_models
        : [];
    if (!rows.length) {
        return ['qwen3-max', 'Moonshot-Kimi-K2-Instruct', 'deepseek-r1', 'MiniMax-M2.1'];
    }
    return rows.map(function(x) { return String(x || '').trim(); }).filter(function(x) { return !!x; });
}

function _renderModelOptionsHtml(rows, selected) {
    var val = String(selected || '');
    return rows.map(function(model) {
        var m = String(model || '').trim();
        var sel = (m === val) ? ' selected' : '';
        return '<option value="' + escapeHtml(m) + '"' + sel + '>' + escapeHtml(m) + '</option>';
    }).join('');
}

function loadTeamModelConfigs(silent) {
    if (!silent) setTeamModelStatus('模型配置加载中...', 'loading');
    fetchTeamApiJson('/api/team/models?ts=' + Date.now(), {cache: 'no-store'})
        .then(function(data) {
            var models = (data && data.models) || {};
            teamModelConfig = {
                allowed_models: Array.isArray(models.allowed_models) ? models.allowed_models : [],
                team_default_model: String(models.team_default_model || ''),
                team_agent_models: (models.team_agent_models && typeof models.team_agent_models === 'object')
                    ? models.team_agent_models
                    : {},
                runtime: (models.runtime && typeof models.runtime === 'object') ? models.runtime : {},
            };
            renderTeamModelSelectors();
            setTeamModelStatus('模型配置已加载', 'ok');
        })
        .catch(function(err) {
            setTeamModelStatus('模型配置加载失败: ' + String((err && err.message) || err || ''), 'error');
        });
}

function renderTeamModelSelectors() {
    var allowed = _teamModelAllowed();
    var defaultSel = document.getElementById('team-model-default');
    var agentSel = document.getElementById('team-model-agent');
    var agentValSel = document.getElementById('team-model-agent-value');
    if (defaultSel) {
        var defaultModel = String((teamModelConfig && teamModelConfig.team_default_model) || allowed[0] || 'qwen3-max');
        defaultSel.innerHTML = _renderModelOptionsHtml(allowed, defaultModel);
    }
    if (agentValSel) {
        var aid = agentSel ? String(agentSel.value || 'director') : 'director';
        var per = (teamModelConfig && teamModelConfig.team_agent_models) || {};
        var selected = String(per[aid] || (teamModelConfig && teamModelConfig.team_default_model) || allowed[0] || 'qwen3-max');
        agentValSel.innerHTML = _renderModelOptionsHtml(allowed, selected);
    }
}

function onTeamModelAgentChanged() {
    renderTeamModelSelectors();
}

function saveTeamDefaultModel() {
    var sel = document.getElementById('team-model-default');
    if (!sel) return;
    var model = String(sel.value || '').trim();
    if (!model) return;
    setTeamModelStatus('保存默认模型中...', 'loading');
    fetchTeamApiJson('/api/team/models/default', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({model: model})
    })
    .then(function(data) {
        teamModelConfig = (data && data.models) || teamModelConfig;
        renderTeamModelSelectors();
        setTeamModelStatus('团队默认模型已更新', 'ok');
    })
    .catch(function(err) {
        setTeamModelStatus('保存失败: ' + String((err && err.message) || err || ''), 'error');
    });
}

function saveTeamAgentModel() {
    var aidSel = document.getElementById('team-model-agent');
    var modelSel = document.getElementById('team-model-agent-value');
    if (!aidSel || !modelSel) return;
    var aid = String(aidSel.value || '').trim();
    var model = String(modelSel.value || '').trim();
    if (!aid || !model) return;
    setTeamModelStatus('保存智能体模型中...', 'loading');
    fetchTeamApiJson('/api/team/models/' + encodeURIComponent(aid), {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({model: model})
    })
    .then(function(data) {
        teamModelConfig = (data && data.models) || teamModelConfig;
        renderTeamModelSelectors();
        var name = TEAM_AGENT_NAMES[aid] || aid;
        setTeamModelStatus(name + ' 模型已更新', 'ok');
    })
    .catch(function(err) {
        setTeamModelStatus('保存失败: ' + String((err && err.message) || err || ''), 'error');
    });
}

function loadTeamPromptConfigs(silent) {
    if (!silent) setTeamPromptStatus('正在加载...', 'loading');
    fetchTeamApiJson('/api/team/prompts?ts=' + Date.now(), {cache: 'no-store'})
        .then(function(data) {
            clearRuntimeAlert('prompts');
            teamPromptConfigs = Array.isArray(data.prompts) ? data.prompts : [];
            renderTeamPromptAgentOptions();
            onTeamPromptAgentChanged();
            setTeamPromptStatus('已加载', 'ok');
        })
        .catch(function(err) {
            // 网络抖动/本地服务短暂繁忙时，自动重试一次
            setTimeout(function() {
                fetchTeamApiJson('/api/team/prompts?ts=' + Date.now(), {cache: 'no-store'})
                    .then(function(data) {
                        clearRuntimeAlert('prompts');
                        teamPromptConfigs = Array.isArray(data.prompts) ? data.prompts : [];
                        renderTeamPromptAgentOptions();
                        onTeamPromptAgentChanged();
                        setTeamPromptStatus('已加载（重试成功）', 'ok');
                    })
                    .catch(function(err2) {
                        var msg = String((err2 && err2.message) || (err && err.message) || err2 || err || '');
                        if (/failed to fetch/i.test(msg)) {
                            msg = '网络连接中断或服务未响应（请确认 AlphaFin 服务在运行）';
                        }
                        setRuntimeAlert('prompts', '提示词管理中心加载失败：' + msg, 'error');
                        setTeamPromptStatus('加载失败: ' + msg, 'error');
                    });
            }, 350);
        });
}

function renderTeamPromptAgentOptions() {
    var sel = document.getElementById('team-prompt-agent');
    if (!sel) return;
    if (!teamPromptConfigs.length) {
        sel.innerHTML = '<option value="">暂无可配置项</option>';
        return;
    }

    var prev = sel.value;
    var groups = {};
    teamPromptConfigs.forEach(function(item) {
        var cat = String(item.category || '未分类');
        if (!groups[cat]) groups[cat] = [];
        groups[cat].push(item);
    });
    var cats = Object.keys(groups);
    sel.innerHTML = cats.map(function(cat) {
        var options = groups[cat].map(function(item) {
            var flag = item.is_overridden ? '（已覆盖）' : '';
            return '<option value="' + escapeHtml(String(item.key || '')) + '">' +
                escapeHtml(String(item.name || item.key || '')) + flag + '</option>';
        }).join('');
        return '<optgroup label="' + escapeHtml(cat) + '">' + options + '</optgroup>';
    }).join('');
    if (prev) sel.value = prev;
    if (!sel.value && teamPromptConfigs[0]) sel.value = teamPromptConfigs[0].key;
}

function getCurrentTeamPromptItem() {
    var sel = document.getElementById('team-prompt-agent');
    var key = sel ? sel.value : '';
    for (var i = 0; i < teamPromptConfigs.length; i++) {
        if (String(teamPromptConfigs[i].key) === String(key)) return teamPromptConfigs[i];
    }
    return null;
}

function onTeamPromptAgentChanged() {
    var textArea = document.getElementById('team-prompt-text');
    var descEl = document.getElementById('team-prompt-desc');
    var item = getCurrentTeamPromptItem();
    if (!textArea) return;
    if (!item) {
        textArea.value = '';
        if (descEl) descEl.innerHTML = '这里会显示当前提示词条目的用途说明';
        renderTeamPromptRuntime();
        return;
    }
    textArea.value = String(item.prompt || '');
    if (descEl) {
        descEl.innerHTML =
            '<div style="font-size:12px;font-weight:700;color:#0f172a;">' +
                escapeHtml(String(item.name || item.key || '')) +
            '</div>' +
            '<div style="margin-top:4px;font-size:12px;color:#475569;">' +
                '分类：' + escapeHtml(String(item.category || '未分类')) +
                ' · 类型：' + escapeHtml(String(item.kind || 'prompt')) +
            '</div>' +
            '<div style="margin-top:6px;font-size:12px;color:#64748b;line-height:1.6;">' +
                escapeHtml(String(item.description || '暂无说明')) +
            '</div>';
    }
    renderTeamPromptRuntime();
}

function saveTeamPromptConfig() {
    var item = getCurrentTeamPromptItem();
    var textArea = document.getElementById('team-prompt-text');
    if (!item || !textArea) return;

    var promptText = String(textArea.value || '').trim();
    if (!promptText) {
        setTeamPromptStatus('提示词不能为空', 'error');
        return;
    }

    setTeamPromptStatus('保存中...', 'loading');
    fetchTeamApiJson('/api/team/prompts/' + encodeURIComponent(item.key), {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({prompt: promptText})
    })
    .then(function(data) {
        if (data && data.error) throw new Error(data.error);
        teamPromptConfigs = Array.isArray(data.prompts) ? data.prompts : teamPromptConfigs;
        renderTeamPromptAgentOptions();
        onTeamPromptAgentChanged();
        if (data && data.runtime_applied) {
            setTeamPromptStatus('保存成功，已同步到运行中智能体', 'ok');
        } else {
            setTeamPromptStatus('保存成功（将在智能体初始化时生效）', 'ok');
        }
    })
    .catch(function(err) {
        setTeamPromptStatus('保存失败: ' + String((err && err.message) || err || ''), 'error');
    });
}

function resetTeamPromptConfig() {
    var item = getCurrentTeamPromptItem();
    if (!item) return;
    setTeamPromptStatus('恢复默认中...', 'loading');
    fetchTeamApiJson('/api/team/prompts/' + encodeURIComponent(item.key) + '/reset', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({})
    })
    .then(function(data) {
        if (data && data.error) throw new Error(data.error);
        teamPromptConfigs = Array.isArray(data.prompts) ? data.prompts : teamPromptConfigs;
        renderTeamPromptAgentOptions();
        onTeamPromptAgentChanged();
        setTeamPromptStatus('已恢复默认提示词', 'ok');
    })
    .catch(function(err) {
        setTeamPromptStatus('恢复失败: ' + String((err && err.message) || err || ''), 'error');
    });
}

// ──────────────── 工具 / Skill 透明审查 ────────────────
function setToolAuditStatus(text, level) {
    var el = document.getElementById('tool-audit-status');
    if (!el) return;
    el.textContent = text || '';
    el.classList.remove('ok', 'error', 'loading');
    if (level === 'ok') el.classList.add('ok');
    else if (level === 'error') el.classList.add('error');
    else if (level === 'loading') el.classList.add('loading');
}

function riskLevelLabel(level) {
    var lv = String(level || '').toLowerCase();
    if (lv === 'high') return '高风险';
    if (lv === 'medium') return '中风险';
    return '低风险';
}

function fetchTeamApiJson(path, options) {
    var primary = String(path || '');
    if (!primary) {
        return Promise.reject(new Error('接口路径为空'));
    }
    return fetchJsonStrict(primary, options).catch(function(err) {
        var msg = String((err && err.message) || err || '');
        var is404 = (msg.indexOf('接口不存在') >= 0) || /(^|\s)404(\s|$)|HTTP\s*404/i.test(msg);
        if (!is404 || primary.indexOf('/team/') === 0) {
            throw err;
        }
        var fallback = '/team' + (primary.charAt(0) === '/' ? primary : ('/' + primary));
        return fetchJsonStrict(fallback, options);
    });
}

function encodeToolAuditTargetId(value) {
    return encodeURIComponent(String(value || ''));
}

function decodeToolAuditTargetId(value) {
    try {
        return decodeURIComponent(String(value || ''));
    } catch (e) {
        return String(value || '');
    }
}

function bindToolAuditActionDelegation() {
    var listEl = document.getElementById('tool-audit-list');
    if (!listEl || listEl.dataset.bound === '1') return;
    listEl.dataset.bound = '1';
    listEl.addEventListener('click', function(event) {
        var target = event.target;
        if (!target || !target.closest) return;
        var btn = target.closest('.js-tool-audit-action');
        if (!btn) return;
        var action = String(btn.getAttribute('data-action') || '').trim();
        var targetType = String(btn.getAttribute('data-target-type') || 'tool').trim();
        var targetId = decodeToolAuditTargetId(btn.getAttribute('data-target-id') || '');
        if (!targetId) {
            setToolAuditStatus('对象ID为空，无法执行操作', 'error');
            return;
        }
        if (action === 'source') {
            viewToolAuditSource(targetType, targetId);
            return;
        }
        if (action === 'review') {
            runToolAuditReview(targetType, targetId);
        }
    });
}

function bindPromptProfileActionDelegation() {
    var listEl = document.getElementById('agent-status-list');
    if (!listEl || listEl.dataset.promptBound === '1') return;
    listEl.dataset.promptBound = '1';
    listEl.addEventListener('click', function(event) {
        var target = event.target;
        if (!target || !target.closest) return;
        var btn = target.closest('.js-open-prompt-profile');
        if (!btn) return;
        var aid = String(btn.getAttribute('data-agent-id') || '').trim();
        if (!aid) return;
        event.preventDefault();
        showAgentPromptProfile(aid);
    });
}

function loadToolAuditCatalog(silent) {
    if (!silent) setToolAuditStatus('正在加载工具与 Skill 清单...', 'loading');
    return fetchTeamApiJson('/api/team/tools_audit/catalog?ts=' + Date.now())
        .then(function(data) {
            toolAuditCatalog = data || {};
            toolAuditLoaded = true;
            renderToolAuditCatalog();
            setToolAuditStatus('清单已加载', 'ok');
            return data;
        })
        .catch(function(err) {
            setToolAuditStatus('加载失败: ' + String((err && err.message) || err || ''), 'error');
            throw err;
        });
}

function renderToolAuditCatalog() {
    var summaryEl = document.getElementById('tool-audit-summary');
    var listEl = document.getElementById('tool-audit-list');
    if (!summaryEl || !listEl) return;
    if (!toolAuditCatalog) {
        summaryEl.textContent = '点击“展示工具与 Skill 清单”后查看';
        listEl.innerHTML = '<div class="activity-empty">尚未加载工具与 Skill 清单</div>';
        return;
    }

    var summary = toolAuditCatalog.summary || {};
    summaryEl.textContent =
        '工具 ' + (summary.tool_count || 0) +
        ' 个，Skill ' + (summary.skill_count || 0) +
        ' 个（已批准 ' + (summary.skill_approved_count || 0) +
        ' / 待审核 ' + (summary.skill_pending_count || 0) +
        '），高风险能力 ' + ((summary.high_risk_tools || 0) + (summary.high_risk_skills || 0)) + ' 个';

    var tools = Array.isArray(toolAuditCatalog.tools) ? toolAuditCatalog.tools : [];
    var skills = Array.isArray(toolAuditCatalog.skills) ? toolAuditCatalog.skills : [];

    function renderToolItem(t) {
        var risk = String(t.risk_level || 'low');
        var params = Array.isArray(t.parameter_keys) ? t.parameter_keys.join(', ') : '';
        var agents = Array.isArray(t.agent_names) ? t.agent_names.join('、') : '';
        var targetId = encodeToolAuditTargetId(String(t.name || ''));
        return '<div class="tool-audit-item">' +
            '<div class="tool-audit-item-head">' +
            '<span class="tool-audit-item-name">工具 · ' + escapeHtml(String(t.name || '')) + '</span>' +
            '<span class="tool-audit-risk ' + escapeHtml(risk) + '">' + riskLevelLabel(risk) + '</span>' +
            '</div>' +
            '<div class="tool-audit-item-desc">' + escapeHtml(String(t.description || '')) + '</div>' +
            '<div class="tool-audit-item-meta">参数: ' + escapeHtml(params || '(无)') + '</div>' +
            '<div class="tool-audit-item-meta">可用智能体: ' + escapeHtml(agents || '(未知)') + '</div>' +
            '<div class="tool-audit-item-actions">' +
            '<button class="btn btn-outline btn-sm js-tool-audit-action" data-action="source" data-target-type="tool" data-target-id="' + escapeHtml(targetId) + '">查看底层代码</button>' +
            '<button class="btn btn-outline btn-sm js-tool-audit-action" data-action="review" data-target-type="tool" data-target-id="' + escapeHtml(targetId) + '">风控官审查</button>' +
            '</div>' +
            '</div>';
    }

    function renderSkillItem(s) {
        var risk = String(s.risk_level || 'low');
        var status = s.approved ? '已批准' : '待审核';
        var targetId = encodeToolAuditTargetId(String(s.id || ''));
        return '<div class="tool-audit-item">' +
            '<div class="tool-audit-item-head">' +
            '<span class="tool-audit-item-name">Skill · ' + escapeHtml(String(s.name || '')) + '</span>' +
            '<span class="tool-audit-risk ' + escapeHtml(risk) + '">' + riskLevelLabel(risk) + '</span>' +
            '</div>' +
            '<div class="tool-audit-item-desc">' + escapeHtml(String(s.description || '')) + '</div>' +
            '<div class="tool-audit-item-meta">ID: ' + escapeHtml(String(s.id || '')) + '</div>' +
            '<div class="tool-audit-item-meta">分类: ' + escapeHtml(String(s.category || '-')) +
            ' | 状态: ' + escapeHtml(status) +
            ' | 创建者: ' + escapeHtml(String(s.creator || '-')) +
            '</div>' +
            '<div class="tool-audit-item-actions">' +
            '<button class="btn btn-outline btn-sm js-tool-audit-action" data-action="source" data-target-type="skill" data-target-id="' + escapeHtml(targetId) + '">查看底层代码</button>' +
            '<button class="btn btn-outline btn-sm js-tool-audit-action" data-action="review" data-target-type="skill" data-target-id="' + escapeHtml(targetId) + '">风控官审查</button>' +
            '</div>' +
            '</div>';
    }

    listEl.innerHTML =
        '<div>' +
        '<div class="tool-audit-section-title">工具列表（' + tools.length + '）</div>' +
        '<div class="tool-audit-items">' + (tools.length ? tools.map(renderToolItem).join('') : '<div class="activity-empty">暂无工具</div>') + '</div>' +
        '</div>' +
        '<div>' +
        '<div class="tool-audit-section-title">Skill 列表（' + skills.length + '）</div>' +
        '<div class="tool-audit-items">' + (skills.length ? skills.map(renderSkillItem).join('') : '<div class="activity-empty">暂无 Skill</div>') + '</div>' +
        '</div>';
}

function viewToolAuditSource(targetType, targetId) {
    var sourceEl = document.getElementById('tool-audit-source');
    if (!sourceEl) return;
    if (!targetId) {
        sourceEl.textContent = '源码加载失败: 缺少对象ID';
        setToolAuditStatus('源码加载失败', 'error');
        return;
    }
    sourceEl.textContent = '加载源码中...';
    setToolAuditStatus('源码加载中...', 'loading');
    var url = '/api/team/tools_audit/source?target_type=' +
        encodeURIComponent(String(targetType || 'tool')) +
        '&target_id=' + encodeURIComponent(String(targetId || ''));
    fetchTeamApiJson(url)
        .then(function(data) {
            var header = [
                '对象类型: ' + String(data.target_type || '-'),
                '对象ID: ' + String(data.target_id || '-'),
                '路径: ' + String(data.path || '-'),
                '处理函数: ' + String(data.handler_name || '-'),
                '行号: ' + String(data.line_start || 1) + ' - ' + String(data.line_end || 1),
                ''
            ].join('\n');
            sourceEl.textContent = header + String(data.code || '');
            setToolAuditStatus('源码已加载: ' + String(targetId || ''), 'ok');
        })
        .catch(function(err) {
            sourceEl.textContent = '源码加载失败: ' + String((err && err.message) || err || '');
            setToolAuditStatus('源码加载失败', 'error');
        });
}

function runToolAuditReview(targetType, targetId) {
    var reviewEl = document.getElementById('tool-audit-review');
    if (reviewEl) {
        reviewEl.textContent = '风控官审查中，请稍候...';
    }
    setToolAuditStatus('正在执行风控审查...', 'loading');
    fetchTeamApiJson('/api/team/tools_audit/review', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            target_type: targetType,
            target_id: targetId || ''
        })
    }).then(function(data) {
        renderToolAuditReviewResult(data || {});
        setToolAuditStatus('风控审查完成', 'ok');
    }).catch(function(err) {
        if (reviewEl) {
            reviewEl.textContent = '风控审查失败: ' + String((err && err.message) || err || '');
        }
        setToolAuditStatus('风控审查失败', 'error');
    });
}

function renderToolAuditReviewResult(data) {
    var reviewEl = document.getElementById('tool-audit-review');
    if (!reviewEl) return;

    function formatStaticAudit(sr) {
        if (!sr) return '静态审查: (无)';
        var issues = Array.isArray(sr.issues) ? sr.issues : [];
        var positives = Array.isArray(sr.positives) ? sr.positives : [];
        var lines = [
            '静态审查等级: ' + String(sr.level || '-'),
            '安全性评分: ' + String(sr.security_score || '-'),
            '可靠性评分: ' + String(sr.reliability_score || '-')
        ];
        if (positives.length) {
            lines.push('通过项: ' + positives.join('；'));
        }
        if (issues.length) {
            lines.push('问题项: ' + issues.join('；'));
        }
        return lines.join('\n');
    }

    var lines = [];
    if (String(data.target_type || '') === 'all') {
        var summary = data.summary || {};
        lines.push('【全量审查】');
        lines.push('总项数: ' + String(summary.total_items || 0));
        lines.push('高风险项: ' + String(summary.high_risk_items || 0));
        lines.push('最低安全分: ' + String(summary.min_security_score || 0));
        lines.push('最低可靠性分: ' + String(summary.min_reliability_score || 0));
        lines.push('');
        var items = Array.isArray(data.items) ? data.items : [];
        if (items.length) {
            lines.push('--- 静态审查清单 ---');
            items.forEach(function(item) {
                var sr = item.static_audit || {};
                lines.push(
                    '[' + String(item.target_type || '-') + '] ' +
                    String(item.target_id || '-') +
                    ' | 风险=' + String(item.risk_level || '-') +
                    ' | 安全=' + String(sr.security_score || '-') +
                    ' | 可靠=' + String(sr.reliability_score || '-')
                );
            });
            lines.push('');
        }
    } else {
        lines.push('【单项审查】');
        lines.push('对象: ' + String(data.target_type || '-') + ' / ' + String(data.target_id || '-'));
        lines.push(formatStaticAudit(data.static_audit || {}));
        lines.push('');
    }

    var llm = data.llm_audit || {};
    lines.push('--- 风控官审查结论 ---');
    if (llm && llm.available) {
        lines.push(String(llm.review || '(无结论内容)'));
    } else {
        lines.push('风控官不可用或审查失败: ' + String((llm && llm.error) || '未知错误'));
    }

    reviewEl.textContent = lines.join('\n');
}

// ──────────────── 直连问答工作流配置 ────────────────
function setAskWorkflowStatus(text, level) {
    var el = document.getElementById('ask-workflow-status');
    if (!el) return;
    el.textContent = text || '';
    el.classList.remove('ok', 'error', 'loading');
    if (level === 'ok') el.classList.add('ok');
    else if (level === 'error') el.classList.add('error');
    else if (level === 'loading') el.classList.add('loading');
}

function normalizeAskSessionId(sessionId) {
    var s = String(sessionId || '').trim();
    if (!s) return '';
    if (!/^user_ask_[A-Za-z0-9_-]{4,64}$/.test(s)) return '';
    return s.slice(0, 80);
}

function _newAskChatId() {
    return 'chat_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 8);
}

function _newAskMessageId() {
    return 'msg_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 8);
}

function _newAskChatSession() {
    var now = Date.now();
    return {
        id: _newAskChatId(),
        title: '新聊天',
        created_at: now,
        updated_at: now,
        backend_sessions: {},
        messages: []
    };
}

function _getActiveAskChat() {
    for (var i = 0; i < askChatSessions.length; i++) {
        if (String(askChatSessions[i].id) === String(askActiveChatId)) return askChatSessions[i];
    }
    return null;
}

function _buildAskChatTitleByQuestion(question) {
    var q = String(question || '').trim().replace(/\s+/g, ' ');
    if (!q) return '新聊天';
    return q.length > 18 ? (q.slice(0, 18) + '...') : q;
}

function _truncateChatSystemText(text, limit) {
    var raw = String(text || '').trim().replace(/\s+/g, ' ');
    var maxLen = Number(limit || 220);
    if (!raw) return '';
    return raw.length > maxLen ? (raw.slice(0, maxLen) + '...') : raw;
}

function _stripHtmlText(rawHtml) {
    var html = String(rawHtml || '');
    if (!html) return '';
    return html
        .replace(/<style[\s\S]*?<\/style>/gi, ' ')
        .replace(/<script[\s\S]*?<\/script>/gi, ' ')
        .replace(/<[^>]+>/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function bindSessionToChat(sessionId, chatId, meta) {
    var sid = String(sessionId || '').trim();
    var cid = String(chatId || '').trim();
    if (!sid || !cid) return;
    taskChatBindings[sid] = Object.assign({
        chat_id: cid,
        session_id: sid,
        updated_at: Date.now()
    }, meta || {});
}

function getChatBindingBySession(sessionId) {
    var sid = String(sessionId || '').trim();
    if (!sid || !taskChatBindings[sid]) return null;
    taskChatBindings[sid].updated_at = Date.now();
    return taskChatBindings[sid];
}

function appendSystemMessageToChat(chatId, title, text, options) {
    var chat = getAskChatById(chatId) || ensureAskChatSessionReady();
    if (!chat) return null;
    if (!Array.isArray(chat.messages)) chat.messages = [];
    var opts = options && typeof options === 'object' ? options : {};
    var now = Date.now();
    var msg = {
        id: _newAskMessageId(),
        role: 'system',
        title: String(title || '系统消息'),
        text: String(text || ''),
        html: opts.html ? String(opts.html) : '',
        meta_text: String(opts.meta_text || ''),
        session_id: String(opts.session_id || ''),
        created_at: now
    };
    chat.messages.push(msg);
    _touchAskChat(chat);
    if (msg.session_id) {
        bindSessionToChat(msg.session_id, chat.id, {label: String(title || '系统消息')});
    }
    if (String(chat.id) === String(askActiveChatId)) renderAskChatMessages();
    return msg;
}

function appendSystemMessageToActiveChat(title, text, options) {
    var chat = ensureAskChatSessionReady();
    if (!chat) return null;
    return appendSystemMessageToChat(chat.id, title, text, options);
}

function renderSystemChatMessage(msg) {
    var title = escapeHtml(String((msg && msg.title) || '系统消息'));
    var metaText = String((msg && msg.meta_text) || '').trim();
    var text = escapeHtml(String((msg && msg.text) || ''));
    var htmlBlock = msg && msg.html ? String(msg.html) : '';
    var previewRaw = htmlBlock
        ? _stripHtmlText(htmlBlock)
        : (String((msg && msg.text) || '').trim() || String((msg && msg.meta_text) || '').trim());
    var preview = _truncateChatSystemText(previewRaw || '点击展开查看执行详情', 96);
    var hasError = /失败|错误|异常/.test(String((msg && msg.title) || '') + ' ' + String((msg && msg.text) || ''));
    var openAttr = hasError ? ' open' : '';
    var bodyContent = htmlBlock
        ? htmlBlock
        : (text ? ('<div class="ask-system-text">' + text + '</div>') : '<div class="ask-system-text">暂无详细内容</div>');
    return '<details class="ask-system-collapse"' + openAttr + '>' +
        '<summary class="ask-system-summary">' +
            '<span class="ask-system-summary-title">⚙️ ' + title + '</span>' +
            '<span class="ask-system-summary-preview">' + escapeHtml(preview) + '</span>' +
        '</summary>' +
        '<div class="ask-system-collapse-body">' +
            (metaText ? ('<div class="ask-system-meta">' + escapeHtml(metaText) + '</div>') : '') +
            bodyContent +
        '</div>' +
        '</details>';
}

function _touchAskChat(chat) {
    if (!chat) return;
    chat.updated_at = Date.now();
    saveAskSessionMap();
    renderAskChatSessionList();
}

function loadAskSessionMap() {
    askSessionMap = {};
    askChatSessions = [];
    askActiveChatId = '';
    try {
        var chatRaw = localStorage.getItem(ASK_CHAT_STORAGE_KEY);
        var chatActive = localStorage.getItem(ASK_CHAT_ACTIVE_STORAGE_KEY);
        var parsedChats = chatRaw ? JSON.parse(chatRaw) : [];
        if (Array.isArray(parsedChats)) {
            askChatSessions = parsedChats.filter(function(c) {
                return c && typeof c === 'object' && c.id;
            }).slice(0, 40).map(function(c) {
                var backend = (c.backend_sessions && typeof c.backend_sessions === 'object') ? c.backend_sessions : {};
                var cleanedBackend = {};
                Object.keys(backend).forEach(function(agentId) {
                    var sid = normalizeAskSessionId(backend[agentId]);
                    if (sid) cleanedBackend[String(agentId)] = sid;
                });
                return {
                    id: String(c.id),
                    title: String(c.title || '新聊天'),
                    created_at: Number(c.created_at || Date.now()),
                    updated_at: Number(c.updated_at || Date.now()),
                    backend_sessions: cleanedBackend,
                    messages: Array.isArray(c.messages) ? c.messages : []
                };
            });
        }
        askActiveChatId = String(chatActive || '');

        // 兼容旧版本：如果没有聊天会话，读取旧的按智能体 session map 作为默认会话
        var raw = localStorage.getItem(ASK_SESSION_STORAGE_KEY);
        var parsed = raw ? JSON.parse(raw) : {};
        if (parsed && typeof parsed === 'object') {
            Object.keys(parsed).forEach(function(agentId) {
                var sid = normalizeAskSessionId(parsed[agentId]);
                if (sid) askSessionMap[String(agentId)] = sid;
            });
        }
    } catch (e) {
        askSessionMap = {};
        askChatSessions = [];
        askActiveChatId = '';
    }

    if (!askChatSessions.length) {
        var first = _newAskChatSession();
        if (askSessionMap && Object.keys(askSessionMap).length) {
            first.backend_sessions = Object.assign({}, askSessionMap);
        }
        askChatSessions = [first];
        askActiveChatId = first.id;
    } else {
        var hasActive = askChatSessions.some(function(c) { return String(c.id) === String(askActiveChatId); });
        if (!hasActive) {
            askActiveChatId = String(askChatSessions[0].id);
        }
    }
    renderAskChatSessionList();
    renderAskChatMessages();
}

function saveAskSessionMap() {
    var active = _getActiveAskChat();
    if (active && active.backend_sessions && typeof active.backend_sessions === 'object') {
        askSessionMap = Object.assign({}, active.backend_sessions);
    }
    try {
        localStorage.setItem(ASK_SESSION_STORAGE_KEY, JSON.stringify(askSessionMap || {}));
        localStorage.setItem(ASK_CHAT_STORAGE_KEY, JSON.stringify(askChatSessions || []));
        localStorage.setItem(ASK_CHAT_ACTIVE_STORAGE_KEY, String(askActiveChatId || ''));
    } catch (e) {}
}

function getAskChatById(chatId) {
    var id = String(chatId || '');
    if (!id) return null;
    for (var i = 0; i < askChatSessions.length; i++) {
        if (String(askChatSessions[i].id) === id) return askChatSessions[i];
    }
    return null;
}

function setAskSessionForAgentInChat(chatId, agentId, sessionId) {
    var aid = String(agentId || '');
    if (!aid) return '';
    var sid = normalizeAskSessionId(sessionId);
    var chat = getAskChatById(chatId);
    if (!chat) return '';
    if (!chat.backend_sessions || typeof chat.backend_sessions !== 'object') {
        chat.backend_sessions = {};
    }
    if (!sid) {
        if (Object.prototype.hasOwnProperty.call(chat.backend_sessions, aid)) {
            delete chat.backend_sessions[aid];
            saveAskSessionMap();
            if (String(chat.id) === String(askActiveChatId)) updateAskSessionHint();
        }
        return '';
    }
    if (chat.backend_sessions[aid] !== sid) {
        chat.backend_sessions[aid] = sid;
        saveAskSessionMap();
        if (String(chat.id) === String(askActiveChatId)) updateAskSessionHint();
    }
    return sid;
}

function getAskSessionForAgent(agentId) {
    var aid = String(agentId || '');
    if (!aid) return '';
    var active = _getActiveAskChat();
    if (!active || !active.backend_sessions) return '';
    return normalizeAskSessionId(active.backend_sessions[aid] || '');
}

function setAskSessionForAgent(agentId, sessionId) {
    var active = _getActiveAskChat();
    if (!active) return '';
    return setAskSessionForAgentInChat(active.id, agentId, sessionId);
}

function switchAskChatSession(chatId) {
    var target = String(chatId || '');
    var exists = askChatSessions.some(function(c) { return String(c.id) === target; });
    if (!exists) return;
    askActiveChatId = target;
    saveAskSessionMap();
    renderAskChatSessionList();
    renderAskChatMessages();
    updateAskSessionHint();
}

function createAskChatSession(makeActive) {
    var chat = _newAskChatSession();
    askChatSessions.unshift(chat);
    if (askChatSessions.length > 40) askChatSessions = askChatSessions.slice(0, 40);
    if (makeActive !== false) {
        askActiveChatId = chat.id;
    }
    saveAskSessionMap();
    renderAskChatSessionList();
    renderAskChatMessages();
    updateAskSessionHint();
}

function ensureAskChatSessionReady() {
    var active = _getActiveAskChat();
    if (active) return active;
    createAskChatSession(true);
    return _getActiveAskChat();
}

function appendAskTurnToChat(chat, question, agentId, statusText) {
    var target = chat || ensureAskChatSessionReady();
    if (!target) return null;
    if (!Array.isArray(target.messages)) target.messages = [];
    var now = Date.now();
    var userMsg = {
        id: _newAskMessageId(),
        role: 'user',
        text: String(question || ''),
        created_at: now
    };
    var pendingMsg = {
        id: _newAskMessageId(),
        role: 'assistant',
        pending: true,
        status_text: String(statusText || '正在处理中...'),
        agent_id: String(agentId || ''),
        agent_label: String((AGENT_ICONS[agentId] || '🤖') + ' ' + (agentId || '智能体')),
        created_at: now
    };
    target.messages.push(userMsg, pendingMsg);
    if (target.title === '新聊天' || target.messages.length <= 4) {
        target.title = _buildAskChatTitleByQuestion(question);
    }
    _touchAskChat(target);
    if (String(target.id) === String(askActiveChatId)) renderAskChatMessages();
    return {chat_id: target.id, pending_id: pendingMsg.id};
}

function updateAskPendingMessage(chatId, pendingId, patch) {
    var chat = getAskChatById(chatId);
    if (!chat || !Array.isArray(chat.messages)) return false;
    for (var i = chat.messages.length - 1; i >= 0; i--) {
        var msg = chat.messages[i];
        if (!msg || String(msg.id) !== String(pendingId)) continue;
        chat.messages[i] = Object.assign({}, msg, patch || {});
        _touchAskChat(chat);
        if (String(chat.id) === String(askActiveChatId)) renderAskChatMessages();
        return true;
    }
    return false;
}

function markAskPendingStatus(chatId, pendingId, statusText) {
    updateAskPendingMessage(chatId, pendingId, {
        pending: true,
        error: '',
        status_text: String(statusText || '正在处理中...')
    });
}

function markAskPendingError(chatId, pendingId, errorText) {
    updateAskPendingMessage(chatId, pendingId, {
        pending: false,
        error: String(errorText || '处理失败，请重试'),
        status_text: ''
    });
}

function markAskPendingDone(chatId, pendingId, agentId, html) {
    var label = String((AGENT_ICONS[agentId] || '🤖') + ' ' + (agentId || '智能体'));
    updateAskPendingMessage(chatId, pendingId, {
        pending: false,
        error: '',
        status_text: '',
        agent_id: String(agentId || ''),
        agent_label: label,
        html: String(html || '')
    });
}

function deleteAskChatSession(chatId, evt) {
    if (evt && typeof evt.stopPropagation === 'function') evt.stopPropagation();
    var id = String(chatId || '');
    if (!id) return;
    var chat = getAskChatById(id);
    if (!chat) return;
    var title = String(chat.title || '该聊天');
    var hasMessages = Array.isArray(chat.messages) && chat.messages.length > 0;
    var confirmText = hasMessages
        ? ('确认删除聊天“' + title + '”及其历史记录吗？')
        : ('确认删除空聊天“' + title + '”吗？');
    if (!window.confirm(confirmText)) return;

    askChatSessions = askChatSessions.filter(function(c) {
        return String((c && c.id) || '') !== id;
    });
    if (!askChatSessions.length) {
        var first = _newAskChatSession();
        askChatSessions = [first];
        askActiveChatId = first.id;
    } else if (String(askActiveChatId || '') === id) {
        askActiveChatId = String((askChatSessions[0] && askChatSessions[0].id) || '');
    }

    Object.keys(askPendingMessageByTask || {}).forEach(function(taskId) {
        var row = askPendingMessageByTask[taskId] || {};
        if (String(row.chat_id || '') === id) {
            delete askPendingMessageByTask[taskId];
        }
    });
    Object.keys(taskChatBindings || {}).forEach(function(sessionId) {
        var row = taskChatBindings[sessionId] || {};
        if (String(row.chat_id || '') === id) {
            delete taskChatBindings[sessionId];
        }
    });

    saveAskSessionMap();
    renderAskChatSessionList();
    renderAskChatMessages();
    updateAskSessionHint();
}

function renderAskChatSessionList() {
    var box = document.getElementById('ask-chat-session-list');
    if (!box) return;
    box.innerHTML = askChatSessions.map(function(chat) {
        var active = String(chat.id) === String(askActiveChatId);
        var cls = active ? 'ask-chat-session-item active' : 'ask-chat-session-item';
        var chatId = escapeHtml(String(chat.id));
        return '<div class="ask-chat-session-entry' + (active ? ' active' : '') + '">' +
            '<button class="' + cls + '" onclick="switchAskChatSession(\'' + chatId + '\')">' +
                escapeHtml(String(chat.title || '新聊天')) +
            '</button>' +
            '<button class="ask-chat-session-delete" title="删除该聊天" onclick="deleteAskChatSession(\'' + chatId + '\', event)">×</button>' +
        '</div>';
    }).join('');
}

function updateAskWorkspaceStage(hasMessages) {
    var card = document.getElementById('workspace-chat-card');
    if (!card) return;
    var hasHistory = !!hasMessages;
    card.classList.toggle('chat-has-history', hasHistory);
    card.classList.toggle('chat-empty-stage', !hasHistory);
}

function renderAskChatMessages() {
    var responseDiv = document.getElementById('ask-response');
    if (!responseDiv) return;
    var chat = _getActiveAskChat();
    if (!chat || !Array.isArray(chat.messages) || !chat.messages.length) {
        updateAskWorkspaceStage(false);
        responseDiv.innerHTML = '<div class="ask-chat-empty">暂无聊天记录，输入问题后开始对话。</div>';
        return;
    }
    updateAskWorkspaceStage(true);
    var html = [];
    for (var i = 0; i < chat.messages.length; i++) {
        var msg = chat.messages[i] || {};
        if (msg.role === 'user') {
            html.push(
                '<div class="ask-msg user">' +
                '<div class="ask-msg-role">👤 你</div>' +
                '<div class="ask-msg-body">' + escapeHtml(String(msg.text || '')) + '</div>' +
                '</div>'
            );
        } else if (msg.role === 'system') {
            html.push(
                '<div class="ask-msg system">' +
                '<div class="ask-msg-role">⚙️ 系统</div>' +
                '<div class="ask-msg-body">' + renderSystemChatMessage(msg) + '</div>' +
                '</div>'
            );
        } else {
            var bodyHtml = '';
            if (msg.pending) {
                bodyHtml = '<span class="spinner"></span> ' + escapeHtml(String(msg.status_text || '正在处理中...'));
            } else if (msg.error) {
                bodyHtml = '<span style="color:#ef4444">' + escapeHtml(String(msg.error || '处理失败，请重试')) + '</span>';
            } else {
                bodyHtml = msg.html
                    ? String(msg.html)
                    : renderAskMarkdownBlock(String(msg.text || ''));
            }
            html.push(
                '<div class="ask-msg assistant">' +
                '<div class="ask-msg-role">' + escapeHtml(String(msg.agent_label || '🤖 智能体')) + '</div>' +
                '<div class="ask-msg-body">' + bodyHtml + '</div>' +
                '</div>'
            );
        }
    }
    responseDiv.innerHTML = html.join('');
    responseDiv.scrollTop = responseDiv.scrollHeight;
}

function updateAskSessionHint() {
    var el = document.getElementById('ask-session-hint');
    if (!el) return;
    var aid = getSelectedAskAgentId();
    var sid = getAskSessionForAgent(aid);
    if (sid) {
        el.textContent = '会话上下文：当前聊天连续线程已开启（' + aid + ' · ' + sid + '）';
        el.className = 'ask-session-hint active';
        return;
    }
    el.textContent = '会话上下文：当前聊天暂未建立线程记忆';
    el.className = 'ask-session-hint';
}

function resetAskSession(silent) {
    var aid = getSelectedAskAgentId();
    if (!aid) return;
    var active = _getActiveAskChat();
    if (active && active.backend_sessions && Object.prototype.hasOwnProperty.call(active.backend_sessions, aid)) {
        delete active.backend_sessions[aid];
        _touchAskChat(active);
    }
    updateAskSessionHint();
    if (!silent) {
        alert('已为 ' + aid + ' 切换到新会话。后续提问不再继承旧上下文。');
    }
}

function getSelectedAskAgentId() {
    var sel = document.getElementById('ask-agent');
    return sel ? String(sel.value || 'director') : 'director';
}

function isAskDirectorMode() {
    return getSelectedAskAgentId() === 'director';
}

function getAskModeText(mode) {
    var m = String(mode || '').toLowerCase();
    if (m === 'quick') return '快速';
    if (m === 'deep') return '深度';
    if (m === 'team') return '团队协作';
    return '快速';
}

function getSelectedAskMode() {
    var active = document.querySelector('.ask-mode-btn.active');
    var mode = active ? String(active.getAttribute('data-mode') || 'quick') : 'quick';
    if (['quick', 'deep', 'team'].indexOf(mode) < 0) mode = 'quick';
    return mode;
}

function getAskWebEnabled() {
    var el = document.getElementById('ask-enable-web');
    return !!(el && el.checked);
}

function persistAskWebEnabled(enabled) {
    try {
        localStorage.setItem(ASK_WEB_STORAGE_KEY, enabled ? '1' : '0');
    } catch (e) {}
}

function onAskWebToggleChanged() {
    persistAskWebEnabled(getAskWebEnabled());
    updateAskRoutingModeUI();
}

function loadAskWebToggle() {
    var checked = true;
    try {
        var raw = String(localStorage.getItem(ASK_WEB_STORAGE_KEY) || '1');
        checked = !(raw === '0' || raw === 'false' || raw === 'off');
    } catch (e) {
        checked = true;
    }
    var el = document.getElementById('ask-enable-web');
    if (el) el.checked = checked;
}

function persistAskMode(mode) {
    try {
        localStorage.setItem(ASK_MODE_STORAGE_KEY, String(mode || 'quick'));
    } catch (e) {}
}

function selectAskMode(mode, silent) {
    var target = String(mode || '').toLowerCase();
    if (['quick', 'deep', 'team'].indexOf(target) < 0) target = 'quick';
    document.querySelectorAll('.ask-mode-btn').forEach(function(el) {
        var active = String(el.getAttribute('data-mode') || '') === target;
        el.classList.toggle('active', active);
    });
    persistAskMode(target);
    var advancedPanel = document.getElementById('ask-advanced-panel');
    if (advancedPanel && target === 'team') {
        advancedPanel.open = true;
    }
    updateAskRoutingModeUI();
    if (!silent && target === 'team') {
        maybePromptTeamTimeLimitModal();
    }
    if (!silent) {
        var hint = document.getElementById('ask-mode-hint');
        if (hint && !hint.textContent) hint.textContent = '已切换到' + getAskModeText(target) + '模式';
    }
}

function loadAskMode() {
    var mode = 'quick';
    try {
        mode = String(localStorage.getItem(ASK_MODE_STORAGE_KEY) || 'quick');
    } catch (e) {
        mode = 'quick';
    }
    if (['quick', 'deep', 'team'].indexOf(mode) < 0) mode = 'quick';
    selectAskMode(mode, true);
}

function getSelectNumberOptions(selectEl) {
    if (!selectEl || !selectEl.options) return [];
    var out = [];
    for (var i = 0; i < selectEl.options.length; i++) {
        var n = Number(selectEl.options[i].value);
        if (isFinite(n)) out.push(n);
    }
    return out;
}

function setSelectNearestNumericValue(selectEl, target) {
    if (!selectEl) return Number(target || 0);
    var options = getSelectNumberOptions(selectEl);
    if (!options.length) {
        selectEl.value = String(target);
        return Number(target || 0);
    }
    var t = Number(target);
    if (!isFinite(t)) t = options[0];
    var best = options[0];
    var bestGap = Math.abs(options[0] - t);
    for (var i = 1; i < options.length; i++) {
        var gap = Math.abs(options[i] - t);
        if (gap < bestGap) {
            best = options[i];
            bestGap = gap;
        }
    }
    selectEl.value = String(best);
    return best;
}

function toMinutesFromSeconds(seconds) {
    var n = Number(seconds || 0);
    if (!isFinite(n) || n <= 0) return 8;
    return Math.max(1, Math.round(n / 60));
}

function toSecondsFromMinutes(minutes) {
    var n = Number(minutes || 0);
    if (!isFinite(n) || n <= 0) return 480;
    return Math.max(60, Math.round(n * 60));
}

function persistUnifiedTaskTimeLimit(minutes) {
    try {
        localStorage.setItem(UNIFIED_TIME_LIMIT_STORAGE_KEY, String(minutes || 8));
    } catch (e) {}
}

function readUnifiedTaskTimeLimit() {
    try {
        var raw = localStorage.getItem(UNIFIED_TIME_LIMIT_STORAGE_KEY);
        var n = Number(raw);
        if (isFinite(n) && n > 0) return n;
    } catch (e) {}
    return 8;
}

function applyUnifiedTaskTimeLimit(minutes, options) {
    var opts = options && typeof options === 'object' ? options : {};
    var m = Number(minutes || 8);
    if (!isFinite(m) || m <= 0) m = 8;
    m = Math.max(3, Math.min(20, Math.round(m)));

    var unifiedSel = document.getElementById('unified-task-time-limit');
    if (unifiedSel) {
        m = setSelectNearestNumericValue(unifiedSel, m);
    }

    var analyzeSel = document.getElementById('analyze-time-limit');
    if (analyzeSel) setSelectNearestNumericValue(analyzeSel, m);

    var pfSel = document.getElementById('pf-manual-time-limit');
    if (pfSel) setSelectNearestNumericValue(pfSel, m);

    var modalSel = document.getElementById('ask-time-limit-modal-select');
    if (modalSel) setSelectNearestNumericValue(modalSel, m);

    var askTimeoutSel = document.getElementById('ask-workflow-time-limit');
    if (askTimeoutSel) setSelectNearestNumericValue(askTimeoutSel, toSecondsFromMinutes(m));

    renderAskTeamTimeChip();
    if (!opts.silentPersist) persistUnifiedTaskTimeLimit(m);
    return m;
}

function updateUnifiedTaskTimeLimit() {
    var sel = document.getElementById('unified-task-time-limit');
    var minutes = sel ? Number(sel.value || 8) : 8;
    applyUnifiedTaskTimeLimit(minutes);
    if (askWorkflowConfig) {
        updateAskWorkflowTimeout();
    }
}

function loadUnifiedTaskTimeLimit() {
    applyUnifiedTaskTimeLimit(readUnifiedTaskTimeLimit(), {silentPersist: true});
}

function renderAskTeamTimeChip() {
    var chip = document.getElementById('ask-team-time-chip');
    var btn = document.getElementById('ask-team-time-btn');
    if (!chip || !btn) return;
    var teamEnabled = isAskDirectorMode() && getSelectedAskMode() === 'team';
    if (!teamEnabled) {
        chip.style.display = 'none';
        btn.style.display = 'none';
        return;
    }
    var askSel = document.getElementById('ask-workflow-time-limit');
    var sec = askSel ? Number(askSel.value || 480) : 480;
    var minutes = toMinutesFromSeconds(sec);
    chip.textContent = '团队时限：' + minutes + '分钟';
    chip.style.display = 'inline-flex';
    btn.style.display = 'inline-flex';
}

function openAskTimeLimitModal() {
    var modal = document.getElementById('ask-time-limit-modal');
    if (!modal) return;
    var askSel = document.getElementById('ask-workflow-time-limit');
    var sec = askSel ? Number(askSel.value || 480) : 480;
    applyUnifiedTaskTimeLimit(toMinutesFromSeconds(sec), {silentPersist: true});
    modal.style.display = 'flex';
}

function closeAskTimeLimitModal() {
    var modal = document.getElementById('ask-time-limit-modal');
    if (modal) modal.style.display = 'none';
}

function applyAskTimeLimitFromModal() {
    var sel = document.getElementById('ask-time-limit-modal-select');
    var minutes = sel ? Number(sel.value || 8) : 8;
    applyUnifiedTaskTimeLimit(minutes);
    closeAskTimeLimitModal();
    if (askWorkflowConfig) {
        updateAskWorkflowTimeout();
    }
}

function maybePromptTeamTimeLimitModal() {
    if (!(isAskDirectorMode() && getSelectedAskMode() === 'team')) return;
    var shown = '0';
    try {
        shown = String(localStorage.getItem(TEAM_MODE_TIMEOUT_PROMPT_STORAGE_KEY) || '0');
    } catch (e) {}
    if (shown === '1') return;
    openAskTimeLimitModal();
    try {
        localStorage.setItem(TEAM_MODE_TIMEOUT_PROMPT_STORAGE_KEY, '1');
    } catch (e) {}
}

function focusAgentDetails() {
    setWorkbenchOpen(true);
    setAskPanelOpen(false);
    switchTab('status');
    var side = document.querySelector('.side-panel');
    if (side) {
        side.scrollIntoView({behavior: 'smooth', block: 'start'});
    }
}

function handleAskCollapsibleToggle(el) {
    if (!el || !el.open) return;
    setTimeout(function() {
        try {
            el.scrollIntoView({behavior: 'smooth', block: 'nearest'});
        } catch (e) {}
    }, 60);
}

function updateAskRoutingModeUI() {
    var box = document.querySelector('.ask-workflow-box');
    var hint = document.getElementById('ask-routing-hint');
    var modeHint = document.getElementById('ask-mode-hint');
    var directorMode = isAskDirectorMode();
    var askMode = getSelectedAskMode();
    var webEnabled = getAskWebEnabled();
    var teamEnabled = directorMode && askMode === 'team';

    if (box) {
        box.classList.toggle('disabled', !teamEnabled);
    }
    if (hint) {
        if (teamEnabled) {
            hint.textContent = '当前：总监团队协作模式（将按下方团队流程执行）';
            hint.className = 'ask-routing-hint director';
        } else {
            hint.textContent = '当前：直连模式（团队流程暂不生效）';
            hint.className = 'ask-routing-hint expert';
        }
    }
    if (modeHint) {
        if (askMode === 'quick') {
            modeHint.textContent = directorMode
                ? '快速模式：轻量直答，事实型问题再联网核验，不走团队分配。'
                : '快速模式：由当前专家直接答复，普通寒暄不联网、事实型问题再核验。';
        } else if (askMode === 'deep') {
            modeHint.textContent = directorMode
                ? '深度模式：由决策总监深度分析，允许更长推理与更多工具轮次。'
                : '深度模式：由当前专家深度分析，不走团队分配。';
        } else {
            modeHint.textContent = directorMode
                ? '团队协作：先分配再协作，必要时讨论，最后总监答复。'
                : '团队协作仅对“决策总监”生效；当前会按专家直连处理。';
        }
        modeHint.textContent += webEnabled ? '（联网：开启）' : '（联网：关闭）';
    }

    if (!teamEnabled) {
        if (!directorMode && askMode === 'team') {
            setAskWorkflowStatus('团队协作仅在“决策总监”下生效', 'error');
        } else {
            setAskWorkflowStatus('当前非团队协作模式：团队流程不生效', '');
        }
    } else if (askWorkflowConfig) {
        setAskWorkflowStatus('当前为总监团队协作模式', 'ok');
    }
    renderAskTeamTimeChip();
}

function onAskAgentChanged() {
    updateAskRoutingModeUI();
    updateAskSessionHint();
}

function normalizeWorkflowForEditor(workflow) {
    var cfg = workflow && typeof workflow === 'object' ? workflow : {};
    var mode = String(cfg.mode || 'auto');
    if (['auto', 'director_only', 'custom'].indexOf(mode) < 0) mode = 'auto';
    var limits = cfg.limits && typeof cfg.limits === 'object' ? cfg.limits : {};
    if (!limits.timeout_seconds) limits.timeout_seconds = 480;
    return {
        mode: mode,
        name: String(cfg.name || ''),
        auto_for_simple: cfg.auto_for_simple !== false,
        custom_steps: Array.isArray(cfg.custom_steps) ? cfg.custom_steps : [],
        limits: limits
    };
}

function inferAskWorkflowIntent(workflow) {
    var cfg = normalizeWorkflowForEditor(workflow || {});
    if (cfg.mode === 'director_only') return 'director_only';
    if (cfg.mode === 'auto') return 'conditional_meeting';
    if (cfg.mode !== 'custom') return 'conditional_meeting';

    var steps = Array.isArray(cfg.custom_steps) ? cfg.custom_steps : [];
    var meetingStep = null;
    for (var i = 0; i < steps.length; i++) {
        if (String(steps[i].type || '') === 'meeting') {
            meetingStep = steps[i];
            break;
        }
    }
    if (!meetingStep) return 'team_research';
    var m = String(meetingStep.mode || 'auto').toLowerCase();
    if (m === 'always') return 'force_meeting';
    if (m === 'never') return 'team_research';
    return 'conditional_meeting';
}

function buildWorkflowFromIntent(intent) {
    var timeoutSel = document.getElementById('ask-workflow-time-limit');
    var timeoutSeconds = timeoutSel ? Number(timeoutSel.value || 480) : 480;
    if (!isFinite(timeoutSeconds) || timeoutSeconds <= 0) timeoutSeconds = 480;
    var limits = {max_parallel_agents: 4, max_meeting_rounds: 2, timeout_seconds: Math.floor(timeoutSeconds)};
    if (intent === 'director_only') {
        return {
            mode: 'director_only',
            name: '只回答我（总监）',
            auto_for_simple: true,
            custom_steps: [],
            limits: limits
        };
    }
    if (intent === 'team_research') {
        return {
            mode: 'custom',
            name: '先团队研究再答复',
            auto_for_simple: false,
            custom_steps: [
                {id: 'route', type: 'assign_by_router', name: '总监路由分配'},
                {id: 'team', type: 'agent_task', name: '团队并行研究', source: 'assigned', parallel: true},
                {id: 'meeting', type: 'meeting', name: '会议步骤', mode: 'never', max_rounds: 1},
                {id: 'final', type: 'director_synthesis', name: '总监最终答复'}
            ],
            limits: limits
        };
    }
    if (intent === 'force_meeting') {
        return {
            mode: 'custom',
            name: '先开会再答复',
            auto_for_simple: false,
            custom_steps: [
                {id: 'route', type: 'assign_by_router', name: '总监路由分配'},
                {id: 'team', type: 'agent_task', name: '团队并行研究', source: 'assigned', parallel: true},
                {id: 'meeting', type: 'meeting', name: '强制会议讨论', mode: 'always', max_rounds: 2},
                {id: 'final', type: 'director_synthesis', name: '总监最终答复'}
            ],
            limits: limits
        };
    }
    return {
        mode: 'auto',
        name: '自动推荐',
        auto_for_simple: true,
        custom_steps: [],
        limits: limits
    };
}

function getAskWorkflowIntentSummary(intent) {
    var m = {
        conditional_meeting: '当前策略：系统自动分配任务，只有关键分歧才开会，兼顾效率与质量。',
        director_only: '当前策略：仅由决策总监回答，适合快速问答与简单问题。',
        team_research: '当前策略：先团队研究再答复，不触发会议流程，适合常规分析。',
        force_meeting: '当前策略：先团队研究并强制会议讨论，再由总监统一决策答复。'
    };
    return m[intent] || m.conditional_meeting;
}

function renderAskWorkflowSelector() {
    if (!askWorkflowConfig) return;
    var intent = inferAskWorkflowIntent(askWorkflowConfig);
    document.querySelectorAll('.ask-flow-card').forEach(function(el) {
        var active = String(el.getAttribute('data-intent') || '') === intent;
        el.classList.toggle('active', active);
    });
    var summaryEl = document.getElementById('ask-workflow-summary');
    if (summaryEl) {
        var timeoutSeconds = Number((askWorkflowConfig.limits || {}).timeout_seconds || 480);
        summaryEl.textContent = getAskWorkflowIntentSummary(intent) + ' 当前时限：' + formatDurationClock(timeoutSeconds);
    }
    var timeoutSel = document.getElementById('ask-workflow-time-limit');
    if (timeoutSel) {
        setSelectNearestNumericValue(
            timeoutSel,
            Number((askWorkflowConfig.limits || {}).timeout_seconds || 480)
        );
    }
    applyUnifiedTaskTimeLimit(toMinutesFromSeconds((askWorkflowConfig.limits || {}).timeout_seconds || 480));
}

function updateAskWorkflowTimeout() {
    if (!askWorkflowConfig) return;
    var timeoutSel = document.getElementById('ask-workflow-time-limit');
    var timeoutSeconds = timeoutSel ? Number(timeoutSel.value || 480) : 480;
    if (!isFinite(timeoutSeconds) || timeoutSeconds <= 0) timeoutSeconds = 480;
    var payload = normalizeWorkflowForEditor(askWorkflowConfig);
    if (!payload.limits || typeof payload.limits !== 'object') payload.limits = {};
    payload.limits.timeout_seconds = Math.floor(timeoutSeconds);
    setAskWorkflowStatus('保存时限中...', 'loading');
    fetch('/api/team/user_ask/workflow', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({workflow: payload})
    }).then(function(res) {
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
    }).then(function(data) {
        askWorkflowConfig = normalizeWorkflowForEditor(data.workflow || payload);
        renderAskWorkflowSelector();
        setAskWorkflowStatus('团队协作时限已更新', 'ok');
    }).catch(function(err) {
        setAskWorkflowStatus('时限保存失败: ' + String((err && err.message) || err || ''), 'error');
    });
}

function selectAskWorkflowIntent(intent) {
    if (!isAskDirectorMode()) {
        setAskWorkflowStatus('请先将“专家”切换为“决策总监”，再设置流程', 'error');
        return;
    }
    if (getSelectedAskMode() !== 'team') {
        setAskWorkflowStatus('请先切换到“团队协作”模式，再设置流程', 'error');
        return;
    }
    var chosen = String(intent || '').trim();
    if (!chosen) chosen = 'conditional_meeting';
    var payload = buildWorkflowFromIntent(chosen);

    setAskWorkflowStatus('应用中...', 'loading');
    fetch('/api/team/user_ask/workflow', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({workflow: payload})
    }).then(function(res) {
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
    }).then(function(data) {
        askWorkflowConfig = normalizeWorkflowForEditor(data.workflow || payload);
        askWorkflowPresets = (data.presets && typeof data.presets === 'object') ? data.presets : askWorkflowPresets;
        renderAskWorkflowSelector();
        setAskWorkflowStatus('已切换：' + (askWorkflowConfig.name || '流程已更新'), 'ok');
        updateAskRoutingModeUI();
    }).catch(function(err) {
        setAskWorkflowStatus('切换失败: ' + String((err && err.message) || err || ''), 'error');
    });
}

function loadAskWorkflowConfig(silent) {
    if (!silent) setAskWorkflowStatus('加载中...', 'loading');
    fetch('/api/team/user_ask/workflow', {cache: 'no-store'})
        .then(function(res) {
            if (!res.ok) throw new Error('HTTP ' + res.status);
            return res.json();
        })
        .then(function(data) {
            askWorkflowConfig = normalizeWorkflowForEditor(data.workflow || {});
            askWorkflowPresets = (data.presets && typeof data.presets === 'object') ? data.presets : {};
            renderAskWorkflowSelector();
            setAskWorkflowStatus('已加载', 'ok');
            updateAskRoutingModeUI();
        })
        .catch(function(err) {
            setAskWorkflowStatus('加载失败: ' + String((err && err.message) || err || ''), 'error');
        });
}

// 兼容旧入口：保留空实现，避免旧事件引用报错
function onAskWorkflowModeChanged() {}
function applyAskWorkflowPreset() {}
function saveAskWorkflowConfig() {
    var intent = inferAskWorkflowIntent(askWorkflowConfig || {});
    selectAskWorkflowIntent(intent);
}

// ──────────────── 直连问答反馈 ────────────────
function setAskFeedbackStatus(text, level) {
    var el = document.getElementById('ask-feedback-status');
    if (!el) return;
    el.textContent = text || '';
    el.classList.remove('ok', 'error', 'loading');
    if (level === 'ok') el.classList.add('ok');
    else if (level === 'error') el.classList.add('error');
    else if (level === 'loading') el.classList.add('loading');
}

function syncAskFeedbackScore(val) {
    var scoreInput = document.getElementById('ask-feedback-score');
    var rangeInput = document.getElementById('ask-feedback-score-range');
    var n = parseInt(val, 10);
    if (!isFinite(n)) n = 8;
    n = Math.max(0, Math.min(10, n));
    if (scoreInput) scoreInput.value = String(n);
    if (rangeInput) rangeInput.value = String(n);
}

function hideAskFeedbackBox() {
    var box = document.getElementById('ask-feedback-box');
    if (box) box.style.display = 'none';
    setAskFeedbackStatus('', '');
}

function showAskFeedbackBox() {
    var box = document.getElementById('ask-feedback-box');
    if (!box) return;
    box.style.display = 'block';
    syncAskFeedbackScore(8);
    var suggestion = document.getElementById('ask-feedback-suggestion');
    if (suggestion) suggestion.value = '';
    setAskFeedbackStatus('可提交评分，反馈将写入团队经验库', '');
}

function submitAskFeedback() {
    if (!lastAskMeta || !lastAskMeta.ask_id) {
        setAskFeedbackStatus('没有可评分的问答记录', 'error');
        return;
    }
    var scoreInput = document.getElementById('ask-feedback-score');
    var score = parseInt(scoreInput ? scoreInput.value : '0', 10);
    if (!isFinite(score) || score < 0 || score > 10) {
        setAskFeedbackStatus('评分需在 0-10 之间', 'error');
        return;
    }
    var suggestionInput = document.getElementById('ask-feedback-suggestion');
    var suggestion = suggestionInput ? String(suggestionInput.value || '').trim() : '';

    setAskFeedbackStatus('提交中...', 'loading');
    fetch('/api/team/ask_feedback/' + encodeURIComponent(lastAskMeta.ask_id), {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({score: score, suggestion: suggestion})
    }).then(function(res) {
        return res.json().then(function(data) {
            if (!res.ok) {
                var msg = (data && (data.error || data.message)) || ('HTTP ' + res.status);
                throw new Error(msg);
            }
            return data || {};
        });
    }).then(function(data) {
        var n = Array.isArray(data.updated_agents) ? data.updated_agents.length : 0;
        setAskFeedbackStatus('已提交，已更新智能体记忆: ' + n + ' 个', 'ok');
    }).catch(function(err) {
        setAskFeedbackStatus('提交失败: ' + String((err && err.message) || err || ''), 'error');
    });
}

// ──────────────── 初始化 ────────────────
document.addEventListener('DOMContentLoaded', function() {
    setWorkbenchOpen(true);
    setAskPanelOpen(true);
    setSceneVisibility(readSceneVisibility(), {silentPersist: true});
    bindToolAuditActionDelegation();
    bindPromptProfileActionDelegation();
    loadAskSessionMap();
    loadAskMode();
    loadAskWebToggle();
    loadUnifiedTaskTimeLimit();
    loadStatus();
    loadTeamPromptConfigs(true);
    loadTeamModelConfigs(true);
    loadAskWorkflowConfig(true);
    renderTeamContextList();
    hideAskFeedbackBox();
    updateAskRoutingModeUI();
    updateAskSessionHint();
    refreshTeamObservability();
    var scoreInput = document.getElementById('ask-feedback-score');
    if (scoreInput) {
        scoreInput.addEventListener('input', function() {
            syncAskFeedbackScore(this.value);
        });
    }
    loadPortfolioConfig();
    loadPortfolioStatus();
    loadReports();
    startActivityStream();
    syncWorkflowSessionOptions();
    // 定时刷新状态
    setInterval(loadStatus, STATUS_POLL_INTERVAL_MS);
    setInterval(loadReports, REPORT_POLL_INTERVAL_MS);
    setInterval(function() { refreshTraceRuns(); }, TRACE_POLL_INTERVAL_MS);
});

// ──────────────── 智能体状态 ────────────────
function loadStatus() {
    return fetchJsonStrict('/api/team/status', {cache: 'no-store'})
        .then(function(data) {
            clearRuntimeAlert('status');
            latestOrchestratorState = data.orchestrator || {};
            latestPortfolioSchedulerState = data.portfolio_scheduler || latestPortfolioSchedulerState;
            latestWaitingOvertimeSessions = Array.isArray(data.waiting_overtime) ? data.waiting_overtime : [];
            latestAgentStatuses = Array.isArray(data.agents) ? data.agents : [];
            if (data.models && typeof data.models === 'object') {
                teamModelConfig = data.models;
                renderTeamModelSelectors();
            }
            cachePromptProfiles(latestAgentStatuses);
            renderAgentStatus(data.agents || []);
            updateOrchestratorUI(data.orchestrator || {});
            renderSessionTimingBanner();
            renderSessionProgressBoard();
            renderSessionOvertimePanel();
            renderTeamPromptRuntime();
            var now = Date.now();
            if (now - lastTraceRefreshTs > 15000) {
                refreshTraceRuns((data.orchestrator || {}).current_session || '');
                lastTraceRefreshTs = now;
            }
            // 通知3D场景更新
            if (window.updateSceneAgents) {
                window.updateSceneAgents(data.agents || []);
            }
            return data;
        })
        .catch(function(err) {
            setRuntimeAlert('status', '团队状态同步失败：' + _errText(err), 'error');
            return null;
        });
}

function renderAgentStatus(agents) {
    var container = document.getElementById('agent-status-list');
    if (!container) return;
    var html = '';
    var rows = Array.isArray(agents) ? agents.slice() : [];
    latestAgentStatuses = rows.slice();
    cachePromptProfiles(rows);
    rows.sort(function(a, b) {
        var aScore = String((a && a.agent_id) || '') === 'director' ? 0 : 1;
        var bScore = String((b && b.agent_id) || '') === 'director' ? 0 : 1;
        if (aScore !== bScore) return aScore - bScore;
        return String((a && a.agent_id) || '').localeCompare(String((b && b.agent_id) || ''));
    });
    rows.forEach(function(a) {
        agentColors[a.agent_id] = a.color;
        var icon = AGENT_ICONS[a.agent_id] || '🤖';
        var isDirector = String(a.agent_id || '') === 'director';
        var statusText = {
            idle: '空闲', thinking: '思考中', using_tool: '使用工具', speaking: '输出中', offline: '离线', busy: '繁忙'
        }[a.status] || a.status;
        if (String(a.status || '') === 'idle' && a.current_task && Number(a.last_runtime_at || 0) > 0) {
            statusText = '最近完成';
        }
        var task = a.current_task ? a.current_task : a.description || '';
        var taskEsc = escapeHtml(task);
        var workflowLabel = a.current_workflow_label || (a.status === 'idle' ? '空闲待命' : '临时任务');
        var workflowKey = String(a.current_workflow || 'idle').replace(/[^a-z_]/g, '') || 'idle';
        var stepText = String(a.current_step || '').trim();
        var nextText = String(a.next_step || '').trim();
        var reasonText = String(a.current_step_reason || '').trim();
        var toolText = String(a.current_tool || '').trim();
        var planDetails = Array.isArray(a.current_plan_details) ? a.current_plan_details : [];
        var sessionTiming = (a && a.session_timing && a.session_timing.active) ? a.session_timing : null;
        var promptProfile = (a && a.prompt_profile && typeof a.prompt_profile === 'object') ? a.prompt_profile : {};
        var progressText = '';
        if (a.current_step_index && a.current_step_total) {
            progressText = '步骤 ' + a.current_step_index + '/' + a.current_step_total;
        }
        var planHtml = '';
        if (sessionTiming && (isDirector || String(a.status || '') !== 'idle')) {
            var timingState = String(sessionTiming.state || 'running');
            var timingColor = timingState === 'expired' ? '#b91c1c' : (timingState === 'converging' ? '#b45309' : '#1d4ed8');
            var timingBg = timingState === 'expired' ? 'rgba(254,242,242,0.92)' : (timingState === 'converging' ? 'rgba(255,247,237,0.92)' : 'rgba(239,246,255,0.92)');
            planHtml += '<div style="margin-top:6px;padding:8px 10px;border-radius:12px;border:1px solid rgba(148,163,184,0.18);background:' + timingBg + ';">' +
                '<div style="font-size:12px;font-weight:700;color:' + timingColor + ';">' + escapeHtml(formatDeadlineBadge(sessionTiming)) + '</div>' +
                '<div style="margin-top:4px;font-size:11px;color:#475569;">任务截止: ' + escapeHtml(formatUnixTime(sessionTiming.deadline_ts || 0)) + '</div>' +
                '</div>';
        }
        if (stepText || nextText || reasonText || toolText) {
            planHtml += '<div class="agent-task" style="margin-top:4px;color:#334155;">';
            if (progressText || stepText) {
                planHtml += '<div><strong>' + escapeHtml(progressText || '') +
                    (stepText ? (' · ' + escapeHtml(stepText)) : '') + '</strong></div>';
            }
            if (toolText) {
                planHtml += '<div style="margin-top:2px;">当前工具：' + escapeHtml(toolText) + '</div>';
            }
            if (reasonText) {
                planHtml += '<div style="margin-top:2px;">原因：' + escapeHtml(reasonText) + '</div>';
            }
            if (nextText) {
                planHtml += '<div style="margin-top:2px;color:#64748b;">下一步：' + escapeHtml(nextText) + '</div>';
            }
            planHtml += '</div>';
        }
        if (planDetails.length) {
            var planRows = planDetails.map(function(step) {
                var idx = Number(step.index || 0) || 0;
                var state = String(step.status || 'pending');
                var stateText = state === 'completed' ? '已完成' : (state === 'running' ? '执行中' : '待执行');
                var stateColor = state === 'completed' ? '#15803d' : (state === 'running' ? '#b45309' : '#94a3b8');
                var bg = state === 'completed' ? 'rgba(22,163,74,0.08)' : (state === 'running' ? 'rgba(245,158,11,0.12)' : 'rgba(148,163,184,0.08)');
                var title = escapeHtml(String(step.title || ('步骤' + idx)));
                var goal = escapeHtml(String(step.goal || '').trim());
                var summary = escapeHtml(String(step.summary || '').trim());
                var preferredTools = Array.isArray(step.preferred_tools) ? step.preferred_tools.filter(Boolean).slice(0, 3).join(', ') : '';
                return '<div style="margin-top:6px;padding:7px 8px;border-radius:10px;border:1px solid rgba(148,163,184,0.18);background:' + bg + ';">' +
                    '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start;">' +
                    '<div style="font-size:12px;font-weight:700;color:#1e293b;">' + idx + '. ' + title + '</div>' +
                    '<div style="font-size:11px;color:' + stateColor + ';white-space:nowrap;">' + escapeHtml(stateText) + '</div>' +
                    '</div>' +
                    (goal ? ('<div style="margin-top:4px;font-size:11px;color:#475569;line-height:1.45;">目标：' + goal + '</div>') : '') +
                    (preferredTools ? ('<div style="margin-top:3px;font-size:11px;color:#64748b;">工具：' + escapeHtml(preferredTools) + '</div>') : '') +
                    (summary ? ('<div style="margin-top:4px;font-size:11px;color:#334155;line-height:1.45;">结果：' + summary + '</div>') : '') +
                    '</div>';
            }).join('');
            planHtml += '<div style="margin-top:8px;padding:8px 10px;border-radius:12px;border:1px solid ' +
                (isDirector ? 'rgba(217,119,6,0.28);background:rgba(255,247,237,0.92);' : 'rgba(148,163,184,0.16);background:rgba(248,250,252,0.88);') +
                '">' +
                '<div style="font-size:12px;font-weight:700;color:' + (isDirector ? '#9a3412' : '#1e293b') + ';">' +
                (isDirector ? '决策专家总控流程' : '任务拆解清单') + '</div>' +
                planRows +
                '</div>';
        }
        var promptHtml = '';
        var promptMods = Array.isArray(promptProfile.modifiers) ? promptProfile.modifiers : [];
        if (promptProfile.agent_name || promptMods.length) {
            var agentIdJs = JSON.stringify(String(a.agent_id || ''));
            promptHtml =
                '<div style="margin-top:8px;padding:8px 10px;border-radius:12px;border:1px solid rgba(148,163,184,0.16);background:rgba(255,255,255,0.75);">' +
                    '<div style="font-size:12px;font-weight:700;color:#334155;">本次运行时提示词</div>' +
                    '<div style="margin-top:4px;font-size:11px;color:#64748b;">模式：' +
                        escapeHtml(getAskModeText(String(promptProfile.response_style || 'auto'))) +
                        ' · 工作流：' + escapeHtml(String(promptProfile.workflow || '-')) +
                    '</div>' +
                    renderPromptProfileChips(promptProfile, 4) +
                    '<div style="margin-top:8px;">' +
                        '<button type="button" class="btn btn-outline btn-sm js-open-prompt-profile" data-agent-id="' + escapeHtml(String(a.agent_id || '')) + '" onclick="showAgentPromptProfile(' + agentIdJs + '); return false;">查看详情</button>' +
                    '</div>' +
                '</div>';
        }
        var cardStyle = isDirector
            ? ' style="border:1px solid rgba(217,119,6,0.35);box-shadow:0 14px 30px rgba(194,65,12,0.10);background:linear-gradient(180deg,rgba(255,247,237,0.98),rgba(255,255,255,0.98));"'
            : '';
        var nameExtra = isDirector
            ? '<span style="margin-left:6px;padding:2px 8px;border-radius:999px;background:rgba(245,158,11,0.14);color:#9a3412;font-size:11px;font-weight:700;">总控</span>'
            : '';

        html += '<div class="agent-status-card"' + cardStyle + '>' +
            '<div class="agent-avatar" style="background:' + a.color + '">' + icon + '</div>' +
            '<div class="agent-info">' +
            '<div class="agent-name">' + a.name + nameExtra + '</div>' +
            '<div class="agent-workflow">' +
            '<span class="agent-workflow-badge ' + workflowKey + '">' + escapeHtml(workflowLabel) + '</span>' +
            '<span class="agent-phase">' + escapeHtml(statusText) + '</span>' +
            '</div>' +
            '<div class="agent-task" title="' + taskEsc + '">' +
            taskEsc +
            '</div>' + planHtml + promptHtml + '</div>' +
            '<div class="agent-status-dot ' + a.status + '"></div>' +
            '</div>';
    });
    container.innerHTML = html;
    renderTeamPromptRuntime();
}

function updateOrchestratorUI(state) {
    var btn = document.getElementById('btn-toggle-auto');
    if (!btn) return;
    var autoTitle = document.getElementById('btn-toggle-auto-title');
    var autoIcon = document.getElementById('btn-toggle-auto-icon');
    latestOrchestratorState = state || {};
    autoRunning = !state.paused;
    if (autoRunning) {
        if (autoTitle) autoTitle.textContent = '暂停自动研究';
        if (autoIcon) autoIcon.textContent = '⏸';
        if (!autoTitle && !autoIcon) btn.textContent = '⏸ 暂停自动研究';
        btn.classList.add('btn-active');
    } else {
        if (autoTitle) autoTitle.textContent = '启动自动研究';
        if (autoIcon) autoIcon.textContent = '▶';
        if (!autoTitle && !autoIcon) btn.textContent = '▶ 启动自动研究';
        btn.classList.remove('btn-active');
    }
    // 更新周期选择器
    var sel = document.getElementById('cycle-interval');
    if (sel) {
        if (state.manual_only) {
            sel.value = '0';
        } else if (state.cycle_interval) {
            sel.value = String(state.cycle_interval);
        }
    }

    var idleCb = document.getElementById('idle-enabled');
    if (idleCb) {
        idleCb.checked = !!state.idle_enabled;
    }
    var idleSel = document.getElementById('idle-interval');
    if (idleSel && state.idle_interval) {
        idleSel.value = String(state.idle_interval);
    }
    var officeCb = document.getElementById('office-chat-enabled');
    if (officeCb) {
        officeCb.checked = !!state.office_chat_enabled;
    }
    var officeSel = document.getElementById('office-chat-interval');
    if (officeSel && state.office_chat_interval) {
        officeSel.value = String(state.office_chat_interval);
    }
    renderTokenBudget(state && state.budget ? state.budget : null);
    renderSessionTimingBanner();
    renderSessionProgressBoard();
    renderSessionOvertimePanel();
    renderTeamPromptRuntime();
}

function formatBudgetPercent(v) {
    var n = Number(v || 0);
    if (!isFinite(n)) return '0%';
    return (n * 100).toFixed(1) + '%';
}

function renderTokenBudget(snapshot) {
    var el = document.getElementById('token-budget-panel');
    if (!el) return;
    var s = snapshot || latestBudgetSnapshot;
    if (!s || typeof s !== 'object') {
        el.innerHTML = '<div class="activity-empty">暂无预算数据</div>';
        return;
    }
    latestBudgetSnapshot = s;
    var level = String(s.level || 'normal');
    var dayLimitText = Number(s.day_limit || 0) > 0 ? String(s.day_limit) : '∞';
    var sessionLimitText = Number(s.session_limit || 0) > 0 ? String(s.session_limit) : '∞';
    el.innerHTML =
        '<div class="budget-chip-row">' +
            '<span class="budget-chip ' + escapeHtml(level) + '">状态: ' + escapeHtml(level) + '</span>' +
            '<span class="budget-chip">日占用: ' + escapeHtml(formatBudgetPercent(s.day_ratio)) + '</span>' +
            '<span class="budget-chip">会话占用: ' + escapeHtml(formatBudgetPercent(s.session_ratio)) + '</span>' +
        '</div>' +
        '<div>当日: ' + escapeHtml(String(s.day_used || 0)) + ' / ' + escapeHtml(dayLimitText) +
        ' ｜ 会话: ' + escapeHtml(String(s.session_used || 0)) + ' / ' + escapeHtml(sessionLimitText) + '</div>';
}

function refreshTeamObservability() {
    var sid = '';
    if (latestTraceRuns && latestTraceRuns.length > 0) {
        sid = String((latestTraceRuns[0] && latestTraceRuns[0].session_id) || '');
    }
    var url = '/api/team/token_budget';
    if (sid) url += '?session_id=' + encodeURIComponent(sid);
    fetchJsonStrict(url)
        .then(function(data) {
            renderTokenBudget(data && data.budget ? data.budget : null);
        })
        .catch(function() {
            renderTokenBudget(null);
        });
    refreshTraceRuns(sid);
}

function renderTraceRuns(runs) {
    var panel = document.getElementById('trace-run-panel');
    if (!panel) return;
    var arr = Array.isArray(runs) ? runs : [];
    latestTraceRuns = arr;
    if (!arr.length) {
        panel.innerHTML = '<div class="activity-empty">暂无 Trace 运行记录</div>';
        return;
    }
    panel.innerHTML = arr.slice(0, 8).map(function(item) {
        var runId = String(item.run_id || '');
        var status = String(item.status || '-');
        var workflow = String(item.workflow || '-');
        var started = item.started_at ? new Date(Number(item.started_at) * 1000).toLocaleString('zh-CN') : '-';
        var duration = Number(item.duration_ms || 0);
        return '<div class="trace-run-item">' +
            '<div class="trace-run-head">' +
                '<span class="trace-run-id">' + escapeHtml(runId) + '</span>' +
                '<button class="btn btn-outline btn-sm" onclick="loadTraceDetail(\'' + escapeHtml(runId) + '\')">查看</button>' +
            '</div>' +
            '<div class="trace-run-meta">workflow=' + escapeHtml(workflow) +
                ' ｜ status=' + escapeHtml(status) +
                ' ｜ duration=' + escapeHtml(String(duration)) + 'ms</div>' +
            '<div class="trace-run-meta">' + escapeHtml(started) + '</div>' +
        '</div>';
    }).join('');
}

function refreshTraceRuns(sessionId) {
    var sid = String(sessionId || '').trim();
    var url = '/api/team/trace/runs?limit=20';
    if (sid) url += '&session_id=' + encodeURIComponent(sid);
    fetchJsonStrict(url)
        .then(function(data) {
            var runs = (data && Array.isArray(data.runs)) ? data.runs : [];
            renderTraceRuns(runs);
        })
        .catch(function(err) {
            var panel = document.getElementById('trace-run-panel');
            if (panel) {
                panel.innerHTML = '<div class="activity-empty">Trace 加载失败: ' +
                    escapeHtml(String((err && err.message) || err || '未知错误')) + '</div>';
            }
        });
}

function loadTraceDetail(runId) {
    var rid = String(runId || '').trim();
    var input = document.getElementById('trace-run-id-input');
    if (!rid && input) rid = String(input.value || '').trim();
    if (!rid) return;
    if (input) input.value = rid;
    var detailEl = document.getElementById('trace-detail-panel');
    if (!detailEl) return;
    detailEl.textContent = 'Trace详情加载中...';

    fetchJsonStrict('/api/team/trace/' + encodeURIComponent(rid) + '?limit=2000')
        .then(function(data) {
            var run = data && data.run ? data.run : {};
            var spans = data && Array.isArray(data.spans) ? data.spans : [];
            var head = [
                'run_id: ' + String(run.run_id || rid),
                'workflow: ' + String(run.workflow || '-'),
                'status: ' + String(run.status || '-'),
                'session: ' + String(run.session_id || '-'),
                'spans: ' + spans.length,
                '---'
            ].join('\n');
            var body = spans.slice(0, 200).map(function(s, idx) {
                return (
                    '[' + (idx + 1) + '] ' + String(s.span_type || '-') +
                    ' | ' + String(s.name || '-') +
                    ' | agent=' + String(s.agent_id || '-') +
                    ' | status=' + String(s.status || '-') +
                    ' | duration=' + String(s.duration_ms || 0) + 'ms\n' +
                    '  input: ' + String(s.input_preview || '').slice(0, 220) + '\n' +
                    '  output: ' + String(s.output_preview || '').slice(0, 220)
                );
            }).join('\n\n');
            detailEl.textContent = head + '\n' + (body || '暂无 span 数据');
        })
        .catch(function(err) {
            detailEl.textContent = 'Trace详情加载失败: ' + String((err && err.message) || err || '');
        });
}

// ──────────────── SSE 活动流 ────────────────
function startActivityStream() {
    if (activitySource) activitySource.close();

    activitySource = new EventSource('/api/team/activity');
    activitySource.onmessage = function(event) {
        try {
            var msg = JSON.parse(event.data);
            if (msg.type === 'heartbeat') return;
            appendActivity(msg);
            var meta = (msg && msg.metadata && typeof msg.metadata === 'object') ? msg.metadata : {};
            var phase = String(meta.phase || '').trim();
            var sid = String(meta.session_id || '').trim();
            if ((phase === 'session_overtime_waiting' || phase === 'session_overtime_recovered') && sid) {
                upsertWaitingOvertimeRow({
                    session_id: sid,
                    session_timing: meta.session_timing || {},
                    session_progress: meta.session_progress || {},
                    session_overtime: meta.session_overtime || {
                        active: true,
                        waiting: true,
                        session_id: sid,
                        message: '任务已达到设定时限，请选择继续等待或立即停止任务。',
                    },
                });
                renderSessionTimingBanner();
                renderSessionProgressBoard();
                renderSessionOvertimePanel();
                renderTeamPromptRuntime();
                requestSessionOvertimePrompt(
                    sid,
                    String(meta.mode || meta.workflow || ''),
                    String((meta.session_progress && meta.session_progress.title) || ''),
                    false
                ).catch(function() {});
                setTimeout(function() { loadStatus(); }, 120);
            } else if ((phase === 'session_overtime_extended' || phase === 'session_overtime_summarize' || phase === 'session_overtime_stop') && sid) {
                removeWaitingOvertimeRow(sid);
                renderSessionOvertimePanel();
                setTimeout(function() { loadStatus(); }, 120);
            }
            // 通知3D场景
            if (window.onAgentActivity) {
                window.onAgentActivity(msg);
            }
        } catch(e) {}
    };
    activitySource.onerror = function() {
        // 自动重连
        setTimeout(startActivityStream, 3000);
    };
}

function getActivityScopeText(scope) {
    return {
        auto_research: '自动研究',
        idle_learning: '闲时学习',
        office_chat: '同事闲聊',
        portfolio_investment: '投资执行',
        market_watch: '盘中盯盘',
        meeting_result: '会议结果',
        user_ask: '直连问答',
        system: '其他'
    }[scope] || '其他';
}

function getMessageSessionId(msg) {
    if (!msg || !msg.metadata) return '';
    var sid = msg.metadata.session_id;
    return sid ? String(sid).trim() : '';
}

function getPhaseLabel(phase) {
    var mapping = {
        meeting_plan: '会议规划',
        meeting_start: '会议启动',
        meeting_order: '会议顺序',
        meeting_round: '会议轮次',
        meeting_turn: '当前发言',
        meeting: '成员发言',
        meeting_decision: '轮次决策',
        meeting_summary: '会议共识',
        meeting_result: '会议结果',
        meeting_end: '会议结束',
        user_ask_assign: '问答分配',
        user_ask_plan: '问答任务分解',
        user_ask_phase1_done: '问答阶段一完成',
        user_ask_phase2_done: '问答阶段二完成',
        user_ask_synthesizing: '问答汇总',
        user_ask_final: '问答最终答复',
        user_ask_custom_step: '自定义流程步骤',
        user_ask_workflow_updated: '问答流程更新',
        task_plan: '任务拆解',
        task_step: '执行步骤',
        session_stopped: '任务停止',
        idle_synthesis: '闲时学习报告',
        direct_answer: '直接答复',
        chat: '对话回复'
    };
    return mapping[String(phase || '').trim()] || String(phase || '');
}

function getScopeBySessionId(sessionId) {
    if (!sessionId) return '';
    if (sessionId.indexOf('idle_') === 0) return 'idle_learning';
    if (sessionId.indexOf('chat_') === 0) return 'office_chat';
    if (sessionId.indexOf('inv_') === 0) return 'portfolio_investment';
    if (sessionId.indexOf('watch_') === 0) return 'market_watch';
    if (sessionId.indexOf('user_ask_') === 0 || sessionId.indexOf('user_ask') === 0) return 'user_ask';
    return 'auto_research';
}

function classifyActivityScope(msg) {
    if (!msg) return 'system';
    var meta = msg.metadata || {};
    var phase = String(meta.phase || '').trim();
    if (phase === 'meeting_result' || phase === 'meeting_summary' || phase === 'meeting_end' ||
        (phase === 'meeting_decision' && meta.meeting_active === false)) {
        return 'meeting_result';
    }
    var mode = meta.mode || meta.workflow || '';
    if (mode === 'idle_learning') return 'idle_learning';
    if (mode === 'office_chat') return 'office_chat';
    if (mode === 'auto_research') return 'auto_research';
    if (mode === 'portfolio_investment') return 'portfolio_investment';
    if (mode === 'market_watch') return 'market_watch';
    if (mode === 'user_ask') return 'user_ask';

    var bySession = getScopeBySessionId(meta.session_id || '');
    if (bySession) return bySession;

    var from = msg.from || '';
    var text = (msg.content || '') + ' ' + (meta.title || '');
    if (from === 'portfolio') {
        if (text.indexOf('盯盘') >= 0 || text.indexOf('盘中') >= 0) return 'market_watch';
        return 'portfolio_investment';
    }
    if (from === 'orchestrator') {
        if (text.indexOf('同事闲聊') >= 0 || text.indexOf('同事交流') >= 0) return 'office_chat';
        if (text.indexOf('闲时学习') >= 0) return 'idle_learning';
        if (text.indexOf('研究周期') >= 0 || text.indexOf('自动研究') >= 0 || text.indexOf('分析') >= 0) return 'auto_research';
        return 'auto_research';
    }
    return 'system';
}

function activityMatchesFilter(msg) {
    if (activityFilter === 'all') return true;
    return classifyActivityScope(msg) === activityFilter;
}

function normalizeParticipantList(value) {
    if (Array.isArray(value)) {
        return value.map(function(v) { return String(v || '').trim(); }).filter(Boolean);
    }
    if (typeof value === 'string') {
        return value.split(/[,\s]+/).map(function(v) { return String(v || '').trim(); }).filter(Boolean);
    }
    return [];
}

function extractMeetingReasonFromContent(text) {
    var raw = String(text || '');
    var m = raw.match(/（([^（）]+)）/);
    if (m && m[1]) return m[1].trim();
    return '';
}

function parseMeetingMeta(msg) {
    if (!msg) return null;
    var meta = msg.metadata || {};
    var phase = String(meta.phase || '').trim();
    var hasMeetingHint = !!(
        phase || meta.meeting_topic || meta.meeting_round || meta.meeting_source ||
        meta.meeting_order || meta.meeting_speaker
    );
    if (!hasMeetingHint) return null;
    if (phase && phase.indexOf('meeting_') !== 0 && phase !== 'meeting') return null;

    var source = String(meta.meeting_source || '').trim();
    var sourceLabel = {
        director: '总监发起',
        agents: '成员协作触发',
        'director+agents': '总监+成员触发',
        none: '未触发会议'
    }[source] || source;

    return {
        phase: phase || 'meeting_event',
        source: source,
        sourceLabel: sourceLabel,
        topic: String(meta.meeting_topic || '').trim(),
        reason: String(meta.meeting_reason || meta.reason || '').trim(),
        active: !!meta.meeting_active,
        round: Number(meta.meeting_round || 0),
        roundTotal: Number(meta.meeting_round_total || 0),
        nextFocus: String(meta.next_focus || '').trim(),
        participants: normalizeParticipantList(meta.participants),
        order: normalizeParticipantList(meta.meeting_order),
        speaker: String(meta.meeting_speaker || '').trim(),
        speakerSeq: Number(meta.meeting_speaker_seq || 0),
        speakerTotal: Number(meta.meeting_speaker_total || 0)
    };
}

function isMeetingPlanTriggered(msg, meetingMeta) {
    if (!meetingMeta || meetingMeta.phase !== 'meeting_plan') return false;
    var text = String((msg && msg.content) || '');
    if (meetingMeta.source === 'none') return false;
    if (text.indexOf('不召开会议') >= 0 || text.indexOf('本轮不召开') >= 0 || text.indexOf('无需额外会议') >= 0) {
        return false;
    }
    if (text.indexOf('会议触发') >= 0 || text.indexOf('召开会议') >= 0) return true;
    return !!meetingMeta.source;
}

function buildMeetingPillsHtml(msg) {
    var meeting = parseMeetingMeta(msg);
    if (!meeting) return '';

    var pills = [];
    if (meeting.phase === 'meeting_plan') {
        var triggered = isMeetingPlanTriggered(msg, meeting);
        pills.push({
            klass: triggered ? 'meeting-on' : 'meeting-off',
            text: triggered ? '触发会议' : '不召开'
        });
        if (meeting.sourceLabel) {
            pills.push({klass: 'meeting-source', text: meeting.sourceLabel});
        }
    } else if (meeting.phase === 'meeting_decision') {
        pills.push({
            klass: meeting.active ? 'meeting-next' : 'meeting-stop',
            text: meeting.active ? '继续讨论' : '结束会议'
        });
    } else if (meeting.phase === 'meeting_result' || meeting.phase === 'meeting_summary') {
        pills.push({klass: 'meeting-summary', text: '会议结论'});
    } else if (meeting.phase === 'meeting_end') {
        pills.push({klass: 'meeting-stop', text: '会议结束'});
    } else if (meeting.phase === 'meeting_order') {
        pills.push({klass: 'meeting-source', text: '发言顺序'});
    } else if (meeting.phase === 'meeting_turn') {
        pills.push({klass: 'meeting-next', text: '当前发言'});
    } else {
        pills.push({klass: 'meeting-source', text: '会议过程'});
    }

    if (meeting.round > 0) {
        var roundText = '第' + meeting.round + '轮';
        if (meeting.roundTotal > 0) roundText += '/' + meeting.roundTotal;
        pills.push({klass: 'meeting-round', text: roundText});
    }
    if (meeting.speaker) {
        var speakerText = meeting.speaker;
        if (meeting.speakerSeq > 0 && meeting.speakerTotal > 0) {
            speakerText += ' ' + meeting.speakerSeq + '/' + meeting.speakerTotal;
        }
        pills.push({klass: 'meeting-people', text: speakerText});
    }
    if (meeting.participants.length > 0) {
        pills.push({klass: 'meeting-people', text: '参会' + meeting.participants.length + '人'});
    }

    return '<div class="activity-meeting-pills">' + pills.map(function(p) {
        return '<span class="activity-meeting-pill ' + p.klass + '">' + escapeHtml(p.text) + '</span>';
    }).join('') + '</div>';
}

function renderMeetingDetailPanel(msg) {
    var meeting = parseMeetingMeta(msg);
    if (!meeting) return '';

    var rows = [];
    var decisionClass = '';
    if (meeting.phase === 'meeting_plan') {
        var triggered = isMeetingPlanTriggered(msg, meeting);
        rows.push({key: '触发结果', value: triggered ? '触发会议讨论' : '本轮不召开会议'});
        decisionClass = triggered ? 'meeting-ok' : 'meeting-stop';
        if (meeting.sourceLabel) rows.push({key: '触发来源', value: meeting.sourceLabel});
    } else if (meeting.phase === 'meeting_order') {
        rows.push({key: '会议阶段', value: '已发布发言顺序'});
    } else if (meeting.phase === 'meeting_turn') {
        rows.push({key: '会议阶段', value: '当前发言中'});
        decisionClass = 'meeting-ok';
    } else if (meeting.phase === 'meeting_decision') {
        rows.push({key: '轮次决策', value: meeting.active ? '继续下一轮讨论' : '会议结束'});
        decisionClass = meeting.active ? 'meeting-ok' : 'meeting-stop';
        if (meeting.round > 0) {
            var roundDecisionText = '第' + meeting.round + '轮';
            if (meeting.roundTotal > 0) roundDecisionText += '/' + meeting.roundTotal;
            rows.push({key: '会议轮次', value: roundDecisionText});
        }
    } else if (meeting.phase === 'meeting_result' || meeting.phase === 'meeting_summary') {
        rows.push({key: '会议结果', value: '已形成会议共识'});
        decisionClass = 'meeting-ok';
    } else if (meeting.phase === 'meeting_end') {
        rows.push({key: '会议结果', value: '会议已结束'});
        decisionClass = 'meeting-stop';
    } else {
        rows.push({key: '会议阶段', value: meeting.phase});
    }

    if (meeting.topic) rows.push({key: '会议主题', value: meeting.topic});
    if (meeting.round > 0 && meeting.phase !== 'meeting_decision') {
        var roundText = '第' + meeting.round + '轮';
        if (meeting.roundTotal > 0) roundText += '/' + meeting.roundTotal;
        rows.push({key: '会议轮次', value: roundText});
    } else if (meeting.roundTotal > 0 && meeting.round <= 0) {
        rows.push({key: '总轮次', value: String(meeting.roundTotal)});
    }
    if (meeting.order.length > 0) {
        rows.push({key: '发言顺序', value: meeting.order.join(' -> ')});
    }
    if (meeting.speaker) {
        var speakerText = meeting.speaker;
        if (meeting.speakerSeq > 0 && meeting.speakerTotal > 0) {
            speakerText += '（' + meeting.speakerSeq + '/' + meeting.speakerTotal + '）';
        }
        rows.push({key: '当前发言', value: speakerText});
    }
    if (meeting.reason) {
        rows.push({key: '触发理由', value: meeting.reason});
    } else {
        var fallbackReason = extractMeetingReasonFromContent(msg.content);
        if (fallbackReason) rows.push({key: '决策理由', value: fallbackReason});
    }
    if (meeting.nextFocus) rows.push({key: '下一轮聚焦', value: meeting.nextFocus});

    var rowsHtml = rows.map(function(item, idx) {
        var valueClass = 'meeting-detail-val';
        if (idx === 0 && decisionClass) valueClass += ' ' + decisionClass;
        return '<div class="meeting-detail-row">' +
            '<span class="meeting-detail-key">' + escapeHtml(item.key) + '</span>' +
            '<span class="' + valueClass + '">' + escapeHtml(item.value) + '</span>' +
            '</div>';
    }).join('');

    var membersHtml = '';
    if (meeting.participants.length > 0) {
        membersHtml = '<div class="meeting-detail-members">' + meeting.participants.map(function(name) {
            return '<span class="meeting-member-chip">' + escapeHtml(name) + '</span>';
        }).join('') + '</div>';
    }

    return '<div class="meeting-detail-panel">' +
        '<div class="meeting-detail-title">会议决策视图</div>' +
        rowsHtml +
        membersHtml +
        '</div>';
}

function buildActivityItem(msg) {
    var fromName = msg.from || 'system';
    var icon = AGENT_ICONS[fromName] || '⚙️';
    var color = agentColors[fromName] || '#6b7280';
    var timeStr = new Date(msg.timestamp * 1000).toLocaleTimeString('zh-CN', {hour:'2-digit', minute:'2-digit', second:'2-digit'});
    var text = msg.content || '';
    if (text.length > 150) text = text.substring(0, 150) + '...';
    var scope = classifyActivityScope(msg);

    var div = document.createElement('div');
    div.className = 'activity-item';
    div.setAttribute('data-type', msg.type || '');
    div.setAttribute('data-scope', scope);
    div.innerHTML = '<span class="activity-time">' + timeStr + '</span>' +
        '<span class="activity-from" style="color:' + color + '">' + icon + ' ' + fromName + '</span>' +
        '<span class="activity-scope">' + getActivityScopeText(scope) + '</span>' +
        buildMeetingPillsHtml(msg) +
        '<div class="activity-text">' + escapeHtml(text) + '</div>';
    div._msgData = msg;
    div.onclick = function() { showActivityDetail(this._msgData); };
    return div;
}

function buildChatActivityMirrorKey(msg) {
    var meta = (msg && msg.metadata && typeof msg.metadata === 'object') ? msg.metadata : {};
    return [
        String(getMessageSessionId(msg) || ''),
        String(msg && msg.timestamp || ''),
        String(msg && msg.from || ''),
        String(msg && msg.type || ''),
        String(meta.phase || ''),
        String(msg && msg.content || '')
    ].join('|');
}

function getActivityFullText(msg) {
    var meta = (msg && msg.metadata && typeof msg.metadata === 'object') ? msg.metadata : {};
    return String((meta.full_content || msg.content || '')).trim();
}

function isUserAskFinalActivity(msg) {
    var meta = (msg && msg.metadata && typeof msg.metadata === 'object') ? msg.metadata : {};
    return String(meta.phase || '').trim() === 'user_ask_final';
}

function pruneDuplicateAskReplyEvents(incomingMsg) {
    if (!isUserAskFinalActivity(incomingMsg)) return;
    var sid = getMessageSessionId(incomingMsg);
    var actor = String((incomingMsg && incomingMsg.from) || '');
    var finalText = getActivityFullText(incomingMsg);
    if (!sid || !actor || !finalText) return;
    activityBuffer = activityBuffer.filter(function(oldMsg) {
        if (!oldMsg || String(oldMsg.type || '') !== 'speaking') return true;
        if (getMessageSessionId(oldMsg) !== sid) return true;
        if (String(oldMsg.from || '') !== actor) return true;
        return getActivityFullText(oldMsg) !== finalText;
    });
}

function shouldMirrorActivityToChat(msg) {
    var sid = getMessageSessionId(msg);
    if (!sid || !getChatBindingBySession(sid)) return false;
    var meta = (msg && msg.metadata && typeof msg.metadata === 'object') ? msg.metadata : {};
    var phase = String(meta.phase || '').trim();
    if (phase === 'user_ask_final') return false;
    if (msg.type === 'speaking' && (sid.indexOf('user_ask_') === 0 || phase.indexOf('user_ask_') === 0)) return false;
    if (phase.indexOf('meeting_') === 0) return true;
    if (phase.indexOf('task_') === 0) return true;
    if (phase.indexOf('user_ask_') === 0) return true;
    if (phase.indexOf('session_') === 0) return true;
    if (msg.type === 'tool_call' || msg.type === 'speaking') return true;
    return msg.type === 'status';
}

function buildActivityMirrorHtml(msg) {
    var meta = (msg && msg.metadata && typeof msg.metadata === 'object') ? msg.metadata : {};
    var timeText = formatTimeOnly(msg && msg.timestamp);
    var actor = String((msg && msg.from) || 'system');
    var icon = AGENT_ICONS[actor] || '⚙️';
    var phase = getPhaseLabel(String(meta.phase || msg.type || '进度'));
    var content = _truncateChatSystemText(String((meta.full_content || msg.content || '')).trim(), 150);
    return '<div class="ask-activity-inline">' +
        '<div class="ask-activity-inline-head">' +
            '<span>' + escapeHtml(icon + ' ' + actor) + '</span>' +
            '<span class="ask-activity-inline-phase">' + escapeHtml(phase || '进度') + '</span>' +
            '<span>' + escapeHtml(timeText || '') + '</span>' +
        '</div>' +
        buildMeetingPillsHtml(msg) +
        '<div class="ask-activity-inline-text">' + escapeHtml(content || '任务进度更新') + '</div>' +
        '</div>';
}

function mirrorActivityToChat(msg) {
    if (!shouldMirrorActivityToChat(msg)) return;
    var sid = getMessageSessionId(msg);
    var binding = getChatBindingBySession(sid);
    if (!binding || !binding.chat_id) return;
    var key = buildChatActivityMirrorKey(msg);
    if (chatActivityMirrorSeen[key]) return;
    chatActivityMirrorSeen[key] = 1;
    appendSystemMessageToChat(
        binding.chat_id,
        '任务过程更新',
        '',
        {
            session_id: sid,
            html: buildActivityMirrorHtml(msg),
            meta_text: binding.label ? ('会话 ' + sid + ' · ' + binding.label) : ('会话 ' + sid)
        }
    );
}

function buildDateDivider(label, className) {
    var div = document.createElement('div');
    div.className = className || 'activity-date-divider';
    div.textContent = label || '-';
    return div;
}

function isActivityDateExpanded(ymd) {
    if (!ymd || ymd === '-') return false;
    return !!activityDateExpanded[ymd];
}

function buildActivityDateToggle(ymd, count, expanded) {
    var btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'activity-date-toggle' + (expanded ? ' expanded' : '');
    btn.setAttribute('data-date', ymd);
    btn.innerHTML =
        '<span class="activity-date-arrow">' + (expanded ? '▼' : '▶') + '</span>' +
        '<span class="activity-date-label">📅 ' + escapeHtml(ymd) + '</span>' +
        '<span class="activity-date-count">' + count + '条</span>';
    btn.onclick = function() {
        toggleActivityDate(this.getAttribute('data-date'));
    };
    return btn;
}

function toggleActivityDate(ymd) {
    if (!ymd || ymd === '-') return;
    activityDateExpanded[ymd] = !isActivityDateExpanded(ymd);
    renderActivityList();
}

function toYmdFromUnix(ts) {
    var n = Number(ts || 0);
    if (!isFinite(n) || n <= 0) return '-';
    var d = n > 1e12 ? new Date(n) : new Date(n * 1000);
    if (isNaN(d.getTime())) return '-';
    return d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate());
}

function renderActivityList() {
    var container = document.getElementById('activity-log');
    if (!container) return;

    var filtered = activityBuffer.filter(activityMatchesFilter);
    filtered.sort(function(a, b) {
        return Number(b.timestamp || 0) - Number(a.timestamp || 0);
    });
    var visible = filtered.slice(0, ACTIVITY_MAX_VISIBLE);
    container.innerHTML = '';

    if (visible.length === 0) {
        var msg = activityFilter === 'all' ? '等待智能体开始工作...' : '当前分类暂无日志';
        container.innerHTML = '<div class="activity-empty">' + msg + '</div>';
        return;
    }

    var grouped = {};
    var dateOrder = [];
    visible.forEach(function(m) {
        var ymd = toYmdFromUnix(m.timestamp);
        if (!grouped[ymd]) {
            grouped[ymd] = [];
            dateOrder.push(ymd);
            if (!Object.prototype.hasOwnProperty.call(activityDateExpanded, ymd)) {
                activityDateExpanded[ymd] = false;  // 默认折叠，只展示日期行
            }
        }
        grouped[ymd].push(m);
    });

    var frag = document.createDocumentFragment();
    dateOrder.forEach(function(ymd) {
        var groupWrap = document.createElement('div');
        groupWrap.className = 'activity-date-group';

        var expanded = isActivityDateExpanded(ymd);
        var items = grouped[ymd] || [];
        var toggleBtn = buildActivityDateToggle(ymd, items.length, expanded);
        groupWrap.appendChild(toggleBtn);

        var body = document.createElement('div');
        body.className = 'activity-date-body' + (expanded ? '' : ' collapsed');
        if (!expanded) body.style.display = 'none';
        items.forEach(function(m) {
            body.appendChild(buildActivityItem(m));
        });
        groupWrap.appendChild(body);
        frag.appendChild(groupWrap);
    });
    container.appendChild(frag);
    container.scrollTop = 0;
}

function setActivityFilter(filterKey) {
    activityFilter = filterKey || 'all';
    document.querySelectorAll('.activity-filter-btn').forEach(function(btn) {
        btn.classList.toggle('active', btn.getAttribute('data-filter') === activityFilter);
    });
    activityRenderDirty = false;
    renderActivityList();
}

function isPanelTabActive(tabName) {
    var el = document.getElementById('tab-' + tabName);
    return !!(el && el.classList.contains('active'));
}

function scheduleActivityRender() {
    activityRenderDirty = true;
    if (activityRenderTimer) return;
    activityRenderTimer = setTimeout(function() {
        activityRenderTimer = null;
        if (!activityRenderDirty) return;
        var activityActive = isPanelTabActive('activity');
        var workflowActive = isPanelTabActive('workflow');
        if (!activityActive && !workflowActive) return;
        activityRenderDirty = false;
        if (activityActive) {
            renderActivityList();
        }
        if (workflowActive) {
            syncWorkflowSessionOptions();
            renderWorkflowLane();
        }
    }, ACTIVITY_RENDER_DEBOUNCE_MS);
}

function appendActivity(msg) {
    pruneDuplicateAskReplyEvents(msg);
    activityBuffer.push(msg);
    if (activityBuffer.length > ACTIVITY_MAX_BUFFER) {
        activityBuffer = activityBuffer.slice(-ACTIVITY_MAX_BUFFER);
    }
    mirrorActivityToChat(msg);
    scheduleActivityRender();
}

// ──────────────── 工作流透视（泳道图） ────────────────
function collectWorkflowSessions() {
    var map = {};
    activityBuffer.forEach(function(msg) {
        var sid = getMessageSessionId(msg);
        if (!sid) return;
        var meta = msg.metadata || {};
        var item = map[sid];
        if (!item) {
            item = {
                session_id: sid,
                first_ts: Number(msg.timestamp || 0),
                last_ts: Number(msg.timestamp || 0),
                count: 0,
                mode: String(meta.mode || meta.workflow || '').trim(),
                scope: classifyActivityScope(msg),
                topic: String(meta.meeting_topic || meta.topic || meta.theme || '').trim(),
                has_meeting: false
            };
            map[sid] = item;
        }
        item.count += 1;
        var ts = Number(msg.timestamp || 0);
        if (ts > item.last_ts) item.last_ts = ts;
        if (ts < item.first_ts) item.first_ts = ts;
        if (!item.mode) item.mode = String(meta.mode || meta.workflow || '').trim();
        if (!item.topic) item.topic = String(meta.meeting_topic || meta.topic || meta.theme || '').trim();
        if (String(meta.phase || '').indexOf('meeting_') === 0) item.has_meeting = true;
    });
    var rows = Object.keys(map).map(function(k) { return map[k]; });
    rows.sort(function(a, b) {
        return (b.last_ts || 0) - (a.last_ts || 0);
    });
    workflowSessionSnapshot = rows;
    return rows;
}

function formatTimeOnly(ts) {
    var n = Number(ts || 0);
    if (!isFinite(n) || n <= 0) return '--:--:--';
    var d = n > 1e12 ? new Date(n) : new Date(n * 1000);
    if (isNaN(d.getTime())) return '--:--:--';
    return pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds());
}

function formatSessionMode(scope, mode) {
    if (mode) {
        if (mode === 'auto_research') return '自动研究';
        if (mode === 'idle_learning') return '闲时学习';
        if (mode === 'office_chat') return '同事闲聊';
        if (mode === 'portfolio_investment') return '投资执行';
        if (mode === 'market_watch') return '盘中盯盘';
        if (mode === 'user_ask') return '直连问答';
    }
    return getActivityScopeText(scope || 'system');
}

function buildSessionOptionLabel(session) {
    var modeText = formatSessionMode(session.scope, session.mode);
    var timeText = formatTimeOnly(session.last_ts);
    return '[' + timeText + '] ' + session.session_id + ' · ' + modeText + ' · ' + session.count + '条';
}

function syncWorkflowSessionOptions() {
    var sel = document.getElementById('workflow-session-select');
    if (!sel) return;
    var sessions = collectWorkflowSessions();
    var prev = workflowSessionId || sel.value || '';
    var html = '';
    sessions.forEach(function(s) {
        html += '<option value="' + escapeHtml(s.session_id) + '">' +
            escapeHtml(buildSessionOptionLabel(s)) +
            '</option>';
    });
    sel.innerHTML = html;
    if (!sessions.length) {
        workflowSessionId = '';
        return;
    }
    var found = sessions.some(function(s) { return s.session_id === prev; });
    workflowSessionId = found ? prev : sessions[0].session_id;
    sel.value = workflowSessionId;
}

function onWorkflowSessionChanged() {
    var sel = document.getElementById('workflow-session-select');
    workflowSessionId = sel ? String(sel.value || '') : '';
    renderWorkflowLane();
}

function refreshWorkflowLane() {
    syncWorkflowSessionOptions();
    renderWorkflowLane();
}

function getWorkflowEventKind(msg) {
    var meta = msg.metadata || {};
    var phase = String(meta.phase || '');
    if (phase.indexOf('meeting_') === 0) return 'meeting';
    if (msg.type === 'tool_call') return 'tool';
    return 'normal';
}

function getWorkflowActorLabel(actor) {
    var id = String(actor || 'system');
    var icon = AGENT_ICONS[id] || '⚙️';
    return icon + ' ' + id;
}

function buildWorkflowEventMeta(msg) {
    var meta = msg.metadata || {};
    var items = [];
    if (meta.current_step) {
        items.push('当前: ' + meta.current_step);
    }
    if (meta.next_step) {
        items.push('下一步: ' + meta.next_step);
    }
    if (msg.type === 'tool_call' && meta.tool) {
        items.push('工具: ' + meta.tool);
    }
    var meeting = parseMeetingMeta(msg);
    if (meeting && meeting.speaker) {
        var speaker = meeting.speaker;
        if (meeting.speakerSeq > 0 && meeting.speakerTotal > 0) {
            speaker += ' (' + meeting.speakerSeq + '/' + meeting.speakerTotal + ')';
        }
        items.push('当前发言: ' + speaker);
    }
    if (meeting && meeting.round > 0) {
        var roundText = '第' + meeting.round + '轮';
        if (meeting.roundTotal > 0) roundText += '/' + meeting.roundTotal;
        items.push(roundText);
    }
    if (meeting && meeting.order && meeting.order.length > 0 && msg.type !== 'speaking') {
        items.push('顺序: ' + meeting.order.join(' -> '));
    }
    if (msg.to && msg.to !== 'all') {
        items.push('目标: ' + msg.to);
    }
    return items.join(' · ');
}

function getWorkflowGroupInfo(msg) {
    var meta = msg.metadata || {};
    var step = String(meta.current_step || '').trim();
    var phase = String(meta.phase || '').trim();
    var msgType = String(msg.type || '').trim();
    var packed = [phase, step, msgType].join(' ').toLowerCase();

    if (
        packed.indexOf('final') >= 0 ||
        packed.indexOf('reply') >= 0 ||
        packed.indexOf('direct_answer') >= 0 ||
        step.indexOf('最终') >= 0 ||
        step.indexOf('最终答复') >= 0 ||
        phase.indexOf('user_ask_final') >= 0
    ) {
        return {label: '最终答复', collapsible: false};
    }
    if (packed.indexOf('reasoning') >= 0 || step.indexOf('形成结论') >= 0 || step.indexOf('回复') >= 0) {
        return {label: '综合判断', collapsible: true};
    }
    if (msgType === 'tool_call' || packed.indexOf('tool') >= 0) return {label: '工具执行', collapsible: true};
    if (packed.indexOf('task_plan') >= 0 || packed.indexOf('assign') >= 0 || packed.indexOf('分解') >= 0) return {label: '任务拆解', collapsible: true};
    if (phase.indexOf('phase1') === 0) return {label: '阶段1研究', collapsible: true};
    if (phase.indexOf('phase2') === 0) return {label: '阶段2评估', collapsible: true};
    if (phase.indexOf('meeting_') === 0 || packed.indexOf('meeting') >= 0) return {label: '会议讨论', collapsible: true};
    if (phase.indexOf('synthesis') >= 0 || phase.indexOf('summary') >= 0) return {label: '结果汇总', collapsible: true};
    return {label: getPhaseLabel(phase || msg.type || 'event') || '事件', collapsible: true};
}

function groupWorkflowEvents(events) {
    var groups = [];
    (events || []).forEach(function(msg) {
        var info = getWorkflowGroupInfo(msg);
        var label = info && info.label ? info.label : '事件';
        var collapsible = !(info && info.collapsible === false);
        var last = groups.length ? groups[groups.length - 1] : null;
        if (!last || last.label !== label || last.collapsible !== collapsible) {
            groups.push({
                label: label,
                collapsible: collapsible,
                events: [msg],
                first_ts: Number(msg.timestamp || 0),
                last_ts: Number(msg.timestamp || 0)
            });
            return;
        }
        last.events.push(msg);
        last.last_ts = Number(msg.timestamp || 0);
    });
    return groups;
}

function renderWorkflowSummary(session, messages) {
    var summary = document.getElementById('workflow-summary');
    if (!summary) return;
    if (!session || !messages || !messages.length) {
        summary.innerHTML = '<div class="activity-empty">暂无可回放会话</div>';
        return;
    }
    var participants = {};
    var meetingCount = 0;
    messages.forEach(function(msg) {
        var from = String(msg.from || 'system');
        participants[from] = true;
        var phase = String((msg.metadata && msg.metadata.phase) || '');
        if (phase.indexOf('meeting_') === 0) meetingCount += 1;
    });
    var modeText = formatSessionMode(session.scope, session.mode);
    var startText = formatUnixTime(session.first_ts);
    var endText = formatUnixTime(session.last_ts);
    var participantCount = Object.keys(participants).length;
    var chips = [
        {klass: 'mode-' + String(session.scope || 'system'), text: modeText},
        {klass: 'mode-system', text: '会话: ' + session.session_id},
        {klass: 'mode-system', text: '事件: ' + session.count},
        {klass: 'mode-system', text: '参与方: ' + participantCount},
        {klass: 'mode-system', text: '会议节点: ' + meetingCount}
    ];
    summary.innerHTML =
        '<div class="workflow-summary-title">工作流会话回放</div>' +
        '<div class="workflow-summary-sub">' +
        '开始: ' + escapeHtml(startText) + ' · 最近更新: ' + escapeHtml(endText) +
        (session.topic ? (' · 主题: ' + escapeHtml(session.topic)) : '') +
        '</div>' +
        '<div class="workflow-summary-chips">' + chips.map(function(chip) {
            return '<span class="workflow-chip ' + chip.klass + '">' + escapeHtml(chip.text) + '</span>';
        }).join('') + '</div>';
}

function renderWorkflowLane() {
    var container = document.getElementById('workflow-lanes');
    if (!container) return;
    var sessions = workflowSessionSnapshot.length ? workflowSessionSnapshot : collectWorkflowSessions();
    if (!sessions.length) {
        container.innerHTML = '<div class="activity-empty">等待活动日志...</div>';
        renderWorkflowSummary(null, null);
        return;
    }

    if (!workflowSessionId) workflowSessionId = sessions[0].session_id;
    var selected = sessions.filter(function(s) { return s.session_id === workflowSessionId; })[0] || sessions[0];
    workflowSessionId = selected.session_id;

    var sel = document.getElementById('workflow-session-select');
    if (sel && sel.value !== workflowSessionId) sel.value = workflowSessionId;

    var sessionMessages = activityBuffer.filter(function(msg) {
        return getMessageSessionId(msg) === workflowSessionId;
    }).sort(function(a, b) {
        return Number(a.timestamp || 0) - Number(b.timestamp || 0);
    });

    renderWorkflowSummary(selected, sessionMessages);
    if (!sessionMessages.length) {
        container.innerHTML = '<div class="activity-empty">当前会话暂无可展示日志</div>';
        return;
    }

    var laneMap = {};
    var laneOrder = [];
    sessionMessages.forEach(function(msg) {
        var actor = String(msg.from || 'system');
        if (!laneMap[actor]) {
            laneMap[actor] = [];
            laneOrder.push(actor);
        }
        laneMap[actor].push(msg);
    });

    var preferred = ['director', 'analyst', 'risk', 'intel', 'quant', 'restructuring', 'auditor', 'portfolio', 'orchestrator', 'system'];
    laneOrder.sort(function(a, b) {
        var ia = preferred.indexOf(a);
        var ib = preferred.indexOf(b);
        if (ia < 0) ia = 999;
        if (ib < 0) ib = 999;
        if (ia !== ib) return ia - ib;
        return String(a).localeCompare(String(b));
    });

    var html = '';
    laneOrder.forEach(function(actor) {
        var events = laneMap[actor] || [];
        var groups = groupWorkflowEvents(events);
        var eventHtml = groups.map(function(group, groupIdx) {
            var bodyHtml = (group.events || []).map(function(msg) {
                var meta = msg.metadata || {};
                var phase = String(meta.phase || '');
                var phaseText = getPhaseLabel(phase || msg.type || 'event');
                var content = String((meta.full_content || msg.content || '')).trim();
                if (content.length > 160) content = content.slice(0, 160) + '...';
                var kind = getWorkflowEventKind(msg);
                var eventMeta = buildWorkflowEventMeta(msg);
                return '<div class="workflow-event" data-kind="' + kind + '">' +
                    '<div class="workflow-event-head">' +
                    '<span class="workflow-event-phase">' + escapeHtml(phaseText || '事件') + '</span>' +
                    '<span class="workflow-event-time">' + escapeHtml(formatTimeOnly(msg.timestamp)) + '</span>' +
                    '</div>' +
                    '<div class="workflow-event-text">' + escapeHtml(content || '(空)') + '</div>' +
                    (eventMeta ? '<div class="workflow-event-meta">' + escapeHtml(eventMeta) + '</div>' : '') +
                    '</div>';
            }).join('');
            if (group.collapsible === false) {
                return '<div class="workflow-group workflow-group-final">' +
                    '<div class="workflow-group-summary workflow-group-summary-static">' +
                        '<span class="workflow-group-title">' + escapeHtml(group.label || '最终答复') + '</span>' +
                        '<span class="workflow-group-meta">' + escapeHtml(formatTimeOnly(group.first_ts)) + ' - ' +
                            escapeHtml(formatTimeOnly(group.last_ts)) + ' · ' +
                            escapeHtml(String((group.events || []).length)) + ' 条</span>' +
                    '</div>' +
                    '<div class="workflow-group-body">' + bodyHtml + '</div>' +
                    '</div>';
            }
            return '<details class="workflow-group"' + (groupIdx === 0 ? ' open' : '') + '>' +
                '<summary class="workflow-group-summary">' +
                    '<span class="workflow-group-title">' + escapeHtml(group.label || '任务阶段') + '</span>' +
                    '<span class="workflow-group-meta">' + escapeHtml(formatTimeOnly(group.first_ts)) + ' - ' +
                        escapeHtml(formatTimeOnly(group.last_ts)) + ' · ' +
                        escapeHtml(String((group.events || []).length)) + ' 条</span>' +
                '</summary>' +
                '<div class="workflow-group-body">' + bodyHtml + '</div>' +
                '</details>';
        }).join('');

        html += '<div class="workflow-lane">' +
            '<div class="workflow-lane-head">' +
            '<div class="workflow-lane-title">' + escapeHtml(getWorkflowActorLabel(actor)) + '</div>' +
            '<div class="workflow-lane-count">' + events.length + '条</div>' +
            '</div>' +
            '<div class="workflow-events">' + eventHtml + '</div>' +
            '</div>';
    });
    container.innerHTML = html;
}

function downloadWorkflowSession() {
    if (!workflowSessionId) {
        alert('暂无可下载会话');
        return;
    }
    var messages = activityBuffer.filter(function(msg) {
        return getMessageSessionId(msg) === workflowSessionId;
    }).sort(function(a, b) {
        return Number(a.timestamp || 0) - Number(b.timestamp || 0);
    });
    if (!messages.length) {
        alert('当前会话暂无日志');
        return;
    }
    var session = (workflowSessionSnapshot || []).filter(function(s) {
        return s.session_id === workflowSessionId;
    })[0] || {session_id: workflowSessionId, scope: 'system', mode: '', count: messages.length};

    var lines = [];
    lines.push('# AlphaFin 工作流回放');
    lines.push('');
    lines.push('- 会话ID: ' + workflowSessionId);
    lines.push('- 分类: ' + formatSessionMode(session.scope, session.mode));
    lines.push('- 事件数: ' + messages.length);
    lines.push('- 生成时间: ' + formatUnixTime(Date.now()));
    lines.push('');
    lines.push('## 事件时间线');
    lines.push('');
    messages.forEach(function(msg, idx) {
        var meta = msg.metadata || {};
        var phase = String(meta.phase || '');
        var phaseText = getPhaseLabel(phase || msg.type || 'event');
        var base = String((meta.full_content || msg.content || '')).trim();
        var eventMeta = buildWorkflowEventMeta(msg);
        lines.push((idx + 1) + '. [' + formatUnixTime(msg.timestamp) + '] ' + (msg.from || 'system') + ' - ' + phaseText);
        if (eventMeta) lines.push('   - 元信息: ' + eventMeta);
        lines.push('   - 内容: ' + base);
    });
    var name = sanitizeFilename('workflow_' + workflowSessionId + '_' + Date.now());
    downloadTextFile(name + '.md', lines.join('\n'), 'text/markdown;charset=utf-8');
}

// ──────────────── 记忆系统 / 自进化 ────────────────
function formatShortTs(ts) {
    var n = Number(ts || 0);
    if (!isFinite(n) || n <= 0) return '-';
    return formatUnixTime(n);
}

function renderMemoryMiniList(items, emptyText, kind) {
    var rows = Array.isArray(items) ? items : [];
    if (!rows.length) {
        return '<div class="memory-empty">' + escapeHtml(emptyText || '暂无数据') + '</div>';
    }
    return rows.map(function(item) {
        if (kind === 'reflection') {
            return '<div class="memory-mini-item">' +
                '<div class="memory-mini-title">' + escapeHtml(String(item.workflow || 'reflection')) + '</div>' +
                '<div class="memory-mini-meta">' + escapeHtml(formatShortTs(item.created_at)) +
                (item.session_id ? (' · ' + escapeHtml(String(item.session_id || ''))) : '') + '</div>' +
                '<div class="memory-mini-text">' + escapeHtml(String(item.reflection_preview || item.task_preview || '')) + '</div>' +
                '</div>';
        }
        if (kind === 'conversation') {
            return '<div class="memory-mini-item">' +
                '<div class="memory-mini-title">' + escapeHtml(String(item.role || '-')) + '</div>' +
                '<div class="memory-mini-meta">' + escapeHtml(formatShortTs(item.created_at)) +
                (item.session_id ? (' · ' + escapeHtml(String(item.session_id || ''))) : '') + '</div>' +
                '<div class="memory-mini-text">' + escapeHtml(String(item.preview || '-')) + '</div>' +
                '</div>';
        }
        return '<div class="memory-mini-item">' +
            '<div class="memory-mini-title">' + escapeHtml(String(item.subject || '-')) + '</div>' +
            '<div class="memory-mini-meta">' + escapeHtml(String((item.tier || '').toUpperCase() || '-')) +
            ' · ' + escapeHtml(formatShortTs(item.updated_at)) + '</div>' +
            '<div class="memory-mini-text">' + escapeHtml(String(item.preview || '-')) + '</div>' +
            '</div>';
    }).join('');
}

function renderMemoryTierBlock(title, rows, emptyText) {
    var arr = Array.isArray(rows) ? rows : [];
    return '<div class="memory-subsection">' +
        '<div class="memory-inline-note">' + escapeHtml(String(title || '-')) + '</div>' +
        renderMemoryMiniList(arr, emptyText, 'knowledge') +
        '</div>';
}

function renderMemoryArchitecture(architecture) {
    var el = document.getElementById('memory-architecture');
    if (!el) return;
    var arch = architecture || {};
    var order = Array.isArray(arch.injection_order) ? arch.injection_order : [];
    if (!order.length) {
        el.innerHTML = '<div class="activity-empty">暂无记忆架构说明</div>';
        return;
    }
    var orderHtml = order.map(function(item) {
        return '<div class="memory-order-item">' +
            '<div class="memory-order-index">' + escapeHtml(String(item.order || '-')) + '</div>' +
            '<div class="memory-order-main">' +
                '<div class="memory-order-title">' + escapeHtml(String(item.label || '-')) + '</div>' +
                '<div class="memory-order-meta">注入位置：' + escapeHtml(String(item.position || '-')) + '</div>' +
                '<div class="memory-order-text">' + escapeHtml(String(item.detail || '')) + '</div>' +
            '</div>' +
        '</div>';
    }).join('');
    el.innerHTML =
        '<div class="memory-architecture-card">' +
            '<div class="memory-architecture-title">当前注入顺序</div>' +
            '<div class="memory-architecture-text">' + escapeHtml(String(arch.design_answer || '')) + '</div>' +
            '<div class="memory-inline-note">顺序会根据模式变化（例如快速模式仅注入 1+4+8+9）；下方可查看每个智能体本轮实际命中内容。</div>' +
            '<div class="memory-order-list">' + orderHtml + '</div>' +
            '<details class="memory-source-details">' +
                '<summary>查看团队记忆操作系统提示词</summary>' +
                '<pre class="memory-source-pre">' + escapeHtml(String(arch.memory_operating_system || '暂无')) + '</pre>' +
            '</details>' +
        '</div>';
}

function renderMemoryAgents(agents) {
    var el = document.getElementById('memory-agent-list');
    if (!el) return;
    var rows = Array.isArray(agents) ? agents : [];
    if (!rows.length) {
        el.innerHTML = '<div class="activity-empty">暂无记忆数据</div>';
        return;
    }
    el.innerHTML = rows.map(function(agent) {
        var stats = agent.stats || {};
        var snap = agent.memory_snapshot || {};
        var counts = snap.counts || {};
        var injectionOrder = Array.isArray(snap.injection_order) ? snap.injection_order : [];
        var layers = snap.knowledge_layers || {};
        return '<details class="memory-agent-card">' +
            '<summary class="memory-agent-summary">' +
                '<span class="memory-agent-title">' + escapeHtml(String(agent.name || agent.agent_id || '-')) + '</span>' +
                '<span class="memory-agent-badges">' +
                    '<span class="memory-badge">' + escapeHtml(String(agent.workflow || agent.status || '-')) + '</span>' +
                    '<span class="memory-badge">HOT ' + escapeHtml(String(stats.knowledge_hot || 0)) + '</span>' +
                    '<span class="memory-badge">WARM ' + escapeHtml(String(stats.knowledge_warm || 0)) + '</span>' +
                    '<span class="memory-badge">召回 ' + escapeHtml(String(counts.recalled_conversations || 0)) + '</span>' +
                '</span>' +
            '</summary>' +
            '<div class="memory-agent-body">' +
                '<div class="memory-stat-grid">' +
                    '<div class="memory-stat"><span>长期记忆</span><strong>' + escapeHtml(String(stats.knowledge_total || 0)) + '</strong></div>' +
                    '<div class="memory-stat"><span>对话库</span><strong>' + escapeHtml(String(stats.conversation_count || 0)) + '</strong></div>' +
                    '<div class="memory-stat"><span>反思数</span><strong>' + escapeHtml(String(stats.reflection_count || 0)) + '</strong></div>' +
                    '<div class="memory-stat"><span>模式数</span><strong>' + escapeHtml(String(stats.pattern_count || 0)) + '</strong></div>' +
                '</div>' +
                '<div class="memory-section">' +
                    '<div class="memory-section-title">本轮上下文注入</div>' +
                    '<div class="memory-inline-note">当前问题：' + escapeHtml(String(snap.query_preview || '-')) + '</div>' +
                    '<div class="memory-order-inline">' + injectionOrder.map(function(label, idx) {
                        return '<span class="memory-order-chip">' + escapeHtml(String(idx + 1)) + '. ' + escapeHtml(String(label || '-')) + '</span>';
                    }).join('') + '</div>' +
                '</div>' +
                '<div class="memory-section">' +
                    '<div class="memory-section-title">本轮命中的长期记忆</div>' +
                    '<div class="memory-inline-note">HOT ' + escapeHtml(String(counts.hot || 0)) +
                    ' · WARM ' + escapeHtml(String(counts.warm || 0)) +
                    ' · COLD ' + escapeHtml(String(counts.cold || 0)) + '</div>' +
                    renderMemoryTierBlock('HOT', layers.hot || [], '本轮未命中 HOT 记忆') +
                    renderMemoryTierBlock('WARM', layers.warm || [], '本轮未命中 WARM 记忆') +
                    renderMemoryTierBlock('COLD', layers.cold || [], '本轮未命中 COLD 记忆') +
                '</div>' +
                '<div class="memory-section">' +
                    '<div class="memory-section-title">本轮召回的历史对话</div>' +
                    renderMemoryMiniList(snap.recalled_conversations || [], '本轮未召回历史对话', 'conversation') +
                '</div>' +
                '<div class="memory-section">' +
                    '<div class="memory-section-title">本轮短上下文</div>' +
                    renderMemoryMiniList(snap.local_short_context || [], '本轮没有短上下文注入', 'conversation') +
                '</div>' +
                '<div class="memory-section">' +
                    '<div class="memory-section-title">近期知识写入</div>' +
                    renderMemoryMiniList(agent.recent_knowledge || [], '暂无近期知识沉淀', 'knowledge') +
                '</div>' +
                '<div class="memory-section">' +
                    '<div class="memory-section-title">近期反思</div>' +
                    renderMemoryMiniList(agent.recent_reflections || [], '暂无近期反思', 'reflection') +
                '</div>' +
            '</div>' +
        '</details>';
    }).join('');
}

function renderEvolutionAgents(agents) {
    var el = document.getElementById('evolution-agent-list');
    if (!el) return;
    var rows = Array.isArray(agents) ? agents : [];
    if (!rows.length) {
        el.innerHTML = '<div class="activity-empty">暂无进化数据</div>';
        return;
    }
    el.innerHTML = rows.map(function(agent) {
        var patterns = Array.isArray(agent.patterns) ? agent.patterns : [];
        var patternHtml = patterns.length ? patterns.map(function(item) {
            var badge = item.promoted ? '<span class="memory-badge hot">HOT规则</span>' : '<span class="memory-badge">观察中</span>';
            return '<div class="memory-mini-item">' +
                '<div class="memory-mini-title">' + escapeHtml(String(item.pattern_key || '-')) + '</div>' +
                '<div class="memory-mini-meta">' + badge + ' 成功 ' + escapeHtml(String(item.success_count || 0)) +
                    ' · 失败 ' + escapeHtml(String(item.failure_count || 0)) +
                    ' · 观察 ' + escapeHtml(String(item.observation_count || 0)) + '</div>' +
                '<div class="memory-mini-text">' + escapeHtml(String(item.hot_subject || '尚未晋升 HOT 规则')) + '</div>' +
                '</div>';
        }).join('') : '<div class="memory-empty">暂无模式学习记录</div>';
        return '<details class="memory-agent-card">' +
            '<summary class="memory-agent-summary">' +
                '<span class="memory-agent-title">' + escapeHtml(String(agent.name || agent.agent_id || '-')) + '</span>' +
                '<span class="memory-agent-badges">' +
                    '<span class="memory-badge">反思 ' + escapeHtml(String(agent.reflection_count || 0)) + '</span>' +
                    '<span class="memory-badge">模式 ' + escapeHtml(String(agent.pattern_count || 0)) + '</span>' +
                    '<span class="memory-badge hot">晋升 ' + escapeHtml(String(agent.promoted_count || 0)) + '</span>' +
                '</span>' +
            '</summary>' +
            '<div class="memory-agent-body">' +
                '<div class="memory-section">' +
                    '<div class="memory-section-title">最近模式学习</div>' +
                    patternHtml +
                '</div>' +
                '<div class="memory-section">' +
                    '<div class="memory-section-title">最近反思摘录</div>' +
                    renderMemoryMiniList(agent.recent_reflections || [], '暂无反思记录', 'reflection') +
                '</div>' +
            '</div>' +
        '</details>';
    }).join('');
}

function loadMemoryCenter(force) {
    if (!force && latestMemoryCenter) {
        renderMemoryArchitecture(latestMemoryCenter.architecture || {});
        renderMemoryAgents(latestMemoryCenter.memory_agents || []);
        renderEvolutionAgents(latestMemoryCenter.evolution_agents || []);
        return Promise.resolve(latestMemoryCenter);
    }
    return fetchJsonStrict('/api/team/memory_center?ts=' + Date.now(), {cache: 'no-store'})
        .then(function(data) {
            latestMemoryCenter = data || {};
            renderMemoryArchitecture(latestMemoryCenter.architecture || {});
            renderMemoryAgents(latestMemoryCenter.memory_agents || []);
            renderEvolutionAgents(latestMemoryCenter.evolution_agents || []);
            return latestMemoryCenter;
        })
        .catch(function(err) {
            var text = _errText(err);
            var memoryEl = document.getElementById('memory-agent-list');
            var evoEl = document.getElementById('evolution-agent-list');
            var archEl = document.getElementById('memory-architecture');
            if (archEl) archEl.innerHTML = '<div class="activity-empty">加载失败：' + escapeHtml(text) + '</div>';
            if (memoryEl) memoryEl.innerHTML = '<div class="activity-empty">加载失败：' + escapeHtml(text) + '</div>';
            if (evoEl) evoEl.innerHTML = '<div class="activity-empty">加载失败：' + escapeHtml(text) + '</div>';
            return null;
        });
}

// ──────────────── 研究报告 ────────────────
function loadReports() {
    fetch('/api/team/reports?limit=20')
        .then(function(r) { return r.json(); })
        .then(function(reports) {
            renderReports(reports);
        })
        .catch(function() {});
}

function renderReports(reports) {
    var container = document.getElementById('reports-list');
    if (!container) return;
    if (!reports || reports.length === 0) {
        container.innerHTML = '<div class="activity-empty">暂无研究报告</div>';
        return;
    }
    var html = '';
    reports.forEach(function(r) {
        var titleJs = JSON.stringify((r && r.title) ? String(r.title) : '研究报告');
        html += '<div class="report-card" onclick="viewReport(' + r.id + ')">' +
            '<div class="report-card-head">' +
            '<div class="report-card-title">📄 ' + escapeHtml(r.title) + '</div>' +
            '<button class="report-card-delete" onclick="event.stopPropagation(); deleteReport(' + r.id + ', ' + titleJs + ')">删除</button>' +
            '</div>' +
            '<div class="report-card-meta">' + (r.created_at_str || '') + ' · ' + (r.report_type || '') + '</div>' +
            '</div>';
    });
    container.innerHTML = html;
}

function viewReport(reportId) {
    fetch('/api/team/reports/' + reportId)
        .then(function(r) { return r.json(); })
        .then(function(report) {
            currentReportData = report || null;
            document.getElementById('report-modal-title').textContent = report.title || '研究报告';
            var contentDiv = document.getElementById('report-modal-content');
            if (typeof marked !== 'undefined') {
                contentDiv.innerHTML = marked.parse(report.content || '');
            } else {
                contentDiv.innerHTML = '<pre>' + escapeHtml(report.content || '') + '</pre>';
            }
            document.getElementById('report-modal').style.display = 'flex';
        })
        .catch(function() { alert('加载报告失败'); });
}

function downloadCurrentReport() {
    if (!currentReportData) {
        alert('当前没有可下载的报告');
        return;
    }
    if (currentReportData.id) {
        var a = document.createElement('a');
        a.href = '/api/team/reports/' + currentReportData.id + '/download';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        return;
    }
    var title = sanitizeFilename(currentReportData.title || '研究报告');
    downloadTextFile(title + '.md', currentReportData.content || '', 'text/markdown;charset=utf-8');
}

function deleteCurrentReport() {
    if (!currentReportData || !currentReportData.id) {
        alert('当前没有可删除的报告');
        return;
    }
    deleteReport(currentReportData.id, currentReportData.title || '研究报告', true);
}

function deleteReport(reportId, reportTitle, closeAfterDelete) {
    if (!reportId) {
        alert('报告编号无效');
        return;
    }
    var title = reportTitle || '研究报告';
    if (!window.confirm('确认删除研究报告《' + title + '》吗？删除后无法恢复。')) {
        return;
    }
    fetch('/api/team/reports/' + reportId, {
        method: 'DELETE'
    })
        .then(function(r) {
            return r.json().then(function(data) {
                if (!r.ok) {
                    var msg = (data && data.error) ? data.error : '删除失败';
                    throw new Error(msg);
                }
                return data;
            });
        })
        .then(function() {
            if (currentReportData && Number(currentReportData.id) === Number(reportId)) {
                currentReportData = null;
            }
            if (closeAfterDelete) {
                closeReportModal();
            }
            loadReports();
            alert('研究报告已删除');
        })
        .catch(function(err) {
            alert('删除报告失败: ' + (err && err.message ? err.message : '未知错误'));
        });
}

function closeReportModal() {
    document.getElementById('report-modal').style.display = 'none';
}

// ──────────────── 控制操作 ────────────────
function postTeamConfig(payload) {
    return fetch('/api/team/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload || {}),
        keepalive: true
    }).then(function(r) {
        if (!r.ok) throw new Error('配置保存失败');
        return r.json();
    });
}

function toggleAuto() {
    var nextState = !!autoRunning;
    postTeamConfig({paused: autoRunning})
        .then(function() {
            appendSystemMessageToActiveChat(
                nextState ? '自动研究已暂停' : '自动研究已启动',
                nextState
                    ? '系统已切回手动观察状态，后续不会继续自动发起新一轮研究。'
                    : '系统会按研究周期自动发起新一轮研究，详细过程会在活动流与主聊天中持续更新。'
            );
            loadStatus();
        })
        .catch(function() { alert('更新自动研究开关失败'); });
}

function stopAllTeamWork() {
    var yes = window.confirm('确认停止当前全部工作吗？\n将中止正在进行的分析、会议与直连问答任务。');
    if (!yes) return;

    function finalizeStop(okMsg) {
        stopAskPolling();
        latestWaitingOvertimeSessions = [];
        latestAskSessionTiming = null;
        latestAskSessionProgress = null;
        latestAskSessionOvertime = null;
        if (latestOrchestratorState && typeof latestOrchestratorState === 'object') {
            latestOrchestratorState.session_timing = {active: false, session_id: ''};
            latestOrchestratorState.session_progress = {active: false, session_id: ''};
            latestOrchestratorState.session_overtime = {active: false, session_id: ''};
        }
        if (latestPortfolioSchedulerState && typeof latestPortfolioSchedulerState === 'object') {
            latestPortfolioSchedulerState.session_timing = {active: false, session_id: ''};
            latestPortfolioSchedulerState.session_progress = {active: false, session_id: ''};
            latestPortfolioSchedulerState.session_overtime = {active: false, session_id: ''};
        }
        renderSessionTimingBanner();
        renderSessionProgressBoard();
        renderSessionOvertimePanel();
        runtimeAlerts = {};
        renderRuntimeAlert();
        switchTab('activity');
        loadStatus();
        loadPortfolioStatus();
        appendSystemMessageToActiveChat(
            '团队任务已停止',
            '当前正在进行的分析、会议、问答与投资执行流程均已收到停止信号。'
        );
        alert(okMsg || '已停止当前全部工作');
    }

    function fallbackStopLegacy() {
        var reqs = [
            fetch('/api/team/config', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({paused: true, manual_only: true})
            }),
            fetch('/api/team/portfolio/config', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({auto_run: false, watch_enabled: false})
            }),
            fetch('/api/team/module/stop', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({})
            })
        ];
        return Promise.all(reqs)
            .then(function() { finalizeStop('已停止当前工作（兼容模式）'); })
            .catch(function() { alert('停止失败，请重试'); });
    }

    fetch('/api/team/stop_all_work', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({reason: '用户点击停止当前工作'})
    }).then(function(r) {
        if (r.status === 404) {
            return fallbackStopLegacy();
        }
        return r.text().then(function(t) {
            var data = {};
            try { data = t ? JSON.parse(t) : {}; } catch (e) { data = {}; }
            if (!r.ok || (data && data.error)) {
                throw new Error((data && data.error) || ('HTTP ' + r.status));
            }
            finalizeStop((data && data.message) ? data.message : '已停止当前全部工作');
        });
    }).catch(function() {
        fallbackStopLegacy();
    });
}

function updateCycleInterval() {
    var val = parseInt(document.getElementById('cycle-interval').value);
    if (val === 0) {
        // 仅手动模式：暂停自动
        postTeamConfig({manual_only: true})
            .then(function() { loadStatus(); })
            .catch(function() { alert('切换仅手动模式失败'); });
    } else {
        postTeamConfig({interval: val, manual_only: false})
            .then(function() { loadStatus(); })
            .catch(function() { alert('更新研究周期失败'); });
    }
}

function toggleIdleEnabled() {
    var checked = document.getElementById('idle-enabled').checked;
    postTeamConfig({idle_enabled: checked})
      .then(function() { loadStatus(); })
      .catch(function() { alert('更新闲时学习开关失败'); });
}

function updateIdleInterval() {
    var val = parseInt(document.getElementById('idle-interval').value);
    postTeamConfig({idle_interval: val})
      .then(function() { loadStatus(); })
      .catch(function() { alert('更新闲时学习间隔失败'); });
}

function runIdleLearning() {
    var theme = window.prompt('可选：输入本轮闲时学习主题（留空则自动轮换）', '');
    var payload = {};
    if (theme && theme.trim()) payload.theme = theme.trim();

    fetch('/api/team/idle/run', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
    }).then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.error) {
            alert(d.error);
            return;
        }
        appendSystemMessageToActiveChat(
            '已启动闲时学习',
            '主题：' + (d.theme || theme || '自动轮换') + '。团队会把学习过程与阶段结论同步到当前聊天线程。',
            {
                session_id: d.session || '',
                meta_text: d.session ? ('会话 ' + d.session) : ''
            }
        );
        if (d.session) bindSessionToChat(d.session, askActiveChatId || (ensureAskChatSessionReady() || {}).id, {label: '闲时学习'});
        switchTab('activity');
      })
      .catch(function() { alert('启动闲时学习失败'); });
}

function toggleOfficeChatEnabled() {
    var checked = document.getElementById('office-chat-enabled').checked;
    postTeamConfig({office_chat_enabled: checked})
      .then(function() { loadStatus(); })
      .catch(function() { alert('更新同事闲聊开关失败'); });
}

function updateOfficeChatInterval() {
    var val = parseInt(document.getElementById('office-chat-interval').value);
    postTeamConfig({office_chat_interval: val})
      .then(function() { loadStatus(); })
      .catch(function() { alert('更新同事闲聊间隔失败'); });
}

function runOfficeChat() {
    var topic = window.prompt('可选：输入本轮同事闲聊主题（留空则自动轮换）', '');
    var payload = {};
    if (topic && topic.trim()) payload.topic = topic.trim();

    fetch('/api/team/office_chat/run', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
    }).then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.error) {
            alert(d.error);
            return;
        }
        appendSystemMessageToActiveChat(
            '已启动同事闲聊',
            '主题：' + (d.topic || topic || '自动轮换') + '。本轮交流会作为团队轻量学习与线索发现流程进入主聊天记录。',
            {
                session_id: d.session || '',
                meta_text: d.session ? ('会话 ' + d.session) : ''
            }
        );
        if (d.session) bindSessionToChat(d.session, askActiveChatId || (ensureAskChatSessionReady() || {}).id, {label: '同事闲聊'});
        switchTab('activity');
      })
      .catch(function() { alert('启动同事闲聊失败'); });
}

function setTeamContextStatus(text, level) {
    var el = document.getElementById('team-context-status');
    if (!el) return;
    el.textContent = String(text || '');
    el.classList.remove('ok', 'error', 'loading');
    if (level === 'ok') el.classList.add('ok');
    else if (level === 'error') el.classList.add('error');
    else if (level === 'loading') el.classList.add('loading');
}

function renderTeamContextList() {
    var box = document.getElementById('team-context-list');
    if (!box) return;
    if (!teamContextFiles.length) {
        box.innerHTML = '';
        setTeamContextStatus('未上传上下文', '');
        updateTeamContextSelectedLabel();
        return;
    }
    box.innerHTML = teamContextFiles.map(function(item) {
        var name = escapeHtml(String(item.name || '未命名文件'));
        var kind = escapeHtml(String(item.type || 'document'));
        var pv = escapeHtml(String(item.preview || ''));
        return '<div class="ask-context-chip">' +
            '<div class="title">[' + kind + '] ' + name + '</div>' +
            '<div class="preview">' + pv + '</div>' +
            '</div>';
    }).join('');
    setTeamContextStatus('已加载 ' + teamContextFiles.length + ' 个材料文件', 'ok');
    updateTeamContextSelectedLabel();
}

function updateTeamContextSelectedLabel() {
    var status = document.getElementById('ask-context-selected');
    var inp = document.getElementById('team-context-files');
    if (!status) return;
    var fileNames = [];
    if (inp && inp.files && inp.files.length) {
        for (var i = 0; i < inp.files.length; i++) {
            fileNames.push(String(inp.files[i].name || '未命名文件'));
        }
        if (fileNames.length === 1) {
            status.textContent = '待上传文件：' + fileNames[0];
        } else {
            status.textContent = '待上传文件：' + fileNames.length + ' 个（' + fileNames.slice(0, 3).join('、') + (fileNames.length > 3 ? ' ...' : '') + '）';
        }
        return;
    }
    if (teamContextFiles.length) {
        status.textContent = '已挂载材料：' + teamContextFiles.length + ' 个';
    } else {
        status.textContent = '未选择文件';
    }
}

function onTeamContextFilesChanged() {
    updateTeamContextSelectedLabel();
}

function clearTeamContexts() {
    teamContextIds = [];
    teamContextFiles = [];
    var inp = document.getElementById('team-context-files');
    if (inp) inp.value = '';
    renderTeamContextList();
}

function uploadTeamContextFiles() {
    var inp = document.getElementById('team-context-files');
    if (!inp || !inp.files || !inp.files.length) {
        setTeamContextStatus('请先选择文件', 'error');
        return;
    }
    var fd = new FormData();
    fd.append('module', 'ai_team');
    for (var i = 0; i < inp.files.length; i++) {
        fd.append('files', inp.files[i]);
    }

    setTeamContextStatus('上传与解析中...', 'loading');
    fetch('/api/context/upload', {
        method: 'POST',
        body: fd
    }).then(function(res) {
        return res.text().then(function(text) {
            var data = null;
            try {
                data = text ? JSON.parse(text) : {};
            } catch (e) {
                if (res.status === 404) {
                    throw new Error('上传接口未加载（HTTP 404），请重启 AlphaFin 服务');
                }
                throw new Error('服务返回非 JSON（HTTP ' + res.status + '）');
            }
            if (!res.ok || (data && data.ok === false)) {
                var msg = (data && (data.error || data.message)) || ('HTTP ' + res.status);
                throw new Error(msg);
            }
            return data || {};
        });
    }).then(function(data) {
        if (data.context_id) teamContextIds.push(String(data.context_id));
        var rows = Array.isArray(data.files) ? data.files : [];
        for (var i = 0; i < rows.length; i++) teamContextFiles.push(rows[i]);
        renderTeamContextList();
        if (Array.isArray(data.warnings) && data.warnings.length) {
            setTeamContextStatus('已上传，部分文件有提示：' + data.warnings[0], 'error');
        }
        inp.value = '';
        updateTeamContextSelectedLabel();
    }).catch(function(err) {
        setTeamContextStatus('上传失败: ' + String((err && err.message) || err || ''), 'error');
    });
}

function showAnalyzeDialog() {
    document.getElementById('analyze-modal').style.display = 'flex';
    document.getElementById('analyze-topic').focus();
}

function closeAnalyzeModal() {
    document.getElementById('analyze-modal').style.display = 'none';
}

function startAnalyze() {
    var topic = document.getElementById('analyze-topic').value.trim();
    if (!topic) { alert('请输入分析主题'); return; }
    var timeLimitEl = document.getElementById('analyze-time-limit');
    var timeLimitMinutes = timeLimitEl ? Number(timeLimitEl.value || 8) : 8;
    fetch('/api/team/analyze', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            topic: topic,
            context_ids: teamContextIds,
            time_limit_minutes: timeLimitMinutes
        })
    }).then(function(r) { return r.json(); })
      .then(function(data) {
        closeAnalyzeModal();
        switchTab('activity');
        var ctxMsg = (data && data.context_file_count) ? ('（含材料' + data.context_file_count + '份）') : '';
        appendSystemMessageToActiveChat(
            '已启动手动研究',
            '主题：' + topic + ctxMsg + ' · 时限 ' + (data.time_limit_minutes || timeLimitMinutes) + ' 分钟。接下来团队拆解、执行和汇总过程会在此聊天线程持续更新。',
            {
                session_id: data.session || '',
                meta_text: data.session ? ('会话 ' + data.session) : ''
            }
        );
        if (data.session) bindSessionToChat(data.session, askActiveChatId || (ensureAskChatSessionReady() || {}).id, {label: '手动研究'});
      }).catch(function() { alert('启动分析失败'); });
}

function stopAskPolling() {
    var prevAskId = currentAskId;
    if (currentAskPollTimer) {
        clearTimeout(currentAskPollTimer);
        currentAskPollTimer = null;
    }
    currentAskId = '';
    latestAskSessionProgress = null;
    latestAskSessionOvertime = null;
    latestAskSessionTiming = null;
    if (prevAskId && askPendingMessageByTask[prevAskId]) {
        delete askPendingMessageByTask[prevAskId];
    }
    renderSessionTimingBanner();
    renderSessionProgressBoard();
    renderSessionOvertimePanel();
    renderTeamPromptRuntime();
}

function fetchJsonStrict(url, options) {
    return fetch(url, options).then(function(resp) {
        return resp.text().then(function(text) {
            var data = null;
            var preview = String(text || '').slice(0, 220);
            var htmlLike = /<!doctype|<html|<title>/i.test(preview);
            if (htmlLike) {
                if (resp.status === 404) {
                    throw new Error('接口不存在（当前运行服务可能是旧版本，请重启 AlphaFin 后重试）');
                }
                throw new Error('服务返回非JSON内容 (HTTP ' + resp.status + ')');
            }
            try {
                data = text ? JSON.parse(text) : {};
            } catch (e) {
                throw new Error('服务返回格式异常: ' + String(text || '').slice(0, 180));
            }
            if (!resp.ok) {
                var errMsg = (data && (data.error || data.message)) || ('HTTP ' + resp.status);
                throw new Error(errMsg);
            }
            return data || {};
        });
    });
}

function detectAskTraceSectionTitle(line) {
    var raw = String(line || '').trim();
    if (!raw) return '';
    if (raw.length > 140) return '';

    var normalized = raw
        .replace(/^[>\-\*\+\d\.\)\(\s#`]+/, '')
        .replace(/^[*_~`]+|[*_~`]+$/g, '')
        .trim();
    if (!normalized) return '';

    if (/^因果链(?:路)?(?:\s*[:：].*|\s*[（(].*)?$/.test(normalized)) return '因果链';
    if (/^关键数字推导(?:\s*[:：].*|\s*[（(].*)?$/.test(normalized)) return '关键数字推导';
    if (/^(证据与来源|证据来源|来源与证据)(?:\s*[:：].*|\s*[（(].*)?$/.test(normalized)) return '证据与来源';
    if (/^(不确定性|不确定性与边界)(?:\s*[:：].*|\s*[（(].*)?$/.test(normalized)) return '不确定性';
    return '';
}

function splitAskReplyForDisplay(replyText, replyMeta) {
    var text = String(replyText || '').replace(/\r\n/g, '\n');
    if (!text.trim()) {
        return {main: '', trace: '', labels: []};
    }

    var askMode = String((replyMeta && replyMeta.ask_mode) || '').toLowerCase();
    if (askMode === 'quick') {
        return {main: text, trace: '', labels: []};
    }

    var lines = text.split('\n');
    var firstTraceLine = -1;
    var labels = [];
    var labelSeen = {};

    for (var i = 0; i < lines.length; i++) {
        var label = detectAskTraceSectionTitle(lines[i]);
        if (!label) continue;
        if (firstTraceLine < 0) firstTraceLine = i;
        if (!labelSeen[label]) {
            labels.push(label);
            labelSeen[label] = true;
        }
    }

    if (firstTraceLine < 0) {
        return {main: text, trace: '', labels: []};
    }

    var mainPart = lines.slice(0, firstTraceLine).join('\n').trim();
    var tracePart = lines.slice(firstTraceLine).join('\n').trim();

    var traceLongEnough = tracePart.length >= 900;
    var mainReasonable = mainPart.length >= 120;
    if (!traceLongEnough || !mainReasonable) {
        return {main: text, trace: '', labels: []};
    }

    if (!mainPart) {
        mainPart = '结论已生成，验证过程已折叠在下方，可按需展开查看。';
    }
    return {main: mainPart, trace: tracePart, labels: labels};
}

function renderAskMarkdownBlock(text) {
    if (typeof marked !== 'undefined') {
        return marked.parse(String(text || ''));
    }
    return '<pre style="white-space:pre-wrap;word-break:break-word;margin:0;">' +
        escapeHtml(String(text || '')) + '</pre>';
}

function normalizeAskLinks(links) {
    var rows = Array.isArray(links) ? links : [];
    var out = [];
    var seen = {};
    for (var i = 0; i < rows.length; i++) {
        var row = rows[i];
        if (!row || typeof row !== 'object') continue;
        var url = String(row.url || row.link || '').trim();
        var title = String(row.title || '').trim();
        var source = String(row.source || '').trim();
        var publishedAt = String(row.published_at || '').trim();
        var summary = String(row.summary || row.snippet || '').trim();
        if (!url) continue;
        var key = (url.toLowerCase() + '|' + title);
        if (seen[key]) continue;
        seen[key] = true;
        out.push({
            title: title || url,
            url: url,
            source: source,
            published_at: publishedAt,
            summary: summary
        });
        if (out.length >= 10) break;
    }
    return out;
}

function renderAskLinks(links) {
    var rows = normalizeAskLinks(links);
    if (!rows.length) return '';
    var itemsHtml = rows.map(function(row) {
        var src = [row.source || '-', row.published_at || '-'].join(' · ');
        var summary = row.summary
            ? ('<div class="ask-link-summary">' + escapeHtml(row.summary) + '</div>')
            : '';
        return '<li><a href="' + escapeHtml(row.url) + '" target="_blank" rel="noopener noreferrer">' +
            escapeHtml(row.title) + '</a><span class="ask-link-src">(' + escapeHtml(src) + ')</span>' +
            summary + '</li>';
    }).join('');
    return '<div class="ask-links-box">' +
        '<div class="ask-links-title">联网搜索来源链接</div>' +
        '<ol class="ask-links-list">' + itemsHtml + '</ol>' +
        '</div>';
}

function renderAskSearchStatus(replyMeta, links) {
    var meta = replyMeta || {};
    var webEnabled = !(meta.enable_web_search === false);
    if (!webEnabled) return '';
    var used = !!meta.web_search_used;
    var err = String(meta.web_search_error || '').trim();
    if (err) {
        return '<div class="ask-links-box">' +
            '<div class="ask-links-title">联网检索状态</div>' +
            '<div style="font-size:12px;color:#b45309">已执行联网检索，但检索阶段异常：' +
            escapeHtml(err) + '</div>' +
            '</div>';
    }
    if (used && (!Array.isArray(links) || !links.length)) {
        return '<div class="ask-links-box">' +
            '<div class="ask-links-title">联网检索状态</div>' +
            '<div style="font-size:12px;color:#64748b">已执行联网检索，但当前未返回可展示的网站来源链接。</div>' +
            '</div>';
    }
    return '';
}

function renderAskWebSearchRaw(replyMeta) {
    var meta = replyMeta || {};
    var webEnabled = !(meta.enable_web_search === false);
    if (!webEnabled) return '';
    var used = !!meta.web_search_used;
    var err = String(meta.web_search_error || '').trim();
    var raw = String(meta.web_search_raw || '').trim();
    var title = 'Kimi 联网原始输出（点击展开）';
    var statusHint = used ? '状态：已执行联网检索。' : '状态：联网开关已开启，但本轮未标记为已执行。';
    if (err) statusHint += ' 检索异常：' + err;
    if (!raw) {
        return '<details class="ask-trace-details">' +
            '<summary><span class="ask-trace-title">' + title + '</span></summary>' +
            '<div class="ask-trace-body"><div style="font-size:12px;color:#64748b;">' + escapeHtml(statusHint) + ' 未返回可展示的原始文本。</div></div>' +
            '</details>';
    }
    var fullLen = raw.length;
    var maxLen = 18000;
    if (raw.length > maxLen) {
        raw = raw.slice(0, maxLen) + '\n\n...(原始输出较长，前端仅展示前 ' + maxLen + ' 字符；完整长度 ' + fullLen + ' 字符)';
    }
    return '<details class="ask-trace-details">' +
        '<summary><span class="ask-trace-title">' + title + '</span></summary>' +
        '<div class="ask-trace-body">' +
        '<div style="font-size:12px;color:#64748b;margin:0 0 8px 0;">' + escapeHtml(statusHint) + '</div>' +
        '<pre style="white-space:pre-wrap;word-break:break-word;margin:0;padding:10px 12px;border:1px solid #dbe3ee;border-radius:8px;background:#f8fafc;color:#0f172a;font-size:12px;line-height:1.55;">' +
        escapeHtml(raw) +
        '</pre></div>' +
        '</details>';
}

function buildAskReplyHtml(agentId, question, replyText, replyMeta) {
    var safeReply = replyText || '无回复';
    var askMode = replyMeta && replyMeta.ask_mode ? String(replyMeta.ask_mode) : '';
    var webEnabled = !(replyMeta && replyMeta.enable_web_search === false);
    var webUsed = !!(replyMeta && replyMeta.web_search_used);
    var webError = String((replyMeta && replyMeta.web_search_error) || '').trim();
    var workflowMode = replyMeta && replyMeta.workflow_mode ? String(replyMeta.workflow_mode) : '';
    var workflowName = replyMeta && replyMeta.workflow_name ? String(replyMeta.workflow_name) : '';
    var sessionId = replyMeta && replyMeta.session_id ? normalizeAskSessionId(replyMeta.session_id) : '';
    var sessionTiming = replyMeta && replyMeta.session_timing ? replyMeta.session_timing : null;
    var participants = (replyMeta && Array.isArray(replyMeta.participants)) ? replyMeta.participants : [];
    var taskPlan = (replyMeta && Array.isArray(replyMeta.task_plan)) ? replyMeta.task_plan : [];
    var taskPlanDetails = (replyMeta && Array.isArray(replyMeta.task_plan_details)) ? replyMeta.task_plan_details : [];
    var workflowSteps = (replyMeta && Array.isArray(replyMeta.workflow_steps)) ? replyMeta.workflow_steps : [];
    var runtimeProfile = (replyMeta && replyMeta.prompt_profile && typeof replyMeta.prompt_profile === 'object')
        ? replyMeta.prompt_profile
        : getPromptProfileForAgent(agentId);
    var links = normalizeAskLinks(replyMeta && replyMeta.search_links);
    var searchCount = links.length;
    var searchState = webEnabled
        ? (webError
            ? '执行异常'
            : (webUsed ? ('已执行(' + searchCount + '条来源)') : '未执行'))
        : '关闭';
    var summaryLine = '';
    if (askMode || workflowMode || workflowName || sessionId) {
        summaryLine = '<div style="margin:0 0 8px 0;padding:6px 8px;border:1px solid #e5e7eb;border-radius:8px;font-size:12px;color:#475569;">' +
            (askMode ? ('模式: ' + escapeHtml(getAskModeText(askMode)) + ' · ') : '') +
            '联网: ' + (webEnabled ? '开' : '关') + ' · 检索: ' + escapeHtml(searchState) + ' · ' +
            '工作流: ' + escapeHtml(workflowName || workflowMode || '-') +
            (participants.length ? (' · 参与: ' + escapeHtml(participants.join(', '))) : '') +
            (taskPlan.length ? (' · 执行计划: ' + escapeHtml(taskPlan.join(' -> '))) : '') +
            (!taskPlan.length && workflowSteps.length ? (' · 阶段: ' + escapeHtml(workflowSteps.join(' -> '))) : '') +
            (sessionTiming && sessionTiming.active ? (' · 时限: ' + escapeHtml(formatDeadlineBadge(sessionTiming))) : '') +
            (sessionId ? (' · 会话: ' + escapeHtml(sessionId)) : '') +
            '</div>';
    }
    var planDetailsHtml = '';
    if (taskPlanDetails.length) {
        planDetailsHtml = '<div style="margin:0 0 10px 0;padding:10px 12px;border:1px solid rgba(217,119,6,0.18);border-radius:12px;background:linear-gradient(180deg,rgba(255,247,237,0.95),rgba(255,255,255,0.98));">' +
            '<div style="font-size:12px;font-weight:700;color:#9a3412;margin-bottom:8px;">任务拆解与完成进度</div>' +
            taskPlanDetails.map(function(step) {
                var idx = Number(step.index || 0) || 0;
                var state = String(step.status || 'pending');
                var stateText = state === 'completed' ? '已完成' : (state === 'running' ? '执行中' : '待执行');
                var stateColor = state === 'completed' ? '#15803d' : (state === 'running' ? '#b45309' : '#94a3b8');
                var title = escapeHtml(String(step.title || ('步骤' + idx)));
                var goal = escapeHtml(String(step.goal || '').trim());
                var summary = escapeHtml(String(step.summary || '').trim());
                return '<div style="margin-top:8px;padding:8px 10px;border-radius:10px;border:1px solid rgba(148,163,184,0.16);background:#fff;">' +
                    '<div style="display:flex;justify-content:space-between;gap:8px;">' +
                    '<div style="font-size:12px;font-weight:700;color:#1e293b;">' + idx + '. ' + title + '</div>' +
                    '<div style="font-size:11px;color:' + stateColor + ';">' + stateText + '</div>' +
                    '</div>' +
                    (goal ? ('<div style="margin-top:4px;font-size:11px;color:#475569;">目标：' + goal + '</div>') : '') +
                    (summary ? ('<div style="margin-top:4px;font-size:11px;color:#334155;">结果：' + summary + '</div>') : '') +
                    '</div>';
            }).join('') +
            '</div>';
    }
    var promptSummaryHtml = '';
    var runtimeMods = Array.isArray(runtimeProfile.modifiers) ? runtimeProfile.modifiers : [];
    if (runtimeProfile.agent_name || runtimeMods.length) {
        promptSummaryHtml =
            '<div style="margin:0 0 10px 0;padding:10px 12px;border:1px solid rgba(148,163,184,0.16);border-radius:12px;background:rgba(248,250,252,0.92);">' +
                renderPromptProfileSummary(runtimeProfile) +
            '</div>';
    }
    lastAskDownload = {
        agentId: agentId,
        question: question,
        reply: safeReply,
        askMode: askMode,
        searchLinks: links,
        ts: Date.now()
    };
    lastAskMeta = replyMeta || {};
    var parts = splitAskReplyForDisplay(safeReply, replyMeta || {});
    var mainHtml = renderAskMarkdownBlock(parts.main || safeReply);
    var traceHtml = '';
    if (parts.trace) {
        var chipsHtml = parts.labels.length
            ? '<span class="ask-trace-chips">' + parts.labels.map(function(label) {
                return '<span class="ask-trace-chip">' + escapeHtml(label) + '</span>';
            }).join('') + '</span>'
            : '';
        traceHtml =
            '<details class="ask-trace-details">' +
                '<summary>' +
                    '<span class="ask-trace-title">验证过程（点击展开）</span>' +
                    chipsHtml +
                '</summary>' +
                '<div class="ask-trace-body">' + renderAskMarkdownBlock(parts.trace) + '</div>' +
            '</details>';
    }
    var processHtml = summaryLine +
        planDetailsHtml +
        promptSummaryHtml +
        renderAskSearchStatus(replyMeta, links) +
        traceHtml;
    var hasProcess = String(processHtml || '').replace(/\s+/g, '').length > 0;
    var processBlock = hasProcess
        ? (
            '<details class="ask-process-details">' +
                '<summary><span class="ask-process-title">过程与系统步骤（点击展开）</span></summary>' +
                '<div class="ask-process-body">' + processHtml + '</div>' +
            '</details>'
        )
        : '';

    var finalBlock =
        '<div class="ask-final-block">' +
            '<div class="ask-final-head"><strong>' + (AGENT_ICONS[agentId] || '🤖') + ' ' + agentId + ' · 最终输出</strong></div>' +
            '<div class="ask-main-content">' + mainHtml + '</div>' +
        '</div>';

    var webRawBlock = renderAskWebSearchRaw(replyMeta);
    var linkBlock = renderAskLinks(links);
    return processBlock + finalBlock + webRawBlock + linkBlock;
}

function renderAskReply(agentId, question, replyText, responseDiv, askDownloadBtn, replyMeta) {
    var html = buildAskReplyHtml(agentId, question, replyText, replyMeta);
    if (responseDiv) responseDiv.innerHTML = html;
    if (askDownloadBtn) askDownloadBtn.style.display = 'inline-flex';
    showAskFeedbackBox();
    return html;
}

function pollAskResult(askId, agentId, question, startedAt, askDownloadBtn, pendingCtx) {
    if (!askId) return;
    if (currentAskId && askId !== currentAskId) return;
    var task = pendingCtx || askPendingMessageByTask[askId] || null;
    fetchJsonStrict('/api/team/ask_result/' + askId)
        .then(function(data) {
            if (currentAskId && askId !== currentAskId) return;
            latestAskSessionTiming = (data && data.session_timing && data.session_timing.active) ? data.session_timing : null;
            latestAskSessionProgress = (data && data.session_progress && data.session_progress.active) ? data.session_progress : null;
            latestAskSessionOvertime = (data && data.session_overtime && data.session_overtime.active) ? data.session_overtime : null;
            var liveSessionId = String(
                (data && data.session_id) ||
                (latestAskSessionProgress && latestAskSessionProgress.session_id) ||
                (latestAskSessionTiming && latestAskSessionTiming.session_id) ||
                (latestAskSessionOvertime && latestAskSessionOvertime.session_id) ||
                ''
            ).trim();
            if (liveSessionId && task && task.chat_id) {
                bindSessionToChat(liveSessionId, task.chat_id, {label: '直连问答'});
            }
            if (Array.isArray(data.live_agents)) {
                cachePromptProfiles(data.live_agents);
            }
            if (data.director_live && typeof data.director_live === 'object') {
                cachePromptProfiles([data.director_live]);
            }
            renderSessionTimingBanner();
            renderSessionProgressBoard();
            renderSessionOvertimePanel();
            renderTeamPromptRuntime();
            loadStatus();

            if (data.done) {
                if (data.status === 'done') {
                    if (data.session_id && task && task.chat_id) {
                        setAskSessionForAgentInChat(task.chat_id, agentId, data.session_id);
                        bindSessionToChat(data.session_id, task.chat_id, {label: '直连问答'});
                    } else if (data.session_id) {
                        setAskSessionForAgent(agentId, data.session_id);
                    }
                    if (data.trace_run_id) {
                        var traceInput = document.getElementById('trace-run-id-input');
                        if (traceInput) traceInput.value = String(data.trace_run_id || '');
                        refreshTraceRuns(data.session_id || '');
                    }
                    var finalHtml = renderAskReply(agentId, question, data.reply || '', null, askDownloadBtn, data);
                    if (task && task.chat_id && task.pending_id) {
                        markAskPendingDone(task.chat_id, task.pending_id, agentId, finalHtml);
                    }
                } else {
                    if (task && task.chat_id && task.pending_id) {
                        markAskPendingError(task.chat_id, task.pending_id, data.error || '处理失败，请重试');
                    }
                    lastAskDownload = null;
                    lastAskMeta = null;
                    if (askDownloadBtn) askDownloadBtn.style.display = 'none';
                    hideAskFeedbackBox();
                }
                if (askPendingMessageByTask[askId]) delete askPendingMessageByTask[askId];
                stopAskPolling();
                return;
            }

            var elapsedMs = Date.now() - startedAt;
            var elapsedSec = Math.max(1, Math.floor(elapsedMs / 1000));
            var askMode = String((data && data.ask_mode) || '').toLowerCase();
            var webEnabled = !(data && data.enable_web_search === false);
            var modeText = getAskModeText(askMode || getSelectedAskMode());
            var isTeam = (agentId === 'director' && askMode === 'team');
            var taskPlan = Array.isArray(data.task_plan) ? data.task_plan : [];
            var directorLive = data && data.director_live ? data.director_live : null;
            var sessionTiming = data && data.session_timing ? data.session_timing : null;
            var statusLine = '正在由 ' + agentId + ' 处理中（' + elapsedSec + 's）';
            if (isTeam) {
                statusLine += '，团队协作执行中，可在“活动日志 -> 直连问答”查看分配进度';
            } else {
                statusLine += '，当前为' + modeText + '直连模式';
            }
            statusLine += '（联网' + (webEnabled ? '开启' : '关闭') + '）';
            if (taskPlan.length) {
                statusLine += ' | 计划: ' + taskPlan.join(' -> ');
            }
            if (sessionTiming && sessionTiming.active) {
                statusLine += ' | 时限: ' + formatDeadlineBadge(sessionTiming);
            }
            if (data.session_overtime && data.session_overtime.waiting) {
                statusLine += ' | 已超出你设定的时限，请在顶部选择“继续等待”或“立即停止”';
            }
            if (directorLive) {
                var directorStep = String(directorLive.current_step || '').trim();
                var directorNext = String(directorLive.next_step || '').trim();
                var directorProgress = '';
                if (directorLive.current_step_index && directorLive.current_step_total) {
                    directorProgress = ' ' + directorLive.current_step_index + '/' + directorLive.current_step_total;
                }
                if (directorStep) {
                    statusLine += ' | 决策专家' + directorProgress + '：' + directorStep + (directorNext ? (' -> ' + directorNext) : '');
                }
            }
            var liveAgents = Array.isArray(data.live_agents) ? data.live_agents : [];
            if (liveAgents.length) {
                var detail = liveAgents.slice(0, 4).map(function(row) {
                    var name = String(row.name || row.agent_id || 'agent');
                    var step = String(row.current_step || row.current_workflow_label || row.status || '').trim();
                    var next = String(row.next_step || '').trim();
                    return name + '：' + step + (next ? (' -> ' + next) : '');
                }).join('；');
                if (detail) statusLine += ' | ' + detail;
            }
            if (task && task.chat_id && task.pending_id) {
                markAskPendingStatus(task.chat_id, task.pending_id, statusLine);
            }

            if (elapsedMs > ASK_POLL_TIMEOUT_MS) {
                if (task && task.chat_id && task.pending_id) {
                    markAskPendingStatus(
                        task.chat_id,
                        task.pending_id,
                        '处理时间较长（>' + Math.floor(ASK_POLL_TIMEOUT_MS / 60000) + '分钟），已转后台执行，可稍后在活动日志查看。'
                    );
                }
                stopAskPolling();
                if (askPendingMessageByTask[askId]) delete askPendingMessageByTask[askId];
                return;
            }

            currentAskPollTimer = setTimeout(function() {
                pollAskResult(askId, agentId, question, startedAt, askDownloadBtn, task);
            }, ASK_POLL_INTERVAL_MS);
        })
        .catch(function(err) {
            if (task && task.chat_id && task.pending_id) {
                markAskPendingStatus(
                    task.chat_id,
                    task.pending_id,
                    '任务状态查询异常：' + ((err && err.message) || '查询失败') + '，正在自动重试...'
                );
            }
            currentAskPollTimer = setTimeout(function() {
                pollAskResult(askId, agentId, question, startedAt, askDownloadBtn, task);
            }, 2600);
        });
}

function askAgent() {
    var agentId = document.getElementById('ask-agent').value;
    var question = document.getElementById('ask-input').value.trim();
    if (!question) return;
    var directorMode = (agentId === 'director');
    var askMode = getSelectedAskMode();
    var webEnabled = getAskWebEnabled();
    var teamEnabled = directorMode && askMode === 'team';
    var activeChat = ensureAskChatSessionReady();
    if (!activeChat) return;
    var chatSessionId = getAskSessionForAgent(agentId);
    var askDownloadBtn = document.getElementById('ask-download-btn');
    stopAskPolling();
    lastAskMeta = null;
    if (askDownloadBtn) askDownloadBtn.style.display = 'none';
    hideAskFeedbackBox();
    var waitingText = teamEnabled
        ? ('当前链路：总监团队协作（模式=' + getAskModeText(askMode) + '，联网' + (webEnabled ? '开启' : '关闭') + '）')
        : ('当前链路：' + (directorMode ? '总监直连' : '专家直连') + '（模式=' + getAskModeText(askMode) + '，联网' + (webEnabled ? '开启' : '关闭') + '）');
    var pendingCtx = appendAskTurnToChat(activeChat, question, agentId, waitingText);
    document.getElementById('ask-input').value = '';
    if (!pendingCtx) return;

    fetchJsonStrict('/api/team/ask/' + agentId, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            question: question,
            ask_mode: askMode,
            enable_web_search: webEnabled,
            context_ids: teamContextIds,
            chat_session_id: chatSessionId
        })
    }).then(function(data) {
        if (data && data.ask_id) {
            loadStatus();
            currentAskId = data.ask_id;
            lastAskMeta = {ask_id: currentAskId, ask_mode: askMode};
            askPendingMessageByTask[currentAskId] = pendingCtx;
            var begin = Date.now();
            var ctxHint = (data.context_file_count && data.context_file_count > 0)
                ? ('，已附带材料' + data.context_file_count + '份')
                : '';
            var sidHint = chatSessionId ? ('，延续会话 ' + chatSessionId) : '，新会话';
            markAskPendingStatus(
                pendingCtx.chat_id,
                pendingCtx.pending_id,
                '问题已提交（ask_id=' + currentAskId + '），' +
                (teamEnabled
                    ? ('团队协作模式：总监将分配/协作后再答复' + ctxHint + sidHint + '。')
                    : (getAskModeText(askMode) + '模式：仅由当前智能体直接答复' + ctxHint + sidHint + '。'))
            );
            currentAskPollTimer = setTimeout(function() {
                pollAskResult(currentAskId, agentId, question, begin, askDownloadBtn, pendingCtx);
            }, 200);
            return;
        }

        if (data && Object.prototype.hasOwnProperty.call(data, 'reply')) {
            var syncMeta = data || {};
            if (!syncMeta.ask_id && currentAskId) syncMeta.ask_id = currentAskId;
            if (syncMeta.session_id) {
                setAskSessionForAgentInChat(pendingCtx.chat_id, agentId, syncMeta.session_id);
                bindSessionToChat(syncMeta.session_id, pendingCtx.chat_id, {label: '直连问答'});
            }
            var syncHtml = renderAskReply(agentId, question, data.reply || '', null, askDownloadBtn, syncMeta);
            markAskPendingDone(pendingCtx.chat_id, pendingCtx.pending_id, agentId, syncHtml);
        } else if (data && data.error) {
            markAskPendingError(pendingCtx.chat_id, pendingCtx.pending_id, data.error);
            lastAskDownload = null;
            lastAskMeta = null;
            if (askDownloadBtn) askDownloadBtn.style.display = 'none';
            hideAskFeedbackBox();
        } else {
            markAskPendingError(pendingCtx.chat_id, pendingCtx.pending_id, '请求失败，请重试');
            lastAskDownload = null;
            lastAskMeta = null;
            if (askDownloadBtn) askDownloadBtn.style.display = 'none';
            hideAskFeedbackBox();
        }
    }).catch(function(err) {
        markAskPendingError(pendingCtx.chat_id, pendingCtx.pending_id, (err && err.message) || '请求失败，请重试');
        lastAskDownload = null;
        lastAskMeta = null;
        if (askDownloadBtn) askDownloadBtn.style.display = 'none';
        hideAskFeedbackBox();
        stopAskPolling();
    });
}

function downloadAskResponse() {
    if (!lastAskDownload) {
        alert('暂无可下载的答复');
        return;
    }
    var d = new Date(lastAskDownload.ts || Date.now());
    var ts = d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate()) +
        ' ' + pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds());
    var content = '# 智能体答复记录\n\n' +
        '- 时间: ' + ts + '\n' +
        '- 智能体: ' + (lastAskDownload.agentId || '-') + '\n' +
        '- 模式: ' + getAskModeText(lastAskDownload.askMode || '') + '\n' +
        '- 问题: ' + (lastAskDownload.question || '-') + '\n\n' +
        '## 回复内容\n\n' +
        (lastAskDownload.reply || '');
    var links = normalizeAskLinks(lastAskDownload.searchLinks || []);
    if (links.length) {
        content += '\n\n## 联网搜索来源链接\n\n';
        content += links.map(function(row, idx) {
            return (idx + 1) + '. ' + (row.title || '-') + ' | ' + (row.source || '-') + ' | ' +
                (row.published_at || '-') + ' | ' + row.url;
        }).join('\n');
    }
    var filename = sanitizeFilename('ask_' + (lastAskDownload.agentId || 'agent') + '_' + d.getTime());
    downloadTextFile(filename + '.md', content, 'text/markdown;charset=utf-8');
}

// ──────────────── 面板切换 ────────────────
function switchTab(tabName) {
    document.querySelectorAll('.panel-tab').forEach(function(t) {
        t.classList.toggle('active', t.getAttribute('data-tab') === tabName);
    });
    document.querySelectorAll('.panel-content').forEach(function(c) {
        c.classList.toggle('active', c.id === 'tab-' + tabName);
    });
    if (tabName === 'activity') {
        activityRenderDirty = false;
        renderActivityList();
    } else if (tabName === 'workflow') {
        activityRenderDirty = false;
        syncWorkflowSessionOptions();
        renderWorkflowLane();
    } else if (tabName === 'memory' || tabName === 'evolution') {
        loadMemoryCenter(false);
    }
}

function renderReasoningTrace(trace) {
    if (!trace || typeof trace !== 'object') return '';
    var flags = trace.section_flags || {};
    var causal = Array.isArray(trace.causal_steps) ? trace.causal_steps : [];
    var deriv = Array.isArray(trace.derivation_lines) ? trace.derivation_lines : [];
    var evidence = Array.isArray(trace.evidence) ? trace.evidence : [];
    var rel = trace.evidence_reliability || {};
    var chips = [
        'enforced=' + (trace.enforced ? 'yes' : 'no'),
        'revised=' + (trace.revised ? 'yes' : 'no'),
        'numbers=' + (trace.numeric_claim_count || 0),
        'high=' + (rel.high || 0),
        'medium=' + (rel.medium || 0),
        'internal=' + (rel.internal || 0),
        'low=' + (rel.low || 0)
    ];

    var flagHtml = '<div class="reasoning-flags">' +
        '<span class="reasoning-flag ' + (flags.causal ? 'ok' : 'miss') + '">因果链</span>' +
        '<span class="reasoning-flag ' + (flags.derivation ? 'ok' : 'miss') + '">数字推导</span>' +
        '<span class="reasoning-flag ' + (flags.source ? 'ok' : 'miss') + '">来源标注</span>' +
        '<span class="reasoning-flag ' + (flags.uncertainty ? 'ok' : 'miss') + '">不确定性</span>' +
        '</div>';

    var causalHtml = causal.length
        ? '<ol class="reasoning-list">' + causal.map(function(s) {
            return '<li>' + escapeHtml(s) + '</li>';
        }).join('') + '</ol>'
        : '<div class="reasoning-empty">未提取到明确因果步骤</div>';

    var derivHtml = deriv.length
        ? '<ul class="reasoning-list">' + deriv.map(function(s) {
            return '<li>' + escapeHtml(s) + '</li>';
        }).join('') + '</ul>'
        : '<div class="reasoning-empty">未提取到可回溯数字推导行</div>';

    var evHtml = evidence.length
        ? '<div class="reasoning-evidence">' + evidence.map(function(ev, idx) {
            return '<div class="reasoning-ev-item">' +
                '<div><b>[E' + (idx + 1) + ']</b> ' + escapeHtml(ev.tool || '-') +
                ' <span class="reasoning-ev-rel">[' + escapeHtml(ev.reliability || 'low') + ']</span></div>' +
                '<div class="reasoning-ev-sub">args: ' + escapeHtml(ev.args || '-') + '</div>' +
                '<div class="reasoning-ev-sub">result: ' + escapeHtml(ev.result || '-') + '</div>' +
                '</div>';
        }).join('') + '</div>'
        : '<div class="reasoning-empty">无工具证据（纯上下文推理）</div>';

    return '<div class="reasoning-panel">' +
        '<div class="reasoning-title">推理步骤可视化</div>' +
        '<div class="reasoning-chips">' + chips.map(function(c) {
            return '<span class="reasoning-chip">' + escapeHtml(c) + '</span>';
        }).join('') + '</div>' +
        flagHtml +
        '<div class="reasoning-block"><div class="reasoning-subtitle">因果链路</div>' + causalHtml + '</div>' +
        '<div class="reasoning-block"><div class="reasoning-subtitle">数字推导摘录</div>' + derivHtml + '</div>' +
        '<div class="reasoning-block"><div class="reasoning-subtitle">证据来源</div>' + evHtml + '</div>' +
        '</div>';
}

// ──────────────── 活动详情弹窗 ────────────────
function showActivityDetail(msg) {
    if (!msg) return;
    var modal = document.getElementById('activity-detail-modal');
    if (!modal) return;

    var fromName = msg.from || 'system';
    var icon = AGENT_ICONS[fromName] || '⚙️';
    var typeText = {
        thinking: '思考', tool_call: '工具调用', speaking: '发言',
        report: '报告', error: '错误', status: '状态', consensus: '共识', reasoning: '推理链路',
        task: '任务', question: '提问', review: '审查', alert: '警告'
    }[msg.type] || msg.type || '活动';
    var scope = classifyActivityScope(msg);
    var scopeText = getActivityScopeText(scope);
    var timeStr = new Date(msg.timestamp * 1000).toLocaleString('zh-CN');
    activityDetailSessionId = getMessageSessionId(msg);
    var jumpBtn = document.getElementById('activity-jump-workflow-btn');
    if (jumpBtn) {
        jumpBtn.style.display = activityDetailSessionId ? 'inline-flex' : 'none';
    }

    document.getElementById('activity-detail-title').textContent = icon + ' ' + fromName + ' - ' + typeText;

    var content = '<div style="margin-bottom:12px;font-size:12px;color:#6b7280;">' +
        '<span>时间: ' + timeStr + '</span>' +
        (msg.to ? ' &nbsp;|&nbsp; 目标: ' + msg.to : '') +
        ' &nbsp;|&nbsp; 类型: ' + typeText +
        ' &nbsp;|&nbsp; 分类: ' + scopeText +
        '</div>';
    content += renderMeetingDetailPanel(msg);

    var fullText = (msg.metadata && msg.metadata.full_content) || msg.content || '(无内容)';
    currentActivityDownload = {
        title: (fromName || 'system') + '_' + (typeText || 'activity'),
        content: [
            '时间: ' + timeStr,
            '来源: ' + fromName,
            (msg.to ? '目标: ' + msg.to : ''),
            '类型: ' + typeText,
            '分类: ' + scopeText,
            '',
            '正文:',
            fullText,
            '',
            '附加信息:',
            JSON.stringify(msg.metadata || {}, null, 2)
        ].filter(Boolean).join('\n')
    };
    if (typeof marked !== 'undefined') {
        content += '<div class="report-content">' + marked.parse(fullText) + '</div>';
    } else {
        content += '<pre style="white-space:pre-wrap;word-break:break-all;font-size:13px;line-height:1.6;">' + escapeHtml(fullText) + '</pre>';
    }

    var trace = msg.metadata && msg.metadata.reasoning_trace;
    if (trace) {
        content += renderReasoningTrace(trace);
    }

    if (msg.metadata && Object.keys(msg.metadata).length > 0) {
        content += '<div style="margin-top:12px;padding-top:12px;border-top:1px solid #e5e7eb;">' +
            '<div style="font-size:12px;font-weight:600;color:#6b7280;margin-bottom:4px;">附加信息</div>' +
            '<pre style="font-size:12px;color:#374151;background:#f9fafb;padding:8px;border-radius:6px;white-space:pre-wrap;">' +
            escapeHtml(JSON.stringify(msg.metadata, null, 2)) + '</pre></div>';
    }

    document.getElementById('activity-detail-content').innerHTML = content;
    modal.style.display = 'flex';
}

function closeActivityDetail() {
    var modal = document.getElementById('activity-detail-modal');
    if (modal) modal.style.display = 'none';
}

function showAgentPromptProfile(agentId) {
    try {
        var aid = String(agentId || '').trim();
        if (!aid) return;
        var modal = document.getElementById('prompt-profile-modal');
        var titleEl = document.getElementById('prompt-profile-title');
        var contentEl = document.getElementById('prompt-profile-content');
        if (!modal || !titleEl || !contentEl) return;

        var row = findLatestAgentStatus(aid) || {};
        var profile = getPromptProfileForAgent(aid) || {};
        var modifiers = Array.isArray(profile.modifiers) ? profile.modifiers : [];
        var agentName = String(profile.agent_name || row.name || aid);
        titleEl.textContent = agentName + ' · 运行时提示词详情';

        var html = '';
        html += '<div style="font-size:13px;color:#334155;line-height:1.7;">';
        html += '<div><strong>智能体：</strong>' + escapeHtml(agentName) + '</div>';
        html += '<div><strong>工作流：</strong>' + escapeHtml(String(profile.workflow || row.current_workflow_label || '-')) + '</div>';
        html += '<div><strong>响应模式：</strong>' + escapeHtml(getAskModeText(String(profile.response_style || 'auto'))) + '</div>';
        if (row.current_step || row.next_step) {
            html += '<div><strong>当前推进：</strong>' + escapeHtml(String(row.current_step || '-'));
            if (row.next_step) html += ' -> ' + escapeHtml(String(row.next_step || ''));
            html += '</div>';
        }
        html += '</div>';

        if (modifiers.length) {
            html += '<div style="margin-top:12px;font-size:12px;font-weight:700;color:#334155;">提示词增强项</div>';
            html += '<div class="session-prompt-chips" style="margin-top:8px;">' +
                modifiers.map(function(item) {
                    return '<span class="session-prompt-chip">' + escapeHtml(String(item || '')) + '</span>';
                }).join('') +
                '</div>';
        } else {
            html += '<div style="margin-top:12px;font-size:12px;color:#64748b;">当前未捕获到更多运行时提示词增强项。</div>';
        }

        html += '<div style="margin-top:12px;padding:10px 12px;border-radius:10px;background:#f8fafc;border:1px solid rgba(148,163,184,0.18);">';
        html += '<div style="font-size:12px;font-weight:700;color:#475569;margin-bottom:6px;">原始画像数据</div>';
        html += '<pre style="font-size:12px;line-height:1.5;white-space:pre-wrap;color:#0f172a;margin:0;">' +
            escapeHtml(JSON.stringify(profile || {}, null, 2)) +
            '</pre></div>';

        contentEl.innerHTML = html;
        modal.style.display = 'flex';
    } catch (err) {
        var modalFallback = document.getElementById('prompt-profile-modal');
        var contentFallback = document.getElementById('prompt-profile-content');
        if (contentFallback) {
            contentFallback.innerHTML =
                '<div style="font-size:13px;color:#991b1b;">提示词详情渲染失败：' +
                escapeHtml(String((err && err.message) || err || '未知错误')) +
                '</div>';
        }
        if (modalFallback) modalFallback.style.display = 'flex';
    }
}

function closePromptProfileModal() {
    var modal = document.getElementById('prompt-profile-modal');
    if (modal) modal.style.display = 'none';
}

window.showAgentPromptProfile = showAgentPromptProfile;
window.closePromptProfileModal = closePromptProfileModal;

function jumpToWorkflowFromActivity() {
    if (!activityDetailSessionId) return;
    workflowSessionId = activityDetailSessionId;
    switchTab('workflow');
    refreshWorkflowLane();
}

function downloadCurrentActivityDetail() {
    if (!currentActivityDownload) {
        alert('当前没有可下载的内容');
        return;
    }
    var name = sanitizeFilename(currentActivityDownload.title || 'activity_detail');
    downloadTextFile(name + '.md', currentActivityDownload.content || '', 'text/markdown;charset=utf-8');
}

// ══════════════ 投资收益面板 ══════════════

var pfNavChart = null;
var pfNavDays = 7;
var pfPollingTimer = null;
var pfNavLastData = null;

// 模式切换：显示/隐藏标的输入框
document.addEventListener('DOMContentLoaded', function() {
    var modeSel = document.getElementById('pf-mode');
    var targetInp = document.getElementById('pf-target-code');
    if (modeSel) {
        modeSel.addEventListener('change', function() {
            var inp = document.getElementById('pf-target-code');
            if (inp) inp.style.display = this.value === 'target' ? 'inline-block' : 'none';
            savePortfolioBasicConfig();
        });
    }
    if (targetInp) {
        targetInp.addEventListener('blur', savePortfolioBasicConfig);
        targetInp.addEventListener('keydown', function(e) {
            if (e.key === 'Enter') {
                savePortfolioBasicConfig();
            }
        });
    }
});

// ── 初始化组合 ──
function portfolioInit() {
    var mode = document.getElementById('pf-mode').value;
    var target = document.getElementById('pf-target-code').value.trim();
    if (mode === 'target' && !target) { alert('请输入标的代码'); return; }
    fetch('/api/team/portfolio/init', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({mode: mode, target_code: target})
    }).then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.error) { alert(d.error); return; }
        alert('投资组合初始化成功！');
        loadPortfolioStatus();
    }).catch(function() { alert('初始化失败'); });
}

function loadPortfolioConfig() {
    fetch('/api/team/portfolio/config')
        .then(function(r) { return r.json(); })
        .then(function(cfg) {
            if (!cfg || cfg.initialized === false) return;
            applyPortfolioConfig(cfg);
        })
        .catch(function() {});
}

function applyPortfolioConfig(cfg) {
    var modeSel = document.getElementById('pf-mode');
    var targetInp = document.getElementById('pf-target-code');
    var autoCb = document.getElementById('pf-auto-run');

    if (modeSel) {
        modeSel.value = cfg.mode || 'free';
    }
    if (targetInp) {
        targetInp.value = cfg.target_code || '';
        targetInp.style.display = (cfg.mode === 'target') ? 'inline-block' : 'none';
    }
    if (autoCb && cfg.auto_run !== undefined) {
        autoCb.checked = !!cfg.auto_run;
    }
}

function savePortfolioBasicConfig() {
    var modeSel = document.getElementById('pf-mode');
    var targetInp = document.getElementById('pf-target-code');
    if (!modeSel || !targetInp) return;
    fetch('/api/team/portfolio/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            mode: modeSel.value || 'free',
            target_code: (targetInp.value || '').trim()
        })
    }).catch(function() {});
}

// ── 自动运行开关 ──
function togglePortfolioAuto() {
    var checked = document.getElementById('pf-auto-run').checked;
    fetch('/api/team/portfolio/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({auto_run: checked})
    }).then(function() {
        loadPortfolioStatus();
    }).catch(function() {});
}

function togglePortfolioWatchEnabled() {
    var checked = document.getElementById('pf-watch-enabled').checked;
    fetch('/api/team/portfolio/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({watch_enabled: checked})
    }).then(function() {
        loadPortfolioStatus();
    }).catch(function() {});
}

function updatePortfolioWatchInterval() {
    var val = parseInt(document.getElementById('pf-watch-interval').value);
    fetch('/api/team/portfolio/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({watch_interval: val})
    }).then(function() {
        loadPortfolioStatus();
    }).catch(function() {});
}

// ── 手动触发 ──
function portfolioRunManual() {
    var timeLimitEl = document.getElementById('pf-manual-time-limit');
    var timeLimitMinutes = timeLimitEl ? Number(timeLimitEl.value || 10) : 10;
    fetch('/api/team/portfolio/run', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({time_limit_minutes: timeLimitMinutes})
    }).then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.error) { alert(d.error); return; }
        alert('投资周期已启动 · 时限 ' + (d.time_limit_minutes || timeLimitMinutes) + ' 分钟');
        switchTab('activity');
    }).catch(function() { alert('启动失败'); });
}

function portfolioRunWatchNow() {
    var timeLimitEl = document.getElementById('pf-manual-time-limit');
    var timeLimitMinutes = timeLimitEl ? Number(timeLimitEl.value || 10) : 10;
    fetch('/api/team/portfolio/watch/run', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({time_limit_minutes: timeLimitMinutes})
    }).then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.error) { alert(d.error); return; }
        alert('盘中盯盘已启动 · 时限 ' + (d.time_limit_minutes || timeLimitMinutes) + ' 分钟');
        switchTab('activity');
      }).catch(function() { alert('启动盯盘失败'); });
}

// ── 加载组合状态 ──
function loadPortfolioStatus() {
    fetchJsonStrict('/api/team/portfolio/status', {cache: 'no-store'})
        .then(function(d) {
            clearRuntimeAlert('portfolio_status');
            renderPortfolioStatus(d);
        })
        .catch(function(err) {
            setRuntimeAlert('portfolio_status', '投资收益状态加载失败：' + _errText(err), 'error');
        });

    fetchJsonStrict('/api/team/portfolio/signals?limit=20', {cache: 'no-store'})
        .then(function(d) {
            clearRuntimeAlert('portfolio_signals');
            renderSignals(d);
        })
        .catch(function(err) {
            setRuntimeAlert('portfolio_signals', '交易信号加载失败：' + _errText(err), 'error');
        });

    fetchJsonStrict('/api/team/portfolio/compensation', {cache: 'no-store'})
        .then(function(d) {
            clearRuntimeAlert('portfolio_compensation');
            renderSalary(d);
        })
        .catch(function(err) {
            setRuntimeAlert('portfolio_compensation', '智能体薪资加载失败：' + _errText(err), 'error');
        });

    loadNavChart();
}

function formatMoney(val) {
    if (val === null || val === undefined) return '--';
    if (Math.abs(val) >= 10000) return (val / 10000).toFixed(2) + '万';
    return val.toFixed(2);
}

function formatPct(val) {
    if (val === null || val === undefined) return '--';
    var n = Number(val);
    if (!isFinite(n)) return '--';
    return (n * 100).toFixed(2) + '%';
}

function formatPctPoint(val) {
    if (val === null || val === undefined) return '--';
    var n = Number(val);
    if (!isFinite(n)) return '--';
    return n.toFixed(2) + '%';
}

function pad2(n) {
    return n < 10 ? '0' + n : String(n);
}

function formatYmd(ymd) {
    if (!ymd) return '-';
    var s = String(ymd);
    if (s.length === 8 && /^\d{8}$/.test(s)) {
        return s.slice(0, 4) + '-' + s.slice(4, 6) + '-' + s.slice(6, 8);
    }
    return s;
}

function formatUnixTime(ts) {
    if (ts === null || ts === undefined || ts === '') return '-';
    var n = Number(ts);
    if (!isFinite(n) || n <= 0) return '-';
    var d = n > 1e12 ? new Date(n) : new Date(n * 1000);
    if (isNaN(d.getTime())) return '-';
    return d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate()) +
        ' ' + pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds());
}

function toDateKeyFromSignal(signal) {
    var sdate = formatYmd(signal && signal.signal_date);
    if (sdate && sdate !== '-') return sdate;
    var created = formatUnixTime(signal && signal.created_at);
    if (created && created !== '-') return created.slice(0, 10);
    var edate = formatYmd(signal && signal.execute_date);
    return edate && edate !== '-' ? edate : '-';
}

function signalSortValue(signal) {
    var ts = Number(signal && signal.created_at);
    if (isFinite(ts) && ts > 0) return ts > 1e12 ? ts / 1000 : ts;
    var ymd = String(signal && signal.signal_date || '');
    if (/^\d{8}$/.test(ymd)) {
        var d = new Date(ymd.slice(0, 4) + '-' + ymd.slice(4, 6) + '-' + ymd.slice(6, 8) + 'T00:00:00');
        if (!isNaN(d.getTime())) return Math.floor(d.getTime() / 1000);
    }
    return 0;
}

function priceSourceText(source) {
    var m = {
        daily_kline_open: '日线库开盘价',
        tushare_daily_open: 'Tushare日线开盘价',
        tushare_rt_min: 'Tushare分时价',
        latest_close_fallback: '最近收盘价兜底'
    };
    return m[source] || (source || '-');
}

function renderDbUpdateTip(data) {
    var el = document.getElementById('pf-db-update-tip');
    if (!el) return;
    var scheduler = (data && data.scheduler) || {};
    var marketData = (data && data.market_data) || {};
    var updateTime = scheduler.db_auto_update_time || '06:00';
    var lastRun = formatYmd(scheduler.last_db_update_date);
    var maxKlineDate = formatYmd(marketData.daily_kline_max_trade_date);

    var text = '数据库更新状态: ';
    var cls = 'pf-runtime-tip';
    if (!scheduler.db_auto_update_enabled) {
        text += '已关闭自动更新';
        cls += ' warn';
    } else if (scheduler.db_update_running) {
        text += '进行中（每日 ' + updateTime + '）';
        cls += ' warn';
    } else {
        text += '已开启（每日 ' + updateTime + '）';
        if (lastRun && lastRun !== '-') {
            text += ' · 最近任务: ' + lastRun;
            cls += ' ok';
        } else {
            text += ' · 最近任务: 未记录';
            cls += ' warn';
        }
        if (maxKlineDate && maxKlineDate !== '-') {
            text += ' · 行情库最新日: ' + maxKlineDate;
        } else {
            text += ' · 行情库最新日: 未知';
            cls += ' warn';
        }
    }
    el.className = cls;
    el.textContent = text;
}

function renderPortfolioStatus(d) {
    if (!d || d.error) return;
    latestPortfolioSchedulerState = (d && d.scheduler) || {};
    applyPortfolioConfig(d);
    renderSessionTimingBanner();
    renderSessionProgressBoard();
    renderSessionOvertimePanel();
    renderTeamPromptRuntime();
    var setVal = function(id, val, cls) {
        var el = document.getElementById(id);
        if (el) { el.textContent = val; el.className = 'pf-card-value' + (cls ? ' ' + cls : ''); }
    };

    var totalAssets = Number(d.total_assets || d.current_cash || 0);
    var cash = Number(d.current_cash || 0);
    var mv = Number(d.market_value || 0);
    var nav = Number(d.nav || 1);
    var dailyRet = Number(d.daily_return || 0); // 后端返回的已是百分比值
    var totalRet = nav - 1;
    var maxDD = Number(d.max_drawdown || 0);

    setVal('pf-total-assets', formatMoney(totalAssets));
    setVal('pf-cash', formatMoney(cash));
    setVal('pf-market-value', formatMoney(mv));
    setVal('pf-nav', nav.toFixed(4));
    setVal('pf-daily-return', formatPctPoint(dailyRet), dailyRet >= 0 ? 'positive' : 'negative');
    setVal('pf-total-return', formatPct(totalRet), totalRet >= 0 ? 'positive' : 'negative');
    setVal('pf-max-drawdown', formatPct(maxDD), 'negative');
    renderDbUpdateTip(d);

    // 夏普比率单独获取
    fetch('/api/team/portfolio/stats')
        .then(function(r) { return r.json(); })
        .then(function(s) {
            setVal('pf-sharpe', s.sharpe_ratio !== undefined ? s.sharpe_ratio.toFixed(2) : '--');
        }).catch(function() {});

    // 持仓
    var tbody = document.getElementById('pf-positions-body');
    if (tbody) {
        var positions = d.positions || d.holdings || [];
        if (positions.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" class="pf-empty">暂无持仓</td></tr>';
        } else {
            var html = '';
            positions.forEach(function(p) {
                var qty = p.quantity != null ? p.quantity : (p.total_quantity || 0);
                var cost = p.cost_price != null ? p.cost_price : (p.avg_cost || 0);
                var price = p.current_price != null ? p.current_price : (p.latest_price || 0);
                var pnl = 0;
                if (price && cost) {
                    pnl = (price - cost) / cost;
                } else if (p.pnl_pct != null) {
                    pnl = p.pnl_pct / 100.0;
                }
                var pnlCls = pnl >= 0 ? 'positive' : 'negative';
                var ratio = 0;
                if (p.weight != null) {
                    ratio = p.weight / 100.0;
                } else if (totalAssets > 0 && price && qty) {
                    ratio = price * qty / totalAssets;
                }
                html += '<tr>' +
                    '<td>' + p.ts_code + '</td>' +
                    '<td>' + (p.name || '') + '</td>' +
                    '<td>' + qty + '</td>' +
                    '<td>' + (cost || 0).toFixed(2) + '</td>' +
                    '<td>' + (price || 0).toFixed(2) + '</td>' +
                    '<td class="' + pnlCls + '">' + formatPct(pnl) + '</td>' +
                    '<td>' + formatPct(ratio) + '</td>' +
                    '</tr>';
            });
            tbody.innerHTML = html;
        }
    }

    // 更新自动运行开关
    var cb = document.getElementById('pf-auto-run');
    if (cb) {
        if (d.scheduler && d.scheduler.auto_enabled !== undefined) {
            cb.checked = !!d.scheduler.auto_enabled;
        } else if (d.auto_run !== undefined) {
            cb.checked = !!d.auto_run;
        }
    }
    var watchCb = document.getElementById('pf-watch-enabled');
    if (watchCb && d.scheduler && d.scheduler.watch_enabled !== undefined) {
        watchCb.checked = !!d.scheduler.watch_enabled;
    }
    var watchSel = document.getElementById('pf-watch-interval');
    if (watchSel && d.scheduler && d.scheduler.watch_interval) {
        watchSel.value = String(d.scheduler.watch_interval);
    }
}

function renderSignals(signals) {
    var container = document.getElementById('pf-signals-list');
    if (!container) return;
    if (!signals || !Array.isArray(signals) || signals.length === 0) {
        container.innerHTML = '<div class="pf-empty">暂无交易信号</div>';
        return;
    }
    pfSignalMap = {};
    var statusText = {
        pending_risk: '待风控', pending_director: '待总监',
        approved: '已批准', rejected: '已拒绝', executed: '已执行', expired: '已过期'
    };
    var sorted = signals.slice().sort(function(a, b) {
        return signalSortValue(b) - signalSortValue(a);
    });
    var html = '';
    var lastDate = '';
    sorted.forEach(function(s) {
        pfSignalMap[String(s.id)] = s;
        var dateKey = toDateKeyFromSignal(s);
        if (dateKey !== lastDate) {
            html += '<div class="pf-date-divider">📅 ' + dateKey + '</div>';
            lastDate = dateKey;
        }
        var dirBadge = s.direction === 'buy' ? 'buy' : 'sell';
        var dirText = s.direction === 'buy' ? '买入' : '卖出';
        var stText = statusText[s.status] || s.status;
        var createdText = formatUnixTime(s.created_at);
        var signalDateText = formatYmd(s.signal_date);
        var executeDateText = formatYmd(s.execute_date);
        var execPriceText = (s.execute_price == null ? '-' : Number(s.execute_price).toFixed(3));
        var execSourceText = priceSourceText(s.execute_price_source);
        var execTradeDateText = formatYmd(s.executed_trade_date);
        html += '<div class="pf-signal-card" onclick="showSignalDetailById(' + s.id + ')">' +
            '<div class="pf-signal-header">' +
            '<span class="pf-signal-code">' + s.ts_code + (s.name ? ' ' + s.name : '') + '</span>' +
            '<span>' +
            '<span class="pf-signal-badge ' + dirBadge + '">' + dirText + '</span> ' +
            '<span class="pf-signal-badge ' + s.status + '">' + stText + '</span>' +
            '</span></div>' +
            '<div class="pf-signal-time">产生: ' + createdText +
            ' · 信号日: ' + signalDateText +
            ' · 计划执行: ' + executeDateText + '</div>' +
            (s.status === 'executed' ? (
                '<div class="pf-signal-exec">执行: ' + execTradeDateText +
                ' @' + execPriceText + ' · 来源: ' + execSourceText + '</div>'
            ) : '') +
            '<div class="pf-signal-reason" title="' + escapeHtml(s.reason || '') + '">' + escapeHtml(s.reason || '') + '</div>' +
            '<div class="pf-signal-tip">点击查看完整内容</div>';
        // 人工审核按钮（仅pending状态显示）
        if (s.status === 'pending_risk' || s.status === 'pending_director') {
            var reviewType = s.status === 'pending_risk' ? 'risk' : 'director';
            var approveText = s.status === 'pending_risk' ? '风控通过' : '总监批准';
            var rejectText = s.status === 'pending_risk' ? '反对并上报总监' : '总监否决';
            html += '<div class="pf-signal-actions">' +
                '<button class="pf-btn-approve" onclick="event.stopPropagation(); reviewSignal(' + s.id + ',true,\'' + reviewType + '\')">' + approveText + '</button>' +
                '<button class="pf-btn-reject" onclick="event.stopPropagation(); reviewSignal(' + s.id + ',false,\'' + reviewType + '\')">' + rejectText + '</button>' +
                '</div>';
        }
        html += '</div>';
    });
    container.innerHTML = html;
}

function showSignalDetailById(signalId) {
    var s = pfSignalMap[String(signalId)];
    if (!s) return;
    showSignalDetail(s);
}

function showSignalDetail(signal) {
    var modal = document.getElementById('activity-detail-modal');
    if (!modal) return;

    var statusText = {
        pending_risk: '待风控', pending_director: '待总监',
        approved: '已批准', rejected: '已拒绝', executed: '已执行', expired: '已过期'
    };
    var dirText = signal.direction === 'buy' ? '买入' : '卖出';
    var stText = statusText[signal.status] || signal.status || '-';
    var execPriceText = signal.execute_price == null ? '-' : Number(signal.execute_price).toFixed(3);
    var execSourceText = priceSourceText(signal.execute_price_source);

    document.getElementById('activity-detail-title').textContent =
        '📌 交易信号 #' + signal.id + ' · ' + (signal.ts_code || '');

    var summary = [
        '方向: ' + dirText,
        '状态: ' + stText,
        '发起人: ' + (signal.proposed_by || '-'),
        '产生时间: ' + formatUnixTime(signal.created_at),
        '信号日期: ' + formatYmd(signal.signal_date),
        '计划执行日: ' + formatYmd(signal.execute_date),
        '实际执行日: ' + formatYmd(signal.executed_trade_date),
        '执行时间: ' + formatUnixTime(signal.executed_at),
        '执行价格: ' + execPriceText,
        '价格来源: ' + execSourceText,
        '目标仓位: ' + (signal.target_ratio == null ? '-' : signal.target_ratio),
        '数量: ' + (signal.quantity == null ? '-' : signal.quantity),
        '实际数量: ' + (signal.execute_qty == null ? '-' : signal.execute_qty),
    ];

    var content = '<div style="margin-bottom:12px;font-size:12px;color:#6b7280;">' +
        summary.join(' &nbsp;|&nbsp; ') + '</div>';

    content += '<div style="margin-top:10px;">' +
        '<div style="font-size:12px;font-weight:600;color:#6b7280;margin-bottom:4px;">信号理由</div>' +
        '<pre style="white-space:pre-wrap;word-break:break-all;font-size:13px;line-height:1.6;">' +
        escapeHtml(signal.reason || '(无)') + '</pre></div>';

    if (signal.execute_message) {
        content += '<div style="margin-top:12px;padding-top:12px;border-top:1px solid #e5e7eb;">' +
            '<div style="font-size:12px;font-weight:600;color:#6b7280;margin-bottom:4px;">执行回执</div>' +
            '<pre style="white-space:pre-wrap;word-break:break-all;font-size:13px;line-height:1.6;">' +
            escapeHtml(signal.execute_message) + '</pre></div>';
    }

    if (signal.risk_review || signal.director_review) {
        content += '<div style="margin-top:12px;padding-top:12px;border-top:1px solid #e5e7eb;">' +
            '<div style="font-size:12px;font-weight:600;color:#6b7280;margin-bottom:4px;">审核意见</div>';
        if (signal.risk_review) {
            content += '<div style="margin-bottom:6px;"><b>风控审核:</b><pre style="white-space:pre-wrap;word-break:break-all;font-size:13px;line-height:1.6;">' +
                escapeHtml(signal.risk_review) + '</pre></div>';
        }
        if (signal.director_review) {
            content += '<div><b>总监审核:</b><pre style="white-space:pre-wrap;word-break:break-all;font-size:13px;line-height:1.6;">' +
                escapeHtml(signal.director_review) + '</pre></div>';
        }
        content += '</div>';
    }

    currentActivityDownload = {
        title: 'trade_signal_' + (signal.id || 'unknown') + '_' + (signal.ts_code || ''),
        content: [
            '交易信号 #' + (signal.id || '-'),
            '标的: ' + (signal.ts_code || '-'),
            '方向: ' + dirText,
            '状态: ' + stText,
            '发起人: ' + (signal.proposed_by || '-'),
            '产生时间: ' + formatUnixTime(signal.created_at),
            '信号日期: ' + formatYmd(signal.signal_date),
            '计划执行日: ' + formatYmd(signal.execute_date),
            '实际执行日: ' + formatYmd(signal.executed_trade_date),
            '执行时间: ' + formatUnixTime(signal.executed_at),
            '执行价格: ' + execPriceText,
            '价格来源: ' + execSourceText,
            '目标仓位: ' + (signal.target_ratio == null ? '-' : signal.target_ratio),
            '数量: ' + (signal.quantity == null ? '-' : signal.quantity),
            '实际数量: ' + (signal.execute_qty == null ? '-' : signal.execute_qty),
            '执行金额: ' + (signal.execute_amount == null ? '-' : signal.execute_amount),
            '执行成本: ' + (signal.execute_cost == null ? '-' : signal.execute_cost),
            '',
            '信号理由:',
            (signal.reason || '(无)'),
            '',
            '风控审核:',
            (signal.risk_review || '(无)'),
            '',
            '总监审核:',
            (signal.director_review || '(无)')
        ].join('\n')
    };

    document.getElementById('activity-detail-content').innerHTML = content;
    modal.style.display = 'flex';
}

function reviewSignal(signalId, approved, type) {
    var reviewText = '';
    if (type === 'risk') {
        reviewText = approved ? '风控通过（人工）' : '风控反对（人工，提交总监裁决）';
    } else {
        reviewText = approved ? '总监批准（人工）' : '总监否决（人工）';
    }
    fetch('/api/team/portfolio/signals/' + signalId + '/review', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({approved: approved, review_text: reviewText, type: type})
    }).then(function(r) { return r.json(); })
      .then(function() { loadPortfolioStatus(); })
      .catch(function() { alert('审核失败'); });
}

// ── 薪资面板 ──
function renderSalary(data) {
    var container = document.getElementById('pf-salary-panel');
    if (!container) return;
    var rows = Array.isArray(data && data.agents) ? data.agents : [];
    var rowMap = {};
    rows.forEach(function(item) {
        if (!item) return;
        var aid = String(item.agent_id || '').trim();
        if (!aid) return;
        rowMap[aid] = item;
    });
    if (!rows.length) {
        // 后端暂未返回工资流水时，前端先渲染完整团队卡片，避免“薪资区域消失”。
        TEAM_AGENT_ORDER.forEach(function(aid) {
            rowMap[aid] = {
                agent_id: aid,
                name: TEAM_AGENT_NAMES[aid] || aid,
                total_salary: 0,
                total_bonus: 0,
                total_penalty: 0
            };
        });
    }
    var html = '';
    TEAM_AGENT_ORDER.forEach(function(aid) {
        var a = rowMap[aid] || {
            agent_id: aid,
            name: TEAM_AGENT_NAMES[aid] || aid,
            total_salary: 0,
            total_bonus: 0,
            total_penalty: 0
        };
        var statusRow = findLatestAgentStatus(aid) || {};
        var icon = AGENT_ICONS[a.agent_id] || '🤖';
        var color = agentColors[a.agent_id] || statusRow.color || '#6b7280';
        var balance = (a.total_salary || 0) + (a.total_bonus || 0) - (a.total_penalty || 0);
        var balCls = balance >= 0 ? 'positive' : 'negative';
        html += '<div class="pf-salary-card">' +
            '<div class="pf-salary-avatar" style="background:' + color + '">' + icon + '</div>' +
            '<div class="pf-salary-name">' + (a.name || statusRow.name || TEAM_AGENT_NAMES[aid] || a.agent_id) + '</div>' +
            '<div class="pf-salary-balance ' + balCls + '">' + formatMoney(balance) + '</div>' +
            '<div class="pf-salary-detail">' +
            '工资:' + formatMoney(a.total_salary || 0) +
            ' 奖:' + formatMoney(a.total_bonus || 0) +
            ' 罚:' + formatMoney(a.total_penalty || 0) +
            '</div></div>';
    });
    container.innerHTML = html;
}

// ── 净值曲线 (ECharts) ──
function loadNavChart() {
    var params = pfNavDays > 0 ? '?days=' + pfNavDays : '';
    fetchJsonStrict('/api/team/portfolio/nav' + params, {cache: 'no-store'})
        .then(function(d) {
            clearRuntimeAlert('portfolio_nav');
            pfNavLastData = d;
            renderNavChart(d);
        })
        .catch(function(err) {
            setRuntimeAlert('portfolio_nav', '净值曲线加载失败：' + _errText(err), 'error');
            var chartDom = document.getElementById('pf-nav-chart');
            // 接口偶发失败时保留上一版曲线，避免“闪现后消失”
            if (pfNavLastData && Array.isArray(pfNavLastData) && pfNavLastData.length > 0) {
                renderNavChart(pfNavLastData);
                return;
            }
            if (chartDom) {
                if (pfNavChart) {
                    try { pfNavChart.dispose(); } catch (e) {}
                    pfNavChart = null;
                }
                chartDom.innerHTML = '<div class="pf-empty" style="padding-top:80px;">净值数据加载失败</div>';
            }
        });
}

function changeNavRange(days) {
    pfNavDays = days;
    document.querySelectorAll('.pf-range-btn').forEach(function(b) {
        b.classList.toggle('active', parseInt(b.getAttribute('data-days')) === days);
    });
    loadNavChart();
}

function renderNavChartFallback(data, chartDom) {
    if (!data || !Array.isArray(data) || data.length === 0) {
        chartDom.innerHTML = '<div class="pf-empty" style="padding-top:80px;">暂无净值数据</div>';
        return;
    }
    var width = Math.max(300, chartDom.clientWidth || 300);
    var height = 200;
    var left = 38, right = 10, top = 12, bottom = 26;
    var plotW = width - left - right;
    var plotH = height - top - bottom;

    var navs = data.map(function(d) { return Number(d.nav || 1); });
    var benches = data.map(function(d) { return Number(d.benchmark_nav || 1); });
    var allVals = navs.concat(benches).filter(function(v) { return isFinite(v); });
    if (allVals.length === 0) {
        chartDom.innerHTML = '<div class="pf-empty" style="padding-top:80px;">暂无净值数据</div>';
        return;
    }
    var minVal = Math.min.apply(null, allVals);
    var maxVal = Math.max.apply(null, allVals);
    if (maxVal - minVal < 1e-6) {
        maxVal += 0.002;
        minVal -= 0.002;
    } else {
        var pad = (maxVal - minVal) * 0.1;
        maxVal += pad;
        minVal -= pad;
    }

    function xAt(i) {
        if (data.length <= 1) return left + plotW / 2;
        return left + (plotW * i / (data.length - 1));
    }
    function yAt(v) {
        return top + (maxVal - v) * plotH / (maxVal - minVal);
    }
    function toPath(vals) {
        var p = '';
        vals.forEach(function(v, i) {
            var x = xAt(i).toFixed(2);
            var y = yAt(Number(v || 1)).toFixed(2);
            p += (i === 0 ? 'M' : 'L') + x + ' ' + y + ' ';
        });
        return p.trim();
    }

    var navPath = toPath(navs);
    var benchPath = toPath(benches);
    var firstDate = formatYmd(data[0].trade_date);
    var lastDate = formatYmd(data[data.length - 1].trade_date);

    chartDom.innerHTML =
        '<svg width="100%" height="' + height + '" viewBox="0 0 ' + width + ' ' + height + '" xmlns="http://www.w3.org/2000/svg">' +
        '<rect x="0" y="0" width="' + width + '" height="' + height + '" fill="transparent"/>' +
        '<line x1="' + left + '" y1="' + (top + plotH) + '" x2="' + (left + plotW) + '" y2="' + (top + plotH) + '" stroke="#cbd5e1" stroke-width="1"/>' +
        '<line x1="' + left + '" y1="' + top + '" x2="' + left + '" y2="' + (top + plotH) + '" stroke="#cbd5e1" stroke-width="1"/>' +
        '<path d="' + benchPath + '" fill="none" stroke="#f59e0b" stroke-width="1.8" stroke-dasharray="4 3"/>' +
        '<path d="' + navPath + '" fill="none" stroke="#2563eb" stroke-width="2.2"/>' +
        '<text x="' + left + '" y="' + (height - 6) + '" fill="#64748b" font-size="10">' + firstDate + '</text>' +
        '<text x="' + (left + plotW) + '" y="' + (height - 6) + '" text-anchor="end" fill="#64748b" font-size="10">' + lastDate + '</text>' +
        '<text x="' + left + '" y="' + (top - 2) + '" fill="#64748b" font-size="10">' + maxVal.toFixed(3) + '</text>' +
        '<text x="' + left + '" y="' + (top + plotH + 12) + '" fill="#64748b" font-size="10">' + minVal.toFixed(3) + '</text>' +
        '<text x="' + (left + 2) + '" y="' + (top + 12) + '" fill="#2563eb" font-size="10">组合净值</text>' +
        '<text x="' + (left + 64) + '" y="' + (top + 12) + '" fill="#f59e0b" font-size="10">基准净值</text>' +
        '</svg>';
}

function renderNavChart(data, retryCount) {
    retryCount = retryCount || 0;
    var chartDom = document.getElementById('pf-nav-chart');
    if (!chartDom) return;
    if (!data || !Array.isArray(data) || data.length === 0) {
        if (pfNavChart) { pfNavChart.dispose(); pfNavChart = null; }
        chartDom.innerHTML = '<div class="pf-empty" style="padding-top:80px;">暂无净值数据</div>';
        return;
    }
    var w = chartDom.clientWidth || 0;
    var h = chartDom.clientHeight || 0;
    if (w < 40 || h < 40) {
        if (retryCount < 6) {
            setTimeout(function() { renderNavChart(data, retryCount + 1); }, 120);
            return;
        }
        renderNavChartFallback(data, chartDom);
        return;
    }
    if (typeof echarts === 'undefined') {
        if (pfNavChart) { pfNavChart.dispose(); pfNavChart = null; }
        renderNavChartFallback(data, chartDom);
        return;
    }

    if (pfNavChart) {
        try {
            var dom = pfNavChart.getDom();
            if (!dom || dom !== chartDom || !chartDom.contains(dom)) {
                pfNavChart.dispose();
                pfNavChart = null;
            }
        } catch (e) {
            pfNavChart = null;
        }
    }

    if (!pfNavChart) {
        chartDom.innerHTML = '';
        pfNavChart = echarts.init(chartDom);
    }
    var dates = data.map(function(d) { return d.trade_date; });
    var navs = data.map(function(d) { return d.nav; });
    var benchmarks = data.map(function(d) { return d.benchmark_nav || 1; });

    pfNavChart.setOption({
        tooltip: {
            trigger: 'axis',
            formatter: function(params) {
                var s = params[0].axisValueLabel + '<br/>';
                params.forEach(function(p) {
                    var v = Number(p.value);
                    s += p.marker + p.seriesName + ': ' + (isFinite(v) ? v.toFixed(4) : '-') + '<br/>';
                });
                return s;
            }
        },
        legend: { data: ['组合净值', '基准净值'], top: 4, textStyle: {fontSize: 11} },
        grid: { top: 30, left: 45, right: 12, bottom: 24 },
        xAxis: { type: 'category', data: dates, axisLabel: {fontSize: 10} },
        yAxis: { type: 'value', scale: true, axisLabel: {fontSize: 10} },
        series: [
            {
                name: '组合净值', type: 'line', data: navs,
                lineStyle: {width: 2, color: '#2563eb'},
                itemStyle: {color: '#2563eb'},
                areaStyle: {color: 'rgba(37,99,235,0.08)'},
                symbol: 'none', smooth: true
            },
            {
                name: '基准净值', type: 'line', data: benchmarks,
                lineStyle: {width: 1.5, color: '#f59e0b', type: 'dashed'},
                itemStyle: {color: '#f59e0b'},
                symbol: 'none', smooth: true
            }
        ]
    });
    pfNavChart.resize();
}

// 标签页切换时加载投资数据
var _origSwitchTab = switchTab;
switchTab = function(tabName) {
    _origSwitchTab(tabName);
    if (tabName === 'status') {
        refreshTeamObservability();
    }
    if (tabName === 'prompts') {
        loadTeamModelConfigs(true);
    }
    if (tabName === 'workflow') {
        refreshWorkflowLane();
    }
    if (tabName === 'memory' || tabName === 'evolution') {
        loadMemoryCenter(false);
    }
    if (tabName === 'portfolio') {
        loadPortfolioStatus();
        setTimeout(function() {
            if (pfNavChart) {
                pfNavChart.resize();
            } else if (pfNavLastData && Array.isArray(pfNavLastData) && pfNavLastData.length > 0) {
                renderNavChart(pfNavLastData);
            } else {
                loadNavChart();
            }
        }, 160);
        setTimeout(function() {
            if (pfNavChart) pfNavChart.resize();
        }, 480);
        // 启动轮询
        if (!pfPollingTimer) {
            pfPollingTimer = setInterval(loadPortfolioStatus, 15000);
        }
    } else {
        // 停止轮询
        if (pfPollingTimer) {
            clearInterval(pfPollingTimer);
            pfPollingTimer = null;
        }
    }
};

// 窗口resize重绘图表
window.addEventListener('resize', function() {
    if (pfNavChart) {
        pfNavChart.resize();
    } else if (pfNavLastData && Array.isArray(pfNavLastData) && pfNavLastData.length > 0) {
        renderNavChart(pfNavLastData);
    }
});

// ──────────────── 工具函数 ────────────────
function escapeHtml(text) {
    var div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
