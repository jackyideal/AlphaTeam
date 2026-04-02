/**
 * AlphaFin 3D Office Scene - Full office with walking agents
 * Uses Three.js (ES module) + CSS2DRenderer
 * White theme · Fintech office · Differentiated characters
 */
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { CSS2DRenderer, CSS2DObject } from 'three/addons/renderers/CSS2DRenderer.js';

// ════════════════ External API (registered early as fallback) ════════════════
window.updateSceneAgents = function() {};
window.onAgentActivity = function() {};

// ════════════════ Constants ════════════════

var AGENT_META = {
    director:  { name: '决策总监', color: '#e74c3c', icon: '👑', deskPos: null },
    analyst:   { name: '投资分析师', color: '#3498db', icon: '📊', deskPos: new THREE.Vector3(-5, 0, -2) },
    risk:      { name: '风控官', color: '#e67e22', icon: '🛡️', deskPos: new THREE.Vector3(-2.5, 0, -2) },
    intel:     { name: '市场情报员', color: '#2ecc71', icon: '🌐', deskPos: new THREE.Vector3(-5, 0, -5) },
    quant:     { name: '量化策略师', color: '#9b59b6', icon: '📈', deskPos: new THREE.Vector3(-2.5, 0, -5) },
    restructuring: { name: '资产重组专家', color: '#0ea5a4', icon: '🏗️', deskPos: new THREE.Vector3(-7.5, 0, -3.5) },
    auditor:   { name: '反思审计员', color: '#1abc9c', icon: '🔍', deskPos: new THREE.Vector3(6, 0, 3) },
};

var ZONE = {
    meetingTable: new THREE.Vector3(4.5, 0, -3.5),
    toolStation:  new THREE.Vector3(-8, 0, 5.5),
    breakRoom:    new THREE.Vector3(1, 0, 3.5),
    pantry:       new THREE.Vector3(7, 0, -6.5),
};

var MEETING_SEATS = [
    new THREE.Vector3(4.5, 0, -2),
    new THREE.Vector3(6, 0, -3),
    new THREE.Vector3(6, 0, -4.2),
    new THREE.Vector3(4.5, 0, -5),
    new THREE.Vector3(3, 0, -4.2),
    new THREE.Vector3(3, 0, -3),
    new THREE.Vector3(5.4, 0, -2.2),
];

// Fallback positions (used when desk position is missing)
var BREAK_POSITIONS = [
    new THREE.Vector3(-4, 0, -0.5),    // director: near center corridor
    new THREE.Vector3(-6, 0, -3.5),    // analyst: near own desk
    new THREE.Vector3(1, 0, 4.0),      // risk: break room sofa
    new THREE.Vector3(-3, 0, 3.0),     // intel: near tool station
    new THREE.Vector3(7, 0, -5.5),     // quant: near pantry
    new THREE.Vector3(-7.0, 0, -3.0),  // restructuring: near event desk
    new THREE.Vector3(7, 0, 1.5),      // auditor: near own desk
];

// Per-tool position map: each tool has its own 3D location
var TOOL_POSITION_MAP = {
    // ── 数据终端区 (Data Terminals, left wall) ──
    get_kline:             new THREE.Vector3(-7.5, 0, 3.5),
    get_financials:        new THREE.Vector3(-7.5, 0, 4.5),
    get_chip_distribution: new THREE.Vector3(-7.5, 0, 5.5),
    query_database:        new THREE.Vector3(-7.0, 0, 6.5),
    // ── 情报站 (Intelligence, bottom-left) ──
    get_stock_news:        new THREE.Vector3(-5.5, 0, 7.5),
    get_sector_report:     new THREE.Vector3(-4.0, 0, 7.5),
    web_search:            new THREE.Vector3(-2.5, 0, 7.5),
    // ── 分析引擎 (Analysis, bottom-center) ──
    run_indicator:         new THREE.Vector3(-0.5, 0, 7.5),
    save_knowledge:        new THREE.Vector3(1.0, 0, 7.5),
    // ── 协作 (Collaboration, meeting table area) ──
    send_message_to_agent: new THREE.Vector3(3.0, 0, -3.0),
    // ── Skill 工坊 (right wall) ──
    create_skill:          new THREE.Vector3(7.5, 0, 6.0),
    execute_skill:         new THREE.Vector3(7.5, 0, 7.0),
    list_skills:           new THREE.Vector3(7.0, 0, 6.5),
    // ── 投资决策台 (Investment Desk, right side) ──
    submit_trade_signal:   new THREE.Vector3(5.0, 0, 1.5),
    review_trade_signal:   new THREE.Vector3(5.0, 0, 2.5),
    get_portfolio_status:  new THREE.Vector3(6.5, 0, 1.5),
    flag_risk_warning:     new THREE.Vector3(6.5, 0, 2.5),
};

// Fallback positions for unknown tools
var TOOL_FALLBACK_POSITIONS = [
    new THREE.Vector3(-7.5, 0, 3.5),
    new THREE.Vector3(-7.5, 0, 4.5),
    new THREE.Vector3(-7.5, 0, 5.5),
    new THREE.Vector3(-7.0, 0, 6.5),
    new THREE.Vector3(-7.5, 0, 7.0),
    new THREE.Vector3(-7.0, 0, 5.5),
];

var AGENT_ORDER = ['director', 'analyst', 'risk', 'intel', 'quant', 'restructuring', 'auditor'];

var STATUS_COLORS = {
    idle: 0x9ca3af,
    thinking: 0xf59e0b,
    using_tool: 0x3b82f6,
    speaking: 0x10b981,
    offline: 0x4b5563,
};

var MEETING_PHASE_LABEL = {
    meeting_plan: '会议规划',
    meeting_start: '会议启动',
    meeting_order: '发言顺序',
    meeting_round: '轮次讨论',
    meeting_turn: '当前发言',
    meeting_decision: '轮次决策',
    meeting_result: '结果通知',
    meeting: '成员发言',
    meeting_summary: '会议共识',
    meeting_end: '会议结束'
};

var MEETING_SOURCE_LABEL = {
    director: '总监发起',
    agents: '成员协作触发',
    'director+agents': '总监+成员触发',
    none: '未触发会议'
};

// ════════════════ Globals ════════════════

var renderer, labelRenderer, scene, camera, controls, clock;
var agents = {};
var commLines = [];
var M = {};  // Shared materials
var DEFAULT_CAMERA_POS = new THREE.Vector3(0, 14, 14);
var DEFAULT_CAMERA_TARGET = new THREE.Vector3(0, 0, -1);
var scenePaused = false;
var sceneActiveFrameIntervalMs = 1000 / 16;
var sceneIdleFrameIntervalMs = 1000 / 3;
var sceneLastFrameTs = 0;
var sceneBurstUntilTs = 0;
var meetingState = { active: false, participants: [], seatMap: {} };
var meetingPanelState = {
    active: false,
    phase: '',
    topic: '',
    reason: '',
    decision: '',
    source: '',
    nextFocus: '',
    round: 0,
    roundTotal: 0,
    order: [],
    speaker: '',
    speakerSeq: 0,
    speakerTotal: 0,
    participants: [],
    updatedAt: 0
};

function escapeHtmlLite(text) {
    var div = document.createElement('div');
    div.textContent = String(text == null ? '' : text);
    return div.innerHTML;
}

function normalizeMeetingParticipants(value) {
    if (Array.isArray(value)) {
        return value.map(function(v) { return String(v || '').trim(); }).filter(Boolean);
    }
    if (typeof value === 'string') {
        return value.split(/[,\s]+/).map(function(v) { return String(v || '').trim(); }).filter(Boolean);
    }
    return [];
}

function parseReasonFromContent(text) {
    var raw = String(text || '');
    var zh = raw.match(/（([^（）]+)）/);
    if (zh && zh[1]) return zh[1].trim();
    var en = raw.match(/\(([^()]+)\)/);
    if (en && en[1]) return en[1].trim();
    return '';
}

function isMeetingPlanTriggered(msg, meta) {
    var source = String((meta && meta.meeting_source) || '').trim();
    if (source === 'none') return false;
    var text = String((msg && msg.content) || '');
    if (text.indexOf('不召开会议') >= 0 || text.indexOf('本轮不召开') >= 0 || text.indexOf('无需额外会议') >= 0) {
        return false;
    }
    if (text.indexOf('会议触发') >= 0 || text.indexOf('召开会议') >= 0) {
        return true;
    }
    return !!source;
}

function buildMeetingDecisionText(msg, meta, phase) {
    if (phase === 'meeting_plan') {
        return isMeetingPlanTriggered(msg, meta) ? '触发会议讨论' : '本轮不召开会议';
    }
    if (phase === 'meeting_decision') {
        return meta && meta.meeting_active ? '继续下一轮讨论' : '本轮决定结束';
    }
    if (phase === 'meeting_result') return '会议结果已发布';
    if (phase === 'meeting_start') return '团队进入会议室';
    if (phase === 'meeting_order') return '已明确会议发言顺序';
    if (phase === 'meeting_turn') return '按顺序逐位发言';
    if (phase === 'meeting_round') {
        var roundNo = Number((meta && meta.meeting_round) || 0);
        return roundNo > 0 ? ('会议进行中（第' + roundNo + '轮）') : '会议进行中';
    }
    if (phase === 'meeting_summary') return '形成会议共识';
    if (phase === 'meeting_end') return '会议结束';
    return '';
}

function formatMeetingUpdateTime(ts) {
    var n = Number(ts || 0);
    if (!isFinite(n) || n <= 0) return '';
    var d = n > 1e12 ? new Date(n) : new Date(n * 1000);
    if (isNaN(d.getTime())) return '';
    return d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function renderSceneMeetingPanel() {
    var badge = document.getElementById('scene-meeting-badge');
    var content = document.getElementById('scene-meeting-content');
    if (!badge || !content) return;

    badge.className = 'scene-meeting-badge ' + (meetingPanelState.active ? 'active' : 'idle');
    badge.textContent = meetingPanelState.active ? '开会中' : '待机';

    var rows = [];
    var phaseText = MEETING_PHASE_LABEL[meetingPanelState.phase] || meetingPanelState.phase;
    var sourceText = MEETING_SOURCE_LABEL[meetingPanelState.source] || meetingPanelState.source;
    if (phaseText) {
        rows.push('<div class="scene-meeting-row"><span class="scene-meeting-key">阶段</span><span class="scene-meeting-val">' +
            escapeHtmlLite(phaseText) + '</span></div>');
    }
    if (meetingPanelState.decision) {
        rows.push('<div class="scene-meeting-row"><span class="scene-meeting-key">决策</span><span class="scene-meeting-val scene-meeting-decision">' +
            escapeHtmlLite(meetingPanelState.decision) + '</span></div>');
    }
    if (meetingPanelState.topic) {
        rows.push('<div class="scene-meeting-row"><span class="scene-meeting-key">主题</span><span class="scene-meeting-val">' +
            escapeHtmlLite(meetingPanelState.topic) + '</span></div>');
    }
    if (meetingPanelState.reason) {
        rows.push('<div class="scene-meeting-row"><span class="scene-meeting-key">理由</span><span class="scene-meeting-val">' +
            escapeHtmlLite(meetingPanelState.reason) + '</span></div>');
    }
    if (meetingPanelState.nextFocus) {
        rows.push('<div class="scene-meeting-row"><span class="scene-meeting-key">聚焦</span><span class="scene-meeting-val">' +
            escapeHtmlLite(meetingPanelState.nextFocus) + '</span></div>');
    }
    if (meetingPanelState.round > 0) {
        var roundText = '第' + meetingPanelState.round + '轮';
        if (meetingPanelState.roundTotal > 0) roundText += '/' + meetingPanelState.roundTotal;
        rows.push('<div class="scene-meeting-row"><span class="scene-meeting-key">轮次</span><span class="scene-meeting-val">' +
            escapeHtmlLite(roundText) + '</span></div>');
    } else if (meetingPanelState.roundTotal > 0) {
        rows.push('<div class="scene-meeting-row"><span class="scene-meeting-key">总轮次</span><span class="scene-meeting-val">' +
            escapeHtmlLite(String(meetingPanelState.roundTotal)) + '</span></div>');
    }
    if (meetingPanelState.order && meetingPanelState.order.length > 0) {
        rows.push('<div class="scene-meeting-row"><span class="scene-meeting-key">顺序</span><span class="scene-meeting-val">' +
            escapeHtmlLite(meetingPanelState.order.join(' -> ')) + '</span></div>');
    }
    if (meetingPanelState.speaker) {
        var speakerText = meetingPanelState.speaker;
        if (meetingPanelState.speakerSeq > 0 && meetingPanelState.speakerTotal > 0) {
            speakerText += ' (' + meetingPanelState.speakerSeq + '/' + meetingPanelState.speakerTotal + ')';
        }
        rows.push('<div class="scene-meeting-row"><span class="scene-meeting-key">发言人</span><span class="scene-meeting-val">' +
            escapeHtmlLite(speakerText) + '</span></div>');
    }
    if (sourceText) {
        rows.push('<div class="scene-meeting-row"><span class="scene-meeting-key">触发源</span><span class="scene-meeting-val">' +
            escapeHtmlLite(sourceText) + '</span></div>');
    }

    var membersHtml = '';
    if (meetingPanelState.participants && meetingPanelState.participants.length > 0) {
        membersHtml = '<div class="scene-meeting-members">' + meetingPanelState.participants.map(function(id) {
            var name = AGENT_META[id] ? AGENT_META[id].name : id;
            return '<span class="scene-meeting-member">' + escapeHtmlLite(name) + '</span>';
        }).join('') + '</div>';
    }

    var updateText = formatMeetingUpdateTime(meetingPanelState.updatedAt);
    var footerHtml = updateText
        ? '<div class="scene-meeting-updated">更新: ' + escapeHtmlLite(updateText) + '</div>'
        : '';

    if (!rows.length && !membersHtml) {
        content.innerHTML = '<div class="scene-meeting-empty">等待会议触发...</div>';
        return;
    }
    content.innerHTML = rows.join('') + membersHtml + footerHtml;
}

function updateMeetingPanelFromActivity(msg) {
    if (!msg) return;
    var meta = msg.metadata || {};
    var phase = String(meta.phase || '').trim();
    var hasMeetingHint = (
        (phase && phase.indexOf('meeting') === 0) ||
        typeof meta.meeting_active === 'boolean' ||
        meta.meeting_topic || meta.meeting_round ||
        meta.meeting_source || meta.meeting_reason || meta.next_focus
    );
    if (!hasMeetingHint) return;

    if (phase) meetingPanelState.phase = phase;
    if (typeof meta.meeting_active === 'boolean') {
        meetingPanelState.active = !!meta.meeting_active;
    }
    if (meta.meeting_topic) {
        meetingPanelState.topic = String(meta.meeting_topic || '').trim();
    }
    if (Object.prototype.hasOwnProperty.call(meta, 'meeting_round')) {
        meetingPanelState.round = Number(meta.meeting_round || 0) || 0;
    }
    if (Object.prototype.hasOwnProperty.call(meta, 'meeting_round_total')) {
        meetingPanelState.roundTotal = Number(meta.meeting_round_total || 0) || 0;
    }

    var order = normalizeMeetingParticipants(meta.meeting_order);
    if (order.length > 0) {
        meetingPanelState.order = order;
    } else if (!meetingPanelState.active && (phase === 'meeting_end' || phase === 'meeting_summary')) {
        meetingPanelState.order = [];
    }

    var participants = normalizeMeetingParticipants(meta.participants);
    if (participants.length > 0) {
        meetingPanelState.participants = participants;
    } else if (!meetingPanelState.active && (phase === 'meeting_end' || phase === 'meeting_summary')) {
        meetingPanelState.participants = [];
    }

    var reason = String(meta.meeting_reason || meta.reason || '').trim();
    if (!reason) reason = parseReasonFromContent(msg.content);
    if (reason) {
        meetingPanelState.reason = reason;
    } else if (phase === 'meeting_start' || phase === 'meeting_end') {
        meetingPanelState.reason = '';
    }

    if (Object.prototype.hasOwnProperty.call(meta, 'next_focus')) {
        meetingPanelState.nextFocus = String(meta.next_focus || '').trim();
    }

    if (Object.prototype.hasOwnProperty.call(meta, 'meeting_source')) {
        meetingPanelState.source = String(meta.meeting_source || '').trim();
    }

    if (Object.prototype.hasOwnProperty.call(meta, 'meeting_speaker')) {
        meetingPanelState.speaker = String(meta.meeting_speaker || '').trim();
    } else if (phase === 'meeting_round' || phase === 'meeting_start' || phase === 'meeting_end' || phase === 'meeting_summary') {
        meetingPanelState.speaker = '';
    }
    if (Object.prototype.hasOwnProperty.call(meta, 'meeting_speaker_seq')) {
        meetingPanelState.speakerSeq = Number(meta.meeting_speaker_seq || 0) || 0;
    } else if (!meetingPanelState.speaker) {
        meetingPanelState.speakerSeq = 0;
    }
    if (Object.prototype.hasOwnProperty.call(meta, 'meeting_speaker_total')) {
        meetingPanelState.speakerTotal = Number(meta.meeting_speaker_total || 0) || 0;
    } else if (!meetingPanelState.speaker) {
        meetingPanelState.speakerTotal = 0;
    }

    var decision = buildMeetingDecisionText(msg, meta, phase);
    if (decision) {
        meetingPanelState.decision = decision;
    } else if (phase === 'meeting') {
        meetingPanelState.decision = '会议成员发言中';
    } else if (phase === 'meeting_turn') {
        meetingPanelState.decision = '按顺序发言中';
    }

    var ts = Number(msg.timestamp || 0);
    if (isFinite(ts) && ts > 0) {
        meetingPanelState.updatedAt = ts;
    }
    renderSceneMeetingPanel();
}

function getDeskSeatPosition(agentId, idx, zOffset) {
    var meta = AGENT_META[agentId] || {};
    var deskPos = meta.deskPos;
    var offset = typeof zOffset === 'number' ? zOffset : 0.7;
    if (deskPos) {
        return new THREE.Vector3(deskPos.x, 0, deskPos.z + offset);
    }
    return BREAK_POSITIONS[idx % BREAK_POSITIONS.length].clone();
}

// ════════════════ Error Display ════════════════

function showError(msg) {
    var el = document.getElementById('scene-loading');
    if (el) {
        var txt = el.querySelector('.scene-loading-text');
        if (txt) txt.textContent = '3D 场景加载失败: ' + msg;
        el.style.color = '#ef4444';
    }
    console.error('[3D Scene] ' + msg);
}

function setCameraDistance(distance) {
    if (!camera || !controls) return;
    var minDist = Number(controls.minDistance || 2);
    var maxDist = Number(controls.maxDistance || 60);
    var clamped = Math.max(minDist, Math.min(maxDist, Number(distance || minDist)));
    var dir = new THREE.Vector3().subVectors(camera.position, controls.target);
    if (dir.lengthSq() < 1e-8) dir.set(0, 0.6, 1.0);
    dir.normalize();
    camera.position.copy(controls.target).addScaledVector(dir, clamped);
    controls.update();
}

function zoomSceneByFactor(factor) {
    if (!camera || !controls) return;
    var f = Number(factor || 1);
    if (!isFinite(f) || f <= 0) return;
    var currentDist = camera.position.distanceTo(controls.target);
    setCameraDistance(currentDist * f);
}

function resetSceneView() {
    if (!camera || !controls) return;
    camera.position.copy(DEFAULT_CAMERA_POS);
    controls.target.copy(DEFAULT_CAMERA_TARGET);
    controls.update();
}

function setScenePaused(paused) {
    scenePaused = !!paused;
    if (!scenePaused) {
        sceneLastFrameTs = 0;
        bumpSceneActivity(4000);
    }
}

window.setTeamScenePaused = function(paused) {
    setScenePaused(paused);
};

function syncScenePausedByVisibility() {
    setScenePaused(!!document.hidden);
}

function bumpSceneActivity(durationMs) {
    var d = Number(durationMs || 3000);
    if (!isFinite(d) || d <= 0) d = 3000;
    var until = performance.now() + d;
    if (until > sceneBurstUntilTs) {
        sceneBurstUntilTs = until;
    }
}

function bindSceneZoomControls() {
    var btnIn = document.getElementById('scene-zoom-in');
    var btnOut = document.getElementById('scene-zoom-out');
    var btnReset = document.getElementById('scene-zoom-reset');

    if (btnIn) {
        btnIn.addEventListener('click', function(e) {
            e.preventDefault();
            zoomSceneByFactor(0.85);
        });
    }
    if (btnOut) {
        btnOut.addEventListener('click', function(e) {
            e.preventDefault();
            zoomSceneByFactor(1.18);
        });
    }
    if (btnReset) {
        btnReset.addEventListener('click', function(e) {
            e.preventDefault();
            resetSceneView();
        });
    }
}

// ════════════════ Init ════════════════

function init() {
    var canvas = document.getElementById('team-canvas');
    var container = document.getElementById('scene-container');
    if (!canvas || !container) { showError('找不到画布元素'); return; }

    try {
        renderer = new THREE.WebGLRenderer({
            canvas: canvas,
            antialias: false,
            alpha: false,
            powerPreference: 'low-power'
        });
        renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.2));
        renderer.shadowMap.enabled = false;
        renderer.shadowMap.type = THREE.PCFSoftShadowMap;
        renderer.toneMapping = THREE.ACESFilmicToneMapping;
        renderer.toneMappingExposure = 1.2;

        labelRenderer = new CSS2DRenderer();
        labelRenderer.setSize(container.clientWidth, container.clientHeight);
        labelRenderer.domElement.style.position = 'absolute';
        labelRenderer.domElement.style.top = '0';
        labelRenderer.domElement.style.left = '0';
        labelRenderer.domElement.style.pointerEvents = 'none';
        container.appendChild(labelRenderer.domElement);

        scene = new THREE.Scene();
        scene.background = new THREE.Color(0xf0f4f8);
        scene.fog = new THREE.Fog(0xf0f4f8, 22, 40);

        camera = new THREE.PerspectiveCamera(50, container.clientWidth / container.clientHeight, 0.1, 100);
        camera.position.copy(DEFAULT_CAMERA_POS);
        camera.lookAt(DEFAULT_CAMERA_TARGET);

        controls = new OrbitControls(camera, renderer.domElement);
        controls.enableDamping = true;
        controls.dampingFactor = 0.08;
        controls.maxPolarAngle = Math.PI * 0.45;
        controls.minDistance = 5;
        controls.maxDistance = 30;
        controls.target.copy(DEFAULT_CAMERA_TARGET);
        controls.addEventListener('start', function() { bumpSceneActivity(5000); });
        controls.addEventListener('change', function() { bumpSceneActivity(1800); });
        controls.addEventListener('end', function() { bumpSceneActivity(2400); });
        canvas.addEventListener('wheel', function() { bumpSceneActivity(2000); }, { passive: true });
        canvas.addEventListener('pointerdown', function() { bumpSceneActivity(3500); });
        canvas.addEventListener('touchstart', function() { bumpSceneActivity(3500); }, { passive: true });

        clock = new THREE.Clock();

        initMaterials();
        buildLighting();
        buildFloor();
        buildWalls();
        buildCeiling();
        buildWorkstations();
        buildDirectorDesk();
        buildMeetingRoom();
        buildToolStation();
        buildBreakRoom();
        buildPantry();
        buildDecorations();
        initAgents();
        renderSceneMeetingPanel();

        onResize();
        window.addEventListener('resize', onResize);
        new ResizeObserver(onResize).observe(container);
        document.addEventListener('visibilitychange', syncScenePausedByVisibility);
        window.addEventListener('blur', syncScenePausedByVisibility);
        window.addEventListener('focus', syncScenePausedByVisibility);
        setupClickInteraction(canvas);
        bindSceneZoomControls();
        registerExternalAPI();
        syncScenePausedByVisibility();
        bumpSceneActivity(6000);
        animate();

        var loadingEl = document.getElementById('scene-loading');
        if (loadingEl) loadingEl.style.display = 'none';
    } catch (e) {
        showError(e.message || String(e));
    }
}

// ════════════════ Materials ════════════════

function initMaterials() {
    M.floor = new THREE.MeshStandardMaterial({ color: 0xe8ecf1, roughness: 0.8 });
    M.floorAccent = new THREE.MeshStandardMaterial({ color: 0xd1d8e0, roughness: 0.7 });
    M.wall = new THREE.MeshStandardMaterial({ color: 0xffffff, roughness: 0.9 });
    M.wallAccent = new THREE.MeshStandardMaterial({ color: 0xf5f5f5, roughness: 0.85 });
    M.deskTop = new THREE.MeshStandardMaterial({ color: 0xc4a882, roughness: 0.5 });
    M.chair = new THREE.MeshStandardMaterial({ color: 0x4a4a55, roughness: 0.7 });
    M.screen = new THREE.MeshStandardMaterial({ color: 0x1a1a2e, roughness: 0.3, metalness: 0.5 });
    M.glass = new THREE.MeshStandardMaterial({ color: 0xaaccee, transparent: true, opacity: 0.12, roughness: 0.1, metalness: 0.3 });
    M.sofa = new THREE.MeshStandardMaterial({ color: 0x6b7b9e, roughness: 0.8 });
    M.plant = new THREE.MeshStandardMaterial({ color: 0x4caf50, roughness: 0.9 });
    M.pot = new THREE.MeshStandardMaterial({ color: 0xb8926a, roughness: 0.8 });
    M.metal = new THREE.MeshStandardMaterial({ color: 0xaaaabb, roughness: 0.4, metalness: 0.6 });
    M.skin = new THREE.MeshStandardMaterial({ color: 0xf5d0a9, roughness: 0.8 });
    M.terminal = new THREE.MeshStandardMaterial({ color: 0x3a3a45, roughness: 0.5, metalness: 0.4 });
    M.window = new THREE.MeshStandardMaterial({ color: 0x87ceeb, emissive: 0x87ceeb, emissiveIntensity: 0.3, transparent: true, opacity: 0.7 });
    M.ceiling = new THREE.MeshStandardMaterial({ color: 0xfafafa, roughness: 0.9 });
    M.shoe = new THREE.MeshStandardMaterial({ color: 0x333333, roughness: 0.8 });
    M.baseboard = new THREE.MeshStandardMaterial({ color: 0xd0d5dd, roughness: 0.7 });
    M.wood = new THREE.MeshStandardMaterial({ color: 0xa0845c, roughness: 0.6 });
    M.whiteboard = new THREE.MeshStandardMaterial({ color: 0xf8f9fa, roughness: 0.2, metalness: 0.1 });
    M.paper = new THREE.MeshStandardMaterial({ color: 0xfdfde8, roughness: 0.9 });
    M.ceramic = new THREE.MeshStandardMaterial({ color: 0xffffff, roughness: 0.4, metalness: 0.1 });
    M.frame = new THREE.MeshStandardMaterial({ color: 0x78716c, roughness: 0.5, metalness: 0.3 });
    // Server/rack: light silver-gray, NOT black
    M.rackBody = new THREE.MeshStandardMaterial({ color: 0xc8cdd3, roughness: 0.5, metalness: 0.4 });
    M.rackFront = new THREE.MeshStandardMaterial({ color: 0xb0b8c1, roughness: 0.4, metalness: 0.5 });
}

// ════════════════ Lighting ════════════════

function buildLighting() {
    scene.add(new THREE.AmbientLight(0xffffff, 0.8));

    var mainLight = new THREE.DirectionalLight(0xffffff, 1.0);
    mainLight.position.set(5, 12, 5);
    mainLight.castShadow = true;
    mainLight.shadow.mapSize.set(1024, 1024);
    mainLight.shadow.camera.left = -15;
    mainLight.shadow.camera.right = 15;
    mainLight.shadow.camera.top = 15;
    mainLight.shadow.camera.bottom = -15;
    scene.add(mainLight);

    var fillLight = new THREE.DirectionalLight(0xfff5e6, 0.3);
    fillLight.position.set(-5, 8, -5);
    scene.add(fillLight);
}

// ════════════════ Floor ════════════════

function buildFloor() {
    var floor = new THREE.Mesh(new THREE.PlaneGeometry(22, 18), M.floor);
    floor.rotation.x = -Math.PI / 2;
    floor.position.set(0, -0.01, -1);
    floor.receiveShadow = true;
    scene.add(floor);

    var grid = new THREE.GridHelper(22, 22, 0xd0d5dd, 0xe0e5eb);
    grid.position.set(0, 0.005, -1);
    scene.add(grid);

    // Corridor accent strips
    var strip1 = new THREE.Mesh(new THREE.PlaneGeometry(22, 0.4), M.floorAccent);
    strip1.rotation.x = -Math.PI / 2;
    strip1.position.set(0, 0.001, 1);
    scene.add(strip1);

    var strip2 = new THREE.Mesh(new THREE.PlaneGeometry(0.4, 14), M.floorAccent);
    strip2.rotation.x = -Math.PI / 2;
    strip2.position.set(0.5, 0.001, -1);
    scene.add(strip2);

    // Meeting room floor circle
    var meetFloor = new THREE.Mesh(new THREE.CircleGeometry(2.8, 48),
        new THREE.MeshStandardMaterial({ color: 0xd4dbe5, roughness: 0.7 }));
    meetFloor.rotation.x = -Math.PI / 2;
    meetFloor.position.set(ZONE.meetingTable.x, 0.01, ZONE.meetingTable.z);
    scene.add(meetFloor);

    // Workspace carpet
    var workCarpet = new THREE.Mesh(new THREE.PlaneGeometry(5, 5.5),
        new THREE.MeshStandardMaterial({ color: 0xd6dde6, roughness: 0.9 }));
    workCarpet.rotation.x = -Math.PI / 2;
    workCarpet.position.set(-3.75, 0.008, -3.5);
    scene.add(workCarpet);

    // Break room rug
    var rug = new THREE.Mesh(new THREE.PlaneGeometry(3.5, 2.8),
        new THREE.MeshStandardMaterial({ color: 0xb8c5d6, roughness: 0.95 }));
    rug.rotation.x = -Math.PI / 2;
    rug.position.set(ZONE.breakRoom.x, 0.008, 4.0);
    scene.add(rug);
}

// ════════════════ Walls ════════════════

function buildWalls() {
    var backWall = new THREE.Mesh(new THREE.BoxGeometry(22, 5, 0.2), M.wall);
    backWall.position.set(0, 2.5, -8);
    backWall.receiveShadow = true;
    scene.add(backWall);

    var leftWall = new THREE.Mesh(new THREE.BoxGeometry(0.2, 5, 18), M.wallAccent);
    leftWall.position.set(-9, 2.5, -1);
    scene.add(leftWall);

    var rightWall = new THREE.Mesh(new THREE.BoxGeometry(0.2, 5, 18), M.wallAccent);
    rightWall.position.set(9, 2.5, -1);
    scene.add(rightWall);

    // Baseboards
    scene.add(makeMesh(new THREE.BoxGeometry(22, 0.12, 0.04), M.baseboard, [0, 0.06, -7.88]));
    scene.add(makeMesh(new THREE.BoxGeometry(0.04, 0.12, 18), M.baseboard, [-8.88, 0.06, -1]));
    scene.add(makeMesh(new THREE.BoxGeometry(0.04, 0.12, 18), M.baseboard, [8.88, 0.06, -1]));

    // Windows → Finance data display panels
    var panelConfigs = [
        { x: -5, title: '📊 K线走势', bgColor: 0x0f172a }, // left: candlestick chart
        { x: 0, title: '📈 市场指数', bgColor: 0x0c1e3a },  // center: market indices
        { x: 5, title: '🔢 量化因子', bgColor: 0x111827 },  // right: quant factors
    ];
    panelConfigs.forEach(function(cfg) {
        // Sleek screen frame
        scene.add(makeMesh(new THREE.BoxGeometry(3.2, 2.7, 0.05), M.metal, [cfg.x, 3, -7.88]));
        // Dark screen background
        var screenBg = makeMesh(new THREE.PlaneGeometry(2.9, 2.4),
            new THREE.MeshStandardMaterial({ color: cfg.bgColor, roughness: 0.2 }), [cfg.x, 3, -7.86]);
        scene.add(screenBg);

        // Subtle grid lines on screen (horizontal)
        var gridMat = new THREE.MeshStandardMaterial({ color: 0x1e3a5f, emissive: 0x1e3a5f, emissiveIntensity: 0.15 });
        for (var gl = 0; gl < 5; gl++) {
            var gridLine = makeMesh(new THREE.PlaneGeometry(0.008, 2.7), gridMat,
                [cfg.x, 2.1 + gl * 0.45, -7.855]);
            scene.add(gridLine);
        }
        // Vertical grid lines
        for (var vl = 0; vl < 6; vl++) {
            var vLine = makeMesh(new THREE.PlaneGeometry(2.2, 0.005), gridMat,
                [cfg.x - 1.2 + vl * 0.5, 3, -7.855]);
            scene.add(vLine);
        }

        // Window sill
        scene.add(makeMesh(new THREE.BoxGeometry(3.3, 0.05, 0.18), M.metal, [cfg.x, 1.65, -7.78]));
        // Ambient glow from screens
        var glow = new THREE.PointLight(0x3b82f6, 0.12, 5);
        glow.position.set(cfg.x, 3, -7.0);
        scene.add(glow);
    });

    // ── Left panel: K-line candlestick bars ──
    var candleColors = [0x22c55e, 0xef4444, 0x22c55e, 0xef4444, 0x22c55e, 0x22c55e, 0xef4444, 0x22c55e, 0xef4444, 0x22c55e];
    var candleHeights = [0.6, 0.9, 0.45, 1.1, 0.7, 0.55, 0.85, 0.5, 0.75, 0.65];
    candleColors.forEach(function(cc, ci) {
        var ch = candleHeights[ci];
        var cx = -6.15 + ci * 0.25;
        // Candle body
        scene.add(makeMesh(new THREE.BoxGeometry(0.12, ch, 0.01),
            new THREE.MeshStandardMaterial({ color: cc, emissive: cc, emissiveIntensity: 0.4 }),
            [cx, 2.2 + ch / 2, -7.85]));
        // Wick
        scene.add(makeMesh(new THREE.BoxGeometry(0.02, ch * 0.4, 0.01),
            new THREE.MeshStandardMaterial({ color: cc, emissive: cc, emissiveIntensity: 0.2 }),
            [cx, 2.2 + ch + ch * 0.15, -7.85]));
    });
    // Trend line across candles
    var trendMat = new THREE.MeshStandardMaterial({ color: 0xfbbf24, emissive: 0xfbbf24, emissiveIntensity: 0.5 });
    var trendLine = makeMesh(new THREE.BoxGeometry(2.5, 0.02, 0.01), trendMat, [-5, 2.9, -7.845]);
    trendLine.rotation.z = 0.15;
    scene.add(trendLine);
    addLabel('📊 K线走势', [-5, 4.45, -7.7], '#60a5fa');

    // ── Center panel: market index lines ──
    var indexColors = [0x3b82f6, 0x10b981, 0xf59e0b];
    var indexLabels = ['上证', '深证', '创业板'];
    indexColors.forEach(function(ic, ii) {
        var iMat = new THREE.MeshStandardMaterial({ color: ic, emissive: ic, emissiveIntensity: 0.5 });
        var baseY = 2.4 + ii * 0.55;
        // Simulated line chart segments
        var prevX = -1.2;
        var prevY = baseY;
        for (var seg = 0; seg < 8; seg++) {
            var nextX = prevX + 0.3;
            var nextY = baseY + (Math.sin(seg * 1.3 + ii * 2) * 0.15);
            var segLen = Math.sqrt(Math.pow(nextX - prevX, 2) + Math.pow(nextY - prevY, 2));
            var segAngle = Math.atan2(nextY - prevY, nextX - prevX);
            var segMesh = makeMesh(new THREE.BoxGeometry(segLen, 0.025, 0.01), iMat,
                [(prevX + nextX) / 2, (prevY + nextY) / 2, -7.845]);
            segMesh.rotation.z = segAngle;
            scene.add(segMesh);
            prevX = nextX;
            prevY = nextY;
        }
    });
    // Index value labels
    addLabel('📈 市场指数', [0, 4.45, -7.7], '#60a5fa');

    // ── Right panel: quant factor bars + pie chart ──
    // Factor horizontal bars
    var factorNames = ['动量', 'PE', '波动', 'ROE', '市值'];
    var factorWidths = [1.8, 1.3, 0.9, 1.6, 1.1];
    var factorColors = [0x6366f1, 0x8b5cf6, 0xa78bfa, 0xc4b5fd, 0xddd6fe];
    factorNames.forEach(function(fn, fi) {
        var fw = factorWidths[fi];
        var fy = 3.8 - fi * 0.35;
        scene.add(makeMesh(new THREE.BoxGeometry(fw, 0.15, 0.01),
            new THREE.MeshStandardMaterial({ color: factorColors[fi], emissive: factorColors[fi], emissiveIntensity: 0.35 }),
            [5 - 1.3 + fw / 2, fy, -7.845]));
    });
    // Mini pie chart (ring segments approximated with arcs)
    var pieColors = [0x3b82f6, 0x10b981, 0xf59e0b, 0xef4444];
    var pieAngles = [0, Math.PI * 0.5, Math.PI * 1.1, Math.PI * 1.7];
    pieColors.forEach(function(pc, pi) {
        var startA = pieAngles[pi];
        var endA = pi < 3 ? pieAngles[pi + 1] : Math.PI * 2;
        var arc = makeMesh(new THREE.RingGeometry(0.2, 0.45, 16, 1, startA, endA - startA),
            new THREE.MeshStandardMaterial({ color: pc, emissive: pc, emissiveIntensity: 0.3, side: THREE.DoubleSide }),
            [5.8, 2.4, -7.845]);
        scene.add(arc);
    });
    addLabel('🔢 量化因子', [5, 4.45, -7.7], '#60a5fa');

    // ── AlphaFin Logo (prominent, centered on back wall above windows) ──
    // Logo background panel (high on wall, above everything)
    var logoBg = makeMesh(new THREE.BoxGeometry(5.5, 0.9, 0.03),
        new THREE.MeshStandardMaterial({ color: 0x1e3a5f, roughness: 0.2, metalness: 0.3 }),
        [0, 4.55, -7.9]);
    scene.add(logoBg);
    // Logo accent bars (gold lines above & below)
    var goldBarMat = new THREE.MeshStandardMaterial({ color: 0xd4af37, emissive: 0xd4af37, emissiveIntensity: 0.5, metalness: 0.8 });
    scene.add(makeMesh(new THREE.BoxGeometry(4.8, 0.04, 0.02), goldBarMat, [0, 4.98, -7.88]));
    scene.add(makeMesh(new THREE.BoxGeometry(4.8, 0.04, 0.02), goldBarMat, [0, 4.12, -7.88]));
    // Logo text via CSS2D (large, prominent, high position)
    var logoDiv = document.createElement('div');
    logoDiv.innerHTML = '<div style="font-size:22px;font-weight:900;letter-spacing:6px;color:#d4af37;text-shadow:0 1px 8px rgba(212,175,55,0.4);">ALPHAFIN</div>' +
        '<div style="font-size:9px;color:#94a3b8;letter-spacing:3px;margin-top:3px;">INTELLIGENT INVESTMENT RESEARCH</div>';
    logoDiv.style.cssText = 'text-align:center;background:rgba(15,23,42,0.85);padding:8px 24px;border-radius:4px;' +
        'box-shadow:0 4px 20px rgba(0,0,0,0.25);border:1px solid rgba(212,175,55,0.3);';
    var logoObj = new CSS2DObject(logoDiv);
    logoObj.position.set(0, 4.55, -7.75);
    scene.add(logoObj);
}

// ════════════════ Ceiling (clean, no light strips) ════════════════

function buildCeiling() {
    var ceil = new THREE.Mesh(new THREE.PlaneGeometry(22, 18), M.ceiling);
    ceil.rotation.x = Math.PI / 2;
    ceil.position.set(0, 5, -1);
    scene.add(ceil);

    // Recessed downlights only (subtle, no visible fixtures)
    [[-4, -3], [4, -3], [-4, 2], [4, 2], [0, -1], [0, -5], [-4, -6], [4, -6]].forEach(function(p) {
        var pl = new THREE.PointLight(0xffffff, 0.25, 8);
        pl.position.set(p[0], 4.8, p[1]);
        scene.add(pl);
    });
}

// ════════════════ Desk & Chair Builders ════════════════

function makeMesh(geo, mat, pos) {
    var m = new THREE.Mesh(geo, mat);
    if (pos) m.position.set(pos[0], pos[1], pos[2]);
    return m;
}

function buildDesk(x, z, agentColor) {
    var g = new THREE.Group();
    var top = new THREE.Mesh(new THREE.BoxGeometry(1.6, 0.06, 0.9), M.deskTop);
    top.position.y = 0.75; top.castShadow = true;
    g.add(top);

    [[-0.7, -0.35], [0.7, -0.35], [-0.7, 0.35], [0.7, 0.35]].forEach(function(p) {
        g.add(makeMesh(new THREE.BoxGeometry(0.06, 0.75, 0.06), M.metal, [p[0], 0.375, p[1]]));
    });

    g.add(makeMesh(new THREE.BoxGeometry(0.04, 0.25, 0.04), M.metal, [0, 1.0, -0.25]));
    var mon = makeMesh(new THREE.BoxGeometry(0.6, 0.4, 0.03), M.screen, [0, 1.28, -0.25]);
    g.add(mon);

    var glowMat = new THREE.MeshStandardMaterial({ color: agentColor, emissive: agentColor, emissiveIntensity: 0.4 });
    g.add(makeMesh(new THREE.PlaneGeometry(0.55, 0.35), glowMat, [0, 1.28, -0.235]));

    // Keyboard + mouse
    g.add(makeMesh(new THREE.BoxGeometry(0.35, 0.015, 0.12),
        new THREE.MeshStandardMaterial({ color: 0xdde1e7, roughness: 0.6 }), [0, 0.79, 0.1]));
    g.add(makeMesh(new THREE.BoxGeometry(0.05, 0.015, 0.08),
        new THREE.MeshStandardMaterial({ color: 0xdde1e7, roughness: 0.6 }), [0.35, 0.79, 0.1]));

    g.position.set(x, 0, z);
    scene.add(g);
    return g;
}

function buildChair(x, z, rotation) {
    var g = new THREE.Group();
    g.add(makeMesh(new THREE.BoxGeometry(0.45, 0.06, 0.45), M.chair, [0, 0.45, 0]));
    g.add(makeMesh(new THREE.BoxGeometry(0.45, 0.5, 0.06), M.chair, [0, 0.73, -0.2]));
    g.add(makeMesh(new THREE.CylinderGeometry(0.03, 0.03, 0.45, 6), M.metal, [0, 0.225, 0]));
    g.add(makeMesh(new THREE.CylinderGeometry(0.2, 0.2, 0.03, 8), M.metal, [0, 0.015, 0]));
    g.position.set(x, 0, z);
    g.rotation.y = rotation || 0;
    scene.add(g);
    return g;
}

// ════════════════ Workstations ════════════════

function buildWorkstations() {
    ['intel', 'quant', 'analyst', 'risk', 'restructuring'].forEach(function(id) {
        var pos = AGENT_META[id].deskPos;
        buildDesk(pos.x, pos.z, new THREE.Color(AGENT_META[id].color));
        buildChair(pos.x, pos.z + 0.7, Math.PI);
    });
    buildDesk(AGENT_META.auditor.deskPos.x, AGENT_META.auditor.deskPos.z, new THREE.Color(AGENT_META.auditor.color));
    buildChair(AGENT_META.auditor.deskPos.x, AGENT_META.auditor.deskPos.z + 0.7, Math.PI);

    addLabel('💼 研究工位', [-3.75, 2.5, -1.0], '#475569');
    addLabel('🏗 事件工位', [-7.4, 2.5, -2.6], '#0f766e');
    addLabel('🔍 审计工位', [6, 2.5, 2.0], '#475569');
}

// ════════════════ Director's Desk (prestigious) ════════════════

function buildDirectorDesk() {
    var g = new THREE.Group();
    // L-shaped desk
    var top1 = new THREE.Mesh(new THREE.BoxGeometry(2.0, 0.06, 1.0), M.deskTop);
    top1.position.y = 0.78; top1.castShadow = true; g.add(top1);
    var top2 = new THREE.Mesh(new THREE.BoxGeometry(0.8, 0.06, 0.6), M.deskTop);
    top2.position.set(0.6, 0.78, -0.8); g.add(top2);
    // Front panel
    g.add(makeMesh(new THREE.BoxGeometry(2.0, 0.6, 0.04),
        new THREE.MeshStandardMaterial({ color: 0xb89a70, roughness: 0.6 }), [0, 0.5, 0.5]));
    // Legs
    [[-0.9, 0.45], [0.9, 0.45], [-0.9, -0.45], [0.9, -0.45]].forEach(function(p) {
        g.add(makeMesh(new THREE.BoxGeometry(0.06, 0.78, 0.06), M.metal, [p[0], 0.39, p[1]]));
    });
    // Dual monitors
    for (var dm = 0; dm < 2; dm++) {
        g.add(makeMesh(new THREE.BoxGeometry(0.04, 0.3, 0.04), M.metal, [-0.35 + dm * 0.7, 1.05, -0.3]));
        g.add(makeMesh(new THREE.BoxGeometry(0.6, 0.4, 0.03), M.screen, [-0.35 + dm * 0.7, 1.32, -0.3]));
        var c = dm === 0 ? 0xe74c3c : 0x2563eb;
        g.add(makeMesh(new THREE.PlaneGeometry(0.55, 0.35),
            new THREE.MeshStandardMaterial({ color: c, emissive: c, emissiveIntensity: 0.35 }),
            [-0.35 + dm * 0.7, 1.32, -0.285]));
    }
    // Keyboard, mouse, nameplate
    g.add(makeMesh(new THREE.BoxGeometry(0.4, 0.015, 0.13),
        new THREE.MeshStandardMaterial({ color: 0xdde1e7 }), [0, 0.82, 0.1]));
    g.add(makeMesh(new THREE.BoxGeometry(0.05, 0.015, 0.08),
        new THREE.MeshStandardMaterial({ color: 0xdde1e7 }), [0.45, 0.82, 0.1]));
    g.add(makeMesh(new THREE.BoxGeometry(0.3, 0.08, 0.06),
        new THREE.MeshStandardMaterial({ color: 0xd4af37, roughness: 0.3, metalness: 0.7 }), [-0.6, 0.83, 0.3]));

    g.position.set(0, 0, -6.5);
    scene.add(g);
    buildChair(0, -5.7, Math.PI);
    AGENT_META.director.deskPos = new THREE.Vector3(0, 0, -6.5);
}

// ════════════════ Meeting Room ════════════════

function buildMeetingRoom() {
    var cx = ZONE.meetingTable.x, cz = ZONE.meetingTable.z;
    // Table
    scene.add(makeMesh(new THREE.CylinderGeometry(1.5, 1.5, 0.08, 24), M.deskTop, [cx, 0.75, cz]));
    scene.add(makeMesh(new THREE.CylinderGeometry(0.15, 0.2, 0.75, 12), M.metal, [cx, 0.375, cz]));
    // Chairs
    for (var i = 0; i < MEETING_SEATS.length; i++) {
        var a = (i / MEETING_SEATS.length) * Math.PI * 2;
        buildChair(cx + Math.sin(a) * 2.2, cz + Math.cos(a) * 2.2, -a + Math.PI);
    }
    // Large presentation screen
    scene.add(makeMesh(new THREE.BoxGeometry(3.5, 2.0, 0.1), M.screen, [cx, 2.8, -7.84]));
    var scrMat = new THREE.MeshStandardMaterial({ color: 0x1a3a6a, emissive: 0x1a3a6a, emissiveIntensity: 0.35 });
    scene.add(makeMesh(new THREE.PlaneGeometry(3.3, 1.8), scrMat, [cx, 2.8, -7.78]));
    addLabel('📊 团队研究讨论', [cx, 3.95, -7.7], '#60a5fa');

    // Glass partition with frosted strip & label
    scene.add(makeMesh(new THREE.BoxGeometry(0.06, 3.5, 5), M.glass, [1.5, 1.75, cz]));
    scene.add(makeMesh(new THREE.BoxGeometry(0.01, 0.5, 4.8),
        new THREE.MeshStandardMaterial({ color: 0xffffff, transparent: true, opacity: 0.25, roughness: 0.95 }),
        [1.5, 2.2, cz]));
    addLabel('会议室 · Conference Room', [1.5, 2.2, cz], '#94a3b8');
}

// ════════════════ Tool Station (light-colored server rack) ════════════════

function buildToolStation() {
    // Data terminals along left wall
    for (var i = 0; i < 3; i++) {
        var tg = new THREE.Group();
        tg.add(makeMesh(new THREE.BoxGeometry(0.4, 1.2, 0.6), M.terminal, [0, 0.6, 0]));
        var scrMat = new THREE.MeshStandardMaterial({ color: 0x00dd77, emissive: 0x00dd77, emissiveIntensity: 0.5 + i * 0.1 });
        tg.add(makeMesh(new THREE.PlaneGeometry(0.5, 0.5), scrMat, [0.21, 0.9, 0]));
        tg.children[1].rotation.y = Math.PI / 2;
        tg.position.set(-8.55, 0, 3.5 + i * 1.0);
        scene.add(tg);
    }

    // Server racks against walls (slim, wall-mounted style)
    function buildWallRack(x, z, rotY) {
        var rackG = new THREE.Group();
        rackG.add(makeMesh(new THREE.BoxGeometry(0.4, 1.8, 1.2), M.rackBody, [0, 0.9, 0]));
        rackG.add(makeMesh(new THREE.BoxGeometry(0.02, 1.7, 1.1), M.rackFront, [0.21, 0.9, 0]));
        for (var sl = 0; sl < 5; sl++) {
            rackG.add(makeMesh(new THREE.BoxGeometry(0.01, 0.01, 1.0),
                new THREE.MeshStandardMaterial({ color: 0x9ca3af }), [0.22, 0.3 + sl * 0.32, 0]));
        }
        for (var tray = 0; tray < 5; tray++) {
            for (var lk = 0; lk < 2; lk++) {
                var lc = lk === 0 ? 0x22cc66 : 0x3b82f6;
                var ledMat = new THREE.MeshStandardMaterial({ color: lc, emissive: lc, emissiveIntensity: 0.7 });
                rackG.add(makeMesh(new THREE.BoxGeometry(0.015, 0.015, 0.015), ledMat,
                    [0.225, 0.22 + tray * 0.32, 0.35 - lk * 0.08]));
            }
        }
        rackG.position.set(x, 0, z);
        rackG.rotation.y = rotY || 0;
        scene.add(rackG);
    }

    // Left wall racks
    buildWallRack(-8.55, 6.0, Math.PI / 2);
    buildWallRack(-8.55, 7.2, Math.PI / 2);
    // Right wall rack
    buildWallRack(8.55, 6.0, -Math.PI / 2);
    buildWallRack(8.55, 7.2, -Math.PI / 2);

    addLabel('🖥 数据中心', [-8.2, 2.2, 6.5], '#059669', 14);

    // ── Skill 工坊 (right wall, near right racks) ──
    // Workbench
    scene.add(makeMesh(new THREE.BoxGeometry(1.8, 0.06, 0.8), M.deskTop, [7.5, 0.75, 6.5]));
    [[0.8, 0.3], [0.8, -0.3], [-0.8, 0.3], [-0.8, -0.3]].forEach(function(p) {
        scene.add(makeMesh(new THREE.BoxGeometry(0.06, 0.75, 0.06), M.metal, [7.5 + p[0], 0.375, 6.5 + p[1]]));
    });
    // Screen on workbench
    scene.add(makeMesh(new THREE.BoxGeometry(0.04, 0.3, 0.04), M.metal, [7.5, 1.0, 6.2]));
    scene.add(makeMesh(new THREE.BoxGeometry(0.6, 0.4, 0.03), M.screen, [7.5, 1.28, 6.2]));
    var skillScrMat = new THREE.MeshStandardMaterial({ color: 0x8855cc, emissive: 0x8855cc, emissiveIntensity: 0.4 });
    scene.add(makeMesh(new THREE.PlaneGeometry(0.55, 0.35), skillScrMat, [7.5, 1.28, 6.215]));
    // Keyboard
    scene.add(makeMesh(new THREE.BoxGeometry(0.35, 0.015, 0.12),
        new THREE.MeshStandardMaterial({ color: 0xdde1e7, roughness: 0.6 }), [7.5, 0.79, 6.8]));

    addLabel('🧪 Skill 工坊', [8.2, 2.2, 6.5], '#8b5cf6', 14);

    // ── Per-tool labels at data terminals ──
    addLabel('K线', [-8.2, 1.9, 3.5], '#059669', 12);
    addLabel('财务', [-8.2, 1.9, 4.5], '#059669', 12);
    addLabel('筹码', [-8.2, 1.9, 5.5], '#059669', 12);
    addLabel('数据库', [-7.5, 1.9, 6.8], '#059669', 12);

    // ── 情报站 (Intelligence Station, bottom-left) ──
    var intelTools = [
        { x: -5.5, z: 7.5, label: '新闻', color: 0x0ea5e9 },
        { x: -4.0, z: 7.5, label: '行业', color: 0x06b6d4 },
        { x: -2.5, z: 7.5, label: '搜索', color: 0x0891b2 },
    ];
    intelTools.forEach(function(t) {
        // Small standing terminal
        scene.add(makeMesh(new THREE.BoxGeometry(0.3, 0.9, 0.3), M.terminal, [t.x, 0.45, t.z]));
        var scrMat = new THREE.MeshStandardMaterial({ color: t.color, emissive: t.color, emissiveIntensity: 0.5 });
        scene.add(makeMesh(new THREE.PlaneGeometry(0.25, 0.3), scrMat, [t.x, 0.85, t.z - 0.16]));
        addLabel(t.label, [t.x, 1.3, t.z], '#0891b2', 12);
    });
    addLabel('🌐 情报站', [-4.0, 1.8, 7.5], '#0891b2', 14);

    // ── 分析引擎 (Analysis Engine, bottom-center) ──
    var analysisTools = [
        { x: -0.5, z: 7.5, label: '指标', color: 0x8b5cf6 },
        { x: 1.0, z: 7.5, label: '记忆', color: 0x7c3aed },
    ];
    analysisTools.forEach(function(t) {
        scene.add(makeMesh(new THREE.BoxGeometry(0.3, 0.9, 0.3), M.terminal, [t.x, 0.45, t.z]));
        var scrMat = new THREE.MeshStandardMaterial({ color: t.color, emissive: t.color, emissiveIntensity: 0.5 });
        scene.add(makeMesh(new THREE.PlaneGeometry(0.25, 0.3), scrMat, [t.x, 0.85, t.z - 0.16]));
        addLabel(t.label, [t.x, 1.3, t.z], '#7c3aed', 12);
    });
    addLabel('⚙ 分析引擎', [0.25, 1.8, 7.5], '#7c3aed', 14);

    // ── 投资决策台 (Investment Desk, right side) ──
    // Desk surface
    scene.add(makeMesh(new THREE.BoxGeometry(2.5, 0.06, 2.0), M.deskTop, [5.75, 0.75, 2.0]));
    // Desk legs
    [[4.6, 1.1], [4.6, 2.9], [6.9, 1.1], [6.9, 2.9]].forEach(function(p) {
        scene.add(makeMesh(new THREE.BoxGeometry(0.06, 0.75, 0.06), M.metal, [p[0], 0.375, p[1]]));
    });
    // 4 small screens on investment desk
    var investTools = [
        { x: 5.0, z: 1.5, label: '提交信号', color: 0xef4444 },
        { x: 5.0, z: 2.5, label: '审核信号', color: 0xf59e0b },
        { x: 6.5, z: 1.5, label: '组合状态', color: 0x3b82f6 },
        { x: 6.5, z: 2.5, label: '风险预警', color: 0xdc2626 },
    ];
    investTools.forEach(function(t) {
        // Screen stand
        scene.add(makeMesh(new THREE.BoxGeometry(0.04, 0.25, 0.04), M.metal, [t.x, 0.91, t.z]));
        // Screen
        scene.add(makeMesh(new THREE.BoxGeometry(0.5, 0.35, 0.03), M.screen, [t.x, 1.16, t.z]));
        var scrMat = new THREE.MeshStandardMaterial({ color: t.color, emissive: t.color, emissiveIntensity: 0.4 });
        scene.add(makeMesh(new THREE.PlaneGeometry(0.45, 0.3), scrMat, [t.x, 1.16, t.z - 0.016]));
        addLabel(t.label, [t.x, 1.5, t.z], '#dc2626', 12);
    });
    addLabel('💰 投资决策台', [5.75, 2.0, 2.0], '#dc2626', 14);
}

// ════════════════ Break Room ════════════════

function buildBreakRoom() {
    // Main sofa
    scene.add(makeMesh(new THREE.BoxGeometry(2.2, 0.3, 0.7), M.sofa, [1, 0.35, 4.5]));
    scene.add(makeMesh(new THREE.BoxGeometry(2.2, 0.5, 0.15), M.sofa, [1, 0.6, 4.85]));
    [[-0.05, 4.5], [2.05, 4.5]].forEach(function(p) {
        scene.add(makeMesh(new THREE.BoxGeometry(0.15, 0.35, 0.7), M.sofa, [p[0], 0.52, p[1]]));
    });

    // Second sofa (L-shape)
    scene.add(makeMesh(new THREE.BoxGeometry(0.7, 0.3, 1.6), M.sofa, [-0.6, 0.35, 4.0]));
    scene.add(makeMesh(new THREE.BoxGeometry(0.15, 0.5, 1.6), M.sofa, [-0.95, 0.6, 4.0]));

    // Coffee table
    scene.add(makeMesh(new THREE.BoxGeometry(1.0, 0.04, 0.5), M.deskTop, [1, 0.45, 3.5]));
    scene.add(makeMesh(new THREE.CylinderGeometry(0.03, 0.03, 0.45, 6), M.metal, [1, 0.225, 3.5]));

    buildPlant(2.8, 3.8);
    buildPlant(-0.5, 4.8);

    // Water cooler
    scene.add(makeMesh(new THREE.CylinderGeometry(0.15, 0.15, 0.9, 8), M.metal, [-0.3, 0.45, 3.2]));
    scene.add(makeMesh(new THREE.CylinderGeometry(0.18, 0.15, 0.15, 8),
        new THREE.MeshStandardMaterial({ color: 0x4488cc, transparent: true, opacity: 0.6 }), [-0.3, 0.97, 3.2]));

    addLabel('☕ 休息区', [ZONE.breakRoom.x, 2, ZONE.breakRoom.z], '#7c3aed');
}

// ════════════════ Pantry / Dining Area ════════════════

function buildPantry() {
    var px = ZONE.pantry.x, pz = ZONE.pantry.z;

    // Counter / bar
    var counterMat = new THREE.MeshStandardMaterial({ color: 0xd6cfc4, roughness: 0.4 });
    scene.add(makeMesh(new THREE.BoxGeometry(2.5, 0.06, 0.7), counterMat, [px, 0.9, pz + 0.5]));
    // Counter legs
    scene.add(makeMesh(new THREE.BoxGeometry(2.5, 0.9, 0.06), M.wallAccent, [px, 0.45, pz + 0.15]));

    // Bar stools (3)
    for (var bs = 0; bs < 3; bs++) {
        var stoolG = new THREE.Group();
        stoolG.add(makeMesh(new THREE.CylinderGeometry(0.15, 0.15, 0.03, 8), M.chair, [0, 0.65, 0]));
        stoolG.add(makeMesh(new THREE.CylinderGeometry(0.03, 0.04, 0.65, 6), M.metal, [0, 0.325, 0]));
        stoolG.add(makeMesh(new THREE.CylinderGeometry(0.15, 0.15, 0.02, 8), M.metal, [0, 0.01, 0]));
        stoolG.position.set(px - 0.8 + bs * 0.8, 0, pz + 1.0);
        scene.add(stoolG);
    }

    // Coffee machine
    var coffeeG = new THREE.Group();
    coffeeG.add(makeMesh(new THREE.BoxGeometry(0.3, 0.45, 0.25),
        new THREE.MeshStandardMaterial({ color: 0x5a5a65, roughness: 0.5, metalness: 0.4 }), [0, 1.15, 0]));
    // Coffee machine buttons
    coffeeG.add(makeMesh(new THREE.SphereGeometry(0.015, 8, 6),
        new THREE.MeshStandardMaterial({ color: 0x22cc66, emissive: 0x22cc66, emissiveIntensity: 0.6 }),
        [0.1, 1.2, 0.13]));
    coffeeG.add(makeMesh(new THREE.SphereGeometry(0.015, 8, 6),
        new THREE.MeshStandardMaterial({ color: 0xf59e0b, emissive: 0xf59e0b, emissiveIntensity: 0.4 }),
        [0.1, 1.28, 0.13]));
    coffeeG.position.set(px + 0.8, 0, pz + 0.5);
    scene.add(coffeeG);

    // Microwave
    scene.add(makeMesh(new THREE.BoxGeometry(0.35, 0.25, 0.3),
        new THREE.MeshStandardMaterial({ color: 0xe0e0e0, roughness: 0.4, metalness: 0.3 }),
        [px - 0.7, 1.05, pz + 0.5]));

    // Fruit bowl on counter
    var bowlMat = new THREE.MeshStandardMaterial({ color: 0xf0ebe4, roughness: 0.5 });
    scene.add(makeMesh(new THREE.SphereGeometry(0.12, 12, 8, 0, Math.PI * 2, 0, Math.PI * 0.5), bowlMat,
        [px, 0.96, pz + 0.5]));
    // Fruits
    var fruitColors = [0xff6b35, 0xffc107, 0xdc3545];
    for (var fi = 0; fi < 3; fi++) {
        scene.add(makeMesh(new THREE.SphereGeometry(0.035, 8, 6),
            new THREE.MeshStandardMaterial({ color: fruitColors[fi], roughness: 0.7 }),
            [px - 0.04 + fi * 0.04, 1.0, pz + 0.48 + (fi - 1) * 0.03]));
    }

    // Small fridge
    var fridgeG = new THREE.Group();
    fridgeG.add(makeMesh(new THREE.BoxGeometry(0.5, 1.3, 0.45),
        new THREE.MeshStandardMaterial({ color: 0xe8eaee, roughness: 0.4, metalness: 0.2 }), [0, 0.65, 0]));
    fridgeG.add(makeMesh(new THREE.BoxGeometry(0.08, 0.15, 0.02),
        new THREE.MeshStandardMaterial({ color: 0xaab0b8, roughness: 0.3, metalness: 0.6 }), [0.2, 0.7, 0.24]));
    fridgeG.position.set(px - 1.0, 0, pz - 0.2);
    scene.add(fridgeG);

    addLabel('🍽 餐饮区', [px, 2, pz + 0.5], '#ea580c');
}

// ════════════════ Decorations ════════════════

function buildDecorations() {
    // ── Left wall: framed landscape pictures ──
    var landscapes = [
        { z: -4.5, w: 2.0, h: 1.3, // Mountain sunset
          sky: 0xfdb88a, mountain: 0x5b7553, ground: 0x8fbc8f, sun: 0xf59e0b },
        { z: -1.5, w: 1.6, h: 1.1, // Lake & forest
          sky: 0x87ceeb, mountain: 0x2d6a4f, ground: 0x52b788, sun: 0x74c0fc },
        { z: 1.5, w: 1.6, h: 1.1, // Ocean waves
          sky: 0xbae6fd, mountain: 0x1e3a5f, ground: 0x0ea5e9, sun: 0xfbbf24 },
    ];
    landscapes.forEach(function(ls) {
        // Frame (dark wood)
        scene.add(makeMesh(new THREE.BoxGeometry(0.04, ls.h + 0.15, ls.w + 0.15),
            new THREE.MeshStandardMaterial({ color: 0x5c4033, roughness: 0.6 }), [-8.87, 2.8, ls.z]));
        // Sky portion (top 60%)
        var skyP = makeMesh(new THREE.PlaneGeometry(ls.h * 0.6, ls.w - 0.06),
            new THREE.MeshStandardMaterial({ color: ls.sky, roughness: 0.9 }), [-8.84, 2.95, ls.z]);
        skyP.rotation.y = Math.PI / 2;
        scene.add(skyP);
        // Mountain / horizon (middle band)
        var mtP = makeMesh(new THREE.PlaneGeometry(ls.h * 0.25, ls.w - 0.06),
            new THREE.MeshStandardMaterial({ color: ls.mountain, roughness: 0.9 }), [-8.835, 2.7, ls.z]);
        mtP.rotation.y = Math.PI / 2;
        scene.add(mtP);
        // Ground / water (bottom 25%)
        var grP = makeMesh(new THREE.PlaneGeometry(ls.h * 0.25, ls.w - 0.06),
            new THREE.MeshStandardMaterial({ color: ls.ground, roughness: 0.9 }), [-8.835, 2.5, ls.z]);
        grP.rotation.y = Math.PI / 2;
        scene.add(grP);
        // Sun/moon accent dot
        var sunDot = makeMesh(new THREE.CircleGeometry(0.08, 16),
            new THREE.MeshStandardMaterial({ color: ls.sun, emissive: ls.sun, emissiveIntensity: 0.3 }), [-8.83, 3.1, ls.z + 0.15]);
        sunDot.rotation.y = Math.PI / 2;
        scene.add(sunDot);
    });

    // ── Right wall: framed art (simple solid color canvases, clean look) ──
    var artColors = [
        { bg: 0xdbeafe, accent: 0x3b82f6 },
        { bg: 0xfce7f3, accent: 0xec4899 },
        { bg: 0xd1fae5, accent: 0x10b981 },
        { bg: 0xede9fe, accent: 0x8b5cf6 },
    ];
    [-5, -2.5, 0, 2.5].forEach(function(z, idx) {
        // Frame
        scene.add(makeMesh(new THREE.BoxGeometry(0.03, 1.0, 1.2), M.frame, [8.88, 2.8, z]));
        // Canvas background
        var canvasBg = makeMesh(new THREE.PlaneGeometry(0.9, 1.1),
            new THREE.MeshStandardMaterial({ color: artColors[idx].bg, roughness: 0.5 }), [8.86, 2.8, z]);
        canvasBg.rotation.y = -Math.PI / 2;
        scene.add(canvasBg);
        // Abstract accent shape (simple circle or rectangle)
        var accent = makeMesh(new THREE.PlaneGeometry(0.4, 0.4),
            new THREE.MeshStandardMaterial({ color: artColors[idx].accent, roughness: 0.6 }), [8.855, 2.85, z]);
        accent.rotation.y = -Math.PI / 2;
        scene.add(accent);
    });

    // ── Back wall: clock ──
    var clockFace = makeMesh(new THREE.CylinderGeometry(0.22, 0.22, 0.03, 24), M.ceramic, [-2.5, 3.6, -7.88]);
    clockFace.rotation.x = Math.PI / 2;
    scene.add(clockFace);
    var clockRim = makeMesh(new THREE.TorusGeometry(0.22, 0.018, 8, 24), M.metal, [-2.5, 3.6, -7.86]);
    scene.add(clockRim);
    scene.add(makeMesh(new THREE.BoxGeometry(0.012, 0.12, 0.008),
        new THREE.MeshStandardMaterial({ color: 0x333 }), [-2.5, 3.65, -7.85]));
    scene.add(makeMesh(new THREE.BoxGeometry(0.008, 0.16, 0.008),
        new THREE.MeshStandardMaterial({ color: 0x333 }), [-2.48, 3.58, -7.85]));

    // Company motto frame
    scene.add(makeMesh(new THREE.BoxGeometry(1.2, 0.7, 0.02), M.frame, [7.5, 3.6, -7.9]));
    addLabel('数据驱动 · 智能决策', [7.5, 3.6, -7.75], '#64748b');

    // ── Bookshelf on left wall ──
    var shelfG = new THREE.Group();
    shelfG.add(makeMesh(new THREE.BoxGeometry(0.08, 2.0, 2.0), M.wood, [0, 1.2, 0]));
    var bookMats = [
        new THREE.MeshStandardMaterial({ color: 0x2563eb, roughness: 0.8 }),
        new THREE.MeshStandardMaterial({ color: 0xdc2626, roughness: 0.8 }),
        new THREE.MeshStandardMaterial({ color: 0x059669, roughness: 0.8 }),
        new THREE.MeshStandardMaterial({ color: 0x7c3aed, roughness: 0.8 }),
        new THREE.MeshStandardMaterial({ color: 0xd97706, roughness: 0.8 }),
    ];
    for (var sh = 0; sh < 4; sh++) {
        shelfG.add(makeMesh(new THREE.BoxGeometry(0.3, 0.025, 2.0), M.wood, [0.1, 0.4 + sh * 0.5, 0]));
        var bx = -0.85;
        for (var bk = 0; bk < 7; bk++) {
            var bw = 0.06 + Math.random() * 0.07;
            var bh = 0.28 + Math.random() * 0.12;
            var book = makeMesh(new THREE.BoxGeometry(0.18, bh, bw), bookMats[(bk + sh * 2) % bookMats.length],
                [0.12, 0.4 + sh * 0.5 + bh / 2 + 0.015, bx + bw / 2]);
            shelfG.add(book);
            bx += bw + 0.015;
        }
    }
    shelfG.position.set(-8.6, 0, -5.5);
    scene.add(shelfG);

    // ── Whiteboard / Kanban near meeting room ──
    scene.add(makeMesh(new THREE.BoxGeometry(0.05, 1.4, 2.3), M.metal, [1.55, 2.5, -3.5]));
    var wbSurface = makeMesh(new THREE.PlaneGeometry(1.3, 2.2), M.whiteboard, [1.58, 2.5, -3.5]);
    wbSurface.rotation.y = -Math.PI / 2;
    scene.add(wbSurface);
    var stickyColors = [0xfde68a, 0xbfdbfe, 0xfecaca, 0xbbf7d0, 0xfed7aa];
    for (var sn = 0; sn < 5; sn++) {
        var sticky = makeMesh(new THREE.PlaneGeometry(0.3, 0.3),
            new THREE.MeshStandardMaterial({ color: stickyColors[sn], roughness: 0.8 }),
            [1.59, 2.85 - Math.floor(sn / 3) * 0.45, -4.1 + (sn % 3) * 0.5]);
        sticky.rotation.y = -Math.PI / 2;
        scene.add(sticky);
    }
    addLabel('📋 看板', [1.65, 3.4, -3.5], '#64748b');

    // ── Filing cabinets (multiple, along walls) ──
    function buildFileCabinet(x, z, rotY) {
        var cabG = new THREE.Group();
        cabG.add(makeMesh(new THREE.BoxGeometry(0.5, 1.0, 0.4), M.metal, [0, 0.5, 0]));
        for (var d = 0; d < 3; d++) {
            cabG.add(makeMesh(new THREE.BoxGeometry(0.15, 0.01, 0.02),
                new THREE.MeshStandardMaterial({ color: 0x888899, metalness: 0.7 }), [0, 0.25 + d * 0.3, 0.21]));
        }
        cabG.position.set(x, 0, z);
        cabG.rotation.y = rotY || 0;
        scene.add(cabG);
    }
    // Right wall cabinets
    buildFileCabinet(7.2, 3.0, 0);
    buildFileCabinet(7.8, 3.0, 0);
    buildFileCabinet(8.5, 1.5, -Math.PI / 2);
    // Left wall cabinets
    buildFileCabinet(-8.5, 1.5, Math.PI / 2);
    buildFileCabinet(-8.5, 2.5, Math.PI / 2);

    // ── Printer station (near right wall) ──
    var prG = new THREE.Group();
    prG.add(makeMesh(new THREE.BoxGeometry(0.55, 0.7, 0.45), M.metal, [0, 0.35, 0]));
    prG.add(makeMesh(new THREE.BoxGeometry(0.5, 0.28, 0.4), M.ceramic, [0, 0.84, 0]));
    prG.add(makeMesh(new THREE.SphereGeometry(0.015, 8, 6),
        new THREE.MeshStandardMaterial({ color: 0x22cc66, emissive: 0x22cc66, emissiveIntensity: 0.8 }),
        [0.2, 0.98, 0.18]));
    // Paper tray on top
    prG.add(makeMesh(new THREE.BoxGeometry(0.35, 0.02, 0.25),
        new THREE.MeshStandardMaterial({ color: 0xf5f5f5, roughness: 0.9 }), [0, 1.0, -0.05]));
    prG.position.set(7.5, 0, 4.5);
    scene.add(prG);

    // ── Second printer (near left wall workstations) ──
    var pr2 = prG.clone();
    pr2.position.set(-6.5, 0, -1.0);
    scene.add(pr2);

    // ── Storage cabinets (taller, wooden) ──
    function buildWoodCabinet(x, z, rotY) {
        var wcG = new THREE.Group();
        wcG.add(makeMesh(new THREE.BoxGeometry(0.8, 1.8, 0.4), M.wood, [0, 0.9, 0]));
        // Doors
        wcG.add(makeMesh(new THREE.BoxGeometry(0.38, 1.6, 0.02),
            new THREE.MeshStandardMaterial({ color: 0xb89a70, roughness: 0.5 }), [-0.2, 0.9, 0.21]));
        wcG.add(makeMesh(new THREE.BoxGeometry(0.38, 1.6, 0.02),
            new THREE.MeshStandardMaterial({ color: 0xb89a70, roughness: 0.5 }), [0.2, 0.9, 0.21]));
        // Door handles
        var handleMat = new THREE.MeshStandardMaterial({ color: 0xaab0b8, metalness: 0.8 });
        wcG.add(makeMesh(new THREE.BoxGeometry(0.02, 0.1, 0.02), handleMat, [-0.03, 0.9, 0.23]));
        wcG.add(makeMesh(new THREE.BoxGeometry(0.02, 0.1, 0.02), handleMat, [0.03, 0.9, 0.23]));
        wcG.position.set(x, 0, z);
        wcG.rotation.y = rotY || 0;
        scene.add(wcG);
    }
    buildWoodCabinet(8.5, 2.5, -Math.PI / 2);
    buildWoodCabinet(8.5, -1.0, -Math.PI / 2);
    buildWoodCabinet(-8.5, -6.8, Math.PI / 2);

    // ── Office supply shelf (small open shelf) ──
    var supG = new THREE.Group();
    supG.add(makeMesh(new THREE.BoxGeometry(0.6, 1.2, 0.3), M.metal, [0, 0.6, 0]));
    // Shelves
    for (var ss = 0; ss < 3; ss++) {
        supG.add(makeMesh(new THREE.BoxGeometry(0.55, 0.02, 0.28), M.metal, [0, 0.3 + ss * 0.35, 0]));
    }
    // Items on shelves (binders, folders)
    var binderMats = [
        new THREE.MeshStandardMaterial({ color: 0x2563eb, roughness: 0.7 }),
        new THREE.MeshStandardMaterial({ color: 0xdc2626, roughness: 0.7 }),
        new THREE.MeshStandardMaterial({ color: 0x059669, roughness: 0.7 }),
    ];
    for (var bn = 0; bn < 3; bn++) {
        supG.add(makeMesh(new THREE.BoxGeometry(0.08, 0.25, 0.22), binderMats[bn],
            [-0.18 + bn * 0.18, 0.45 + bn * 0.35, 0]));
    }
    supG.position.set(4.0, 0, -1.0);
    scene.add(supG);

    // ── Paper stacks on various desks ──
    var paperMat = new THREE.MeshStandardMaterial({ color: 0xfafafa, roughness: 0.95 });
    scene.add(makeMesh(new THREE.BoxGeometry(0.2, 0.04, 0.28), paperMat, [-3.0, 0.8, -3.7]));
    scene.add(makeMesh(new THREE.BoxGeometry(0.2, 0.06, 0.28), paperMat, [-4.5, 0.8, -3.3]));

    // ── Sofas ──
    function buildSofa(x, z, rotY, color) {
        var sG = new THREE.Group();
        var sofaMat = new THREE.MeshStandardMaterial({ color: color || 0x6b7b9e, roughness: 0.8 });
        var sofaDark = new THREE.MeshStandardMaterial({ color: new THREE.Color(color || 0x6b7b9e).multiplyScalar(0.75), roughness: 0.8 });
        // Seat
        sG.add(makeMesh(new THREE.BoxGeometry(1.6, 0.25, 0.7), sofaMat, [0, 0.35, 0]));
        // Backrest
        sG.add(makeMesh(new THREE.BoxGeometry(1.6, 0.5, 0.15), sofaDark, [0, 0.7, -0.28]));
        // Armrests
        sG.add(makeMesh(new THREE.BoxGeometry(0.12, 0.35, 0.6), sofaDark, [-0.74, 0.55, 0.05]));
        sG.add(makeMesh(new THREE.BoxGeometry(0.12, 0.35, 0.6), sofaDark, [0.74, 0.55, 0.05]));
        // Cushions (two)
        var cushionMat = new THREE.MeshStandardMaterial({ color: new THREE.Color(color || 0x6b7b9e).multiplyScalar(1.15), roughness: 0.9 });
        sG.add(makeMesh(new THREE.BoxGeometry(0.65, 0.08, 0.55), cushionMat, [-0.35, 0.52, 0.02]));
        sG.add(makeMesh(new THREE.BoxGeometry(0.65, 0.08, 0.55), cushionMat, [0.35, 0.52, 0.02]));
        // Legs
        var legMat = new THREE.MeshStandardMaterial({ color: 0x4a4a55, metalness: 0.5 });
        [[-0.65, -0.25], [0.65, -0.25], [-0.65, 0.25], [0.65, 0.25]].forEach(function(lp) {
            sG.add(makeMesh(new THREE.CylinderGeometry(0.025, 0.025, 0.2, 6), legMat, [lp[0], 0.1, lp[1]]));
        });
        sG.position.set(x, 0, z);
        sG.rotation.y = rotY || 0;
        scene.add(sG);
    }

    function buildCoffeeTable(x, z) {
        var ctG = new THREE.Group();
        var tableMat = new THREE.MeshStandardMaterial({ color: 0xc4a882, roughness: 0.5 });
        ctG.add(makeMesh(new THREE.BoxGeometry(0.9, 0.04, 0.55), tableMat, [0, 0.38, 0]));
        // Four legs
        var ctLeg = new THREE.MeshStandardMaterial({ color: 0x8b7355, metalness: 0.3 });
        [[-0.35, -0.2], [0.35, -0.2], [-0.35, 0.2], [0.35, 0.2]].forEach(function(lp) {
            ctG.add(makeMesh(new THREE.CylinderGeometry(0.02, 0.02, 0.36, 6), ctLeg, [lp[0], 0.18, lp[1]]));
        });
        ctG.position.set(x, 0, z);
        scene.add(ctG);
    }

    // ── Lounge area (right side, z=5~8): two sofas facing each other with coffee table ──
    buildSofa(5.5, 5.5, 0, 0x5b7ea0);            // backrest at south, facing north toward table
    buildSofa(5.5, 7.5, Math.PI, 0x5b7ea0);      // backrest at north, facing south toward table
    buildCoffeeTable(5.5, 6.5);                    // coffee table between them
    // Magazine + cup on table
    scene.add(makeMesh(new THREE.BoxGeometry(0.22, 0.015, 0.16),
        new THREE.MeshStandardMaterial({ color: 0xe74c3c, roughness: 0.8 }), [5.3, 0.4, 6.5]));
    scene.add(makeMesh(new THREE.CylinderGeometry(0.035, 0.03, 0.06, 8),
        new THREE.MeshStandardMaterial({ color: 0xffffff, roughness: 0.4 }), [5.75, 0.42, 6.45]));

    // ── Reception / waiting area (center-right, near entrance z=7~8) ──
    buildSofa(-1.5, 7.5, Math.PI, 0x8b6f5a);     // warm brown, backrest south, facing north toward table
    buildCoffeeTable(-1.5, 6.5);
    // Small decorative item on table
    scene.add(makeMesh(new THREE.CylinderGeometry(0.06, 0.04, 0.12, 8),
        new THREE.MeshStandardMaterial({ color: 0x2ecc71, roughness: 0.7 }), [-1.5, 0.46, 6.5]));

    // ── Reading corner (left side, near bookshelf) ──
    buildSofa(-6.0, 4.5, -Math.PI / 2, 0x6b8e6b); // sage green, facing east toward table
    buildCoffeeTable(-5.0, 4.5);
    // Book on table
    scene.add(makeMesh(new THREE.BoxGeometry(0.18, 0.03, 0.25),
        new THREE.MeshStandardMaterial({ color: 0x2563eb, roughness: 0.8 }), [-5.0, 0.41, 4.5]));

    // ── Plants scattered around the office ──
    buildPlant(-8.2, -7.0);
    buildPlant(8.2, -7.0);
    buildPlant(-8.2, 5.5);
    buildPlant(8.2, 5.5);
    buildPlant(-1.0, -7.3);
    buildPlant(-5, -7.3);
    buildPlant(5, -7.3);
    // Additional plants near new furniture
    buildPlant(4.5, -2.5);
    buildPlant(6.8, 5.0);
    buildPlant(-6.8, -1.5);
    buildPlant(3.5, 7.0);
    buildPlant(-3.0, 6.5);
    buildTallPlant(7.8, -6.0);
    buildTallPlant(-7.8, 1.0);
    buildTallPlant(3.5, 5.2);
    // Additional tall plants
    buildTallPlant(-7.5, 7.0);
    buildTallPlant(7.5, 1.0);
    buildTallPlant(2.0, 7.5);
    buildTallPlant(-2.5, 7.5);
}

// ════════════════ Plant Builders ════════════════

function buildPlant(x, z) {
    var g = new THREE.Group();
    g.add(makeMesh(new THREE.CylinderGeometry(0.15, 0.12, 0.2, 8), M.pot, [0, 0.1, 0]));
    var f = makeMesh(new THREE.SphereGeometry(0.2, 8, 6), M.plant, [0, 0.4, 0]);
    f.scale.set(1, 1.2, 1);
    g.add(f);
    g.add(makeMesh(new THREE.SphereGeometry(0.15, 8, 6), M.plant, [0.1, 0.55, 0.05]));
    g.position.set(x, 0, z);
    scene.add(g);
}

function buildTallPlant(x, z) {
    var g = new THREE.Group();
    g.add(makeMesh(new THREE.CylinderGeometry(0.2, 0.16, 0.35, 8), M.pot, [0, 0.175, 0]));
    g.add(makeMesh(new THREE.CylinderGeometry(0.03, 0.04, 0.6, 6),
        new THREE.MeshStandardMaterial({ color: 0x8b6b4a, roughness: 0.8 }), [0, 0.65, 0]));
    var f1 = makeMesh(new THREE.SphereGeometry(0.3, 8, 6), M.plant, [0, 1.1, 0]);
    f1.scale.set(1, 1.3, 1);
    g.add(f1);
    g.add(makeMesh(new THREE.SphereGeometry(0.22, 8, 6), M.plant, [0.1, 1.35, 0.08]));
    g.add(makeMesh(new THREE.SphereGeometry(0.18, 8, 6), M.plant, [-0.08, 1.25, -0.1]));
    g.position.set(x, 0, z);
    scene.add(g);
}

// ════════════════ Helper: CSS2D Label ════════════════

function addLabel(text, pos, color, fontSize) {
    var div = document.createElement('div');
    div.textContent = text;
    var fs = fontSize || 11;
    div.style.cssText = 'color:' + (color || '#475569') + ';font-size:' + fs + 'px;font-weight:600;opacity:0.85;text-shadow:0 1px 2px rgba(0,0,0,0.1);';
    var obj = new CSS2DObject(div);
    obj.position.set(pos[0], pos[1], pos[2]);
    scene.add(obj);
}

// ════════════════ Character Builder ════════════════

function buildCharacter(agentId) {
    var meta = AGENT_META[agentId];
    var bodyColor = new THREE.Color(meta.color);
    var bodyMat = new THREE.MeshStandardMaterial({ color: bodyColor, roughness: 0.6 });
    var darkBodyMat = new THREE.MeshStandardMaterial({ color: bodyColor.clone().multiplyScalar(0.7), roughness: 0.7 });

    var group = new THREE.Group();

    var torso = new THREE.Mesh(new THREE.BoxGeometry(0.3, 0.4, 0.2), bodyMat);
    torso.position.y = 1.0; torso.castShadow = true;
    group.add(torso);

    var headScale = (agentId === 'director') ? 1.1 : 1.0;
    var head = new THREE.Mesh(new THREE.SphereGeometry(0.18 * headScale, 12, 10), M.skin);
    head.position.y = 1.38; head.castShadow = true;
    group.add(head);

    var hair = new THREE.Mesh(
        new THREE.SphereGeometry(0.19 * headScale, 12, 6, 0, Math.PI * 2, 0, Math.PI * 0.55), darkBodyMat);
    hair.position.y = 1.42;
    group.add(hair);

    var armGeo = new THREE.CylinderGeometry(0.04, 0.04, 0.35, 6);
    var leftArmPivot = new THREE.Group();
    leftArmPivot.position.set(-0.2, 1.15, 0);
    leftArmPivot.add(makeMesh(armGeo, M.skin, [0, -0.175, 0]));
    group.add(leftArmPivot);

    var rightArmPivot = new THREE.Group();
    rightArmPivot.position.set(0.2, 1.15, 0);
    rightArmPivot.add(makeMesh(armGeo, M.skin, [0, -0.175, 0]));
    group.add(rightArmPivot);

    var legGeo = new THREE.CylinderGeometry(0.05, 0.05, 0.4, 6);
    var shoeGeo = new THREE.BoxGeometry(0.08, 0.04, 0.12);

    var leftLegPivot = new THREE.Group();
    leftLegPivot.position.set(-0.08, 0.8, 0);
    leftLegPivot.add(makeMesh(legGeo, darkBodyMat, [0, -0.2, 0]));
    leftLegPivot.add(makeMesh(shoeGeo, M.shoe, [0, -0.38, 0.02]));
    group.add(leftLegPivot);

    var rightLegPivot = new THREE.Group();
    rightLegPivot.position.set(0.08, 0.8, 0);
    rightLegPivot.add(makeMesh(legGeo, darkBodyMat, [0, -0.2, 0]));
    rightLegPivot.add(makeMesh(shoeGeo, M.shoe, [0, -0.38, 0.02]));
    group.add(rightLegPivot);

    // ── Accessories ──
    if (agentId === 'director') {
        var tieMat = new THREE.MeshStandardMaterial({ color: 0xcc2222, roughness: 0.6 });
        group.add(makeMesh(new THREE.BoxGeometry(0.06, 0.04, 0.04), tieMat, [0, 1.17, 0.11]));
        group.add(makeMesh(new THREE.BoxGeometry(0.04, 0.18, 0.02), tieMat, [0, 1.04, 0.11]));
        group.add(makeMesh(new THREE.BoxGeometry(0.06, 0.06, 0.02), tieMat, [0, 0.92, 0.11]));
    }
    if (agentId === 'analyst') {
        var glassMat = new THREE.MeshStandardMaterial({ color: 0x222222, roughness: 0.3, metalness: 0.5 });
        var lensGeo = new THREE.TorusGeometry(0.04, 0.008, 8, 12);
        var ll = new THREE.Mesh(lensGeo, glassMat); ll.position.set(-0.06, 1.4, 0.16); ll.rotation.y = Math.PI / 2; group.add(ll);
        var rl = new THREE.Mesh(lensGeo, glassMat); rl.position.set(0.06, 1.4, 0.16); rl.rotation.y = Math.PI / 2; group.add(rl);
        group.add(makeMesh(new THREE.BoxGeometry(0.06, 0.008, 0.008), glassMat, [0, 1.42, 0.18]));
    }
    if (agentId === 'risk') {
        var shieldMat = new THREE.MeshStandardMaterial({ color: 0xff8c00, emissive: 0xff8c00, emissiveIntensity: 0.2 });
        var ss = new THREE.Shape();
        ss.moveTo(0, 0.06); ss.lineTo(0.05, -0.03); ss.lineTo(-0.05, -0.03); ss.lineTo(0, 0.06);
        var shield = new THREE.Mesh(new THREE.ShapeGeometry(ss), shieldMat);
        shield.position.set(0, 1.08, 0.11); group.add(shield);
    }
    if (agentId === 'intel') {
        var antMat = new THREE.MeshStandardMaterial({ color: 0x22cc66, roughness: 0.4, metalness: 0.3 });
        group.add(makeMesh(new THREE.CylinderGeometry(0.008, 0.008, 0.2, 6), antMat, [0.08, 1.6, 0]));
        group.add(makeMesh(new THREE.SphereGeometry(0.025, 8, 6),
            new THREE.MeshStandardMaterial({ color: 0x44ff88, emissive: 0x44ff88, emissiveIntensity: 0.5 }), [0.08, 1.72, 0]));
    }
    if (agentId === 'quant') {
        var tabletMat = new THREE.MeshStandardMaterial({ color: 0x2a2a3a, roughness: 0.3, metalness: 0.4 });
        rightArmPivot.add(makeMesh(new THREE.BoxGeometry(0.12, 0.18, 0.015), tabletMat, [0, -0.28, 0.06]));
        rightArmPivot.add(makeMesh(new THREE.PlaneGeometry(0.1, 0.15),
            new THREE.MeshStandardMaterial({ color: 0x8855cc, emissive: 0x8855cc, emissiveIntensity: 0.4 }), [0, -0.28, 0.075]));
    }
    if (agentId === 'restructuring') {
        var folderMat = new THREE.MeshStandardMaterial({ color: 0x0f766e, roughness: 0.5, metalness: 0.2 });
        leftArmPivot.add(makeMesh(new THREE.BoxGeometry(0.16, 0.2, 0.02), folderMat, [0, -0.25, 0.07]));
        leftArmPivot.add(makeMesh(new THREE.PlaneGeometry(0.14, 0.18),
            new THREE.MeshStandardMaterial({ color: 0x14b8a6, emissive: 0x14b8a6, emissiveIntensity: 0.35 }), [0, -0.25, 0.081]));
    }
    if (agentId === 'auditor') {
        var hatMat = new THREE.MeshStandardMaterial({ color: 0x159e8a, roughness: 0.6 });
        group.add(makeMesh(new THREE.CylinderGeometry(0.22, 0.22, 0.02, 12), hatMat, [0, 1.55, 0]));
        group.add(makeMesh(new THREE.CylinderGeometry(0.15, 0.17, 0.1, 12), hatMat, [0, 1.61, 0]));
    }

    // Status light
    var statusLightMat = new THREE.MeshStandardMaterial({ color: 0x9ca3af, emissive: 0x9ca3af, emissiveIntensity: 0.5 });
    var statusLight = new THREE.Mesh(new THREE.SphereGeometry(0.06, 8, 6), statusLightMat);
    statusLight.position.y = 1.7 + (agentId === 'auditor' ? 0.12 : 0);
    group.add(statusLight);

    // Label
    var labelDiv = document.createElement('div');
    labelDiv.className = 'agent-label-3d';
    labelDiv.textContent = meta.icon + ' ' + meta.name;
    labelDiv.style.cssText = 'background:rgba(255,255,255,0.88);padding:2px 8px;border-radius:10px;font-size:10px;color:#333;white-space:nowrap;box-shadow:0 1px 4px rgba(0,0,0,0.12);border:1px solid rgba(0,0,0,0.06);';
    var labelObj = new CSS2DObject(labelDiv);
    labelObj.position.y = 1.9 + (agentId === 'auditor' ? 0.12 : 0);
    group.add(labelObj);

    // Bubble
    var bubbleDiv = document.createElement('div');
    bubbleDiv.className = 'agent-bubble';
    bubbleDiv.style.display = 'none';
    var bubbleObj = new CSS2DObject(bubbleDiv);
    bubbleObj.position.y = 2.2 + (agentId === 'auditor' ? 0.12 : 0);
    group.add(bubbleObj);

    var idx = AGENT_ORDER.indexOf(agentId);
    var startPos = getDeskSeatPosition(agentId, idx, 0.7);
    group.position.copy(startPos);
    scene.add(group);

    return {
        id: agentId, group: group, torso: torso, head: head,
        leftArmPivot: leftArmPivot, rightArmPivot: rightArmPivot,
        leftLegPivot: leftLegPivot, rightLegPivot: rightLegPivot,
        statusLight: statusLight, bubbleDiv: bubbleDiv,
        currentPos: startPos.clone(), targetPos: startPos.clone(),
        isWalking: false, walkSpeed: 2.0, status: 'idle', bubbleTimer: null, lastToolName: '',
    };
}

// ════════════════ Agent Management ════════════════

function initAgents() {
    AGENT_ORDER.forEach(function(id) { agents[id] = buildCharacter(id); });
}

function rebuildMeetingSeatMap(participants) {
    var list = Array.isArray(participants) && participants.length ? participants : AGENT_ORDER.slice();
    var seen = {};
    var clean = [];
    list.forEach(function(id) {
        if (AGENT_ORDER.indexOf(id) >= 0 && !seen[id]) {
            seen[id] = true;
            clean.push(id);
        }
    });
    meetingState.participants = clean;
    meetingState.seatMap = {};
    clean.forEach(function(id, idx) {
        meetingState.seatMap[id] = idx % MEETING_SEATS.length;
    });
}

function setMeetingState(active, participants) {
    var nextActive = !!active;
    var sameActive = meetingState.active === nextActive;
    var sameParticipants = !participants;
    if (participants && Array.isArray(participants)) {
        sameParticipants = meetingState.participants.length === participants.length &&
            meetingState.participants.every(function(id, idx) { return id === participants[idx]; });
    }
    if (sameActive && sameParticipants) return;

    meetingState.active = nextActive;
    if (participants) rebuildMeetingSeatMap(participants);
    if (!meetingState.active) {
        meetingState.participants = [];
        meetingState.seatMap = {};
    }
    meetingPanelState.active = meetingState.active;
    if (participants && Array.isArray(participants) && participants.length > 0) {
        meetingPanelState.participants = normalizeMeetingParticipants(participants);
    }
    if (!meetingPanelState.active && (!participants || !participants.length)) {
        meetingPanelState.participants = [];
    }
    renderSceneMeetingPanel();
    AGENT_ORDER.forEach(function(id) {
        if (agents[id]) updateAgentStatus(id, agents[id].status || 'idle', true);
    });
}

function getTargetPosition(agentId, status) {
    var idx = AGENT_ORDER.indexOf(agentId);
    if (meetingState.active && meetingState.seatMap.hasOwnProperty(agentId)) {
        return MEETING_SEATS[meetingState.seatMap[agentId]].clone();
    }
    switch (status) {
        case 'thinking':
            return getDeskSeatPosition(agentId, idx, 0.7);
        case 'using_tool':
            var ag = agents[agentId];
            var tn = (ag && ag.lastToolName) || '';
            if (tn && TOOL_POSITION_MAP[tn]) {
                return TOOL_POSITION_MAP[tn].clone();
            }
            return TOOL_FALLBACK_POSITIONS[idx % TOOL_FALLBACK_POSITIONS.length].clone();
        case 'speaking':
            return MEETING_SEATS[idx % MEETING_SEATS.length].clone();
        case 'offline':
            return BREAK_POSITIONS[idx % BREAK_POSITIONS.length].clone();
        default:
            // 空闲时回到各自工位
            return getDeskSeatPosition(agentId, idx, 0.7);
    }
}

function updateAgentStatus(agentId, newStatus, forceRetarget) {
    var agent = agents[agentId];
    if (!agent) return;
    if (agent.status === newStatus && !forceRetarget) return;
    agent.status = newStatus;
    var c = STATUS_COLORS[newStatus] || 0x9ca3af;
    agent.statusLight.material.color.set(c);
    agent.statusLight.material.emissive.set(c);
    var target = getTargetPosition(agentId, newStatus);
    agent.targetPos.copy(target);
    if (agent.currentPos.distanceTo(target) > 0.2) agent.isWalking = true;
}

function showBubble(agentId, text) {
    var agent = agents[agentId];
    if (!agent) return;
    agent.bubbleDiv.textContent = text.length > 60 ? text.substring(0, 60) + '...' : text;
    agent.bubbleDiv.style.display = 'block';
    if (agent.bubbleTimer) clearTimeout(agent.bubbleTimer);
    agent.bubbleTimer = setTimeout(function() {
        agent.bubbleDiv.style.display = 'none';
        agent.bubbleTimer = null;
    }, 4000);
}

// ════════════════ Communication Lines ════════════════

function showCommLine(fromId, toId) {
    var fa = agents[fromId], ta = agents[toId];
    if (!fa || !ta) return;
    var start = fa.group.position.clone(); start.y = 1.5;
    var end = ta.group.position.clone(); end.y = 1.5;
    var mid = start.clone().add(end).multiplyScalar(0.5); mid.y += 2;
    var curve = new THREE.QuadraticBezierCurve3(start, mid, end);
    var geo = new THREE.BufferGeometry().setFromPoints(curve.getPoints(20));
    var line = new THREE.Line(geo, new THREE.LineBasicMaterial({
        color: new THREE.Color(AGENT_META[fromId].color), transparent: true, opacity: 0.7 }));
    scene.add(line);
    commLines.push({ line: line, birth: performance.now() / 1000, lifetime: 2.0 });
}

function updateCommLines() {
    var now = performance.now() / 1000;
    for (var i = commLines.length - 1; i >= 0; i--) {
        var cl = commLines[i];
        var age = now - cl.birth;
        if (age > cl.lifetime) {
            scene.remove(cl.line); cl.line.geometry.dispose(); cl.line.material.dispose();
            commLines.splice(i, 1);
        } else {
            cl.line.material.opacity = 0.7 * (1 - age / cl.lifetime);
        }
    }
}

// ════════════════ Animation Loop ════════════════

function animate() {
    requestAnimationFrame(animate);
    if (scenePaused) return;
    var nowMs = performance.now();
    var frameInterval = nowMs < sceneBurstUntilTs ? sceneActiveFrameIntervalMs : sceneIdleFrameIntervalMs;
    if (sceneLastFrameTs && (nowMs - sceneLastFrameTs) < frameInterval) {
        return;
    }
    sceneLastFrameTs = nowMs;
    var delta = Math.min(clock.getDelta(), 0.05);
    var elapsed = clock.getElapsedTime();
    controls.update();

    AGENT_ORDER.forEach(function(id) {
        var agent = agents[id];
        if (!agent) return;

        if (agent.isWalking) {
            var dir = new THREE.Vector3().subVectors(agent.targetPos, agent.currentPos);
            dir.y = 0;
            var dist = dir.length();
            if (dist < 0.15) {
                agent.isWalking = false;
                agent.currentPos.copy(agent.targetPos);
                agent.group.position.copy(agent.currentPos);
                agent.leftLegPivot.rotation.x = 0; agent.rightLegPivot.rotation.x = 0;
                agent.leftArmPivot.rotation.x = 0; agent.rightArmPivot.rotation.x = 0;
                agent.leftArmPivot.rotation.z = 0; agent.rightArmPivot.rotation.z = 0;
            } else {
                dir.normalize();
                agent.currentPos.addScaledVector(dir, Math.min(agent.walkSpeed * delta, dist));
                agent.group.position.copy(agent.currentPos);
                var look = agent.currentPos.clone().add(dir);
                agent.group.lookAt(look.x, agent.group.position.y, look.z);
                var sw = 8;
                agent.leftLegPivot.rotation.x = Math.sin(elapsed * sw) * 0.4;
                agent.rightLegPivot.rotation.x = -Math.sin(elapsed * sw) * 0.4;
                agent.leftArmPivot.rotation.x = -Math.sin(elapsed * sw) * 0.3;
                agent.rightArmPivot.rotation.x = Math.sin(elapsed * sw) * 0.3;
                agent.torso.position.y = 1.0 + Math.abs(Math.sin(elapsed * sw * 2)) * 0.02;
            }
        } else {
            switch (agent.status) {
                case 'thinking':
                    agent.head.position.y = 1.38 + Math.sin(elapsed * 2) * 0.02;
                    agent.rightArmPivot.rotation.x = -0.8 + Math.sin(elapsed * 1.5) * 0.1;
                    agent.leftArmPivot.rotation.x = 0; agent.leftArmPivot.rotation.z = 0; agent.rightArmPivot.rotation.z = 0;
                    break;
                case 'using_tool':
                    agent.leftArmPivot.rotation.x = -0.6;
                    agent.rightArmPivot.rotation.x = -0.6 + Math.sin(elapsed * 6) * 0.1;
                    agent.leftArmPivot.rotation.z = 0; agent.rightArmPivot.rotation.z = 0;
                    break;
                case 'speaking':
                    agent.rightArmPivot.rotation.x = -0.5 + Math.sin(elapsed * 3) * 0.3;
                    agent.rightArmPivot.rotation.z = Math.sin(elapsed * 2) * 0.2;
                    agent.leftArmPivot.rotation.x = 0; agent.leftArmPivot.rotation.z = 0;
                    break;
                default:
                    agent.torso.scale.y = 1 + Math.sin(elapsed * 1.5 + AGENT_ORDER.indexOf(id)) * 0.02;
                    agent.leftArmPivot.rotation.x = 0; agent.rightArmPivot.rotation.x = 0;
                    agent.leftArmPivot.rotation.z = 0; agent.rightArmPivot.rotation.z = 0;
                    break;
            }
        }

        if (agent.status !== 'idle' && agent.status !== 'offline') {
            agent.statusLight.material.emissiveIntensity = 0.5 + Math.sin(elapsed * 4) * 0.3;
        } else {
            agent.statusLight.material.emissiveIntensity = 0.3;
        }
    });

    updateCommLines();
    renderer.render(scene, camera);
    labelRenderer.render(scene, camera);
}

// ════════════════ Resize ════════════════

function onResize() {
    var container = document.getElementById('scene-container');
    if (!container) return;
    var w = container.clientWidth, h = container.clientHeight;
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
    renderer.setSize(w, h);
    labelRenderer.setSize(w, h);
}

// ════════════════ Click Interaction ════════════════

function setupClickInteraction(canvas) {
    var raycaster = new THREE.Raycaster();
    var mouse = new THREE.Vector2();
    canvas.addEventListener('click', function(event) {
        bumpSceneActivity(2800);
        var rect = canvas.getBoundingClientRect();
        mouse.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
        mouse.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
        raycaster.setFromCamera(mouse, camera);
        var closestAgent = null, closestDist = Infinity;
        AGENT_ORDER.forEach(function(id) {
            var agent = agents[id];
            if (!agent) return;
            var hits = raycaster.intersectObjects(agent.group.children, true);
            if (hits.length > 0 && hits[0].distance < closestDist) {
                closestDist = hits[0].distance;
                closestAgent = agent;
            }
        });
        if (closestAgent) {
            var orig = closestAgent.torso.material.emissive.clone();
            closestAgent.torso.material.emissive.set(0xffffff);
            closestAgent.torso.material.emissiveIntensity = 0.5;
            setTimeout(function() {
                closestAgent.torso.material.emissive.copy(orig);
                closestAgent.torso.material.emissiveIntensity = 0;
            }, 300);
            var stMap = { idle: '空闲', thinking: '思考中', using_tool: '使用工具', speaking: '输出中', offline: '离线' };
            var st = stMap[closestAgent.status] || closestAgent.status;
            var extra = '';
            if (closestAgent.status === 'using_tool' && closestAgent.lastToolName) {
                extra = ' [' + closestAgent.lastToolName + ']';
            }
            showBubble(closestAgent.id, AGENT_META[closestAgent.id].name + ' - ' + st + extra);
        }
    });
}

// ════════════════ External API ════════════════

function registerExternalAPI() {
    window.updateSceneAgents = function(agentsList) {
        if (!agentsList) return;
        bumpSceneActivity(2200);
        agentsList.forEach(function(a) {
            if (agents[a.agent_id]) updateAgentStatus(a.agent_id, a.status || 'idle');
        });
    };
    window.onAgentActivity = function(msg) {
        if (!msg) return;
        bumpSceneActivity(3600);
        updateMeetingPanelFromActivity(msg);
        var meta = msg.metadata || {};
        var phase = String(meta.phase || '');
        if (typeof meta.meeting_active === 'boolean') {
            setMeetingState(meta.meeting_active, meta.participants || meetingState.participants);
        } else if (phase === 'meeting' || phase === 'meeting_start' || phase === 'meeting_round') {
            setMeetingState(true, meta.participants || meetingState.participants);
        } else if (phase === 'meeting_summary' || phase === 'meeting_end') {
            setMeetingState(false, meta.participants || meetingState.participants);
        }

        var fromId = msg.from, toId = msg.to;
        if (fromId && agents[fromId] && msg.content) showBubble(fromId, msg.content);
        if (fromId && toId && agents[fromId] && agents[toId]) showCommLine(fromId, toId);
        // Capture tool name for position routing + show tool bubble
        if (msg.type === 'tool_call' && fromId && agents[fromId] && msg.metadata && msg.metadata.tool) {
            var toolName = msg.metadata.tool;
            agents[fromId].lastToolName = toolName;
            var toolLabel = {
                get_kline: '📊 获取K线', get_financials: '📋 获取财务',
                get_chip_distribution: '🎯 筹码分布', get_stock_news: '📰 个股新闻',
                get_sector_report: '🏭 行业报告', web_search: '🔍 网络搜索',
                get_current_time: '🕒 当前时间', get_intraday_index: '📉 指数盯盘',
                get_intraday_sector_heat: '🔥 板块热度', get_intraday_hotrank: '🏆 实时热榜',
                get_intraday_news: '🗞 快讯扫描',
                run_indicator: '⚙ 运行指标', query_database: '🗄 查询数据库',
                send_message_to_agent: '💬 发送消息', save_knowledge: '💾 保存记忆',
                create_skill: '🛠 创建技能', execute_skill: '▶ 执行技能',
                list_skills: '📑 查看技能', submit_trade_signal: '📤 提交交易信号',
                review_trade_signal: '✅ 审核信号', get_portfolio_status: '💰 查看组合',
                flag_risk_warning: '⚠ 风险预警',
            }[toolName] || '🔧 ' + toolName;
            showBubble(fromId, toolLabel);
        }
        if (msg.type === 'status' && fromId && agents[fromId] && msg.status) updateAgentStatus(fromId, msg.status);
    };
}

// ════════════════ Start ════════════════

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}
