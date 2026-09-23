/**
 * Sandbox Monitor - Main Application
 * Entry point and UI coordination
 */

import { Api } from './api.js';
import { AppState } from './state.js';

// Constants
const MAX_CLOSED_SESSIONS = 20;
const CLOSED_SESSIONS_KEY = 'sandbox_monitor_closed_sessions';

// ============================================
// LocalStorage for Closed Sessions
// ============================================

function loadClosedSessions() {
    try {
        const stored = localStorage.getItem(CLOSED_SESSIONS_KEY);
        if (stored) {
            return JSON.parse(stored);
        }
    } catch (e) {
        console.error('Failed to load closed sessions:', e);
    }
    return [];
}

function saveClosedSessions(sessions) {
    try {
        localStorage.setItem(CLOSED_SESSIONS_KEY, JSON.stringify(sessions));
    } catch (e) {
        console.error('Failed to save closed sessions:', e);
    }
}

function addClosedSession(sessionData) {
    const closedSession = {
        ...sessionData,
        closed_at: new Date().toISOString()
    };
    AppState.closedSessions = [closedSession, ...AppState.closedSessions].slice(0, MAX_CLOSED_SESSIONS);
    saveClosedSessions(AppState.closedSessions);
    return closedSession;
}

// ============================================
// Utility Functions
// ============================================

/**
 * Format timestamp to readable string
 */
function formatTime(isoString) {
    if (!isoString) return '--';
    try {
        const date = new Date(isoString);
        return date.toLocaleTimeString('en-US', {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        });
    } catch {
        return isoString;
    }
}

/**
 * Format timestamp to full datetime
 */
function formatDateTime(isoString) {
    if (!isoString) return '--';
    try {
        const date = new Date(isoString);
        return date.toLocaleString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        });
    } catch {
        return isoString;
    }
}

/**
 * Format duration in milliseconds
 */
function formatDuration(ms) {
    if (ms === null || ms === undefined) return '--';
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    const minutes = Math.floor(ms / 60000);
    const seconds = Math.floor((ms % 60000) / 1000);
    return `${minutes}m ${seconds}s`;
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Truncate text - from end if fromEnd is true, from start otherwise
 */
function truncate(text, maxLength = 100, fromEnd = true) {
    if (!text || text.length <= maxLength) return text;
    if (fromEnd) {
        return '...' + text.slice(-maxLength);
    }
    return text.substring(0, maxLength) + '...';
}

// ============================================
// Status Configuration
// ============================================

const STATUS_CONFIG = {
    session: {
        ACTIVE: { cssClass: 'state-active', label: 'Active' },
        CLOSING: { cssClass: 'state-closing', label: 'Closing' },
        CLOSED: { cssClass: 'state-closed', label: 'Closed' },
    },
    runtime: {
        READY: { cssClass: 'state-ready', label: 'Ready' },
        BUSY: { cssClass: 'state-busy', label: 'Busy' },
        LOST: { cssClass: 'state-lost', label: 'Lost' },
        STOPPED: { cssClass: 'state-stopped', label: 'Stopped' },
    },
    execution: {
        RUNNING: { cssClass: 'state-running', label: 'Running' },
        SUCCEEDED: { cssClass: 'state-succeeded', label: 'Success' },
        FAILED: { cssClass: 'state-failed', label: 'Failed' },
        TIMED_OUT: { cssClass: 'state-closing', label: 'Timeout' },
        LOST: { cssClass: 'state-lost', label: 'Lost' },
    }
};

/**
 * Create status badge HTML
 */
function createStatusBadge(type, state) {
    const config = STATUS_CONFIG[type]?.[state];
    if (!config) {
        return `<span class="status-badge">${escapeHtml(state)}</span>`;
    }
    return `<span class="status-badge ${config.cssClass}">${config.label}</span>`;
}

// ============================================
// Render Functions
// ============================================

/**
 * Render connection status indicator
 */
function renderConnectionStatus() {
    const statusEl = document.getElementById('connection-status');
    const statusDot = statusEl.querySelector('.status-dot');
    const statusText = statusEl.querySelector('.status-text');

    if (AppState.isLoading) {
        statusDot.className = 'status-dot connecting';
        statusText.textContent = 'Connecting...';
    } else if (AppState.connected) {
        statusDot.className = 'status-dot connected';
        statusText.textContent = 'Connected';
    } else if (AppState.error) {
        statusDot.className = 'status-dot disconnected';
        statusText.textContent = 'Disconnected';
    } else {
        statusDot.className = 'status-dot disconnected';
        statusText.textContent = 'Disconnected';
    }
}

/**
 * Render last update time
 */
function renderLastUpdate() {
    const el = document.getElementById('last-update');
    if (AppState.lastFetchTime) {
        el.textContent = `Last update: ${formatTime(AppState.lastFetchTime)}`;
    }
}

/**
 * Render capabilities info panel
 */
function renderCapabilities() {
    const caps = AppState.capabilities;
    if (!caps) return;

    document.getElementById('schema-version').textContent = caps.schema_version || '--';
    document.getElementById('deployment-mode').textContent = caps.deployment_mode || '--';
    document.getElementById('log-root').textContent = caps.local_log_root || '--';
    document.getElementById('supported-tools').textContent =
        (caps.supported_tool_kinds || []).join(', ') || '--';
}

/**
 * Render sessions grid
 */
function renderSessions() {
    const grid = document.getElementById('sessions-grid');
    const loadingEl = document.getElementById('loading-state');
    const emptyEl = document.getElementById('empty-state');
    const errorEl = document.getElementById('error-state');
    const paginationEl = document.getElementById('pagination');

    // Update filter counts
    const counts = AppState.sessionCounts;
    document.getElementById('count-all').textContent = counts.all;
    document.getElementById('count-READY').textContent = counts.READY;
    document.getElementById('count-BUSY').textContent = counts.BUSY;
    document.getElementById('count-LOST').textContent = counts.LOST;
    document.getElementById('count-STOPPED').textContent = counts.STOPPED;

    // Handle loading state
    if (AppState.isLoading) {
        loadingEl.style.display = 'flex';
        emptyEl.style.display = 'none';
        errorEl.style.display = 'none';
        grid.innerHTML = '';
        paginationEl.style.display = 'none';
        return;
    }

    // Handle error state
    if (AppState.error) {
        loadingEl.style.display = 'none';
        emptyEl.style.display = 'none';
        errorEl.style.display = 'flex';
        document.getElementById('error-message').textContent = AppState.error;
        grid.innerHTML = '';
        paginationEl.style.display = 'none';
        return;
    }

    // Handle empty state
    if (AppState.sessions.length === 0) {
        loadingEl.style.display = 'none';
        emptyEl.style.display = 'flex';
        errorEl.style.display = 'none';
        grid.innerHTML = '';
        paginationEl.style.display = 'none';
        return;
    }

    // Render sessions
    loadingEl.style.display = 'none';
    emptyEl.style.display = 'none';
    errorEl.style.display = 'none';

    // Use paginated sessions
    grid.innerHTML = AppState.paginatedSessions.map(session => `
        <div class="session-card ${AppState.selectedSessionId === session.workspace_session_id ? 'expanded' : ''} ${session.runtime_state === 'LOST' ? 'has-lost-runtime' : ''}"
             data-session-id="${escapeHtml(session.workspace_session_id)}">
            <div class="card-header">
                <div class="session-id-row">
                    <span class="session-id">${escapeHtml(session.workspace_session_id)}</span>
                    <div class="status-badges">
                        ${createStatusBadge('session', session.state)}
                        ${createStatusBadge('runtime', session.runtime_state)}
                    </div>
                </div>
            </div>
            <div class="card-body">
                <div class="info-row">
                    <span class="info-label">Tool</span>
                    <span class="info-value">${escapeHtml(session.tool_kind)}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Version</span>
                    <span class="info-value">${escapeHtml(session.version || 'default')}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Workspace</span>
                    <span class="info-value truncate" title="${escapeHtml(session.workspace_path)}">${escapeHtml(truncate(session.workspace_path, 40))}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Created</span>
                    <span class="info-value">${formatTime(session.created_at)}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Last Active</span>
                    <span class="info-value">${formatTime(session.last_active_at)}</span>
                </div>
            </div>
            <div class="card-footer">
                <button class="btn btn-primary btn-sm view-history-btn">View History</button>
                <button class="btn btn-danger btn-sm close-session-btn">Close</button>
            </div>
        </div>
    `).join('');

    // Update pagination controls
    const pageInfo = document.getElementById('page-info');
    const prevBtn = document.getElementById('page-prev');
    const nextBtn = document.getElementById('page-next');

    if (AppState.totalPages > 1) {
        paginationEl.style.display = 'flex';
        pageInfo.textContent = `Page ${AppState.currentPage} of ${AppState.totalPages} (${AppState.sessions.length} total)`;
        prevBtn.disabled = AppState.currentPage <= 1;
        nextBtn.disabled = AppState.currentPage >= AppState.totalPages;
    } else {
        paginationEl.style.display = 'none';
    }
}

/**
 * Render recently closed sessions
 */
function renderClosedSessions() {
    const grid = document.getElementById('closed-sessions-grid');
    if (!grid) return;

    // Get log root from capabilities
    const logRoot = AppState.capabilities?.local_log_root || '';

    if (AppState.closedSessions.length === 0) {
        grid.innerHTML = `
            <div class="empty-state">
                <svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="#94a3b8" stroke-width="1.5">
                    <circle cx="12" cy="12" r="10"/>
                    <path d="M12 6v6l4 2"/>
                </svg>
                <p>No recently closed sessions</p>
                <span class="empty-hint">Closed sessions will appear here</span>
            </div>
        `;
        return;
    }

    grid.innerHTML = AppState.closedSessions.map(session => {
        const sessionId = session.workspace_session_id;
        const executionsPath = logRoot ? `${logRoot}/sessions/${sessionId}/executions` : '';

        return `
        <div class="session-card closed-session"
             data-session-id="${escapeHtml(sessionId)}">
            <div class="card-header">
                <div class="session-id-row">
                    <span class="session-id">${escapeHtml(sessionId)}</span>
                    <div class="status-badges">
                        <span class="status-badge state-closed">Closed</span>
                        <span class="status-badge state-stopped">${escapeHtml(session.runtime_state || 'Unknown')}</span>
                    </div>
                </div>
            </div>
            <div class="card-body">
                <div class="info-row">
                    <span class="info-label">Tool</span>
                    <span class="info-value">${escapeHtml(session.tool_kind)}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Version</span>
                    <span class="info-value">${escapeHtml(session.version || 'default')}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Closed At</span>
                    <span class="info-value">${formatDateTime(session.closed_at)}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Last Active</span>
                    <span class="info-value">${formatDateTime(session.last_active_at)}</span>
                </div>
                ${executionsPath ? `
                <div class="info-row log-path-row">
                    <span class="info-label">Log Path</span>
                    <span class="info-value log-path-value">${escapeHtml(executionsPath)}</span>
                </div>
                ` : ''}
            </div>
        </div>
    `}).join('');
}

/**
 * Render session detail modal
 */
async function renderSessionDetail(sessionId) {
    const panel = document.getElementById('detail-content');
    const title = document.getElementById('detail-title');

    title.textContent = `Session: ${sessionId}`;

    // Show loading state first
    panel.innerHTML = `
        <div class="loading-state" style="display: flex; padding: 40px;">
            <div class="spinner"></div>
            <span>Loading session details...</span>
        </div>
    `;

    try {
        // Fetch session info first
        const sessionData = await Api.getSession(sessionId);
        const session = sessionData.workspace_session;

        // Fetch ALL history records (handle pagination)
        let allHistory = [];
        let cursor = null;
        let hasMore = true;
        const maxRecords = 1000; // Safety limit

        // Initial fetch
        let historyData = await Api.getSessionHistory(sessionId, 100, null, 'asc');
        allHistory = historyData.history || [];
        hasMore = historyData.page?.has_more === true;
        cursor = historyData.page?.next_cursor || null;

        // Fetch remaining pages
        while (hasMore && allHistory.length < maxRecords) {
            historyData = await Api.getSessionHistory(sessionId, 100, cursor, 'asc');
            const newItems = historyData.history || [];
            if (newItems.length === 0) break;
            allHistory = allHistory.concat(newItems);
            hasMore = historyData.page?.has_more === true;
            cursor = historyData.page?.next_cursor || null;
        }

        renderDetailContent(panel, session, allHistory);
        attachDetailHandlers(panel);

    } catch (error) {
        panel.innerHTML = `
            <div class="error-state" style="display: flex; padding: 40px;">
                <p class="error-message">Failed to load session details: ${escapeHtml(error.message)}</p>
            </div>
        `;
    }
}

/**
 * Render detail content with history
 */
function renderDetailContent(panel, session, allHistory) {
    panel.innerHTML = `
        <div class="detail-section">
            <h4 class="detail-section-title">Session Info</h4>
            <div class="info-grid" style="grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));">
                <div class="info-item">
                    <span class="info-label">Session ID</span>
                    <span class="info-value mono" style="word-break: break-all; font-size: 12px;">${escapeHtml(session.workspace_session_id)}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Tool Kind</span>
                    <span class="info-value">${escapeHtml(session.tool_kind)}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Version</span>
                    <span class="info-value">${escapeHtml(session.version || 'default')}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Session State</span>
                    <span class="info-value">${createStatusBadge('session', session.state)}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Runtime State</span>
                    <span class="info-value">${createStatusBadge('runtime', session.runtime.state)}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Process ID</span>
                    <span class="info-value mono">${session.runtime.process_id || '--'}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Created</span>
                    <span class="info-value">${formatDateTime(session.timestamps.created_at)}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Last Active</span>
                    <span class="info-value">${formatDateTime(session.timestamps.last_active_at)}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Idle Expires</span>
                    <span class="info-value">${formatDateTime(session.timestamps.idle_expires_at)}</span>
                </div>
            </div>
            <div class="info-item" style="margin-top: 16px;">
                <span class="info-label">Workspace Path</span>
                <span class="info-value info-value-path">${escapeHtml(session.workspace_path)}</span>
            </div>
        </div>

        <div class="detail-section">
            <h4 class="detail-section-title">
                Execution History (${allHistory.length} records)
            </h4>
            ${allHistory.length === 0 ? '<p style="color: var(--text-muted);">No executions yet</p>' : `
                <div class="execution-timeline">
                    ${allHistory.map(item => renderExecutionItem(item)).join('')}
                </div>
            `}
        </div>
    `;
}

/**
 * Attach click handlers for execution items
 */
function attachDetailHandlers(panel) {
    const target = panel || document.getElementById('detail-content');

    // Execution items - detect text selection vs click
    target.querySelectorAll('.execution-item').forEach(item => {
        // Remove existing listener by cloning
        const newItem = item.cloneNode(true);
        item.parentNode.replaceChild(newItem, item);

        let mouseDownTime = 0;
        let hasSelectedText = false;

        newItem.addEventListener('mousedown', () => {
            mouseDownTime = Date.now();
            hasSelectedText = false;
        });

        newItem.addEventListener('mouseup', () => {
            // Check if user selected text
            const selection = window.getSelection();
            hasSelectedText = selection && selection.toString().length > 0;
        });

        newItem.addEventListener('click', (e) => {
            // If text was selected, don't toggle - allow copy
            if (hasSelectedText) {
                hasSelectedText = false;
                return;
            }

            // Only toggle if it was a quick click (not text selection)
            const clickDuration = Date.now() - mouseDownTime;
            if (clickDuration > 200) return;

            newItem.classList.toggle('expanded');
            const details = newItem.querySelector('.execution-details');
            details.style.display = details.style.display === 'none' ? 'block' : 'none';
        });
    });

    // Highlight code blocks
    target.querySelectorAll('pre code.tcl').forEach(block => {
        if (typeof hljs !== 'undefined') {
            hljs.highlightElement(block);
        }
    });
}

/**
 * Legacy function - kept for compatibility
 */

/**
 * Render single execution item
 */
function renderExecutionItem(item) {
    const exitCode = item.result_summary?.exit_code;
    const isSuccess = exitCode === 0;
    const preview = item.result_summary?.output?.preview || '';

    return `
        <div class="execution-item" data-execution-id="${escapeHtml(item.execution_id)}">
            <div class="execution-header">
                <div class="execution-sequence">#${item.sequence}</div>
                <div class="execution-status">
                    ${createStatusBadge('execution', item.state)}
                </div>
                <div class="execution-time">
                    ${formatDuration(item.timing?.duration_ms)}
                </div>
            </div>
            <div class="execution-details" style="display: none;">
                <div class="info-row">
                    <span class="info-label">Request ID</span>
                    <span class="info-value mono">${escapeHtml(item.request_id)}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Execution ID</span>
                    <span class="info-value mono">${escapeHtml(item.execution_id)}</span>
                </div>
                ${item.result_summary?.output?.full_log?.path ? `
                <div class="info-row">
                    <span class="info-label">Full Log Path</span>
                    <span class="info-value mono info-value-path" title="${escapeHtml(item.result_summary.output.full_log.path)}">${escapeHtml(item.result_summary.output.full_log.path)}</span>
                </div>
                ${item.result_summary?.output?.full_log?.written_bytes ? `
                <div class="info-row">
                    <span class="info-label">Full Log Size</span>
                    <span class="info-value">${(item.result_summary.output.full_log.written_bytes / 1024 / 1024).toFixed(2)} MB</span>
                </div>
                ` : ''}
                ` : ''}
                <div class="info-row">
                    <span class="info-label">Submitted</span>
                    <span class="info-value">${formatDateTime(item.timing?.submitted_at)}</span>
                </div>
                ${item.timing?.started_at ? `
                <div class="info-row">
                    <span class="info-label">Started</span>
                    <span class="info-value">${formatDateTime(item.timing.started_at)}</span>
                </div>
                ` : ''}
                ${item.timing?.ended_at ? `
                <div class="info-row">
                    <span class="info-label">Ended</span>
                    <span class="info-value">${formatDateTime(item.timing.ended_at)}</span>
                </div>
                ` : ''}
                <div class="code-section">
                    <div class="code-header">
                        <span class="code-label">Code</span>
                        ${exitCode !== null && exitCode !== undefined ? `
                            <span class="exit-code ${isSuccess ? 'success' : 'error'}">Exit: ${exitCode}</span>
                        ` : ''}
                    </div>
                    <pre class="code-block"><code class="tcl code-content">${escapeHtml(item.code)}</code></pre>
                </div>
                ${preview ? `
                <div class="output-section">
                    <div class="output-header">
                        <span class="output-label">Output Preview</span>
                        <span class="output-stats">
                            ${item.result_summary?.output?.total_bytes ? `${(item.result_summary.output.total_bytes / 1024).toFixed(1)} KB` : ''}
                            ${item.result_summary?.output?.truncated ? '(truncated)' : ''}
                        </span>
                    </div>
                    <pre class="output-block"><code class="output-content">${escapeHtml(truncate(preview, 5000))}</code></pre>
                </div>
                ` : ''}
                ${item.result_summary?.error ? `
                <div class="output-section">
                    <div class="output-header">
                        <span class="output-label">Error</span>
                    </div>
                    <pre class="output-block" style="border-left: 3px solid var(--status-error);"><code class="output-content" style="color: var(--status-error);">${escapeHtml(truncate(item.result_summary.error.message, 5000))}</code></pre>
                </div>
                ` : ''}
            </div>
        </div>
    `;
}

// ============================================
// Modal Control
// ============================================

/**
 * Open session detail modal
 */
function openSessionDetail(sessionId) {
    AppState.selectedSessionId = sessionId;
    renderSessions(); // Update card expanded state

    const overlay = document.getElementById('detail-overlay');
    overlay.style.display = 'flex';
    document.body.style.overflow = 'hidden';

    renderSessionDetail(sessionId);
}

/**
 * Close session detail modal
 */
function closeSessionDetail() {
    const overlay = document.getElementById('detail-overlay');
    overlay.style.display = 'none';
    document.body.style.overflow = '';

    AppState.selectedSessionId = null;
    renderSessions(); // Update card expanded state
}

// ============================================
// Data Fetching
// ============================================

/**
 * Fetch all data from API
 */
async function fetchData() {
    if (AppState.isRefreshing) return;
    AppState.isRefreshing = true;

    try {
        // Fetch capabilities and sessions in parallel
        const [caps, sessionsData] = await Promise.all([
            Api.getCapabilities(),
            Api.listSessions()
        ]);

        AppState.capabilities = caps;
        AppState.allSessions = sessionsData.sessions || [];
        AppState.connected = true;
        AppState.error = null;
        AppState.lastFetchTime = new Date().toISOString();
        AppState.isLoading = false;

        // Adjust refresh rate based on current state
        AppState.adjustRefreshRate();

    } catch (error) {
        console.error('Failed to fetch data:', error);
        AppState.connected = false;
        AppState.error = error.message;
        // Only set isLoading to false if we already loaded once
        if (!AppState.isLoading) {
            // Keep showing old data with error indicator
        } else {
            AppState.isLoading = false;
        }
    } finally {
        AppState.isRefreshing = false;
    }
}

/**
 * Manual refresh
 */
async function refresh() {
    await fetchData();
}

// ============================================
// Confirm Dialog
// ============================================
let _confirmCallback = null;

function showConfirmDialog(message, onConfirm) {
    const overlay = document.getElementById('confirm-overlay');
    const msgEl = document.getElementById('confirm-message');
    const okBtn = document.getElementById('confirm-ok');
    const cancelBtn = document.getElementById('confirm-cancel');

    msgEl.innerHTML = message;
    _confirmCallback = onConfirm;

    overlay.style.display = 'flex';

    // Handle buttons
    const handleOk = () => {
        if (_confirmCallback) {
            _confirmCallback();
        }
    };

    const handleCancel = () => {
        closeConfirmDialog();
    };

    // Remove old listeners and add new ones
    okBtn.replaceWith(okBtn.cloneNode(true));
    cancelBtn.replaceWith(cancelBtn.cloneNode(true));

    document.getElementById('confirm-ok').addEventListener('click', handleOk);
    document.getElementById('confirm-cancel').addEventListener('click', handleCancel);

    // Close on overlay click
    overlay.onclick = (e) => {
        if (e.target === overlay) {
            handleCancel();
        }
    };

    // Close on Escape
    const handleEscape = (e) => {
        if (e.key === 'Escape') {
            handleCancel();
            document.removeEventListener('keydown', handleEscape);
        }
    };
    document.addEventListener('keydown', handleEscape);
}

function closeConfirmDialog() {
    const overlay = document.getElementById('confirm-overlay');
    overlay.style.display = 'none';
    _confirmCallback = null;
}

// ============================================
// UI Event Handlers
// ============================================

function setupEventListeners() {
    // Auto-refresh toggle
    document.getElementById('auto-refresh').addEventListener('change', (e) => {
        AppState.toggleAutoRefresh();
        if (AppState.autoRefresh) {
            AppState.startPolling();
        }
    });

    // Refresh button
    document.getElementById('refresh-btn').addEventListener('click', refresh);

    // Session card buttons (delegated)
    document.getElementById('sessions-grid').addEventListener('click', (e) => {
        // Check if clicking on a button area
        const closeBtn = e.target.closest('.close-session-btn');
        if (closeBtn) {
            e.stopPropagation();
            const card = closeBtn.closest('.session-card');
            const sessionId = card.dataset.sessionId;

            // Find the session data before closing
            const sessionData = AppState.allSessions.find(s => s.workspace_session_id === sessionId);

            showConfirmDialog(
                `Are you sure you want to close session <strong>${sessionId}</strong>?`,
                async () => {
                    try {
                        await Api.closeSession(sessionId);
                        closeConfirmDialog();

                        // Save to closed sessions (persisted to localStorage)
                        if (sessionData) {
                            addClosedSession(sessionData);
                        }

                        // Refresh the list
                        await fetchData();
                        AppState.notify();
                    } catch (error) {
                        alert('Failed to close session: ' + error.message);
                    }
                }
            );
            return;
        }

        const viewBtn = e.target.closest('.view-history-btn');
        if (viewBtn) {
            e.stopPropagation();
            const card = viewBtn.closest('.session-card');
            const sessionId = card.dataset.sessionId;
            openSessionDetail(sessionId);
            return;
        }

        // Click on card body opens detail
        const card = e.target.closest('.session-card');
        if (card && !e.target.closest('.card-footer')) {
            const sessionId = card.dataset.sessionId;
            openSessionDetail(sessionId);
        }
    });

    // Search input
    let searchTimeout;
    document.getElementById('search-input').addEventListener('input', (e) => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            AppState.searchQuery = e.target.value;
            AppState.currentPage = 1;
            AppState.notify();
        }, 300);
    });

    // Filter tabs
    document.getElementById('filter-tabs').addEventListener('click', (e) => {
        const tab = e.target.closest('.filter-tab');
        if (!tab) return;
        document.querySelectorAll('.filter-tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        AppState.filter = tab.dataset.filter;
        AppState.currentPage = 1;
        AppState.notify();
    });

    // Sort select
    document.getElementById('sort-select').addEventListener('change', (e) => {
        const [sortBy, sortOrder] = e.target.value.split('-');
        AppState.sortBy = sortBy;
        AppState.sortOrder = sortOrder;
        AppState.currentPage = 1;
        AppState.notify();
    });

    // Pagination
    document.getElementById('page-prev').addEventListener('click', () => {
        if (AppState.currentPage > 1) {
            AppState.currentPage--;
            AppState.notify();
        }
    });
    document.getElementById('page-next').addEventListener('click', () => {
        if (AppState.currentPage < AppState.totalPages) {
            AppState.currentPage++;
            AppState.notify();
        }
    });

    // Close modal button
    document.getElementById('close-detail').addEventListener('click', closeSessionDetail);

    // Close modal on overlay click
    document.getElementById('detail-overlay').addEventListener('click', (e) => {
        if (e.target.id === 'detail-overlay') {
            closeSessionDetail();
        }
    });

    // Close modal on Escape key
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            closeSessionDetail();
        }
    });
}

// ============================================
// State Subscription
// ============================================

/**
 * Handle state changes
 */
function onStateChange(state) {
    renderConnectionStatus();
    renderLastUpdate();
    renderCapabilities();
    renderSessions();
    renderClosedSessions();
    updateNavCounts();
}

/**
 * Update navigation badge counts
 */
function updateNavCounts() {
    const activeCount = document.getElementById('nav-active-count');
    const closedCount = document.getElementById('nav-closed-count');

    if (activeCount) {
        activeCount.textContent = AppState.allSessions.length;
    }
    if (closedCount) {
        closedCount.textContent = AppState.closedSessions.length;
    }
}

// ============================================
// Initialization
// ============================================

export async function initApp() {
    // Load closed sessions from localStorage
    AppState.closedSessions = loadClosedSessions();

    // Setup navigation
    setupNavigation();

    // Setup event listeners
    setupEventListeners();

    // Subscribe to state changes
    AppState.subscribe(onStateChange);

    // Initial data fetch
    await fetchData();

    // Start polling
    AppState.startPolling();

    // Expose refresh function globally for retry button
    window.app = { refresh };
}

/**
 * Setup sidebar navigation
 */
function setupNavigation() {
    const navItems = document.querySelectorAll('.nav-item');
    const activeView = document.getElementById('active-sessions-view');
    const closedView = document.getElementById('recently-closed-view');

    navItems.forEach(item => {
        item.addEventListener('click', () => {
            const view = item.dataset.view;

            // Update active nav item
            navItems.forEach(n => n.classList.remove('active'));
            item.classList.add('active');

            // Show/hide views
            if (view === 'active-sessions') {
                activeView.style.display = 'block';
                closedView.style.display = 'none';
            } else if (view === 'recently-closed') {
                activeView.style.display = 'none';
                closedView.style.display = 'block';
            }
        });
    });
}

// Start the app when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
} else {
    initApp();
}
