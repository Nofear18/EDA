/**
 * Sandbox Monitor - State Management
 * Centralized state with pub/sub pattern
 */

// Default refresh intervals (in milliseconds)
const DEFAULT_REFRESH_INTERVAL = 5000;  // 5 seconds
const FAST_REFRESH_INTERVAL = 2000;      // 2 seconds for running executions
const CLOSING_REFRESH_INTERVAL = 3000;   // 3 seconds for closing sessions

// Pagination settings
const PAGE_SIZE = 20;
const MAX_CLOSED_SESSIONS = 20;  // Max closed sessions to keep in memory

export const AppState = {
    // Data cache
    capabilities: null,
    allSessions: [],  // Raw data from API
    closedSessions: [],  // Recently closed sessions (kept in memory)

    // UI state
    isLoading: true,
    isRefreshing: false,
    error: null,

    // Connection state
    connected: false,
    lastFetchTime: null,

    // Refresh configuration
    autoRefresh: true,
    refreshInterval: DEFAULT_REFRESH_INTERVAL,

    // Selection state
    selectedSessionId: null,
    expandedExecutionId: null,

    // Filtering & Pagination
    filter: 'all',           // 'all' | 'active' | 'closing' | 'closed'
    searchQuery: '',
    sortBy: 'created_at',    // 'created_at' | 'last_active_at'
    sortOrder: 'desc',       // 'asc' | 'desc'
    currentPage: 1,
    pageSize: PAGE_SIZE,

    // Computed sessions (filtered + sorted)
    get sessions() {
        let filtered = this.allSessions;

        // Apply runtime_state filter
        if (this.filter !== 'all') {
            const filterUpper = this.filter.toUpperCase();
            filtered = filtered.filter(s => s.runtime_state?.toUpperCase() === filterUpper);
        }

        // Apply search filter
        if (this.searchQuery.trim()) {
            const query = this.searchQuery.toLowerCase();
            filtered = filtered.filter(s =>
                s.workspace_session_id?.toLowerCase().includes(query) ||
                (s.tool_kind && s.tool_kind.toLowerCase().includes(query)) ||
                (s.version && s.version.toLowerCase().includes(query))
            );
        }

        // Apply sorting
        filtered = [...filtered].sort((a, b) => {
            let valA = a[this.sortBy] || '';
            let valB = b[this.sortBy] || '';
            // Handle null/undefined
            if (!valA && !valB) return 0;
            if (!valA) return 1;
            if (!valB) return -1;
            // String comparison for other fields
            const cmp = valA < valB ? -1 : valA > valB ? 1 : 0;
            return this.sortOrder === 'desc' ? -cmp : cmp;
        });

        return filtered;
    },

    // Total pages
    get totalPages() {
        return Math.max(1, Math.ceil(this.sessions.length / this.pageSize));
    },

    // Paginated sessions
    get paginatedSessions() {
        const start = (this.currentPage - 1) * this.pageSize;
        return this.sessions.slice(start, start + this.pageSize);
    },

    // Session counts by runtime_state
    get sessionCounts() {
        const counts = {
            all: this.allSessions.length,
            READY: 0,
            BUSY: 0,
            LOST: 0,
            STOPPED: 0
        };
        this.allSessions.forEach(s => {
            const rs = s.runtime_state?.toUpperCase();
            if (rs === 'READY') counts.READY++;
            else if (rs === 'BUSY') counts.BUSY++;
            else if (rs === 'LOST') counts.LOST++;
            else if (rs === 'STOPPED') counts.STOPPED++;
        });
        return counts;
    },

    // Polling timer
    _pollingTimer: null,

    // Subscribers
    _listeners: [],

    /**
     * Subscribe to state changes
     * @param {Function} callback - Function to call on state change
     * @returns {Function} Unsubscribe function
     */
    subscribe(callback) {
        this._listeners.push(callback);
        // Return unsubscribe function
        return () => {
            this._listeners = this._listeners.filter(cb => cb !== callback);
        };
    },

    /**
     * Notify all subscribers of state change
     */
    notify() {
        this._listeners.forEach(cb => cb(this));
    },

    /**
     * Update state and notify subscribers
     * @param {Object} updates - State properties to update
     */
    setState(updates) {
        Object.assign(this, updates);
        this.notify();
    },

    /**
     * Start automatic polling
     */
    startPolling() {
        this.stopPolling();
        if (this.autoRefresh) {
            this._pollingTimer = setInterval(() => {
                this._triggerRefresh();
            }, this.refreshInterval);
        }
    },

    /**
     * Stop automatic polling
     */
    stopPolling() {
        if (this._pollingTimer) {
            clearInterval(this._pollingTimer);
            this._pollingTimer = null;
        }
    },

    /**
     * Adjust refresh rate based on session states
     */
    adjustRefreshRate() {
        if (!this.autoRefresh) return;

        const hasRunning = this.sessions.some(s =>
            s.runtime_state === 'BUSY' ||
            s.state === 'CLOSING'
        );

        if (hasRunning) {
            this.refreshInterval = this.sessions.some(s => s.runtime_state === 'BUSY')
                ? FAST_REFRESH_INTERVAL
                : CLOSING_REFRESH_INTERVAL;
        } else {
            this.refreshInterval = DEFAULT_REFRESH_INTERVAL;
        }

        // Restart polling with new interval
        if (this._pollingTimer) {
            this.startPolling();
        }
    },

    /**
     * Toggle auto refresh
     */
    toggleAutoRefresh() {
        this.autoRefresh = !this.autoRefresh;
        if (this.autoRefresh) {
            this.startPolling();
        } else {
            this.stopPolling();
        }
        this.notify();
    },

    /**
     * Internal method to trigger refresh
     * Override this with actual API calls in app.js
     */
    _triggerRefresh() {
        // This is a placeholder - actual implementation in app.js
        this.notify();
    }
};
