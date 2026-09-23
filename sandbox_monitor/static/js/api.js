/**
 * Sandbox Monitor - API Client
 * Handles all API calls to the proxy server
 */

const API_BASE = '/api';

export const Api = {
    /**
     * Get sandbox capabilities
     */
    async getCapabilities() {
        const resp = await fetch(`${API_BASE}/capabilities`);
        if (!resp.ok) {
            throw new Error(`Failed to fetch capabilities: ${resp.status}`);
        }
        return resp.json();
    },

    /**
     * List all active sessions
     */
    async listSessions() {
        const resp = await fetch(`${API_BASE}/sessions`);
        if (!resp.ok) {
            throw new Error(`Failed to fetch sessions: ${resp.status}`);
        }
        return resp.json();
    },

    /**
     * Get detailed info for a specific session
     */
    async getSession(sessionId) {
        const resp = await fetch(
            `${API_BASE}/sessions/${encodeURIComponent(sessionId)}`
        );
        if (!resp.ok) {
            throw new Error(`Failed to fetch session ${sessionId}: ${resp.status}`);
        }
        return resp.json();
    },

    /**
     * Get execution history for a session
     * @param {string} sessionId - Session ID
     * @param {number} limit - Max records to fetch (0 = all)
     * @param {string} cursor - Pagination cursor
     * @param {string} order - Sort order: 'asc' (oldest first) or 'desc' (newest first, default)
     */
    async getSessionHistory(sessionId, limit = 50, cursor = null, order = 'asc') {
        // limit=0 means fetch all records
        let url = `${API_BASE}/sessions/${encodeURIComponent(sessionId)}/history?limit=${limit}&order=${order}`;
        if (cursor) {
            url += `&cursor=${encodeURIComponent(cursor)}`;
        }
        const resp = await fetch(url);
        if (!resp.ok) {
            throw new Error(
                `Failed to fetch history for ${sessionId}: ${resp.status}`
            );
        }
        return resp.json();
    },

    /**
     * Health check
     */
    async healthCheck() {
        const resp = await fetch(`${API_BASE}/health`);
        return resp.json();
    },

    /**
     * Destroy a workspace session
     * @param {string} sessionId - Session ID to destroy
     */
    async closeSession(sessionId) {
        const resp = await fetch(
            `${API_BASE}/sessions/${encodeURIComponent(sessionId)}`,
            { method: 'DELETE' }
        );
        if (!resp.ok) {
            throw new Error(`Failed to close session ${sessionId}: ${resp.status}`);
        }
        return resp.json();
    }
};
