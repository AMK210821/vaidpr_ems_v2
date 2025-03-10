import config from './config.js';

// Initialize API URL when the script loads
await config.initializeApiUrl();

// API utility functions
const api = {
    // Base URL getter
    get baseUrl() {
        return config.currentApiUrl;
    },

    // Generic fetch wrapper
    async fetch(endpoint, options = {}) {
        const url = `${this.baseUrl}${endpoint}`;
        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json',
                ...options.headers
            },
            credentials: 'include',
            ...options
        };

        try {
            const response = await fetch(url, defaultOptions);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            console.error('API Error:', error);
            throw error;
        }
    },

    // Example API methods
    async login(credentials) {
        return this.fetch('/login', {
            method: 'POST',
            body: JSON.stringify(credentials)
        });
    },

    async getLeaveDetails() {
        return this.fetch('/leave-details');
    },

    // Add more API methods as needed
};

export default api; 