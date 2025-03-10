// API Configuration
const config = {
    // Production API URL (use your Render domain)
    API_BASE_URL: 'https://your-app-name.onrender.com',
    
    // Fallback IP addresses if domain is not available
    API_FALLBACK_URLS: [
        'http://52.41.36.82',
        'http://54.191.253.12',
        'http://44.226.122.3'
    ],
    
    // Current active API URL
    currentApiUrl: null
};

// Function to test API availability
async function testApiAvailability(url) {
    try {
        const response = await fetch(`${url}/health`);
        return response.ok;
    } catch (error) {
        console.error(`Error testing ${url}:`, error);
        return false;
    }
}

// Initialize API URL
async function initializeApiUrl() {
    // Try the main domain first
    if (await testApiAvailability(config.API_BASE_URL)) {
        config.currentApiUrl = config.API_BASE_URL;
        return;
    }

    // Try fallback IPs
    for (const url of config.API_FALLBACK_URLS) {
        if (await testApiAvailability(url)) {
            config.currentApiUrl = url;
            return;
        }
    }

    // If no URL works, use the main domain as fallback
    config.currentApiUrl = config.API_BASE_URL;
}

// Export configuration
export default config; 