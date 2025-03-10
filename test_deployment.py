import requests
import json
from datetime import datetime

def test_health_endpoint(base_url):
    """Test the health check endpoint"""
    try:
        response = requests.get(f"{base_url}/health")
        print("\nHealth Check Test:")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"Health check failed: {e}")
        return False

def test_login(base_url):
    """Test the login endpoint"""
    try:
        data = {
            "email": "admin@example.com",  # Replace with test credentials
            "password": "admin123"
        }
        response = requests.post(f"{base_url}/login", json=data)
        print("\nLogin Test:")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text[:200]}...")  # Print first 200 chars
        return response.status_code in [200, 302]
    except Exception as e:
        print(f"Login test failed: {e}")
        return False

def test_database_connection(base_url):
    """Test database connection through API"""
    try:
        response = requests.get(f"{base_url}/dashboard")
        print("\nDatabase Connection Test:")
        print(f"Status Code: {response.status_code}")
        return response.status_code in [200, 302]
    except Exception as e:
        print(f"Database connection test failed: {e}")
        return False

def run_tests():
    # Test URLs
    urls = [
        "http://localhost:5000",  # Local development
        "http://52.41.36.82",    # Render IP 1
        "http://54.191.253.12",  # Render IP 2
        "http://44.226.122.3"    # Render IP 3
    ]

    print("Starting Deployment Tests...")
    print(f"Test Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    for url in urls:
        print(f"\nTesting URL: {url}")
        print("=" * 50)
        
        health_status = test_health_endpoint(url)
        login_status = test_login(url)
        db_status = test_database_connection(url)
        
        print("\nTest Summary:")
        print(f"Health Check: {'✓' if health_status else '✗'}")
        print(f"Login: {'✓' if login_status else '✗'}")
        print(f"Database: {'✓' if db_status else '✗'}")
        print("=" * 50)

if __name__ == "__main__":
    run_tests() 