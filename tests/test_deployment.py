import os
import unittest
import requests
from dotenv import load_dotenv
import urllib.parse
import psycopg2
from psycopg2.extras import RealDictCursor

# Load environment variables
load_dotenv()

class TestDeployment(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Database connection parameters
        cls.db_params = {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # API base URL
        cls.base_url = os.getenv('API_BASE_URL', 'http://localhost:5000')
        
        # Test credentials
        cls.test_user = {
            'email': 'admin@example.com',
            'password': 'admin123'
        }

    def test_environment_variables(self):
        """Test if all required environment variables are set"""
        required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'SECRET_KEY']
        for var in required_vars:
            self.assertIsNotNone(os.getenv(var), f"Environment variable {var} is not set")

    def test_database_connection(self):
        """Test database connection"""
        try:
            conn = psycopg2.connect(**self.db_params)
            self.assertTrue(conn.closed == 0)
            conn.close()
        except Exception as e:
            self.fail(f"Database connection failed: {str(e)}")

    def test_api_health_check(self):
        """Test API health check endpoint"""
        try:
            response = requests.get(f"{self.base_url}/health")
            self.assertEqual(response.status_code, 200)
        except requests.exceptions.ConnectionError:
            print("Warning: API server is not running. Skipping API tests.")
            self.skipTest("API server is not running")

    def test_authentication(self):
        """Test user authentication"""
        try:
            # Create a session
            session = requests.Session()
            
            # Test login
            response = session.post(
                f"{self.base_url}/login",
                data=self.test_user
            )
            self.assertEqual(response.status_code, 200)
            
            # Test protected endpoint
            response = session.get(f"{self.base_url}/dashboard")
            self.assertEqual(response.status_code, 200)
            
            # Test logout
            response = session.get(f"{self.base_url}/logout")
            self.assertEqual(response.status_code, 200)
            
        except requests.exceptions.ConnectionError:
            print("Warning: API server is not running. Skipping authentication tests.")
            self.skipTest("API server is not running")

    def test_file_upload(self):
        """Test file upload functionality"""
        try:
            # Create a session
            session = requests.Session()
            
            # Login
            response = session.post(
                f"{self.base_url}/login",
                data=self.test_user
            )
            self.assertEqual(response.status_code, 200)
            
            # Create a test file
            test_file = {'file': ('test.txt', 'Hello, World!')}
            
            # Test file upload
            response = session.post(
                f"{self.base_url}/upload",
                files=test_file
            )
            self.assertEqual(response.status_code, 200)
            
        except requests.exceptions.ConnectionError:
            print("Warning: API server is not running. Skipping file upload test.")
            self.skipTest("API server is not running")

    def test_database_operations(self):
        """Test basic database operations"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Test SELECT
            cur.execute("SELECT 1")
            result = cur.fetchone()
            self.assertEqual(result['?column?'], 1)
            
            # Test INSERT
            cur.execute("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50)
                )
            """)
            cur.execute("INSERT INTO test_table (name) VALUES (%s)", ('test',))
            
            # Test UPDATE
            cur.execute("UPDATE test_table SET name = %s WHERE name = %s", ('updated', 'test'))
            
            # Test DELETE
            cur.execute("DELETE FROM test_table")
            
            # Cleanup
            cur.execute("DROP TABLE test_table")
            
            conn.commit()
            cur.close()
            conn.close()
            
        except Exception as e:
            self.fail(f"Database operations failed: {str(e)}")

if __name__ == '__main__':
    unittest.main() 