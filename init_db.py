import mysql.connector
import bcrypt
from datetime import datetime

def init_db():
    try:
        # Connect to MySQL database
        conn = mysql.connector.connect(
            host='localhost',
            port=3307,
            user='root',
            password='',
            database='vaid_ems'
        )
        
        cursor = conn.cursor()
        
        # Create ems table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ems (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Email VARCHAR(255) UNIQUE NOT NULL,
                Name VARCHAR(255) NOT NULL,
                Domain VARCHAR(100),
                Role VARCHAR(50) NOT NULL,
                Pass VARCHAR(255) NOT NULL,
                Mobile VARCHAR(20),
                Adhaar VARCHAR(20),
                Attendance INTEGER DEFAULT 0,
                Leaves INTEGER DEFAULT 0,
                Permission VARCHAR(50) DEFAULT 'basic',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create leave_applications table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS leave_applications (
                id INT AUTO_INCREMENT PRIMARY KEY,
                employee_email VARCHAR(255) NOT NULL,
                subject VARCHAR(255) NOT NULL,
                body TEXT NOT NULL,
                status VARCHAR(50) DEFAULT 'Pending',
                request_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (employee_email) REFERENCES ems(Email)
            )
        ''')
        
        # Create work_log table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                employee_email VARCHAR(255) NOT NULL,
                subject VARCHAR(255) NOT NULL,
                body TEXT NOT NULL,
                status VARCHAR(50) DEFAULT 'Pending',
                assigned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                deadline DATE,
                FOREIGN KEY (employee_email) REFERENCES ems(Email)
            )
        ''')
        
        # Insert default admin user if not exists
        admin_password = 'admin123'
        hashed_password = bcrypt.hashpw(admin_password.encode('utf-8'), bcrypt.gensalt())
        
        cursor.execute('''
            INSERT IGNORE INTO ems 
            (Email, Name, Domain, Role, Pass, Permission)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            'admin@example.com',
            'Admin User',
            'Administration',
            'Admin',
            hashed_password.decode('utf-8'),
            'admin'
        ))
        
        conn.commit()
        print("Database initialized successfully!")
        
    except Exception as e:
        print(f"Error initializing database: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == '__main__':
    init_db() 
