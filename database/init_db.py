import mysql.connector
from config.config import Config

def init_db():
    try:
        # Connect to MySQL server
        conn = mysql.connector.connect(
            host=Config.DB_HOST,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD
        )
        cursor = conn.cursor()

        # Create database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {Config.DB_NAME}")
        cursor.execute(f"USE {Config.DB_NAME}")

        # Create ems table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ems (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Email VARCHAR(255) UNIQUE NOT NULL,
                Name VARCHAR(255) NOT NULL,
                Domain VARCHAR(255) NOT NULL,
                Role VARCHAR(255) NOT NULL,
                Pass VARCHAR(255) NOT NULL,
                Mobile VARCHAR(15) UNIQUE NOT NULL,
                Adhaar VARCHAR(12) UNIQUE NOT NULL,
                dob date NOT NULL,
                Attendance INT DEFAULT 0,
                Leaves INT DEFAULT 0,
                Permission VARCHAR(225) NOT NULL,
                CONSTRAINT chk_mobile CHECK (LENGTH(Mobile) >= 10),
                CONSTRAINT chk_adhaar CHECK (LENGTH(Adhaar) = 12)
            )
        """)

        # Create work_log table with additional fields
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS work_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                employee_email VARCHAR(255) NOT NULL,
                subject VARCHAR(255) NOT NULL,
                body TEXT NOT NULL,
                assigned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                deadline DATETIME NOT NULL,
                status ENUM('Pending', 'In Progress', 'Completed', 'Delayed') DEFAULT 'Pending',
                FOREIGN KEY (employee_email) REFERENCES ems(Email)
            )
        """)

        # Create leave_applications table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS leave_applications (
                id INT AUTO_INCREMENT PRIMARY KEY,
                employee_email VARCHAR(255) NOT NULL,
                subject VARCHAR(255) NOT NULL,
                body TEXT NOT NULL,
                status ENUM('Pending', 'Accepted', 'Declined') DEFAULT 'Pending',
                request_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (employee_email) REFERENCES ems(Email)
            )
        """)

        # Insert default admin user if not exists
        cursor.execute("""
            INSERT IGNORE INTO ems 
            (Email, Name, Domain, Role, Pass, Mobile, Adhaar, Permission) 
            VALUES 
            ('admin@vaidpr.com', 'Admin', 'Administration', 'Admin', 
             '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdsBHrpxFPH.G2.', 
             '9999999999', '123456789012', 'all')
        """)

        conn.commit()
        print("Database initialized successfully!")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    init_db() 
