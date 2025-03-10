import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_mysql_connection():
    try:
        return mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_NAME', 'vaidpr_ems')
        )
    except Exception as e:
        print(f"MySQL connection error: {e}")
        return None

def get_postgres_connection():
    try:
        return psycopg2.connect(os.getenv('DATABASE_URL'))
    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        return None

def create_postgres_tables(pg_conn):
    try:
        cursor = pg_conn.cursor()
        
        # Create ems table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ems (
                id SERIAL PRIMARY KEY,
                Email VARCHAR(255) UNIQUE NOT NULL,
                Name VARCHAR(255) NOT NULL,
                Domain VARCHAR(255),
                Role VARCHAR(50) NOT NULL,
                Pass VARCHAR(255) NOT NULL,
                Mobile VARCHAR(20),
                Adhaar VARCHAR(20),
                Attendance INTEGER DEFAULT 0,
                Leaves INTEGER DEFAULT 0,
                Permission VARCHAR(50) DEFAULT 'basic'
            )
        ''')

        # Create leave_applications table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS leave_applications (
                id SERIAL PRIMARY KEY,
                employee_email VARCHAR(255) REFERENCES ems(Email),
                subject VARCHAR(255) NOT NULL,
                body TEXT,
                status VARCHAR(50) DEFAULT 'Pending',
                request_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create work_log table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_log (
                id SERIAL PRIMARY KEY,
                employee_email VARCHAR(255) REFERENCES ems(Email),
                subject VARCHAR(255) NOT NULL,
                body TEXT,
                status VARCHAR(50) DEFAULT 'Pending',
                assigned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                deadline DATE
            )
        ''')

        pg_conn.commit()
        print("PostgreSQL tables created successfully")
    except Exception as e:
        print(f"Error creating PostgreSQL tables: {e}")
        pg_conn.rollback()
    finally:
        cursor.close()

def migrate_data():
    mysql_conn = get_mysql_connection()
    pg_conn = get_postgres_connection()

    if not mysql_conn or not pg_conn:
        print("Failed to connect to one or both databases")
        return

    try:
        # Create PostgreSQL tables
        create_postgres_tables(pg_conn)

        # Migrate ems table
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        pg_cursor = pg_conn.cursor()

        # Get data from MySQL
        mysql_cursor.execute('SELECT * FROM ems')
        ems_data = mysql_cursor.fetchall()

        # Insert into PostgreSQL
        if ems_data:
            insert_query = '''
                INSERT INTO ems (Email, Name, Domain, Role, Pass, Mobile, Adhaar, Attendance, Leaves, Permission)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (Email) DO UPDATE SET
                    Name = EXCLUDED.Name,
                    Domain = EXCLUDED.Domain,
                    Role = EXCLUDED.Role,
                    Pass = EXCLUDED.Pass,
                    Mobile = EXCLUDED.Mobile,
                    Adhaar = EXCLUDED.Adhaar,
                    Attendance = EXCLUDED.Attendance,
                    Leaves = EXCLUDED.Leaves,
                    Permission = EXCLUDED.Permission
            '''
            values = [(
                row['Email'], row['Name'], row['Domain'], row['Role'],
                row['Pass'], row['Mobile'], row['Adhaar'],
                row['Attendance'], row['Leaves'], row['Permission']
            ) for row in ems_data]
            execute_values(pg_cursor, insert_query, values)

        # Migrate leave_applications table
        mysql_cursor.execute('SELECT * FROM leave_applications')
        leave_data = mysql_cursor.fetchall()

        if leave_data:
            insert_query = '''
                INSERT INTO leave_applications (employee_email, subject, body, status, request_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    employee_email = EXCLUDED.employee_email,
                    subject = EXCLUDED.subject,
                    body = EXCLUDED.body,
                    status = EXCLUDED.status,
                    request_date = EXCLUDED.request_date
            '''
            values = [(
                row['employee_email'], row['subject'], row['body'],
                row['status'], row['request_date']
            ) for row in leave_data]
            execute_values(pg_cursor, insert_query, values)

        # Migrate work_log table
        mysql_cursor.execute('SELECT * FROM work_log')
        work_data = mysql_cursor.fetchall()

        if work_data:
            insert_query = '''
                INSERT INTO work_log (employee_email, subject, body, status, assigned_date, deadline)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    employee_email = EXCLUDED.employee_email,
                    subject = EXCLUDED.subject,
                    body = EXCLUDED.body,
                    status = EXCLUDED.status,
                    assigned_date = EXCLUDED.assigned_date,
                    deadline = EXCLUDED.deadline
            '''
            values = [(
                row['employee_email'], row['subject'], row['body'],
                row['status'], row['assigned_date'], row['deadline']
            ) for row in work_data]
            execute_values(pg_cursor, insert_query, values)

        pg_conn.commit()
        print("Data migration completed successfully")

    except Exception as e:
        print(f"Error during migration: {e}")
        pg_conn.rollback()
    finally:
        mysql_cursor.close()
        pg_cursor.close()
        mysql_conn.close()
        pg_conn.close()

if __name__ == "__main__":
    migrate_data() 