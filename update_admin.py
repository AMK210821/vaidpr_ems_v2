import mysql.connector
from config.config import Config
import bcrypt

def update_admin_password():
    try:
        # Connect to database
        conn = mysql.connector.connect(
            host=Config.DB_HOST,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            database=Config.DB_NAME
        )
        cursor = conn.cursor()

        # Generate new password hash
        password = 'admin123'
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        
        # Update admin password
        cursor.execute("""
            UPDATE ems 
            SET Pass = %s 
            WHERE Email = 'admin@vaidpr.com'
        """, (hashed.decode('utf-8'),))
        
        conn.commit()
        print("Admin password updated successfully!")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    update_admin_password() 