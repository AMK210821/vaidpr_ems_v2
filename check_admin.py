import mysql.connector
from config.config import Config
import bcrypt

def check_admin():
    try:
        # Connect to database
        conn = mysql.connector.connect(
            host=Config.DB_HOST,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            database=Config.DB_NAME
        )
        cursor = conn.cursor(dictionary=True)

        # Check if admin exists
        cursor.execute('SELECT * FROM ems WHERE Email = %s', ('admin@vaidpr.com',))
        admin = cursor.fetchone()

        if admin:
            print("Admin user found:")
            print(f"ID: {admin['id']}")
            print(f"Email: {admin['Email']}")
            print(f"Role: {admin['Role']}")
            print(f"Password Hash: {admin['Pass']}")

            # Test password verification
            test_password = 'admin123'
            try:
                if bcrypt.checkpw(test_password.encode('utf-8'), admin['Pass'].encode('utf-8')):
                    print("\nPassword 'admin123' is correct!")
                else:
                    print("\nPassword 'admin123' is incorrect!")
                    
                # Create a new hash for reference
                new_hash = bcrypt.hashpw('admin123'.encode('utf-8'), bcrypt.gensalt())
                print(f"\nNew hash for 'admin123': {new_hash.decode('utf-8')}")
                
            except Exception as e:
                print(f"\nError checking password: {e}")
        else:
            print("Admin user not found!")

            # Create admin user
            hashed = bcrypt.hashpw('admin123'.encode('utf-8'), bcrypt.gensalt())
            cursor.execute("""
                INSERT INTO ems 
                (Email, Name, Domain, Role, Pass, Mobile, Adhaar, Permission) 
                VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                'admin@vaidpr.com', 'Admin', 'Administration', 'Admin',
                hashed.decode('utf-8'), '9999999999', '999999999999', 'all'
            ))
            conn.commit()
            print("Created new admin user!")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_admin() 