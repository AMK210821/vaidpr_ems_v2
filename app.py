from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, send_file
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_cors import CORS
import mysql.connector
import bcrypt
from config.config import Config
from functools import wraps
from datetime import datetime, timedelta
import os
from werkzeug.utils import secure_filename
import json
import logging
from logging.handlers import RotatingFileHandler
import traceback

app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = Config.SECRET_KEY  # Use secret key from config

# Configure CORS
CORS(app, resources={
    r"/*": {
        "origins": [
            "http://52.41.36.82",
            "http://54.191.253.12",
            "http://44.226.122.3",
            "https://52.41.36.82",
            "https://54.191.253.12",
            "https://44.226.122.3"
        ],
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = 'Please log in to access this page.'
login_manager.login_message_category = 'error'

# Database connection
def get_db_connection():
    try:
        # Connect to MySQL database on Railway
        return mysql.connector.connect(
            host='trolley.proxy.rlwy.net',
            port=19855,
            user='root',
            password='hucNoZjKVOsVWROObvpJrkduvyoYLIxx',
            database='railway'
        )
    except Exception as err:
        print(f"Database connection error: {err}")
        raise

# User class for Flask-Login
class User(UserMixin):
    def __init__(self, id, email, role, permission):
        self.id = id
        self.email = email
        self.role = role
        self.permission = permission

    def get_id(self):
        return str(self.email)  # Use email as the user identifier

@login_manager.user_loader
def load_user(user_id):
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        cur = conn.cursor(dictionary=True)
        cur.execute('SELECT * FROM ems WHERE Email = %s', (user_id,))
        user = cur.fetchone()
        cur.close()
        conn.close()
        
        if user:
            return User(
                id=user['id'],
                email=user['Email'],
                role=user['Role'],
                permission=user['Permission']
            )
        return None
    except Exception as e:
        print(f"Error loading user: {e}")
        return None

# Role-based access control decorator
def role_required(roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('login'))
            if current_user.role not in roles:
                flash('You do not have permission to access this page.', 'error')
                return redirect(url_for('dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

@app.route('/')
def index():
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        
        try:
            conn = get_db_connection()
            if conn is None:
                flash('Database connection error', 'error')
                return redirect(url_for('login'))
            
            cur = conn.cursor(dictionary=True)  # Use MySQL's dictionary cursor
            cur.execute('SELECT * FROM ems WHERE Email = %s', (email,))
            user = cur.fetchone()
            cur.close()
            conn.close()
            
            if user:
                try:
                    # Convert password to bytes for bcrypt
                    stored_password = user['Pass'].encode('utf-8')
                    input_password = password.encode('utf-8')
                    
                    if bcrypt.checkpw(input_password, stored_password):
                        user_obj = User(
                            id=user['id'],
                            email=user['Email'],
                            role=user['Role'],
                            permission=user['Permission']
                        )
                        login_user(user_obj)
                        flash('Login successful!', 'success')
                        return redirect(url_for('dashboard'))
                    else:
                        flash('Invalid password', 'error')
                except KeyError as e:
                    print(f"Database field error: {e}")
                    print(f"Available fields: {user.keys()}")  # Debug print available fields
                    flash('Database configuration error', 'error')
            else:
                flash('Invalid email', 'error')
                
        except Exception as e:
            print(f"Error during login: {e}")
            flash('An error occurred during login', 'error')
        
        return redirect(url_for('login'))
    
    return render_template('login.html')

@app.route('/dashboard')
@login_required
def dashboard():
    print(f"Current user: {current_user}")  # Debug log
    print(f"Current user role: {current_user.role}")  # Debug log
    print(f"Current user email: {current_user.email}")  # Debug log
    
    try:
        if current_user.role == 'Admin':
            return redirect(url_for('admin_dashboard'))
        elif current_user.role == 'HR':
            return redirect(url_for('hr_dashboard'))
        elif current_user.role == 'Employee':
            return redirect(url_for('employee_dashboard'))
        else:
            print(f"Unknown role: {current_user.role}")  # Debug log
            flash('Invalid user role', 'error')
            return redirect(url_for('login'))
    except Exception as e:
        print(f"Error in dashboard route: {e}")  # Debug log
        flash('Error accessing dashboard', 'error')
        return redirect(url_for('login'))

@app.route('/admin')
@login_required
@role_required(['Admin'])
def admin_dashboard():
    print(f"Entering admin dashboard route")  # Debug log
    domain_counts = {}
    accepted_leaves = 0
    pending_leaves = 0
    declined_leaves = 0
    recent_leaves = []
    recent_work = []
    
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor(dictionary=True)
            
            # Get employee counts by domain
            print("Fetching domain counts...")  # Debug log
            cursor.execute('''
                SELECT Domain, COUNT(*) as count 
                FROM ems 
                WHERE Role != 'Admin'
                GROUP BY Domain 
                ORDER BY count DESC
            ''')
            domain_results = cursor.fetchall()
            for row in domain_results:
                domain_counts[row['Domain']] = row['count']
            
            # Get leave counts
            print("Fetching leave counts...")  # Debug log
            cursor.execute('SELECT status, COUNT(*) as count FROM leave_applications GROUP BY status')
            status_results = cursor.fetchall()
            for row in status_results:
                if row['status'] == 'Accepted':
                    accepted_leaves = row['count']
                elif row['status'] == 'Pending':
                    pending_leaves = row['count']
                elif row['status'] == 'Declined':
                    declined_leaves = row['count']
            
            # Get recent leaves
            print("Fetching recent leaves...")  # Debug log
            cursor.execute('''
                SELECT 
                    la.id,
                    la.employee_email,
                    la.subject,
                    la.body,
                    la.status,
                    DATE_FORMAT(la.request_date, '%Y-%m-%d') as formatted_request_date,
                    e.Name as employee_name 
                FROM leave_applications la 
                JOIN ems e ON la.employee_email = e.Email 
                ORDER BY request_date DESC LIMIT 5
            ''')
            recent_leaves = cursor.fetchall()
            
            # Get recent work assignments
            print("Fetching recent work assignments...")  # Debug log
            cursor.execute('''
                SELECT 
                    w.id,
                    w.employee_email,
                    w.subject,
                    w.body,
                    w.status,
                    DATE_FORMAT(w.assigned_date, '%Y-%m-%d') as formatted_assigned_date,
                    DATE_FORMAT(w.deadline, '%Y-%m-%d') as formatted_deadline,
                    e.Name as employee_name 
                FROM work_log w 
                JOIN ems e ON w.employee_email = e.Email 
                ORDER BY w.assigned_date DESC LIMIT 5
            ''')
            recent_work = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            print("Successfully fetched all dashboard data")  # Debug log
            
    except Exception as e:
        print(f"Error fetching admin dashboard data: {e}")
        traceback.print_exc()  # Print full traceback
        flash('Error loading dashboard data', 'error')
    
    return render_template('admin/dashboard.html',
                         domain_counts=domain_counts,
                         accepted_leaves=accepted_leaves,
                         pending_leaves=pending_leaves,
                         declined_leaves=declined_leaves,
                         recent_leaves=recent_leaves,
                         recent_work=recent_work)

@app.route('/hr')
@login_required
@role_required(['HR'])
def hr_dashboard():
    total_employees = 0
    present_today = 0
    pending_leaves = 0
    active_tasks = 0
    recent_leaves = []
    recent_work = []
    
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor(dictionary=True)
            
            # Extract domain from HR email
            hr_domain = current_user.email.split('@')[-1]
            
            # Get total employees count for the HR's domain
            cursor.execute('SELECT COUNT(*) as count FROM ems WHERE Role = "Employee" AND Email LIKE %s', (f'%@{hr_domain}',))
            result = cursor.fetchone()
            total_employees = result['count'] if result else 0
            
            # Get present employees count for the HR's domain
            cursor.execute('SELECT COUNT(*) as count FROM ems WHERE Role = "Employee" AND Attendance = 1 AND Email LIKE %s', (f'%@{hr_domain}',))
            result = cursor.fetchone()
            present_today = result['count'] if result else 0
            
            # Get pending leaves count for the HR's domain
            cursor.execute('SELECT COUNT(*) as count FROM leave_applications WHERE status = "Pending" AND employee_email LIKE %s', (f'%@{hr_domain}',))
            result = cursor.fetchone()
            pending_leaves = result['count'] if result else 0
            
            # Get active tasks count for the HR's domain
            cursor.execute('SELECT COUNT(*) as count FROM work_log WHERE status = "Pending" AND employee_email LIKE %s', (f'%@{hr_domain}',))
            result = cursor.fetchone()
            active_tasks = result['count'] if result else 0
            
            # Get recent leaves for the HR's domain
            cursor.execute('''
                SELECT 
                    la.id,
                    la.employee_email,
                    la.subject,
                    la.body,
                    la.status,
                    DATE_FORMAT(la.request_date, '%Y-%m-%d') as formatted_request_date,
                    e.Name as employee_name 
                FROM leave_applications la 
                JOIN ems e ON la.employee_email = e.Email 
                WHERE la.employee_email LIKE %s
                ORDER BY request_date DESC LIMIT 5
            ''', (f'%@{hr_domain}',))
            recent_leaves = cursor.fetchall()
            
            # Get recent work assignments for the HR's domain
            cursor.execute('''
                SELECT 
                    w.id,
                    w.employee_email,
                    w.subject,
                    w.body,
                    w.status,
                    DATE_FORMAT(w.assigned_date, '%Y-%m-%d') as formatted_assigned_date,
                    DATE_FORMAT(w.deadline, '%Y-%m-%d') as formatted_deadline,
                    e.Name as employee_name 
                FROM work_log w 
                JOIN ems e ON w.employee_email = e.Email 
                WHERE w.employee_email LIKE %s
                ORDER BY w.assigned_date DESC LIMIT 5
            ''', (f'%@{hr_domain}',))
            recent_work = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Error fetching HR dashboard data: {e}")
        flash('Error loading dashboard data', 'error')
    
    return render_template('hr/dashboard.html',
                         total_employees=total_employees,
                         present_today=present_today,
                         pending_leaves=pending_leaves,
                         active_tasks=active_tasks,
                         recent_leaves=recent_leaves,
                         recent_work=recent_work)

@app.route('/employee')
@login_required
@role_required(['Employee'])
def employee_dashboard():
    print(f"Current user email: {current_user.email}")  # Debug log
    
    # Initialize default values
    dashboard_data = {
        'attendance_percentage': 0,
        'completed_tasks': 0,
        'pending_tasks': 0,
        'leaves_taken': 0,
        'total_leaves': 12,  # Annual leave limit
        'work_assignments': [],
        'leave_applications': [],
        'employee_name': "",
        'employee_domain': "",
        'employee_mobile': "",
        'today': datetime.now().strftime('%Y-%m-%d')
    }
    
    try:
        conn = get_db_connection()
        if not conn:
            flash('Database connection error', 'error')
            return render_template('employee/dashboard.html', **dashboard_data)
        
        cursor = conn.cursor(dictionary=True)
        
        try:
            print("Executing employee details query...")
            # Get employee details with proper error handling
            cursor.execute('''
                SELECT Name, Email, Domain, Mobile, COALESCE(Attendance, 0) as Attendance 
                FROM ems 
                WHERE Email = %s
            ''', (current_user.email,))
            employee = cursor.fetchone()
            print(f"Employee details: {employee}")  # Debug log
            
            if not employee:
                flash('Employee details not found', 'error')
                return render_template('employee/dashboard.html', **dashboard_data)
            
            # Update basic employee information
            dashboard_data.update({
                'employee_name': employee['Name'],
                'employee_domain': employee['Domain'],
                'employee_mobile': employee['Mobile']  # This should now be properly passed
            })
            
            print("Calculating attendance percentage...")
            # Calculate attendance percentage safely
            attendance = float(employee['Attendance'])
            dashboard_data['attendance_percentage'] = round(attendance, 2)
            
            print("Executing task counts query...")
            # Get task counts with status - Fixed query to properly count tasks
            cursor.execute('''
                SELECT 
                    status,
                    COUNT(*) as count
                FROM work_log 
                WHERE employee_email = %s
                GROUP BY status
            ''', (current_user.email,))
            task_counts = cursor.fetchall()
            print(f"Task counts: {task_counts}")  # Debug log
            
            # Initialize task counts
            completed_count = 0
            pending_count = 0
            
            # Process task counts
            for count_row in task_counts:
                if count_row['status'] == 'Completed':
                    completed_count = count_row['count']
                elif count_row['status'] == 'Pending':
                    pending_count = count_row['count']
            
            dashboard_data.update({
                'completed_tasks': completed_count,
                'pending_tasks': pending_count
            })
            print(f"Updated task counts - Completed: {completed_count}, Pending: {pending_count}")  # Debug log
            
            print("Executing accepted leaves count query...")
            # Get accepted leaves count
            cursor.execute('''
                SELECT COALESCE(COUNT(*), 0) as count 
                FROM leave_applications 
                WHERE employee_email = %s AND status = 'Accepted'
            ''', (current_user.email,))
            leaves_result = cursor.fetchone()
            print(f"Accepted leaves count: {leaves_result}")  # Debug log
            
            if leaves_result:
                dashboard_data['leaves_taken'] = int(leaves_result['count'])
            
            print("Executing work assignments query...")
            # Get work assignments with safe date formatting
            cursor.execute('''
                SELECT 
                    id,
                    subject,
                    body,
                    COALESCE(status, 'Pending') as status,
                    DATE_FORMAT(assigned_date, '%Y-%m-%d') as formatted_assigned_date,
                    DATE_FORMAT(deadline, '%Y-%m-%d') as formatted_deadline
                FROM work_log 
                WHERE employee_email = %s 
                ORDER BY assigned_date DESC
            ''', (current_user.email,))
            work_result = cursor.fetchall()
            print(f"Work assignments: {work_result}")  # Debug log
            
            if work_result:
                dashboard_data['work_assignments'] = work_result
            
            print("Executing leave applications query...")
            # Get leave applications with safe date formatting
            cursor.execute('''
                SELECT 
                    id,
                    employee_email,
                    subject,
                    body,
                    COALESCE(status, 'Pending') as status,
                    DATE_FORMAT(request_date, '%Y-%m-%d') as formatted_request_date
                FROM leave_applications 
                WHERE employee_email = %s 
                ORDER BY request_date DESC
            ''', (current_user.email,))
            leave_result = cursor.fetchall()
            print(f"Leave applications query result: {leave_result}")  # Debug log
            
            # Always set leave_applications, even if empty
            dashboard_data['leave_applications'] = leave_result if leave_result else []
            
        except mysql.connector.Error as db_error:
            print(f"Database query error: {db_error}")
            print(f"Last executed query: {cursor._last_executed}")
            flash(f'Error loading dashboard data: {str(db_error)}', 'error')
            return render_template('employee/dashboard.html', **dashboard_data)
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Error fetching employee dashboard data: {e}")
        flash('Error loading dashboard data: Server error', 'error')
    
    return render_template('employee/dashboard.html', **dashboard_data)

@app.route('/employee-log')
@login_required
@role_required(['Admin', 'HR'])
def employee_log():
    employees = []
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute('''
                SELECT id, Email, Name, Domain, Role, Mobile, Adhaar, Attendance, Leaves 
                FROM ems 
                WHERE Role != 'Admin' 
                ORDER BY id DESC
            ''')
            employees = cursor.fetchall()
            cursor.close()
            conn.close()
    except Exception as e:
        print(f"Error fetching employees: {e}")
        flash('Error loading employee data', 'error')
    
    return render_template('employee_log.html', employees=employees)

@app.route('/add-employee', methods=['GET', 'POST'])
@login_required
@role_required(['Admin', 'HR'])
def add_employee():
    if request.method == 'POST':
        try:
            email = request.form['email']
            name = request.form['name']
            domain = request.form['domain']
            role = request.form['role']
            password = request.form['password']
            mobile = request.form['mobile']
            adhaar = request.form['adhaar']
            
            # Hash the password
            hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO ems 
                    (Email, Name, Domain, Role, Pass, Mobile, Adhaar, Permission) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    email, name, domain, role, hashed.decode('utf-8'),
                    mobile, adhaar, 'basic'
                ))
                conn.commit()
                cursor.close()
                conn.close()
                
                flash('Employee added successfully!', 'success')
                return redirect(url_for('employee_log'))
                
        except Exception as e:
            print(f"Error adding employee: {e}")
            flash('Error adding employee', 'error')
            
    return render_template('add_employee.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/work-log')
@login_required
def work_log():
    # Redirect to appropriate work log based on role
    if current_user.role in ['Admin', 'HR']:
        return redirect(url_for('admin_work_log'))
    else:
        return redirect(url_for('employee_work_log'))

@app.route('/admin/work-log')
@login_required
@role_required(['Admin', 'HR'])
def admin_work_log():
    employees = []
    work_logs = []
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor(dictionary=True)
            
            # Get employees for the dropdown (only for Admin/HR)
            if current_user.role == 'Admin':
                cursor.execute('SELECT Email, Name FROM ems WHERE Role != "Admin"')
            else:
                # For HR, filter employees by domain
                hr_domain = current_user.email.split('@')[-1]
                cursor.execute('SELECT Email, Name FROM ems WHERE Role = "Employee" AND Email LIKE %s', (f'%@{hr_domain}',))
            employees = cursor.fetchall()
            print(f"Fetched employees: {employees}")  # Debug log
            
            # Get work logs
            if current_user.role == 'Admin':
                # Admins see all work logs
                cursor.execute('''
                    SELECT 
                        w.id,
                        w.employee_email,
                        w.subject,
                        w.body,
                        w.status,
                        DATE_FORMAT(w.assigned_date, '%Y-%m-%d %H:%i') as formatted_assigned_date,
                        DATE_FORMAT(w.deadline, '%Y-%m-%d') as formatted_deadline,
                        e.Name as employee_name 
                    FROM work_log w 
                    JOIN ems e ON w.employee_email = e.Email 
                    ORDER BY w.assigned_date DESC
                ''')
            else:
                # HR sees work logs for their domain
                cursor.execute('''
                    SELECT 
                        w.id,
                        w.employee_email,
                        w.subject,
                        w.body,
                        w.status,
                        DATE_FORMAT(w.assigned_date, '%Y-%m-%d %H:%i') as formatted_assigned_date,
                        DATE_FORMAT(w.deadline, '%Y-%m-%d') as formatted_deadline,
                        e.Name as employee_name 
                    FROM work_log w 
                    JOIN ems e ON w.employee_email = e.Email 
                    WHERE e.Email LIKE %s
                    ORDER BY w.assigned_date DESC
                ''', (f'%@{hr_domain}',))
            work_logs = cursor.fetchall()
            print(f"Fetched work logs: {work_logs}")  # Debug log
            
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Error fetching work log data: {e}")
        traceback.print_exc()  # Print full traceback
        flash('Error loading work log data', 'error')
        
    return render_template('work_log.html', 
                        employees=employees, 
                        work_logs=work_logs, 
                        is_admin=(current_user.role == 'Admin'))

@app.route('/employee/work-log')
@login_required
@role_required(['Employee'])
def employee_work_log():
    work_logs = []
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor(dictionary=True)
            
            # Get only the employee's work logs
            cursor.execute('''
                SELECT 
                    w.id,
                    w.subject,
                    w.body,
                    w.status,
                    DATE_FORMAT(w.assigned_date, '%Y-%m-%d') as formatted_assigned_date,
                    DATE_FORMAT(w.deadline, '%Y-%m-%d') as formatted_deadline
                FROM work_log w 
                WHERE w.employee_email = %s 
                ORDER BY w.assigned_date DESC
            ''', (current_user.email,))
            work_logs = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Error fetching work log data: {e}")
        flash('Error loading work log data', 'error')
        
    return render_template('employee/work_log.html', 
                        work_logs=work_logs,
                        today=datetime.now().strftime('%Y-%m-%d'),
                        is_admin=False)

@app.route('/assign-work', methods=['POST'])
@login_required
@role_required(['Admin', 'HR'])
def assign_work():
    if request.method == 'POST':
        try:
            # Retrieve a single employee email from the form
            employee_email = request.form.get('employee_email')
            subjects = request.form.getlist('subjects[]')
            bodies = request.form.getlist('bodies[]')
            deadlines = request.form.getlist('deadlines[]')
            
            print(f"Assigning work to email: {employee_email}")  # Debug log
            print(f"Subjects: {subjects}, Bodies: {bodies}, Deadlines: {deadlines}")  # Debug log
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                
                # Insert work assignment for the employee
                for subject, body, deadline in zip(subjects, bodies, deadlines):
                    try:
                        cursor.execute("""
                            INSERT INTO work_log 
                            (employee_email, subject, body, deadline) 
                            VALUES (%s, %s, %s, %s)
                        """, (employee_email, subject, body, deadline))
                        print(f"Inserted work for {employee_email} with subject {subject}")  # Debug log
                    except Exception as insert_error:
                        print(f"Error inserting work for {employee_email}: {insert_error}")
                        traceback.print_exc()  # Print full traceback
                
                conn.commit()
                cursor.close()
                conn.close()
                
                flash('Work assigned successfully!', 'success')
                return redirect(url_for('admin_work_log'))
                
        except Exception as e:
            print(f"Error assigning work: {e}")
            traceback.print_exc()  # Print full traceback
            flash('Error assigning work', 'error')
            
    return redirect(url_for('admin_work_log'))

@app.route('/delete-work/<int:work_id>')
@login_required
@role_required(['Admin', 'HR'])
def delete_work(work_id):
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM work_log WHERE id = %s', (work_id,))
            conn.commit()
            cursor.close()
            conn.close()
            
            flash('Work assignment deleted successfully!', 'success')
            
    except Exception as e:
        print(f"Error deleting work: {e}")
        flash('Error deleting work assignment', 'error')
        
    return redirect(url_for('admin_work_log'))

@app.route('/leave-applications')
@login_required
@role_required(['Admin', 'HR'])
def leave_applications():
    leaves = []
    pending_count = 0
    accepted_count = 0
    declined_count = 0
    
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor(dictionary=True)
            
            # Get leave applications with employee names
            cursor.execute('''
                SELECT 
                    la.id,
                    la.employee_email,
                    la.subject,
                    la.body,
                    la.status,
                    DATE_FORMAT(la.request_date, '%Y-%m-%d') as formatted_request_date,
                    e.Name as employee_name 
                FROM leave_applications la 
                JOIN ems e ON la.employee_email = e.Email 
                ORDER BY 
                    CASE 
                        WHEN la.status = 'Pending' THEN 1
                        WHEN la.status = 'Accepted' THEN 2
                        ELSE 3
                    END,
                    la.request_date DESC
            ''')
            leaves = cursor.fetchall()
            
            # Get counts by status
            cursor.execute('''
                SELECT status, COUNT(*) as count 
                FROM leave_applications 
                GROUP BY status
            ''')
            for row in cursor.fetchall():
                if row['status'] == 'Pending':
                    pending_count = row['count']
                elif row['status'] == 'Accepted':
                    accepted_count = row['count']
                elif row['status'] == 'Declined':
                    declined_count = row['count']
            
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Error fetching leave applications: {e}")
        flash('Error loading leave applications', 'error')
    
    return render_template('leave_applications.html',
                         leaves=leaves,
                         pending_count=pending_count,
                         accepted_count=accepted_count,
                         declined_count=declined_count)

@app.route('/accept-leave/<int:leave_id>')
@login_required
@role_required(['Admin', 'HR'])
def accept_leave(leave_id):
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor(dictionary=True)
            
            # Get leave application details
            cursor.execute('''
                SELECT id, employee_email, subject, body, status, request_date
                FROM leave_applications 
                WHERE id = %s
            ''', (leave_id,))
            leave = cursor.fetchone()
            
            if leave and leave['status'] == 'Pending':
                # Update leave status
                cursor.execute('''
                    UPDATE leave_applications 
                    SET status = 'Accepted' 
                    WHERE id = %s
                ''', (leave_id,))
                
                # Increment employee's leave count
                cursor.execute('''
                    UPDATE ems 
                    SET Leaves = Leaves + 1 
                    WHERE Email = %s
                ''', (leave['employee_email'],))
                
                conn.commit()
                flash('Leave application accepted successfully!', 'success')
            else:
                flash('Invalid leave application or already processed', 'error')
            
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Error accepting leave: {e}")
        flash('Error processing leave application', 'error')
        
    return redirect(url_for('leave_applications'))

@app.route('/decline-leave/<int:leave_id>')
@login_required
@role_required(['Admin', 'HR'])
def decline_leave(leave_id):
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            
            # Update leave status
            cursor.execute('''
                UPDATE leave_applications 
                SET status = 'Declined' 
                WHERE id = %s AND status = 'Pending'
            ''', (leave_id,))
            
            if cursor.rowcount > 0:
                conn.commit()
                flash('Leave application declined successfully!', 'success')
            else:
                flash('Invalid leave application or already processed', 'error')
            
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Error declining leave: {e}")
        flash('Error processing leave application', 'error')
        
    return redirect(url_for('leave_applications'))

@app.route('/apply-leave', methods=['POST'])
@login_required
@role_required(['Employee'])
def apply_leave():
    if request.method == 'POST':
        try:
            subject = request.form.get('subject')
            body = request.form.get('body')

            # Validate form data
            if not subject or not body:
                flash('All fields are required', 'error')
                return redirect(url_for('employee_dashboard'))

            conn = get_db_connection()
            if not conn:
                flash('Database connection error', 'error')
                return redirect(url_for('employee_dashboard'))

            try:
                cursor = conn.cursor(dictionary=True)

                # Check remaining leave balance
                cursor.execute('SELECT Leaves FROM ems WHERE Email = %s', (current_user.email,))
                employee = cursor.fetchone()
                leaves_taken = employee['Leaves'] if employee else 0
                total_leaves = 12  # Annual leave limit

                if leaves_taken >= total_leaves:
                    flash('You have exhausted your leave balance for the year', 'error')
                    return redirect(url_for('employee_dashboard'))

                # Insert new leave application
                cursor.execute('''
                    INSERT INTO leave_applications 
                    (employee_email, subject, body, status, request_date) 
                    VALUES (%s, %s, %s, 'Pending', %s)
                ''', (current_user.email, subject, body, datetime.now()))
                
                conn.commit()
                flash('Leave application submitted successfully!', 'success')

            except mysql.connector.Error as db_error:
                conn.rollback()
                print(f"Database error: {db_error}")
                flash('Error submitting leave application', 'error')
            finally:
                cursor.close()
                conn.close()

        except Exception as e:
            print(f"Error submitting leave application: {e}")
            flash('Error submitting leave application', 'error')

    return redirect(url_for('employee_dashboard'))

@app.route('/update-work-status/<int:work_id>', methods=['POST'])
@login_required
@role_required(['Employee'])
def update_work_status(work_id):
    try:
        conn = get_db_connection()
        if not conn:
            flash('Database connection error', 'error')
            return redirect(url_for('employee_dashboard'))

        cursor = conn.cursor(dictionary=True)  # Changed to dictionary cursor
        
        try:
            # First check if the work exists and belongs to the current user
            cursor.execute('''
                SELECT id, status 
                FROM work_log 
                WHERE id = %s AND employee_email = %s
            ''', (work_id, current_user.email))
            work = cursor.fetchone()
            
            if not work:
                flash('Work assignment not found or access denied', 'error')
                return redirect(url_for('employee_dashboard'))
                
            # Update the status to Completed
            cursor.execute('''
                UPDATE work_log 
                SET status = 'Completed' 
                WHERE id = %s AND employee_email = %s
            ''', (work_id, current_user.email))
            
            conn.commit()  # Moved commit here
            flash('Work status updated successfully!', 'success')

        except mysql.connector.Error as db_error:
            print(f"Database error updating work status: {db_error}")
            print(f"Last executed query: {cursor._last_executed}")  # Added query logging
            conn.rollback()
            flash('Error updating work status: Database error', 'error')
        finally:
            cursor.close()
            conn.close()
                
    except Exception as e:
        print(f"Error updating work status: {e}")
        flash('Error updating work status: Unexpected error', 'error')
        
    return redirect(url_for('employee_dashboard'))

@app.route('/edit-employee/<int:employee_id>', methods=['POST'])
@login_required
@role_required(['Admin', 'HR'])
def edit_employee(employee_id):
    if request.method == 'POST':
        try:
            name = request.form['name']
            email = request.form['email']
            domain = request.form['domain']
            role = request.form['role']
            mobile = request.form['mobile']
            adhaar = request.form['adhaar']
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE ems 
                    SET Name = %s, Email = %s, Domain = %s, Role = %s, 
                        Mobile = %s, Adhaar = %s 
                    WHERE id = %s
                """, (name, email, domain, role, mobile, adhaar, employee_id))
                conn.commit()
                cursor.close()
                conn.close()
                
                flash('Employee updated successfully!', 'success')
            
        except Exception as e:
            print(f"Error updating employee: {e}")
            flash('Error updating employee', 'error')
            
    return redirect(url_for('employee_log'))

@app.route('/delete-employee/<int:employee_id>', methods=['POST'])
@login_required
@role_required(['Admin', 'HR'])
def delete_employee(employee_id):
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            
            # First check if the employee exists and is not an Admin
            cursor.execute('SELECT Role FROM ems WHERE id = %s', (employee_id,))
            employee = cursor.fetchone()
            
            if employee and employee[0] != 'Admin':
                # Delete related records first (foreign key constraints)
                cursor.execute('DELETE FROM work_log WHERE employee_email = (SELECT Email FROM ems WHERE id = %s)', (employee_id,))
                cursor.execute('DELETE FROM leave_applications WHERE employee_email = (SELECT Email FROM ems WHERE id = %s)', (employee_id,))
                
                # Then delete the employee
                cursor.execute('DELETE FROM ems WHERE id = %s', (employee_id,))
                conn.commit()
                flash('Employee deleted successfully!', 'success')
            else:
                flash('Cannot delete admin users or employee not found', 'error')
            
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Error deleting employee: {e}")
        flash('Error deleting employee', 'error')
        
    return redirect(url_for('employee_log'))

@app.route('/get-leave-details/<int:leave_id>')
@login_required
def get_leave_details(leave_id):
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute('''
                SELECT 
                    la.id,
                    la.employee_email,
                    la.subject,
                    la.body,
                    la.status,
                    DATE_FORMAT(la.request_date, '%Y-%m-%d %H:%M') as formatted_request_date,
                    e.Name as employee_name 
                FROM leave_applications la 
                JOIN ems e ON la.employee_email = e.Email 
                WHERE la.id = %s
            ''', (leave_id,))
            leave = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if leave:
                return jsonify(leave)
            else:
                return jsonify({'error': 'Leave application not found'}), 404
                
    except Exception as e:
        print(f"Error fetching leave details: {e}")
        return jsonify({'error': 'Error loading leave details'}), 500
        
    return jsonify({'error': 'Error loading leave details'}), 500

@app.route('/health')
def health_check():
    try:
        # Test database connection
        conn = get_db_connection()
        if conn:
            conn.close()
            return jsonify({
                'status': 'healthy',
                'database': 'connected',
                'timestamp': datetime.utcnow().isoformat()
            }), 200
        else:
            return jsonify({
                'status': 'unhealthy',
                'database': 'disconnected',
                'timestamp': datetime.utcnow().isoformat()
            }), 500
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/upload', methods=['POST'])
@login_required
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400
            
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'}), 400
            
        if file:
            # Create upload directory if it doesn't exist
            upload_folder = app.config['UPLOAD_FOLDER']
            os.makedirs(upload_folder, exist_ok=True)
            
            # Save the file
            filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
            file_path = os.path.join(upload_folder, filename)
            file.save(file_path)
            
            return jsonify({
                'message': 'File uploaded successfully',
                'filename': filename,
                'path': file_path
            }), 200
            
    except Exception as e:
        print(f"Error uploading file: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True) 