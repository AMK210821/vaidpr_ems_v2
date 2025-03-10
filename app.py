from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import mysql.connector
import bcrypt
from config.config import Config
from functools import wraps
from datetime import datetime

app = Flask(__name__)
app.config.from_object(Config)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Database connection
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=Config.DB_HOST,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            database=Config.DB_NAME
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
        return None

# User class for Flask-Login
class User(UserMixin):
    def __init__(self, id, email, role, permission):
        self.id = id
        self.email = email
        self.role = role
        self.permission = permission

@login_manager.user_loader
def load_user(user_id):
    try:
        conn = get_db_connection()
        if not conn:
            print("Failed to connect to database in load_user")
            return None
            
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM ems WHERE id = %s', (user_id,))
        user_data = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if user_data:
            return User(
                id=user_data['id'],
                email=user_data['Email'],
                role=user_data['Role'],
                permission=user_data['Permission']
            )
        return None
    except Exception as e:
        print(f"Error in load_user: {e}")
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
        email = request.form['email']
        password = request.form['password']
        
        try:
            conn = get_db_connection()
            if not conn:
                flash('Database connection error', 'error')
                return render_template('login.html')
                
            cursor = conn.cursor(dictionary=True)
            cursor.execute('SELECT * FROM ems WHERE Email = %s', (email,))
            user = cursor.fetchone()
            cursor.close()
            conn.close()

            if user:
                # Print password hash for debugging
                print(f"Stored password hash: {user['Pass']}")
                print(f"Input password: {password}")
                
                try:
                    if bcrypt.checkpw(password.encode('utf-8'), user['Pass'].encode('utf-8')):
                        user_obj = User(
                            id=user['id'],
                            email=user['Email'],
                            role=user['Role'],
                            permission=user['Permission']
                        )
                        login_user(user_obj)
                        print(f"User logged in successfully: {user['Email']}")
                        return redirect(url_for('dashboard'))
                    else:
                        print("Password verification failed")
                        flash('Invalid password', 'error')
                except Exception as e:
                    print(f"Error during password verification: {e}")
                    flash('Error during login', 'error')
            else:
                print(f"No user found with email: {email}")
                flash('Invalid email', 'error')
                
        except Exception as e:
            print(f"Error during login: {e}")
            flash('An error occurred during login', 'error')
            
    return render_template('login.html')

@app.route('/dashboard')
@login_required
def dashboard():
    print(f"Current user role: {current_user.role}")
    if current_user.role == 'Admin':
        return redirect(url_for('admin_dashboard'))
    elif current_user.role == 'HR':
        return redirect(url_for('hr_dashboard'))
    else:
        return redirect(url_for('employee_dashboard'))

@app.route('/admin')
@login_required
@role_required(['Admin'])
def admin_dashboard():
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
            cursor.execute('''
                SELECT Domain, COUNT(*) as count 
                FROM ems 
                WHERE Role != 'Admin'
                GROUP BY Domain 
                ORDER BY count DESC
            ''')
            for row in cursor.fetchall():
                domain_counts[row['Domain']] = row['count']
            
            # Get leave counts
            cursor.execute('SELECT status, COUNT(*) as count FROM leave_applications GROUP BY status')
            for row in cursor.fetchall():
                if row['status'] == 'Accepted':
                    accepted_leaves = row['count']
                elif row['status'] == 'Pending':
                    pending_leaves = row['count']
                elif row['status'] == 'Declined':
                    declined_leaves = row['count']
            
            # Get recent leaves
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
            cursor.execute('''
                SELECT 
                    w.id,
                    w.employee_email,
                    w.subject,
                    w.body,
                    w.status,
                    w.assigned_date,
                    DATE_FORMAT(w.assigned_date, '%Y-%m-%d') as formatted_assigned_date,
                    DATE_FORMAT(w.deadline, '%Y-%m-%d') as formatted_deadline,
                    e.Name as employee_name 
                FROM work_log w 
                JOIN ems e ON w.employee_email = e.Email 
                ORDER BY w.assigned_date DESC LIMIT 5
            ''')
            recent_work = cursor.fetchall()
            
            # Debug logging
            print("Recent work data:", recent_work)
            
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Error fetching dashboard data: {e}")
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
            
            # Get total employees count
            cursor.execute('SELECT COUNT(*) as count FROM ems WHERE Role = "Employee"')
            result = cursor.fetchone()
            total_employees = result['count'] if result else 0
            
            # Get present employees count (assuming attendance is marked daily)
            cursor.execute('SELECT COUNT(*) as count FROM ems WHERE Role = "Employee" AND Attendance = 1')
            result = cursor.fetchone()
            present_today = result['count'] if result else 0
            
            # Get pending leaves count
            cursor.execute('SELECT COUNT(*) as count FROM leave_applications WHERE status = "Pending"')
            result = cursor.fetchone()
            pending_leaves = result['count'] if result else 0
            
            # Get active tasks count
            cursor.execute('SELECT COUNT(*) as count FROM work_log WHERE status = "Pending"')
            result = cursor.fetchone()
            active_tasks = result['count'] if result else 0
            
            # Get recent leaves
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
                cursor.execute('SELECT Email, Name FROM ems WHERE Role = "Employee"')
            employees = cursor.fetchall()
            
            # Get all work logs for Admin/HR view
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
            work_logs = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Error fetching work log data: {e}")
        flash('Error loading work log data', 'error')
        
    return render_template('work_log.html', 
                        employees=employees, 
                        work_logs=work_logs, 
                        is_admin=True)

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
            employee_email = request.form['employee_email']
            subject = request.form['subject']
            body = request.form['body']
            deadline = request.form.get('deadline', datetime.now().strftime('%Y-%m-%d'))
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO work_log 
                    (employee_email, subject, body, deadline) 
                    VALUES (%s, %s, %s, %s)
                """, (employee_email, subject, body, deadline))
                conn.commit()
                cursor.close()
                conn.close()
                
                flash('Work assigned successfully!', 'success')
                return redirect(url_for('admin_work_log'))
                
        except Exception as e:
            print(f"Error assigning work: {e}")
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

if __name__ == '__main__':
    app.run(debug=True) 