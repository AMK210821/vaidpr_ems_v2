USE vaidpr_ems;

-- Drop and recreate the leave_applications table with correct structure
DROP TABLE IF EXISTS leave_applications;

-- Drop and recreate the work_log table
DROP TABLE IF EXISTS work_log;

-- Drop and recreate the ems table
DROP TABLE IF EXISTS ems;

-- Create ems table
CREATE TABLE ems (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Email VARCHAR(255) UNIQUE NOT NULL,
    Name VARCHAR(255) NOT NULL,
    Domain VARCHAR(255) NOT NULL,
    Role VARCHAR(255) NOT NULL,
    Pass VARCHAR(255) NOT NULL,
    Mobile VARCHAR(15) UNIQUE NOT NULL,
    Adhaar VARCHAR(12) UNIQUE NOT NULL,
    Attendance INT DEFAULT 0,
    Leaves INT DEFAULT 0,
    Permission VARCHAR(225) NOT NULL,
    CONSTRAINT chk_mobile CHECK (LENGTH(Mobile) >= 10),
    CONSTRAINT chk_adhaar CHECK (LENGTH(Adhaar) = 12)
);

-- Create work_log table
CREATE TABLE work_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    employee_email VARCHAR(255) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    body TEXT NOT NULL,
    assigned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deadline DATETIME NOT NULL,
    status ENUM('Pending', 'In Progress', 'Completed', 'Delayed') DEFAULT 'Pending',
    FOREIGN KEY (employee_email) REFERENCES ems(Email)
);

-- Create leave_applications table
CREATE TABLE leave_applications (
    id INT AUTO_INCREMENT PRIMARY KEY,
    employee_email VARCHAR(255) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    body TEXT NOT NULL,
    status ENUM('Pending', 'Accepted', 'Declined') DEFAULT 'Pending',
    request_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_email) REFERENCES ems(Email)
);

-- Insert test employee
INSERT INTO ems (Email, Name, Domain, Role, Pass, Mobile, Adhaar, Permission) 
VALUES ('test@example.com', 'Test User', 'IT', 'Employee', 
        '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdsBHrpxFPH.G2.', 
        '1234567890', '123456789012', 'basic');

-- Insert test leave applications
INSERT INTO leave_applications (employee_email, subject, body, status, request_date) 
VALUES 
('test@example.com', 'Sick Leave', 'Not feeling well', 'Pending', NOW()),
('test@example.com', 'Vacation', 'Annual vacation', 'Accepted', NOW());

-- Insert test work assignments
INSERT INTO work_log (employee_email, subject, body, deadline, status) 
VALUES 
('test@example.com', 'Test Task 1', 'Complete this test task', DATE_ADD(NOW(), INTERVAL 7 DAY), 'Pending'),
('test@example.com', 'Test Task 2', 'Another test task', DATE_ADD(NOW(), INTERVAL 14 DAY), 'Pending');