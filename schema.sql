-- Create ems table
CREATE TABLE IF NOT EXISTS ems (
    id SERIAL PRIMARY KEY,
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
);

-- Create leave_applications table
CREATE TABLE IF NOT EXISTS leave_applications (
    id SERIAL PRIMARY KEY,
    employee_email VARCHAR(255) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    body TEXT NOT NULL,
    status VARCHAR(50) DEFAULT 'Pending',
    request_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_email) REFERENCES ems(Email)
);

-- Create work_log table
CREATE TABLE IF NOT EXISTS work_log (
    id SERIAL PRIMARY KEY,
    employee_email VARCHAR(255) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    body TEXT NOT NULL,
    status VARCHAR(50) DEFAULT 'Pending',
    assigned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deadline DATE,
    FOREIGN KEY (employee_email) REFERENCES ems(Email)
);

-- Insert default admin user
INSERT INTO ems (Email, Name, Domain, Role, Pass, Permission)
VALUES (
    'admin@example.com',
    'Admin User',
    'Administration',
    'Admin',
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewKyDAXhZxqXh5G2', -- password: admin123
    'admin'
)
ON CONFLICT (Email) DO NOTHING; 