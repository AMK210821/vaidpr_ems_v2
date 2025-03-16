<<<<<<< HEAD
# vaidpr_ems_v1
=======
# Vaid PR Employee Management System

A comprehensive employee management system built with Python Flask and MySQL.

## Features

- Multi-role authentication (Admin, HR, Employee)
- Employee management
- Work log tracking
- Leave application system
- Real-time dashboard statistics
- Responsive design

## Prerequisites

- Python 3.8 or higher
- MySQL (XAMPP)
- Web browser

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/vaidpr-ems.git
cd vaidpr-ems
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install the required packages:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the root directory with the following content:
```
SECRET_KEY=your-secret-key-here
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=
DB_NAME=vaidpr_ems
```

5. Initialize the database:
```bash
python database/init_db.py
```

## Running the Application

1. Make sure XAMPP MySQL service is running

2. Start the Flask application:
```bash
python app.py
```

3. Open your web browser and navigate to:
```
http://localhost:5000
```


## Project Structure

```
vaidpr-ems/
├── app.py                  # Main application file
├── config/
│   └── config.py          # Configuration settings
├── database/
│   └── init_db.py         # Database initialization
├── static/
│   ├── css/
│   │   └── style.css      # Custom styles
│   ├── js/
│   │   └── main.js        # Custom JavaScript
│   └── images/            # Image assets
├── templates/
│   ├── base.html          # Base template
│   ├── login.html         # Login page
│   ├── admin/            # Admin templates
│   ├── hr/               # HR templates
│   └── employee/         # Employee templates
├── requirements.txt       # Python dependencies
└── README.md             # Project documentation
```

## Contributing

1. Fork the repository
2. Create a new branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support, email support@vaidpr.com or create an issue in the repository. 
>>>>>>> 88aa929 (initial commit)
