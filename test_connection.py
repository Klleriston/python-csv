import pyodbc

conn_string = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=FreelaDev;UID=SA;PWD=YourStrongPassword123;TrustServerCertificate=yes'

try:
    conn = pyodbc.connect(conn_string)
    print("Connection successful!")
    conn.close()
except Exception as e:
    print(f"Connection failed: {str(e)}") 