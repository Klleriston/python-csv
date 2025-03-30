# Commands for run:

```bash
# If using docker-compose:
docker-compose up -d

# Or using docker directly:
docker build -t sql-backup-restore .
docker run -d -p 1433:1433 --name sql_server sql-backup-restore
```

## Check if the restore was successfull

```shell
# View container logs
docker logs sql_server

# Or connect to the container and verify
docker exec -it sql_server /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P 'YourStrongPassword123' -Q "SELECT name FROM sys.databases"
```

### To Connect in code

- Server: localhost,1433 
- Username: SA
- Password: YourStrongPassword123
- Database: FreelaDev

### String Connection:

```python
conn_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=localhost,1433;DATABASE=FreelaDev;UID=SA;PWD=YourStrongPassword123;TrustServerCertificate=yes'
```