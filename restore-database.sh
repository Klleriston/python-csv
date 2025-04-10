#!/bin/bash

/opt/mssql/bin/sqlservr &

echo "Waiting for SQL Server to start up..."
sleep 30s

echo "Restoring database..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "$SA_PASSWORD" -C -Q "RESTORE DATABASE FreelaDev FROM DISK = '/var/opt/mssql/backup/backupFreela.bak' WITH MOVE '32615749000130' TO '/var/opt/mssql/data/FreelaDev.mdf', MOVE '32615749000130_log' TO '/var/opt/mssql/data/FreelaDev_log.ldf'"

echo "Database restored. SQL Server is running."
tail -f /dev/null