FROM mcr.microsoft.com/mssql/server:2022-latest

ENV ACCEPT_EULA=Y
ENV SA_PASSWORD=YourStrongPassword123

RUN mkdir -p /var/opt/mssql/backup

COPY ./backupFreela.bak /var/opt/mssql/backup/

COPY ./restore-database.sh /usr/src/
RUN chmod +x /usr/src/restore-database.sh

CMD /bin/bash /usr/src/restore-database.sh