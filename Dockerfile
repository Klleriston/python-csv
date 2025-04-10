FROM mcr.microsoft.com/mssql/server:2022-latest

ENV ACCEPT_EULA=Y
ENV SA_PASSWORD=YourStrongPassword123

USER root
RUN mkdir -p /var/opt/mssql/backup

# Copiar o arquivo de backup e o script de restauração
COPY --chown=mssql:root ./backupFreela.bak /var/opt/mssql/backup/
COPY --chown=mssql:root ./restore-database.sh /usr/src/
RUN chmod +x /usr/src/restore-database.sh

USER mssql
CMD /bin/bash /usr/src/restore-database.sh