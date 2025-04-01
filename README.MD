# Data-DB-para-CSV: Processamento e Exportação de Dados SQL Server

Este projeto fornece ferramentas para extrair, processar e exportar dados de um banco de dados SQL Server em arquivos CSV, com foco específico em gerenciar grandes volumes de dados ao dividi-los em arquivos menores para compatibilidade com planilhas Excel/Google Sheets.

## Conteúdo

1. [Visão Geral](#visão-geral)
2. [Pré-requisitos](#pré-requisitos)
3. [Instalação](#instalação)
4. [Estrutura do Projeto](#estrutura-do-projeto)
5. [Uso dos Scripts](#uso-dos-scripts)
   - [Configuração do SQL Server com Docker](#configuração-do-sql-server-com-docker)
   - [Exportação de Dados do Banco de Dados](#exportação-de-dados-do-banco-de-dados)
   - [Divisão de Arquivos CSV por Data](#divisão-de-arquivos-csv-por-data)
6. [Testes](#testes)
7. [Solução de Problemas](#solução-de-problemas)

## Visão Geral

Este projeto resolve o problema de extrair dados de um arquivo de backup SQL Server (.bak) e processá-los em arquivos CSV facilmente gerenciáveis. Ele suporta três tipos principais de dados:

1. **Contas a Pagar**: Informações de pagamentos a fornecedores
2. **Contas a Receber**: Informações de pagamentos recebidos de clientes
3. **Contatos**: Informações sobre fornecedores, clientes e outros contatos

Devido ao grande tamanho do conjunto de dados original (3GB), o projeto implementa uma estratégia para dividir os dados em arquivos CSV menores (máximo 2MB) para serem facilmente abertos em Excel ou Google Sheets.

## Pré-requisitos

- Python 3.6+
- Docker (para restaurar o arquivo de backup SQL Server)
- Pacotes Python necessários:
  - pandas
  - pyodbc
  - numpy

## Instalação

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/data-db-para-csv.git
cd data-db-para-csv
```

2. Instale as dependências Python:
```bash
pip install -r requirements.txt
```

## Estrutura do Projeto

```
data-db-para-csv/
│
├── export_spreadsheets_final_fixed.py  # Script para extração de dados do SQL Server
├── split_by_date.py                    # Script para dividir CSV por data
├── test_split_by_date.py               # Testes para o script de divisão
├── Dockerfile                          # Configuração do SQL Server em Docker
├── restore-database.sh                 # Script para restaurar o backup SQL
└── exported_data/                      # Diretório para CSVs exportados
```

## Uso dos Scripts

### Configuração do SQL Server com Docker

O arquivo `Dockerfile` configura um container Docker com SQL Server 2022 e restaura automaticamente o arquivo de backup.

1. Coloque seu arquivo de backup (.bak) no mesmo diretório do Dockerfile
2. Execute os comandos:

```bash
# Construir a imagem Docker
docker build -t sql-backup-restore .

# Executar o container
docker run -d -p 1433:1433 --name sql_server sql-backup-restore
```

### Exportação de Dados do Banco de Dados

O script `export_spreadsheets_final_fixed.py` conecta-se ao banco de dados e exporta os dados para três arquivos CSV separados:

```bash
python export_spreadsheets_final_fixed.py
```

Este script:
1. Conecta-se ao SQL Server (local ou em Docker)
2. Extrai dados usando consultas SQL otimizadas
3. Exporta três arquivos CSV:
   - `contas_a_pagar.csv`: Contas a pagar
   - `contas_a_receber.csv`: Contas a receber
   - `contatos.csv`: Lista de contatos

Parâmetros de conexão padrão:
- Servidor: localhost
- Banco de dados: FreelaDev
- Usuário: SA
- Senha: YourStrongPassword123

Para alterar estes parâmetros, edite as variáveis no início do script.

### Divisão de Arquivos CSV por Data

O script `split_by_date.py` processa os arquivos CSV grandes e os divide em arquivos menores, limitados a 2MB:

```bash
python split_by_date.py
```

Este script:
1. Lê os arquivos CSV do diretório `exported_data/`
2. Cria uma cópia completa dos dados em `exported_data_split/`
3. Divide cada arquivo por intervalo de data (por mês, semana ou dia)
4. Salva os arquivos divididos com nomes incluindo o período da data

**Observação**: Neste script, os nomes "Contas a Pagar" e "Contas a Receber" foram invertidos conforme solicitado. O script `process_accounts_payable()` processa os dados de contas a pagar, mas cria arquivos com "contas_a_receber" no nome, e vice-versa. Esta inversão é apenas nos nomes dos arquivos e mensagens de saída, não afetando o conteúdo dos dados.

## Testes

O projeto inclui testes unitários para verificar o funcionamento correto do script de divisão de arquivos:

```bash
python -m unittest test_split_by_date
```

Os testes verificam:

1. **Funcionalidades básicas**:
   - Estimativa de tamanho de arquivos CSV
   - Divisão por número de linhas
   - Divisão por intervalo de datas

2. **Inversão de nomes**:
   - Verifica que "Contas a Pagar" e "Contas a Receber" estão corretamente invertidos

3. **Limites de tamanho**:
   - Garante que todos os arquivos respeitam o limite de 2MB

4. **Integração**:
   - Executa o processo completo e verifica os resultados

Para executar um teste específico:

```bash
python -m unittest test_split_by_date.TestSplitByDate.test_file_size_limit
```

## Solução de Problemas

### Erro de conexão com o SQL Server

Se encontrar erros de conexão com o SQL Server:

1. Verifique se o contêiner Docker está em execução:
```bash
docker ps
```

2. Verifique os logs do contêiner:
```bash
docker logs sql_server
```

3. Confirme que os parâmetros de conexão estão corretos no script `export_spreadsheets_final_fixed.py`.

### Erro de certificado SSL

Se encontrar erros de certificado SSL, verifique se a opção `TrustServerCertificate=yes` está presente na string de conexão:

```python
conn_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes'
```

### Erros de colunas não encontradas

Se o script relatar erros de colunas não encontradas, pode ser necessário verificar a estrutura real do banco de dados:

1. Conecte-se ao banco de dados e execute:
```sql
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'NOME_DA_TABELA'
```

2. Ajuste os nomes de colunas no script conforme necessário.

---

Este projeto foi desenvolvido para facilitar a exportação de dados de um backup SQL Server para planilhas CSV facilmente gerenciáveis, fornecendo uma solução completa desde a restauração do backup até a divisão de arquivos grandes por data para compatibilidade com Excel/Google Sheets.
