import pandas as pd
import pyodbc
import os
from datetime import datetime

SERVER = 'localhost'
DATABASE = 'FreelaDev'
USERNAME = 'SA'
PASSWORD = 'YourStrongPassword123'

OUTPUT_DIR = 'exported_data'
os.makedirs(OUTPUT_DIR, exist_ok=True)

conn_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes'

def get_connection():
    return pyodbc.connect(conn_string)

def query_to_df(query):
    with get_connection() as conn:
        return pd.read_sql(query, conn)

def column_exists(table_name, column_name):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{column_name}'")
        return cursor.fetchone()[0] > 0

def get_table_columns(table_name):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        return [row[0] for row in cursor.fetchall()]

try:
    print(f"Starting data export at {datetime.now().strftime('%H:%M:%S')}")
    has_txcobr = column_exists('DOC_FINANCEIRO_PARCELA', 'VL_DFINP_TXCOBR')
    print(f"Column VL_DFINP_TXCOBR exists: {has_txcobr}")
    
    print("\nExporting Contas a Pagar...")
    contas_pagar_query = """
    SELECT 
        dfp.DOC_FINANCEIRO_PARCELA_ID AS ID, 
        p.NM_PESS_IDENT AS Fornecedor,
        df.DT_DFIN_EMISS AS [Data emissao],
        dfp.DT_DFINP_VENC AS [Data vencimento],
        dfp.DT_DFINP_QUIT AS [Data Liquidacao],
        dfp.VL_DFINP_PARC AS [Valor documento],
        CASE 
            WHEN dfp.DT_DFINP_QUIT IS NOT NULL THEN 0
            ELSE dfp.VL_DFINP_PARC 
        END AS Saldo,
        CASE 
            WHEN dfp.DT_DFINP_QUIT IS NULL THEN 'Em Aberto' 
            ELSE 'Liquidado' 
        END AS Situação,
        df.CD_DFIN_DOCUM AS [Numero documento],
        cp.DS_CPAG_IDENT AS Categoria,
        dfp.DS_DFINP_HIST AS Historico,
        CASE 
            WHEN dfp.DT_DFINP_QUIT IS NOT NULL THEN 'Sim' 
            ELSE 'Não' 
        END AS Pago,
        CONVERT(VARCHAR(7), df.DT_DFIN_EMISS, 120) AS Competencia,
        tc.DS_TCOBR_IDENT AS [Forma Pagamento],
        ISNULL(dfp.NO_DFINP_COBR_ELE, '') AS [Chave Pix/Codigo Boleto]
    FROM 
        DOC_FINANCEIRO_PARCELA dfp
    JOIN 
        DOC_FINANCEIRO df ON dfp.DOC_FINANCEIRO_ID = df.DOC_FINANCEIRO_ID
    JOIN 
        PESSOA p ON df.PESSOA_ID = p.PESSOA_ID
    LEFT JOIN
        COND_PAGTO cp ON df.COND_PAGTO_ID = cp.COND_PAGTO_ID
    LEFT JOIN
        TIPO_COBR tc ON dfp.TIPO_COBR_ID = tc.TIPO_COBR_ID
    WHERE 
        df.NO_DFIN_TIPO = 2  -- Type 2 = Accounts Payable (Contas a Pagar)
    """
    
    contas_pagar_df = query_to_df(contas_pagar_query)
    contas_pagar_df.to_csv(f'{OUTPUT_DIR}/contas_a_pagar.csv', index=False, encoding='utf-8-sig')
    print(f"Exported {len(contas_pagar_df)} records to contas_a_pagar.csv")
    
    print("\nExporting Contas a Receber...")
    taxas_column = "0 AS Taxas"
    if has_txcobr:
        taxas_column = "dfp.VL_DFINP_TXCOBR AS Taxas" 
    
    contas_receber_query = f"""
    SELECT 
        dfp.DOC_FINANCEIRO_PARCELA_ID AS Id, 
        p.NM_PESS_IDENT AS Cliente,
        df.DT_DFIN_EMISS AS [Data Emissao],
        dfp.DT_DFINP_VENC AS [Data vencimento],
        dfp.DT_DFINP_QUIT AS [Data Liquidacao],
        dfp.VL_DFINP_PARC AS [Valor documento],
        CASE 
            WHEN dfp.DT_DFINP_QUIT IS NOT NULL THEN 0
            ELSE dfp.VL_DFINP_PARC 
        END AS Saldo,
        CASE 
            WHEN dfp.DT_DFINP_QUIT IS NULL THEN 'Em Aberto' 
            ELSE 'Liquidado' 
        END AS Situacao,
        df.CD_DFIN_DOCUM AS [Numero do documento],
        dfp.NO_DFINP_COBR_ELE AS [Numero no banco],
        cp.DS_CPAG_IDENT AS Categoria,
        dfp.DS_DFINP_HIST AS Historico,
        tc.DS_TCOBR_IDENT AS [Forma de recebimento],
        '' AS [Meio de recebimento],
        {taxas_column},
        CONVERT(VARCHAR(7), df.DT_DFIN_EMISS, 120) AS Competencia
    FROM 
        DOC_FINANCEIRO_PARCELA dfp
    JOIN 
        DOC_FINANCEIRO df ON dfp.DOC_FINANCEIRO_ID = df.DOC_FINANCEIRO_ID
    JOIN 
        PESSOA p ON df.PESSOA_ID = p.PESSOA_ID
    LEFT JOIN
        COND_PAGTO cp ON df.COND_PAGTO_ID = cp.COND_PAGTO_ID
    LEFT JOIN
        TIPO_COBR tc ON dfp.TIPO_COBR_ID = tc.TIPO_COBR_ID
    WHERE 
        df.NO_DFIN_TIPO = 1  -- Type 1 = Accounts Receivable (Contas a Receber)
    """
    
    contas_receber_df = query_to_df(contas_receber_query)
    contas_receber_df.to_csv(f'{OUTPUT_DIR}/contas_a_receber.csv', index=False, encoding='utf-8-sig')
    print(f"Exported {len(contas_receber_df)} records to contas_a_receber.csv")
    
    print("\nExporting Contatos...")
    
    contatos_query = """
    SELECT 
        p.PESSOA_ID AS Id, 
        p.NO_PESS_IDENT AS Codigo,
        p.NM_PESS_IDENT AS Nome, 
        p.DS_PESS_FANTA AS Fantasia, 
        p.DS_PESS_ENDER AS Endereço, 
        p.NO_PESS_ENDER AS Numero, 
        p.DS_PESS_ENDER_COMPL AS Complemento, 
        p.DS_PESS_BAIRRO AS Bairro, 
        p.NO_PESS_CEP AS CEP, 
        m.DS_MUN_IDENT AS Cidade, 
        u.CD_UF_IDT AS Estado, 
        p.DS_PESS_ENDER_REFER AS [Observaçoes do contato], 
        (SELECT TOP 1 c.DS_CTT_TTRM FROM CONTATO c WHERE c.PESSOA_ID = p.PESSOA_ID AND c.ID_CTT_PADR = 1) AS Fone, 
        '' AS Fax, 
        '' AS Celular, 
        p.NO_PESS_EMAIL_COBR AS [E-mail], 
        '' AS [Web Site], 
        CASE p.NO_PESS_TIPO 
            WHEN 1 THEN 'Física' 
            WHEN 2 THEN 'Jurídica' 
            ELSE 'Outro' 
        END AS [Tipo Pessoa], 
        p.NO_PESS_CNPJ_CPF AS [CNPJ/CPF], 
        p.NO_PESS_INSCR_ESTAD AS [IE/RG], 
        'Não' AS [IE isento], 
        CASE p.ID_PESS_ATIVA 
            WHEN 1 THEN 'Ativo' 
            ELSE 'Inativo' 
        END AS Situação, 
        p.DS_PESS_OBS AS Observações, 
        CASE p.NO_PESS_EST_CIVIL
            WHEN 1 THEN 'Solteiro(a)'
            WHEN 2 THEN 'Casado(a)'
            WHEN 3 THEN 'Divorciado(a)'
            WHEN 4 THEN 'Viúvo(a)'
            ELSE ''
        END AS [Estado Civil], 
        p.DS_PESS_CARG AS Profissão, 
        CASE p.NO_PESS_SEXO 
            WHEN 1 THEN 'Masculino' 
            WHEN 2 THEN 'Feminino' 
            ELSE 'Outro' 
        END AS Sexo, 
        p.DT_PESS_NASC AS [Data nascimento], 
        p.DS_PESS_NATUR AS Naturalidade,
        '' AS [Lista DE Preço], 
        '' AS Vendedor, 
        p.NO_PESS_EMAIL_COBR AS [E-mail para envio de NFe], 
        '' AS [Tipos de contato], 
        'Não' AS Contribuinte, 
        '' AS [Codigo de regime tributario], 
        p.VL_PESS_LIMIT AS [Limite de credito]
    FROM 
        PESSOA p
    LEFT JOIN 
        MUNICIPIO m ON p.MUNICIPIO_ID = m.MUNICIPIO_ID
    LEFT JOIN 
        UF u ON m.UF_ID = u.UF_ID
    """
    
    contatos_df = query_to_df(contatos_query)
    contatos_df.to_csv(f'{OUTPUT_DIR}/contatos.csv', index=False, encoding='utf-8-sig')
    print(f"Exported {len(contatos_df)} records to contatos.csv")
    
    print(f"All data exported successfully at {datetime.now().strftime('%H:%M:%S')}")
    
except Exception as e:
    print(f"Error: {str(e)}")
    import traceback
    traceback.print_exc() 