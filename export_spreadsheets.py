import pandas as pd
import pyodbc
import os
from datetime import datetime
import split_by_date

# Definição das colunas para garantir que todas sejam exportadas
colunas_contatos = [
    "ID", "Código", "Nome", "Fantasia", "Endereço", "Número", "Complemento",
    "Bairro", "CEP", "Cidade", "Estado", "Observações do contato", "Fone",
    "Fax", "Celular", "E-mail", "Web Site", "Tipo pessoa", "CNPJ / CPF",
    "IE / RG", "IE isento", "Situação", "Observações", "Estado civil",
    "Profissão", "Sexo", "Data nascimento", "Naturalidade", "Nome pai",
    "CPF pai", "Nome mãe", "CPF mãe", "Lista de Preço", "Vendedor",
    "E-mail para envio de NFe", "Tipos de Contatos", "Contribuinte",
    "Código de regime tributário", "Limite de crédito"
]

colunas_contas_pagar = [
    "ID", "Fornecedor", "Data emissao", "Data vencimento", "Data Liquidacao",
    "Valor documento", "Saldo", "Situação", "Numero documento", "Categoria",
    "Historico", "Pago", "Competencia", "Forma Pagamento"
]

colunas_contas_receber = [
    "Id", "Cliente", "Data Emissao", "Data vencimento", "Data Liquidacao",
    "Valor documento", "Saldo", "Situacao", "Numero do documento", "Numero no banco",
    "Categoria", "Historico", "Forma de recebimento", "Meio de recebimento",
    "Taxas"
]

SERVER = 'localhost'
DATABASE = 'FreelaDev'
USERNAME = 'SA'
PASSWORD = 'YourStrongPassword123'

OUTPUT_DIR = 'exported_data'
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(split_by_date.SPLIT_OUTPUT_DIR, exist_ok=True)

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

def create_empty_excel_with_columns(filename, columns):
    """Create an empty Excel file with only the column headers"""
    df = pd.DataFrame(columns=columns)
    excel_path = f'{OUTPUT_DIR}/{filename}'
    df.to_excel(excel_path, index=False)
    print(f"Criado arquivo {filename} vazio com {len(columns)} colunas")

def exportar_e_dividir(df, nome_arquivo, colunas_esperadas, tipo_arquivo):
    """Exporta um DataFrame para Excel e o divide em arquivos de até 2MB"""
    # Garantir que todas as colunas estão presentes
    for coluna in colunas_esperadas:
        if coluna not in df.columns:
            if coluna == 'Contribuinte':
                df[coluna] = 0  # Valor padrão numérico (0 = Não contribuinte)
            else:
                df[coluna] = ''
    
    # Reordenar colunas conforme a lista especificada
    df = df[colunas_esperadas]
    
    # Formatar datas no padrão DD/MM/YYYY
    colunas_data = [col for col in df.columns if 'data' in col.lower() or 'emissao' in col.lower() or 'nascimento' in col.lower() or 'vencimento' in col.lower() or 'liquidacao' in col.lower()]
    for col in colunas_data:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime('%d/%m/%Y')
    
    # Garantir que Contribuinte seja numérico para contatos
    if tipo_arquivo == 'contatos' and 'Contribuinte' in df.columns:
        df['Contribuinte'] = df['Contribuinte'].apply(lambda x: 
            1 if str(x).lower() in ['1', 'sim', 'yes', 'true', 'verdadeiro', 'y', 's', 't', 'v'] 
            else 0)
        
        # Para pessoa jurídica, definir Contribuinte como 1
        if 'Tipo pessoa' in df.columns:
            df.loc[df['Tipo pessoa'] == 'Jurídica', 'Contribuinte'] = 1
    
    # Exportar arquivo completo
    excel_path = f'{OUTPUT_DIR}/{nome_arquivo}'
    df.to_excel(excel_path, index=False)
    print(f"Exportados {len(df)} registros para {nome_arquivo}")
    
    # Dividir o arquivo conforme o tipo
    if tipo_arquivo == 'contatos':
        split_by_date.process_contacts()
    elif tipo_arquivo == 'contas_pagar':
        split_by_date.process_accounts_payable()
    elif tipo_arquivo == 'contas_receber':
        split_by_date.process_accounts_receivable()

try:
    print(f"Iniciando exportação de dados às {datetime.now().strftime('%H:%M:%S')}")
    try:
        has_txcobr = column_exists('DOC_FINANCEIRO_PARCELA', 'VL_DFINP_TXCOBR')
        print(f"Coluna VL_DFINP_TXCOBR existe: {has_txcobr}")
        connected_to_db = True
    except Exception as e:
        print(f"Erro na conexão com o banco de dados: {str(e)}")
        print("Criando arquivos Excel vazios com as colunas especificadas...")
        connected_to_db = False
    
    if not connected_to_db:
        create_empty_excel_with_columns('contatos.xlsx', colunas_contatos)
        create_empty_excel_with_columns('contas_a_pagar.xlsx', colunas_contas_pagar)
        create_empty_excel_with_columns('contas_a_receber.xlsx', colunas_contas_receber)
        print("Criação de arquivos vazios concluída")
        exit(0)
    
    print("\nExportando Contas a Pagar...")
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
        tc.DS_TCOBR_IDENT AS [Forma Pagamento]
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
    exportar_e_dividir(contas_pagar_df, 'contas_a_pagar.xlsx', colunas_contas_pagar, 'contas_pagar')
    
    print("\nExportando Contas a Receber...")
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
        {taxas_column}
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
    exportar_e_dividir(contas_receber_df, 'contas_a_receber.xlsx', colunas_contas_receber, 'contas_receber')
    
    print("\nExportando Contatos...")
    
    contatos_query = """
    SELECT 
        p.PESSOA_ID AS ID, 
        p.NO_PESS_IDENT AS Código,
        p.NM_PESS_IDENT AS Nome, 
        p.DS_PESS_FANTA AS Fantasia, 
        p.DS_PESS_ENDER AS Endereço, 
        p.NO_PESS_ENDER AS Número, 
        p.DS_PESS_ENDER_COMPL AS Complemento, 
        p.DS_PESS_BAIRRO AS Bairro, 
        p.NO_PESS_CEP AS CEP, 
        m.DS_MUN_IDENT AS Cidade, 
        u.CD_UF_IDT AS Estado, 
        p.DS_PESS_ENDER_REFER AS [Observações do contato], 
        (SELECT TOP 1 c.DS_CTT_TTRM FROM CONTATO c WHERE c.PESSOA_ID = p.PESSOA_ID AND c.ID_CTT_PADR = 1) AS Fone, 
        '' AS Fax, 
        '' AS Celular, 
        p.NO_PESS_EMAIL_COBR AS [E-mail], 
        '' AS [Web Site], 
        CASE p.NO_PESS_TIPO 
            WHEN 1 THEN 'Física' 
            WHEN 2 THEN 'Jurídica' 
            ELSE 'Outro' 
        END AS [Tipo pessoa], 
        p.NO_PESS_CNPJ_CPF AS [CNPJ / CPF], 
        p.NO_PESS_INSCR_ESTAD AS [IE / RG], 
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
        END AS [Estado civil], 
        p.DS_PESS_CARG AS Profissão, 
        CASE p.NO_PESS_SEXO 
            WHEN 1 THEN 'Masculino' 
            WHEN 2 THEN 'Feminino' 
            ELSE 'Outro' 
        END AS Sexo, 
        p.DT_PESS_NASC AS [Data nascimento], 
        p.DS_PESS_NATUR AS Naturalidade, 
        '' AS [Nome pai], 
        '' AS [CPF pai], 
        '' AS [Nome mãe], 
        '' AS [CPF mãe], 
        '' AS [Lista de Preço], 
        '' AS Vendedor, 
        p.NO_PESS_EMAIL_COBR AS [E-mail para envio de NFe], 
        '' AS [Tipos de Contatos], 
        CASE 
            WHEN p.NO_PESS_TIPO = 2 THEN 1  -- Contribuinte (1 = Sim) para Pessoa Jurídica
            ELSE 0                          -- Não contribuinte (0 = Não) para outros tipos
        END AS Contribuinte, 
        '' AS [Código de regime tributário],
        0 AS [Limite de crédito]
    FROM 
        PESSOA p
    LEFT JOIN 
        MUNICIPIO m ON p.MUNICIPIO_ID = m.MUNICIPIO_ID
    LEFT JOIN 
        UF u ON m.UF_ID = u.UF_ID
    """
    
    contatos_df = query_to_df(contatos_query)
    exportar_e_dividir(contatos_df, 'contatos.xlsx', colunas_contatos, 'contatos')
    
    # Verificar se há arquivos grandes que precisam ser subdivididos adicionalmente
    split_by_date.adicional_split_large_files()
    
    print(f"Todos os dados exportados e divididos com sucesso às {datetime.now().strftime('%H:%M:%S')}")
    
except Exception as e:
    print(f"Erro: {str(e)}")
    import traceback
    traceback.print_exc() 