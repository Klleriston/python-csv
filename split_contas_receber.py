#!/usr/bin/env python3
import pandas as pd
import os
import math
import re
from datetime import datetime

# Configuração de diretórios
INPUT_DIR = 'exported_data'
OUTPUT_DIR = 'exported_data_split'
os.makedirs(OUTPUT_DIR, exist_ok=True)

MAX_FILE_SIZE = 500 * 1024  # 500KB

# Colunas exatas do contas_receber_template.xls
COLUNAS_TEMPLATE = [
    "Id", "Cliente", "Data Emissao", "Data vencimento", "Data Liquidacao",
    "Valor documento", "Saldo", "Situacao", "Numero do documento", "Numero no banco",
    "Categoria", "Historico", "Forma de recebimento", "Meio de recebimento",
    "Taxas"
]

def formatar_coluna(valor, coluna):
    """Formatar valor de acordo com o tipo de coluna"""
    # Para valores nulos
    if pd.isna(valor) or valor == '':
        # Deixar em branco para todos os campos
        if coluna in ['Valor documento', 'Saldo', 'Taxas']:
            return 0
        else:
            return ''
    
    # Para campos numéricos
    if coluna in ['Valor documento', 'Saldo', 'Taxas']:
        try:
            num = pd.to_numeric(valor, errors='coerce')
            if pd.isna(num):
                return 0
            return num
        except:
            return 0
    
    # Para datas - formato DD/MM/YYYY
    if coluna in ['Data Emissao', 'Data vencimento', 'Data Liquidacao']:
        try:
            data = pd.to_datetime(valor, dayfirst=True, errors='coerce')
            if pd.isna(data):
                return ''
            return data.strftime('%d/%m/%Y')
        except:
            return ''
    
    # Para demais campos, manter como está
    return valor

def garantir_formato_template(df):
    """Garante que o DataFrame segue exatamente a estrutura do template"""
    # Criar DataFrame vazio com as colunas do template
    df_formatado = pd.DataFrame(columns=COLUNAS_TEMPLATE)
    
    # Para cada coluna no template, copiar do DataFrame original ou preencher vazio
    for coluna in COLUNAS_TEMPLATE:
        if coluna in df.columns:
            df_formatado[coluna] = df[coluna].apply(lambda x: formatar_coluna(x, coluna))
        else:
            # Se a coluna não existir, preencher com valores apropriados
            if coluna in ['Valor documento', 'Saldo', 'Taxas']:
                df_formatado[coluna] = 0
            else:
                df_formatado[coluna] = ''
    
    return df_formatado

def dividir_contas_receber():
    """
    Divide a planilha de contas a receber em partes menores de até 500KB,
    utilizando divisão direta por número de linhas.
    """
    print(f"Dividindo planilha de contas a receber em partes de até {MAX_FILE_SIZE/1024:.0f}KB")
    
    # Verificar se o arquivo de contas a receber existe
    arquivo_contas = os.path.join(INPUT_DIR, 'contas_a_receber.xlsx')
    if not os.path.exists(arquivo_contas):
        print(f"Erro: Arquivo {arquivo_contas} não encontrado!")
        return
    
    # Ler a planilha de contas a receber
    print(f"Lendo arquivo {arquivo_contas}...")
    df = pd.read_excel(arquivo_contas)
    total_linhas = len(df)
    print(f"Total de {total_linhas} registros encontrados")
    
    # Garantir que o DataFrame tenha exatamente a mesma estrutura do template
    print("Ajustando formato para seguir o template...")
    df = garantir_formato_template(df)
    
    # Obter tamanho do arquivo
    tamanho_arquivo = os.path.getsize(arquivo_contas)
    tamanho_mb = tamanho_arquivo / (1024 * 1024)
    print(f"Tamanho do arquivo: {tamanho_mb:.2f}MB")

    # Calcular o número aproximado de linhas por arquivo
    # para manter cada um com aproximadamente 500KB
    bytes_por_linha = tamanho_arquivo / total_linhas
    linhas_por_arquivo = int(MAX_FILE_SIZE * 0.95 / bytes_por_linha)  # 5% de margem de segurança
    
    # Garantir um número válido de linhas por arquivo
    linhas_por_arquivo = max(1, min(linhas_por_arquivo, total_linhas))
    
    # Calcular número de arquivos necessários
    total_arquivos = math.ceil(total_linhas / linhas_por_arquivo)
    print(f"Estratégia: Dividir em {total_arquivos} arquivos com aproximadamente {linhas_por_arquivo} linhas cada")

    arquivos_criados = []
    
    # Dividir o DataFrame e salvar cada parte
    for i in range(total_arquivos):
        inicio = i * linhas_por_arquivo
        fim = min((i + 1) * linhas_por_arquivo, total_linhas)
        
        # Extrair parte do DataFrame
        parte = df.iloc[inicio:fim].copy()
        
        # Salvar a parte como arquivo Excel
        nome_arquivo = f"contas_receber_parte_{i+1:03d}.xlsx"  # Usar 3 dígitos para comportar mais arquivos
        caminho_arquivo = os.path.join(OUTPUT_DIR, nome_arquivo)
        parte.to_excel(caminho_arquivo, index=False)
        
        # Verificar tamanho real do arquivo salvo
        tamanho_real = os.path.getsize(caminho_arquivo)
        tamanho_real_kb = tamanho_real / 1024
        
        print(f"Parte {i+1}/{total_arquivos}: {nome_arquivo} - {tamanho_real_kb:.0f}KB, {len(parte)} linhas")
        arquivos_criados.append(caminho_arquivo)
    
    print(f"\nDivisão concluída. {len(arquivos_criados)} arquivos criados no diretório {OUTPUT_DIR}")
    return arquivos_criados

if __name__ == "__main__":
    print(f"Iniciando processamento em {datetime.now().strftime('%H:%M:%S')}")
    dividir_contas_receber()
    print(f"Processamento concluído em {datetime.now().strftime('%H:%M:%S')}") 