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

# Colunas exatas do template.xlsx
COLUNAS_TEMPLATE = [
    'ID', 'Código', 'Nome', 'Fantasia', 'Endereço', 'Número', 'Complemento',
    'Bairro', 'CEP', 'Cidade', 'Estado', 'Observações do contato', 'Fone',
    'Fax', 'Celular', 'E-mail', 'Web Site', 'Tipo pessoa', 'CNPJ / CPF',
    'IE / RG', 'IE isento', 'Situação', 'Observações', 'Estado civil',
    'Profissão', 'Sexo', 'Data nascimento', 'Naturalidade', 'Nome pai',
    'CPF pai', 'Nome mãe', 'CPF mãe', 'Lista de Preço', 'Vendedor',
    'E-mail para envio de NFe', 'Tipos de Contatos', 'Contribuinte',
    'Código de regime tributário', 'Limite de crédito'
]

def limpar_cpf_cnpj(valor):
    """Remove todos os caracteres não numéricos do CPF/CNPJ"""
    if pd.isna(valor) or valor == '':
        return ''
    return re.sub(r'[^0-9]', '', str(valor))

def formatar_coluna(valor, coluna):
    """Formatar valor de acordo com o tipo de coluna"""
    # Para valores nulos
    if pd.isna(valor) or valor == '':
        # Deixar em branco para todos os campos exceto Contribuinte e Limite de crédito
        if coluna == 'Contribuinte':
            return 0
        elif coluna == 'Limite de crédito':
            return 0
        else:
            return ''
    
    # Para CPF/CNPJ - manter apenas números
    if coluna == 'CNPJ / CPF':
        return limpar_cpf_cnpj(valor)
    
    # Para CPF do pai e da mãe - manter apenas números
    if coluna in ['CPF pai', 'CPF mãe']:
        return limpar_cpf_cnpj(valor)
    
    # Para datas - formato DD/MM/YYYY
    if coluna == 'Data nascimento':
        try:
            data = pd.to_datetime(valor, dayfirst=True, errors='coerce')
            if pd.isna(data):
                return ''
            return data.strftime('%d/%m/%Y')
        except:
            return ''
    
    # Para Contribuinte - garantir que seja 0 ou 1
    if coluna == 'Contribuinte':
        try:
            num = pd.to_numeric(valor, errors='coerce')
            if pd.isna(num):
                return 0
            return 1 if num > 0 else 0
        except:
            return 0
    
    # Para Limite de crédito - garantir que seja numérico
    if coluna == 'Limite de crédito':
        try:
            num = pd.to_numeric(valor, errors='coerce')
            if pd.isna(num):
                return 0
            return num
        except:
            return 0
    
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
            if coluna == 'Contribuinte':
                df_formatado[coluna] = 0
            elif coluna == 'Limite de crédito':
                df_formatado[coluna] = 0
            else:
                df_formatado[coluna] = ''
    
    return df_formatado

def dividir_contatos():
    """
    Divide a planilha de contatos em partes menores de até 500KB,
    utilizando divisão direta por número de linhas.
    """
    print(f"Dividindo planilha de contatos em partes de até {MAX_FILE_SIZE/1024:.0f}KB")
    
    # Verificar se o arquivo de contatos existe
    arquivo_contatos = os.path.join(INPUT_DIR, 'contatos.xlsx')
    if not os.path.exists(arquivo_contatos):
        print(f"Erro: Arquivo {arquivo_contatos} não encontrado!")
        return
    
    # Ler a planilha de contatos
    print(f"Lendo arquivo {arquivo_contatos}...")
    df = pd.read_excel(arquivo_contatos)
    total_linhas = len(df)
    print(f"Total de {total_linhas} contatos encontrados")
    
    # Garantir que o DataFrame tenha exatamente a mesma estrutura do template
    print("Ajustando formato para seguir o template...")
    df = garantir_formato_template(df)
    
    # Obter tamanho do arquivo
    tamanho_arquivo = os.path.getsize(arquivo_contatos)
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
        nome_arquivo = f"contatos_parte_{i+1:03d}.xlsx"  # Usar 3 dígitos para comportar mais arquivos
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
    dividir_contatos()
    print(f"Processamento concluído em {datetime.now().strftime('%H:%M:%S')}") 