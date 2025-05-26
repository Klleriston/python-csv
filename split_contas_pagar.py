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

# Estabelecimentos a filtrar
ESTABELECIMENTOS_ALVO = [2, 5]

# Colunas exatas do contas_pagar_template.xls
COLUNAS_TEMPLATE = [
    "Id", "Fornecedor", "Data Emissao", "Data vencimento", "Data Liquidacao",
    "Valor documento", "Saldo", "Situacao", "Numero do documento", "Numero no banco",
    "Categoria", "Historico", "Forma de pagamento", "Meio de pagamento",
    "Taxas", "Estabelecimento_id"  # Adicionada a coluna Estabelecimento_id
]

# Mapeamento de números para nomes dos meses
MESES = {
    1: 'jan', 2: 'fev', 3: 'mar', 4: 'abr', 5: 'mai', 6: 'jun',
    7: 'jul', 8: 'ago', 9: 'set', 10: 'out', 11: 'nov', 12: 'dez',
    'sem_data': 'sem_data'
}

def formatar_coluna(valor, coluna):
    """Formatar valor de acordo com o tipo de coluna"""
    # Para valores nulos
    if pd.isna(valor) or valor == '':
        # Deixar em branco para todos os campos
        if coluna in ['Valor documento', 'Saldo', 'Taxas']:
            return 0
        elif coluna == 'Estabelecimento_id':
            return 0  # Valor padrão para Estabelecimento_id vazio
        else:
            return ''
    
    # Para campos numéricos
    if coluna in ['Valor documento', 'Saldo', 'Taxas', 'Estabelecimento_id']:
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
    
    # Para a coluna "Situacao", transformar "liquidado" em "paga"
    if coluna == 'Situacao' and str(valor).lower() == 'liquidado':
        return 'paga'
    
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
            if coluna in ['Valor documento', 'Saldo', 'Taxas', 'Estabelecimento_id']:
                df_formatado[coluna] = 0
            else:
                df_formatado[coluna] = ''
    
    return df_formatado

def obter_mes_vencimento(valor):
    """Extrai o mês de uma data de vencimento"""
    if pd.isna(valor) or valor == '':
        return 'sem_data'
    
    # Verificar se é uma data
    try:
        data = pd.to_datetime(valor, dayfirst=True, errors='coerce')
        if not pd.isna(data):
            return data.month
    except:
        pass
    
    return 'sem_data'

def dividir_contas_pagar():
    """
    Divide a planilha de contas a pagar por estabelecimento (apenas IDs 2 e 5),
    depois por mês e, dentro de cada mês, em partes menores de até 500KB.
    """
    print(f"Dividindo planilha de contas a pagar por estabelecimento, mês e em partes de até {MAX_FILE_SIZE/1024:.0f}KB")
    
    # Verificar se o arquivo de contas a pagar existe
    arquivo_contas = os.path.join(INPUT_DIR, 'contas_a_pagar.xlsx')
    if not os.path.exists(arquivo_contas):
        print(f"Erro: Arquivo {arquivo_contas} não encontrado!")
        return
    
    # Ler a planilha de contas a pagar
    print(f"Lendo arquivo {arquivo_contas}...")
    df = pd.read_excel(arquivo_contas)
    total_linhas = len(df)
    print(f"Total de {total_linhas} registros encontrados")
    
    # Garantir que o DataFrame tenha exatamente a mesma estrutura do template
    print("Ajustando formato para seguir o template...")
    df = garantir_formato_template(df)
    
    # Filtrar apenas os estabelecimentos alvo
    print(f"Filtrando apenas estabelecimentos com IDs {ESTABELECIMENTOS_ALVO}...")
    df_filtrado = df[df['Estabelecimento_id'].isin(ESTABELECIMENTOS_ALVO)].copy()
    
    total_linhas_filtrado = len(df_filtrado)
    print(f"Total de {total_linhas_filtrado} registros após filtro de estabelecimentos")
    
    if total_linhas_filtrado == 0:
        print("Nenhum registro encontrado com os IDs de estabelecimento solicitados!")
        return
    
    # Extrair o mês da data de vencimento
    print("Agrupando por estabelecimento e mês de vencimento...")
    df_filtrado['mes'] = df_filtrado['Data vencimento'].apply(obter_mes_vencimento)
    
    # Obter tamanho do arquivo
    tamanho_arquivo = os.path.getsize(arquivo_contas)
    tamanho_mb = tamanho_arquivo / (1024 * 1024)
    print(f"Tamanho do arquivo original: {tamanho_mb:.2f}MB")

    # Calcular o número aproximado de bytes por linha
    bytes_por_linha = tamanho_arquivo / total_linhas
    
    # Processar cada estabelecimento separadamente
    arquivos_criados = []
    
    for estabelecimento_id in ESTABELECIMENTOS_ALVO:
        df_estabelecimento = df_filtrado[df_filtrado['Estabelecimento_id'] == estabelecimento_id].copy()
        
        if len(df_estabelecimento) == 0:
            print(f"\nNenhum registro encontrado para estabelecimento ID {estabelecimento_id}")
            continue
            
        print(f"\nProcessando estabelecimento ID {estabelecimento_id} ({len(df_estabelecimento)} registros)")
        
        # Agrupar por mês para este estabelecimento
        meses_unicos = df_estabelecimento['mes'].unique()
        print(f"Encontrados {len(meses_unicos)} meses distintos para estabelecimento ID {estabelecimento_id}")
        
        for mes in sorted(meses_unicos):
            # Filtrar registros do mês
            df_mes = df_estabelecimento[df_estabelecimento['mes'] == mes].copy()
            df_mes = df_mes.drop(columns=['mes'])  # Remover coluna auxiliar
            
            total_linhas_mes = len(df_mes)
            nome_mes = MESES.get(mes, 'mes_desconhecido')
            
            print(f"\nProcessando estabelecimento {estabelecimento_id}, mês {nome_mes} ({total_linhas_mes} registros)")
            
            # Calcular o número aproximado de linhas por arquivo
            # para manter cada um com aproximadamente 500KB
            linhas_por_arquivo = int(MAX_FILE_SIZE * 0.95 / bytes_por_linha)  # 5% de margem de segurança
            
            # Garantir um número válido de linhas por arquivo
            linhas_por_arquivo = max(1, min(linhas_por_arquivo, total_linhas_mes))
            
            # Calcular número de arquivos necessários para este mês
            total_arquivos_mes = math.ceil(total_linhas_mes / linhas_por_arquivo)
            
            if total_arquivos_mes > 1:
                print(f"Estratégia: Dividir estabelecimento {estabelecimento_id}, mês {nome_mes} em {total_arquivos_mes} partes com aproximadamente {linhas_por_arquivo} linhas cada")
            
            # Dividir o DataFrame do mês e salvar cada parte
            for i in range(total_arquivos_mes):
                inicio = i * linhas_por_arquivo
                fim = min((i + 1) * linhas_por_arquivo, total_linhas_mes)
                
                # Extrair parte do DataFrame
                parte = df_mes.iloc[inicio:fim].copy()
                
                # Salvar a parte como arquivo Excel
                if total_arquivos_mes > 1:
                    nome_arquivo = f"contas_a_pagar_est_{estabelecimento_id}_{nome_mes}_parte_{i+1:03d}.xlsx"
                else:
                    nome_arquivo = f"contas_a_pagar_est_{estabelecimento_id}_{nome_mes}.xlsx"
                    
                caminho_arquivo = os.path.join(OUTPUT_DIR, nome_arquivo)
                parte.to_excel(caminho_arquivo, index=False)
                
                # Verificar tamanho real do arquivo salvo
                tamanho_real = os.path.getsize(caminho_arquivo)
                tamanho_real_kb = tamanho_real / 1024
                
                if total_arquivos_mes > 1:
                    print(f"Parte {i+1}/{total_arquivos_mes}: {nome_arquivo} - {tamanho_real_kb:.0f}KB, {len(parte)} linhas")
                else:
                    print(f"{nome_arquivo} - {tamanho_real_kb:.0f}KB, {len(parte)} linhas")
                    
                arquivos_criados.append(caminho_arquivo)
    
    print(f"\nDivisão concluída. {len(arquivos_criados)} arquivos criados no diretório {OUTPUT_DIR}")
    return arquivos_criados

if __name__ == "__main__":
    print(f"Iniciando processamento em {datetime.now().strftime('%H:%M:%S')}")
    dividir_contas_pagar()
    print(f"Processamento concluído em {datetime.now().strftime('%H:%M:%S')}")