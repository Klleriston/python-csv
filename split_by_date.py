import pandas as pd
import os
import math
from datetime import datetime, timedelta

SPLIT_OUTPUT_DIR = 'exported_data_split'
os.makedirs(SPLIT_OUTPUT_DIR, exist_ok=True)

INPUT_DIR = 'exported_data'

MAX_FILE_SIZE = 2000 * 1024  # 2.000KB (um pouco menor que 2MB para garantir compatibilidade)

def estimate_csv_size(df):
    csv_data = df.to_csv(index=False).encode('utf-8-sig')
    return len(csv_data)

def split_by_date_range(df, date_column, max_size):
    df = df.sort_values(by=date_column)
    
    if not pd.api.types.is_datetime64_dtype(df[date_column]):
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    
    hoje = pd.Timestamp.now()
    limite_futuro = hoje + pd.DateOffset(years=10)
    limite_passado = hoje - pd.DateOffset(years=20)
    
    datas_futuro = df[df[date_column] > limite_futuro]
    if len(datas_futuro) > 0:
        print(f"Aviso: Encontradas {len(datas_futuro)} datas além de {limite_futuro.strftime('%d-%m-%Y')}.")
        print("Essas datas podem ser erros de digitação. Registros com erro:")
        
        for i, row in datas_futuro.head(5).iterrows():
            data_atual = row[date_column]
            print(f"  - Registro com ID {row.get('Id', i)}: Data {date_column} = {data_atual.strftime('%d-%m-%Y')}")
        
        report_filename = os.path.join(SPLIT_OUTPUT_DIR, f'erros_datas_futuras_{date_column.replace(" ", "_")}.csv')
        datas_futuro.to_csv(report_filename, index=False, encoding='utf-8-sig')
        print(f"  Registros com datas futuras salvos em: {report_filename}")
    
    datas_antigas = df[df[date_column] < limite_passado]
    if len(datas_antigas) > 0:
        print(f"Aviso: Encontradas {len(datas_antigas)} datas anteriores a {limite_passado.strftime('%d-%m-%Y')}.")
        print("Essas datas podem ser erros de digitação. Registros com erro:")
        
        
        for i, row in datas_antigas.head(5).iterrows():
            data_atual = row[date_column]
            print(f"  - Registro com ID {row.get('Id', i)}: Data {date_column} = {data_atual.strftime('%d-%m-%Y')}")
        
        report_filename = os.path.join(SPLIT_OUTPUT_DIR, f'erros_datas_antigas_{date_column.replace(" ", "_")}.csv')
        datas_antigas.to_csv(report_filename, index=False, encoding='utf-8-sig')
        print(f"  Registros com datas antigas salvos em: {report_filename}")
    
    min_date = df[date_column].min()
    max_date = df[date_column].max()
    
    if pd.isnull(min_date) or pd.isnull(max_date):
        print(f"Aviso: Intervalo de datas inválido detectado. Usando divisão baseada em linhas.")
        return split_by_rows(df, max_size)
    
    print(f"Intervalo de datas: {min_date} até {max_date}")
    
    current_chunks = []
    
    current_date = min_date.replace(month=1, day=1)
    year = current_date.year
    current_date = pd.Timestamp(year=year, month=1, day=1)
    chunks = []
    
    while current_date <= max_date:
        next_date = pd.Timestamp(year=current_date.year + 5, month=1, day=1)
        date_label = f"{current_date.strftime('%Y')}-{next_date.year - 1}"
        
        chunk = df[(df[date_column] >= current_date) & (df[date_column] < next_date)]
        
        if len(chunk) > 0:
            size = estimate_csv_size(chunk)
            chunks.append({
                'start_date': current_date, 
                'end_date': next_date,
                'date_label': date_label,
                'data': chunk,
                'size': size
            })
        
        current_date = next_date
    
    oversized = any(chunk['size'] > MAX_FILE_SIZE for chunk in chunks)
    
    if oversized:
        current_date = min_date.replace(month=1, day=1)
        year = current_date.year
        current_date = pd.Timestamp(year=year, month=1, day=1)
        chunks = []
        
        while current_date <= max_date:
            next_date = current_date + pd.DateOffset(years=1)
            date_label = current_date.strftime('%Y')
            
            chunk = df[(df[date_column] >= current_date) & (df[date_column] < next_date)]
            
            if len(chunk) > 0:
                size = estimate_csv_size(chunk)
                chunks.append({
                    'start_date': current_date, 
                    'end_date': next_date,
                    'date_label': date_label,
                    'data': chunk,
                    'size': size
                })
            
            current_date = next_date
        
        oversized = any(chunk['size'] > MAX_FILE_SIZE for chunk in chunks)
        
        if oversized:
            current_date = min_date
            chunks = []
            
            while current_date <= max_date:
                next_date = current_date + pd.offsets.MonthEnd(1)
                date_label = current_date.strftime('%m-%Y')
                
                chunk = df[(df[date_column] >= current_date) & (df[date_column] < next_date)]
                
                if len(chunk) > 0:
                    size = estimate_csv_size(chunk)
                    chunks.append({
                        'start_date': current_date, 
                        'end_date': next_date,
                        'date_label': date_label,
                        'data': chunk,
                        'size': size
                    })
                
                current_date = next_date
            
            oversized = any(chunk['size'] > MAX_FILE_SIZE for chunk in chunks)
            
            if oversized:
                current_date = min_date
                chunks = []
                
                while current_date <= max_date:
                    next_date = current_date + pd.offsets.Week(1)
                    date_label = f"{current_date.strftime('%d-%m-%Y')}_a_{next_date.strftime('%d-%m-%Y')}"
                    
                    chunk = df[(df[date_column] >= current_date) & (df[date_column] < next_date)]
                    
                    if len(chunk) > 0:
                        size = estimate_csv_size(chunk)
                        chunks.append({
                            'start_date': current_date, 
                            'end_date': next_date,
                            'date_label': date_label,
                            'data': chunk,
                            'size': size
                        })
                    
                    current_date = next_date
                
                if any(chunk['size'] > MAX_FILE_SIZE for chunk in chunks):
                    current_date = min_date
                    chunks = []
                    
                    while current_date <= max_date:
                        next_date = current_date + timedelta(days=1)
                        date_label = current_date.strftime('%d-%m-%Y')
                        
                        chunk = df[(df[date_column] >= current_date) & (df[date_column] < next_date)]
                        
                        if len(chunk) > 0:
                            size = estimate_csv_size(chunk)
                            chunks.append({
                                'start_date': current_date, 
                                'end_date': next_date,
                                'date_label': date_label,
                                'data': chunk,
                                'size': size
                            })
                        
                        current_date = next_date
    
    current_chunks = chunks
    
    if any(chunk['size'] > MAX_FILE_SIZE for chunk in current_chunks):
        oversized_chunks = []
        for chunk in current_chunks:
            if chunk['size'] > MAX_FILE_SIZE:
                row_chunks = split_by_rows(chunk['data'], MAX_FILE_SIZE)
                for i, row_chunk in enumerate(row_chunks):
                    oversized_chunks.append({
                        'start_date': chunk['start_date'],
                        'end_date': chunk['end_date'],
                        'date_label': f"{chunk['date_label']} (parte {i+1})",
                        'data': row_chunk,
                        'size': estimate_csv_size(row_chunk)
                    })
            else:
                oversized_chunks.append(chunk)
        
        current_chunks = oversized_chunks
    
    return current_chunks

def split_by_rows(df, max_size):
    total_rows = len(df)
    
    size_per_row = estimate_csv_size(df) / total_rows if total_rows > 0 else 0
    
    rows_per_chunk = math.floor((max_size * 0.9) / size_per_row) if size_per_row > 0 else 1000
    rows_per_chunk = max(1, rows_per_chunk)
    
    chunks = []
    for i in range(0, total_rows, rows_per_chunk):
        end_idx = min(i + rows_per_chunk, total_rows)
        chunk = df.iloc[i:end_idx]
        
        real_size = estimate_csv_size(chunk)
        
        if real_size > max_size and len(chunk) > 1:
            mid_point = len(chunk) // 2
            first_half = chunk.iloc[:mid_point]
            second_half = chunk.iloc[mid_point:]
            
            chunks.extend(split_by_rows(first_half, max_size))
            chunks.extend(split_by_rows(second_half, max_size))
        else:
            chunks.append(chunk)
    
    return chunks

def verificar_valores_nulos(df, colunas_importantes):
    erros = []
    
    for coluna in colunas_importantes:
        if coluna in df.columns:
            nulos = df[df[coluna].isnull()]
            if len(nulos) > 0:
                erros.append({
                    'coluna': coluna,
                    'registros': nulos
                })
                print(f"Aviso: Encontrados {len(nulos)} registros com valor nulo na coluna '{coluna}'")
    
    return erros

def verificar_inconsistencias(df, tipo_arquivo):
    inconsistencias = {}
    
    if tipo_arquivo == 'contas_pagar' or tipo_arquivo == 'contas_receber':
        data_emissao = 'Data emissao' if 'Data emissao' in df.columns else 'Data Emissao'
        data_vencimento = 'Data vencimento' if 'Data vencimento' in df.columns else None
        data_liquidacao = 'Data Liquidacao' if 'Data Liquidacao' in df.columns else None
        
        if data_emissao and data_vencimento and data_emissao in df.columns and data_vencimento in df.columns:
            df[data_emissao] = pd.to_datetime(df[data_emissao], errors='coerce')
            df[data_vencimento] = pd.to_datetime(df[data_vencimento], errors='coerce')
            
            inconsistentes = df[(~df[data_emissao].isnull()) & 
                                 (~df[data_vencimento].isnull()) & 
                                 (df[data_vencimento] < df[data_emissao])]
            
            if len(inconsistentes) > 0:
                inconsistencias['vencimento_anterior_emissao'] = inconsistentes
                print(f"Aviso: Encontrados {len(inconsistentes)} registros onde a data de vencimento é anterior à data de emissão")
        
        if data_emissao and data_liquidacao and data_emissao in df.columns and data_liquidacao in df.columns:
            df[data_emissao] = pd.to_datetime(df[data_emissao], errors='coerce')
            df[data_liquidacao] = pd.to_datetime(df[data_liquidacao], errors='coerce')
            
            inconsistentes = df[(~df[data_emissao].isnull()) & 
                                 (~df[data_liquidacao].isnull()) & 
                                 (df[data_liquidacao] < df[data_emissao])]
            
            if len(inconsistentes) > 0:
                inconsistencias['liquidacao_anterior_emissao'] = inconsistentes
                print(f"Aviso: Encontrados {len(inconsistentes)} registros onde a data de liquidação é anterior à data de emissão")
        
        if 'Valor documento' in df.columns:
            inconsistentes = df[df['Valor documento'] <= 0]
            if len(inconsistentes) > 0:
                inconsistencias['valor_documento_invalido'] = inconsistentes
                print(f"Aviso: Encontrados {len(inconsistentes)} registros com valor de documento zero ou negativo")
    
    elif tipo_arquivo == 'contatos':
        if 'CNPJ/CPF' in df.columns:
            df['doc_numeric'] = df['CNPJ/CPF'].astype(str).str.replace(r'\D', '', regex=True)
            
            inconsistentes = df[(df['doc_numeric'].str.len() != 11) & 
                                 (df['doc_numeric'].str.len() != 14) &
                                 (df['doc_numeric'].str.len() > 0)] 
            
            if len(inconsistentes) > 0:
                inconsistencias['cpf_cnpj_invalido'] = inconsistentes
                print(f"Aviso: Encontrados {len(inconsistentes)} registros com CPF/CNPJ de tamanho inválido")
            
            df.drop('doc_numeric', axis=1, inplace=True)
    
    return inconsistencias

def preencher_valores_ausentes(df, tipo_arquivo):
    hoje = datetime.now()
    
    if tipo_arquivo == 'contatos':
        if 'Data nascimento' in df.columns:
            data_padrao = hoje
            df['Data nascimento'] = pd.to_datetime(df['Data nascimento'], errors='coerce')
            ausentes = df['Data nascimento'].isnull().sum()
            if ausentes > 0:
                print(f"Preenchendo {ausentes} datas de nascimento ausentes com a data atual ({hoje.strftime('%d-%m-%Y')})")
                df.loc[df['Data nascimento'].isnull(), 'Data nascimento'] = data_padrao
        
        if 'CNPJ/CPF' in df.columns:
            cpf_padrao = '00000000000'
            ausentes = df['CNPJ/CPF'].isnull().sum()
            if ausentes > 0:
                print(f"Preenchendo {ausentes} CPF/CNPJ ausentes com valor padrão (zeros)")
                df['cpf_temp'] = cpf_padrao
                if 'Tipo Pessoa' in df.columns:
                    df.loc[df['Tipo Pessoa'] == 'Jurídica', 'cpf_temp'] = '00000000000000'
                df.loc[df['CNPJ/CPF'].isnull(), 'CNPJ/CPF'] = df.loc[df['CNPJ/CPF'].isnull(), 'cpf_temp']
                df.drop('cpf_temp', axis=1, inplace=True)
        
        if 'Nome' in df.columns:
            ausentes = df['Nome'].isnull().sum()
            if ausentes > 0:
                print(f"Preenchendo {ausentes} nomes ausentes com valor padrão")
                for idx in df[df['Nome'].isnull()].index:
                    id_valor = df.loc[idx, 'Id'] if 'Id' in df.columns and not pd.isnull(df.loc[idx, 'Id']) else idx
                    df.loc[idx, 'Nome'] = f"Cliente {id_valor}"
        
        campos_texto = ['Endereço', 'Bairro', 'Cidade', 'Situação']
        for campo in campos_texto:
            if campo in df.columns:
                ausentes = df[campo].isnull().sum()
                if ausentes > 0:
                    print(f"Preenchendo {ausentes} valores ausentes em '{campo}' com valor padrão")
                    df.loc[df[campo].isnull(), campo] = 'N/A'
        
        if 'Estado' in df.columns:
            ausentes = df['Estado'].isnull().sum()
            if ausentes > 0:
                print(f"Preenchendo {ausentes} estados ausentes com valor padrão (UF)")
                df.loc[df['Estado'].isnull(), 'Estado'] = 'UF'
        
        if 'E-mail' in df.columns:
            ausentes = df['E-mail'].isnull().sum()
            if ausentes > 0:
                print(f"Preenchendo {ausentes} e-mails ausentes com valor padrão")
                for idx in df[df['E-mail'].isnull()].index:
                    id_valor = df.loc[idx, 'Id'] if 'Id' in df.columns and not pd.isnull(df.loc[idx, 'Id']) else idx
                    df.loc[idx, 'E-mail'] = f"contato{id_valor}@exemplo.com"
    
    elif tipo_arquivo in ['contas_pagar', 'contas_receber']:
        data_emissao = 'Data emissao' if 'Data emissao' in df.columns else 'Data Emissao'
        if data_emissao in df.columns:
            data_padrao = hoje
            df[data_emissao] = pd.to_datetime(df[data_emissao], errors='coerce')
            ausentes = df[data_emissao].isnull().sum()
            if ausentes > 0:
                print(f"Preenchendo {ausentes} datas de emissão ausentes com a data atual ({hoje.strftime('%d-%m-%Y')})")
                df.loc[df[data_emissao].isnull(), data_emissao] = data_padrao
        
        data_vencimento = 'Data vencimento' if 'Data vencimento' in df.columns else None
        if data_vencimento and data_vencimento in df.columns:
            df[data_vencimento] = pd.to_datetime(df[data_vencimento], errors='coerce')
            ausentes = df[data_vencimento].isnull().sum()
            if ausentes > 0:
                print(f"Preenchendo {ausentes} datas de vencimento ausentes com mesmo valor da data de emissão")
                df.loc[df[data_vencimento].isnull(), data_vencimento] = df.loc[df[data_vencimento].isnull(), data_emissao]
        
        cliente_col = 'Cliente' if 'Cliente' in df.columns else 'Fornecedor'
        if cliente_col in df.columns:
            ausentes = df[cliente_col].isnull().sum()
            if ausentes > 0:
                print(f"Preenchendo {ausentes} valores ausentes em '{cliente_col}' com valor padrão")
                for idx in df[df[cliente_col].isnull()].index:
                    id_valor = df.loc[idx, 'Id'] if 'Id' in df.columns and not pd.isnull(df.loc[idx, 'Id']) else idx
                    df.loc[idx, cliente_col] = f"Cliente {id_valor}"
        
        if 'Valor documento' in df.columns:
            ausentes = df['Valor documento'].isnull().sum()
            if ausentes > 0:
                print(f"Preenchendo {ausentes} valores de documento ausentes com valor padrão (1.00)")
                df.loc[df['Valor documento'].isnull(), 'Valor documento'] = 1.00
        
        situacao_col = 'Situacao' if 'Situacao' in df.columns else 'Situação'
        if situacao_col in df.columns:
            ausentes = df[situacao_col].isnull().sum()
            if ausentes > 0:
                print(f"Preenchendo {ausentes} situações ausentes com valor padrão ('Em Aberto')")
                df.loc[df[situacao_col].isnull(), situacao_col] = 'Em Aberto'
    
    return df

def process_accounts_payable():
    print("Processando Contas a Pagar...")
    
    file_path = os.path.join(INPUT_DIR, 'contas_a_pagar.xlsx')
    if not os.path.exists(file_path):
        print(f"Erro: Arquivo não encontrado em {file_path}")
        return
        
    df = pd.read_excel(file_path)
    
    if len(df) == 0:
        print("Aviso: Arquivo de Contas a Pagar está vazio")
        return
    
    print("Verificando erros de cadastro...")
    colunas_importantes = ['Data emissao', 'Data vencimento', 'Valor documento', 'Fornecedor', 'Estabelecimento_id']
    erros = verificar_valores_nulos(df, colunas_importantes)
    
    if erros:
        for erro in erros:
            coluna = erro['coluna']
            registros = erro['registros']
            report_filename = os.path.join(SPLIT_OUTPUT_DIR, f'erros_nulos_contas_pagar_{coluna.replace(" ", "_")}.csv')
            registros.to_csv(report_filename, index=False, encoding='utf-8-sig')
            print(f"  Registros com valores nulos em '{coluna}' salvos em: {report_filename}")
        
    inconsistencias = verificar_inconsistencias(df, 'contas_pagar')
    if inconsistencias:
        for tipo, registros in inconsistencias.items():
            report_filename = os.path.join(SPLIT_OUTPUT_DIR, f'erros_inconsistencia_contas_pagar_{tipo}.csv')
            registros.to_csv(report_filename, index=False, encoding='utf-8-sig')
            print(f"  Registros com inconsistência '{tipo}' salvos em: {report_filename}")
    
    print("Preenchendo valores ausentes com padrões...")
    df = preencher_valores_ausentes(df, 'contas_pagar')
    
    complete_file_path = os.path.join(SPLIT_OUTPUT_DIR, 'contas_a_pagar_completo.xlsx')
    df.to_excel(complete_file_path, index=False)
    complete_size = os.path.getsize(complete_file_path)
    print(f"Arquivo completo salvo: {complete_file_path} ({complete_size / (1024*1024):.2f} MB)")

    try:
        import split_contas_pagar
        print("Usando script personalizado para divisão de contas a pagar por estabelecimento...")
        split_contas_pagar.dividir_contas_pagar()
        return
    except Exception as e:
        print(f"Erro ao usar script personalizado: {str(e)}")
        print("Continuando com método padrão de divisão...")

    if complete_size < MAX_FILE_SIZE:
        print("Arquivo completo é menor que 2MB, não é necessário dividir.")
        return

    if 'Data emissao' in df.columns:
        date_column = 'Data emissao'
    elif 'Data Emissao' in df.columns:
        date_column = 'Data Emissao'
    else:
        date_column = None
        print("Aviso: Coluna de data não encontrada para Contas a Pagar.")
    
    if date_column:
        print(f"Dividindo Contas a Pagar por períodos (estratégia: 5 anos → 1 ano → mês → semana → dia)")
        chunks = split_by_date_range(df, date_column, MAX_FILE_SIZE)
        
        for i, chunk in enumerate(chunks):
            file_name = f"contas_a_pagar_{chunk['date_label'].replace(' ', '_').replace(':', '')}.xlsx"
            file_path = os.path.join(SPLIT_OUTPUT_DIR, file_name)
            chunk['data'].to_excel(file_path, index=False)
            chunk_size = os.path.getsize(file_path)
            if chunk_size > MAX_FILE_SIZE:
                print(f"ATENÇÃO: Arquivo {file_path} excede o limite de {MAX_FILE_SIZE/1024:.0f}KB ({chunk_size/1024:.0f}KB). Dividindo novamente...")
                subchunks = split_by_rows(chunk['data'], MAX_FILE_SIZE * 0.95)
                os.remove(file_path)
                for j, subchunk in enumerate(subchunks):
                    subfile_name = f"contas_a_pagar_{chunk['date_label'].replace(' ', '_').replace(':', '')}_parte{j+1}.xlsx"
                    subfile_path = os.path.join(SPLIT_OUTPUT_DIR, subfile_name)
                    subchunk.to_excel(subfile_path, index=False)
                    subchunk_size = os.path.getsize(subfile_path)
                    print(f"  Subparte {j+1}/{len(subchunks)} salva: {subfile_path} ({subchunk_size / 1024:.0f}KB, {len(subchunk)} linhas)")
            else:
                print(f"Parte {i+1}/{len(chunks)} salva: {file_path} ({chunk_size / 1024:.0f}KB, {len(chunk['data'])} linhas)")
    else:
        chunks = split_by_rows(df, MAX_FILE_SIZE)
        
        for i, chunk in enumerate(chunks):
            file_name = f"contas_a_pagar_parte_{i+1}.xlsx"
            file_path = os.path.join(SPLIT_OUTPUT_DIR, file_name)
            chunk.to_excel(file_path, index=False)
            chunk_size = os.path.getsize(file_path)
            print(f"Parte {i+1}/{len(chunks)} salva: {file_path} ({chunk_size / 1024:.0f}KB, {len(chunk)} linhas)")

def process_accounts_receivable():
    print("Processando Contas a Receber...")
    
    file_path = os.path.join(INPUT_DIR, 'contas_a_receber.xlsx')
    if not os.path.exists(file_path):
        print(f"Erro: Arquivo não encontrado em {file_path}")
        return
        
    df = pd.read_excel(file_path)
    
    if len(df) == 0:
        print("Aviso: Arquivo de Contas a Receber está vazio")
        return
    
    print("Verificando erros de cadastro...")
    colunas_importantes = ['Data Emissao', 'Data vencimento', 'Valor documento', 'Cliente', 'Estabelecimento_id']
    erros = verificar_valores_nulos(df, colunas_importantes)
    
    if erros:
        for erro in erros:
            coluna = erro['coluna']
            registros = erro['registros']
            report_filename = os.path.join(SPLIT_OUTPUT_DIR, f'erros_nulos_contas_receber_{coluna.replace(" ", "_")}.xlsx')
            registros.to_excel(report_filename, index=False)
            print(f"  Registros com valores nulos em '{coluna}' salvos em: {report_filename}")
    
    inconsistencias = verificar_inconsistencias(df, 'contas_receber')
    if inconsistencias:
        for tipo, registros in inconsistencias.items():
            report_filename = os.path.join(SPLIT_OUTPUT_DIR, f'erros_inconsistencia_contas_receber_{tipo}.xlsx')
            registros.to_excel(report_filename, index=False)
            print(f"  Registros com inconsistência '{tipo}' salvos em: {report_filename}")
    
    print("Preenchendo valores ausentes com padrões...")
    df = preencher_valores_ausentes(df, 'contas_receber')
    
    complete_file_path = os.path.join(SPLIT_OUTPUT_DIR, 'contas_a_receber_completo.xlsx')
    df.to_excel(complete_file_path, index=False)
    complete_size = os.path.getsize(complete_file_path)
    print(f"Arquivo completo salvo: {complete_file_path} ({complete_size / (1024*1024):.2f} MB)")
    
    try:
        import split_contas_receber
        print("Usando script personalizado para divisão de contas a receber por estabelecimento...")
        split_contas_receber.dividir_contas_receber()
        return
    except Exception as e:
        print(f"Erro ao usar script personalizado: {str(e)}")
        print("Continuando com método padrão de divisão...")
    
    if complete_size < MAX_FILE_SIZE:
        print("Arquivo completo é menor que 2MB, não é necessário dividir.")
        return
    
    if 'Data Emissao' in df.columns:
        date_column = 'Data Emissao'
    elif 'Data emissao' in df.columns:
        date_column = 'Data emissao'
    else:
        date_column = None
        print("Aviso: Coluna de data não encontrada para Contas a Receber.")
    
    if date_column:
        print(f"Dividindo Contas a Receber por períodos (estratégia: 5 anos → 1 ano → mês → semana → dia)")
        chunks = split_by_date_range(df, date_column, MAX_FILE_SIZE)
        
        for i, chunk in enumerate(chunks):
            file_name = f"contas_a_receber_{chunk['date_label'].replace(' ', '_').replace(':', '')}.xlsx"
            file_path = os.path.join(SPLIT_OUTPUT_DIR, file_name)
            chunk['data'].to_excel(file_path, index=False)
            chunk_size = os.path.getsize(file_path)
            if chunk_size > MAX_FILE_SIZE:
                print(f"ATENÇÃO: Arquivo {file_path} excede o limite de {MAX_FILE_SIZE/1024:.0f}KB ({chunk_size/1024:.0f}KB). Dividindo novamente...")
                subchunks = split_by_rows(chunk['data'], MAX_FILE_SIZE * 0.95)
                os.remove(file_path)
                for j, subchunk in enumerate(subchunks):
                    subfile_name = f"contas_a_receber_{chunk['date_label'].replace(' ', '_').replace(':', '')}_parte{j+1}.xlsx"
                    subfile_path = os.path.join(SPLIT_OUTPUT_DIR, subfile_name)
                    subchunk.to_excel(subfile_path, index=False)
                    subchunk_size = os.path.getsize(subfile_path)
                    print(f"  Subparte {j+1}/{len(subchunks)} salva: {subfile_path} ({subchunk_size / 1024:.0f}KB, {len(subchunk)} linhas)")
            else:
                print(f"Parte {i+1}/{len(chunks)} salva: {file_path} ({chunk_size / 1024:.0f}KB, {len(chunk['data'])} linhas)")
    else:
        chunks = split_by_rows(df, MAX_FILE_SIZE)
        
        for i, chunk in enumerate(chunks):
            file_name = f"contas_a_receber_parte_{i+1}.xlsx"
            file_path = os.path.join(SPLIT_OUTPUT_DIR, file_name)
            chunk.to_excel(file_path, index=False)
            chunk_size = os.path.getsize(file_path)
            print(f"Parte {i+1}/{len(chunks)} salva: {file_path} ({chunk_size / 1024:.0f}KB, {len(chunk)} linhas)")

def process_contacts():
    print("Processando Contatos...")
    
    file_path = os.path.join(INPUT_DIR, 'contatos.csv')
    if not os.path.exists(file_path):
        print(f"Erro: Arquivo não encontrado em {file_path}")
        return
        
    df = pd.read_csv(file_path)
    
    if len(df) == 0:
        print("Aviso: Arquivo de Contatos está vazio")
        return
    
    print("Verificando erros de cadastro...")
    colunas_importantes = ['Nome', 'CNPJ/CPF', 'Situação']
    erros = verificar_valores_nulos(df, colunas_importantes)
    
    if erros:
        for erro in erros:
            coluna = erro['coluna']
            registros = erro['registros']
            report_filename = os.path.join(SPLIT_OUTPUT_DIR, f'erros_nulos_contatos_{coluna.replace(" ", "_").replace("/", "_")}.csv')
            registros.to_csv(report_filename, index=False, encoding='utf-8-sig')
            print(f"  Registros com valores nulos em '{coluna}' salvos em: {report_filename}")
    
    inconsistencias = verificar_inconsistencias(df, 'contatos')
    if inconsistencias:
        for tipo, registros in inconsistencias.items():
            report_filename = os.path.join(SPLIT_OUTPUT_DIR, f'erros_inconsistencia_contatos_{tipo}.csv')
            registros.to_csv(report_filename, index=False, encoding='utf-8-sig')
            print(f"  Registros com inconsistência '{tipo}' salvos em: {report_filename}")
    
    print("Preenchendo valores ausentes com padrões...")
    df = preencher_valores_ausentes(df, 'contatos')
    
    complete_file_path = os.path.join(SPLIT_OUTPUT_DIR, 'contatos_completo.csv')
    df.to_csv(complete_file_path, index=False, encoding='utf-8-sig')
    complete_size = os.path.getsize(complete_file_path)
    print(f"Arquivo completo salvo: {complete_file_path} ({complete_size / (1024*1024):.2f} MB)")
    
    if complete_size < MAX_FILE_SIZE:
        print("Arquivo completo é menor que 2MB, não é necessário dividir.")
        return
    
    date_columns = [col for col in df.columns if 'data' in col.lower()]
    
    if date_columns:
        print(f"Encontradas possíveis colunas de data: {', '.join(date_columns)}")
        date_column = date_columns[0]
        print(f"Usando coluna '{date_column}' para dividir por períodos (estratégia: 5 anos → 1 ano → mês → semana → dia)")
        chunks = split_by_date_range(df, date_column, MAX_FILE_SIZE)
        
        for i, chunk in enumerate(chunks):
            file_name = f"contatos_{chunk['date_label'].replace(' ', '_').replace(':', '')}.csv"
            file_path = os.path.join(SPLIT_OUTPUT_DIR, file_name)
            chunk['data'].to_csv(file_path, index=False, encoding='utf-8-sig')
            chunk_size = os.path.getsize(file_path)
            if chunk_size > MAX_FILE_SIZE:
                print(f"ATENÇÃO: Arquivo {file_path} excede o limite de {MAX_FILE_SIZE/1024:.0f}KB ({chunk_size/1024:.0f}KB). Dividindo novamente...")
                subchunks = split_by_rows(chunk['data'], MAX_FILE_SIZE * 0.95)
                os.remove(file_path)
                for j, subchunk in enumerate(subchunks):
                    subfile_name = f"contatos_{chunk['date_label'].replace(' ', '_').replace(':', '')}_parte{j+1}.csv"
                    subfile_path = os.path.join(SPLIT_OUTPUT_DIR, subfile_name)
                    subchunk.to_csv(subfile_path, index=False, encoding='utf-8-sig')
                    subchunk_size = os.path.getsize(subfile_path)
                    print(f"  Subparte {j+1}/{len(subchunks)} salva: {subfile_path} ({subchunk_size / 1024:.0f}KB, {len(subchunk)} linhas)")
            else:
                print(f"Parte {i+1}/{len(chunks)} salva: {file_path} ({chunk_size / 1024:.0f}KB, {len(chunk['data'])} linhas)")
    else:
        print("Nenhuma coluna de data encontrada para Contatos. Dividindo por número de linhas.")
        chunks = split_by_rows(df, MAX_FILE_SIZE)
        
        for i, chunk in enumerate(chunks):
            file_name = f"contatos_parte_{i+1}.csv"
            file_path = os.path.join(SPLIT_OUTPUT_DIR, file_name)
            chunk.to_csv(file_path, index=False, encoding='utf-8-sig')
            chunk_size = os.path.getsize(file_path)
            print(f"Parte {i+1}/{len(chunks)} salva: {file_path} ({chunk_size / 1024:.0f}KB, {len(chunk)} linhas)")

def adicional_split_large_files():
    print("\nVerificando se há arquivos individuais com mais de 2.000KB para subdividir...")
    
    arquivos_ignorar = [
        'contas_a_pagar_completo.csv', 
        'contas_a_receber_completo.csv', 
        'contatos_completo.csv'
    ]
    
    padroes_ignorar = ['erros_']
    
    for arquivo in os.listdir(SPLIT_OUTPUT_DIR):
        if not arquivo.endswith('.csv'):
            continue
            
        if any(ignorar in arquivo for ignorar in arquivos_ignorar):
            continue
            
        if any(padrao in arquivo for padrao in padroes_ignorar):
            continue
        
        caminho_arquivo = os.path.join(SPLIT_OUTPUT_DIR, arquivo)
        tamanho = os.path.getsize(caminho_arquivo)
        
        if tamanho > 1900 * 1024:
            print(f"Encontrado arquivo grande: {arquivo} ({tamanho/1024:.0f}KB)")
            
            df = pd.read_csv(caminho_arquivo)
            
            max_size_smaller = MAX_FILE_SIZE * 0.4
            chunks = split_by_rows(df, max_size_smaller)
            
            os.remove(caminho_arquivo)
            
            base_name = os.path.splitext(arquivo)[0]
            
            for i, chunk in enumerate(chunks):
                new_filename = f"{base_name}_parte_{i+1}.csv"
                new_filepath = os.path.join(SPLIT_OUTPUT_DIR, new_filename)
                chunk.to_csv(new_filepath, index=False, encoding='utf-8-sig')
                new_size = os.path.getsize(new_filepath)
                print(f"  Subdivisão {i+1}/{len(chunks)}: {new_filename} ({new_size/1024:.0f}KB, {len(chunk)} linhas)")
                
                if new_size > 1900 * 1024:
                    print(f"    Novo arquivo ainda excede o limite, subdividindo novamente...")
                    sub_df = pd.read_csv(new_filepath)
                    sub_max_size = MAX_FILE_SIZE * 0.25
                    sub_chunks = split_by_rows(sub_df, sub_max_size)
                    os.remove(new_filepath)
                    for j, sub_chunk in enumerate(sub_chunks):
                        sub_filename = f"{base_name}_parte_{i+1}_{j+1}.csv"
                        sub_filepath = os.path.join(SPLIT_OUTPUT_DIR, sub_filename)
                        sub_chunk.to_csv(sub_filepath, index=False, encoding='utf-8-sig')
                        sub_size = os.path.getsize(sub_filepath)
                        print(f"      Sub-subdivisão {j+1}/{len(sub_chunks)}: {sub_filename} ({sub_size/1024:.0f}KB, {len(sub_chunk)} linhas)")

if __name__ == "__main__":
    print(f"Iniciando processo de divisão de dados às {datetime.now().strftime('%H:%M:%S')}")
    print(f"Arquivos maiores que {MAX_FILE_SIZE/(1024)} KB serão divididos")
    print(f"Estratégia: Agrupar por períodos de 5 anos, reduzindo gradualmente até encontrar tamanho adequado")
    
    estatisticas = {
        'contas_pagar': {
            'nulos': 0,
            'datas_futuro': 0,
            'datas_antigas': 0,
            'inconsistencias': 0
        },
        'contas_receber': {
            'nulos': 0,
            'datas_futuro': 0,
            'datas_antigas': 0,
            'inconsistencias': 0
        },
        'contatos': {
            'nulos': 0,
            'datas_futuro': 0,
            'datas_antigas': 0,
            'inconsistencias': 0
        }
    }
    
    if not os.path.exists(INPUT_DIR):
        print(f"Erro: Diretório de entrada {INPUT_DIR} não encontrado")
    else:
        process_accounts_payable()
        process_accounts_receivable()
        process_contacts()
        
        adicional_split_large_files()
        
        arquivos_erro = [f for f in os.listdir(SPLIT_OUTPUT_DIR) if f.startswith('erros_')]
        for arquivo in arquivos_erro:
            try:
                caminho = os.path.join(SPLIT_OUTPUT_DIR, arquivo)
                if os.path.exists(caminho) and os.path.getsize(caminho) > 0:
                    df_erro = pd.read_csv(caminho)
                    qtd_erros = len(df_erro)
                    
                    if 'contas_pagar' in arquivo:
                        if 'nulos' in arquivo:
                            estatisticas['contas_pagar']['nulos'] += qtd_erros
                        elif 'datas_futuras' in arquivo:
                            estatisticas['contas_pagar']['datas_futuro'] += qtd_erros
                        elif 'datas_antigas' in arquivo:
                            estatisticas['contas_pagar']['datas_antigas'] += qtd_erros
                        elif 'inconsistencia' in arquivo:
                            estatisticas['contas_pagar']['inconsistencias'] += qtd_erros
                    
                    elif 'contas_receber' in arquivo:
                        if 'nulos' in arquivo:
                            estatisticas['contas_receber']['nulos'] += qtd_erros
                        elif 'datas_futuras' in arquivo:
                            estatisticas['contas_receber']['datas_futuro'] += qtd_erros
                        elif 'datas_antigas' in arquivo:
                            estatisticas['contas_receber']['datas_antigas'] += qtd_erros
                        elif 'inconsistencia' in arquivo:
                            estatisticas['contas_receber']['inconsistencias'] += qtd_erros
                    
                    elif 'contatos' in arquivo:
                        if 'nulos' in arquivo:
                            estatisticas['contatos']['nulos'] += qtd_erros
                        elif 'datas_futuras' in arquivo:
                            estatisticas['contatos']['datas_futuro'] += qtd_erros
                        elif 'datas_antigas' in arquivo:
                            estatisticas['contatos']['datas_antigas'] += qtd_erros
                        elif 'inconsistencia' in arquivo:
                            estatisticas['contatos']['inconsistencias'] += qtd_erros
            except:
                pass 
    
    print(f"\n{'='*40}")
    print(f"RESUMO DE ERROS ENCONTRADOS")
    print(f"{'='*40}")
    print(f"Contas a Pagar:")
    print(f"  - Campos nulos: {estatisticas['contas_pagar']['nulos']}")
    print(f"  - Datas futuras: {estatisticas['contas_pagar']['datas_futuro']}")
    print(f"  - Datas muito antigas: {estatisticas['contas_pagar']['datas_antigas']}")
    print(f"  - Inconsistências: {estatisticas['contas_pagar']['inconsistencias']}")
    
    print(f"\nContas a Receber:")
    print(f"  - Campos nulos: {estatisticas['contas_receber']['nulos']}")
    print(f"  - Datas futuras: {estatisticas['contas_receber']['datas_futuro']}")
    print(f"  - Datas muito antigas: {estatisticas['contas_receber']['datas_antigas']}")
    print(f"  - Inconsistências: {estatisticas['contas_receber']['inconsistencias']}")
    
    print(f"\nContatos:")
    print(f"  - Campos nulos: {estatisticas['contatos']['nulos']}")
    print(f"  - Datas futuras: {estatisticas['contatos']['datas_futuro']}")
    print(f"  - Datas muito antigas: {estatisticas['contatos']['datas_antigas']}")
    print(f"  - Inconsistências: {estatisticas['contatos']['inconsistencias']}")
    print(f"{'='*40}")
    
    print(f"Divisão de dados concluída às {datetime.now().strftime('%H:%M:%S')}") 