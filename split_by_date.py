import pandas as pd
import os
import math
from datetime import datetime, timedelta

SPLIT_OUTPUT_DIR = 'exported_data_split'
os.makedirs(SPLIT_OUTPUT_DIR, exist_ok=True)

INPUT_DIR = 'exported_data'

MAX_FILE_SIZE = 2 * 1024 * 1024  # 2MB in bytes

def estimate_csv_size(df):
    csv_data = df.to_csv(index=False).encode('utf-8-sig')
    return len(csv_data)

def split_by_date_range(df, date_column, max_size):
    df = df.sort_values(by=date_column)
    
    if not pd.api.types.is_datetime64_dtype(df[date_column]):
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    
    min_date = df[date_column].min()
    max_date = df[date_column].max()
    
    if pd.isnull(min_date) or pd.isnull(max_date):
        print(f"Warning: Invalid date range detected. Using row-based splitting instead.")
        return split_by_rows(df, max_size)
    
    print(f"Date range: {min_date} to {max_date}")
    
    current_chunks = []
    
    for period in ['M', 'W', 'D']:
        current_date = min_date
        chunks = []
        
        while current_date <= max_date:
            if period == 'M':
                next_date = current_date + pd.offsets.MonthEnd(1)
                date_label = current_date.strftime('%Y-%m')
            elif period == 'W':
                next_date = current_date + pd.offsets.Week(1)
                date_label = f"{current_date.strftime('%Y-%m-%d')} to {next_date.strftime('%Y-%m-%d')}"
            else:  # 'D' 
                next_date = current_date + timedelta(days=1)
                date_label = current_date.strftime('%Y-%m-%d')
            
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
        
        if not oversized or period == 'D':
            current_chunks = chunks
            break

    if any(chunk['size'] > MAX_FILE_SIZE for chunk in current_chunks):
        oversized_chunks = []
        for chunk in current_chunks:
            if chunk['size'] > MAX_FILE_SIZE:
                row_chunks = split_by_rows(chunk['data'], MAX_FILE_SIZE)
                for i, row_chunk in enumerate(row_chunks):
                    oversized_chunks.append({
                        'start_date': chunk['start_date'],
                        'end_date': chunk['end_date'],
                        'date_label': f"{chunk['date_label']} (part {i+1})",
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
    
    rows_per_chunk = math.floor(MAX_FILE_SIZE / size_per_row) if size_per_row > 0 else 1000
    rows_per_chunk = max(1, rows_per_chunk)  
    
    num_chunks = math.ceil(total_rows / rows_per_chunk)
    
    chunks = []
    for i in range(num_chunks):
        start_idx = i * rows_per_chunk
        end_idx = min((i + 1) * rows_per_chunk, total_rows)
        chunk = df.iloc[start_idx:end_idx]
        chunks.append(chunk)
    
    return chunks

def process_accounts_payable():
    print("Processing Contas a Receber...")
    
    file_path = os.path.join(INPUT_DIR, 'contas_a_pagar.csv')
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return
        
    df = pd.read_csv(file_path)
    
    if len(df) == 0:
        print("Warning: Contas a Receber file is empty")
        return
    
    complete_file_path = os.path.join(SPLIT_OUTPUT_DIR, 'contas_a_receber_completo.csv')
    df.to_csv(complete_file_path, index=False, encoding='utf-8-sig')
    complete_size = os.path.getsize(complete_file_path)
    print(f"Saved complete file: {complete_file_path} ({complete_size / (1024*1024):.2f} MB)")
    
    if complete_size < MAX_FILE_SIZE:
        print("Complete file is less than 2MB, no need to split.")
        return
    
    if 'Data emissao' in df.columns:
        date_column = 'Data emissao'
    elif 'Data Emissao' in df.columns:
        date_column = 'Data Emissao'
    else:
        date_column = None
        print("Warning: Date column not found for Contas a Receber.")
    
    if date_column:
        chunks = split_by_date_range(df, date_column, MAX_FILE_SIZE)
        
        for i, chunk in enumerate(chunks):
            file_name = f"contas_a_receber_{chunk['date_label'].replace(' ', '_').replace(':', '')}.csv"
            file_path = os.path.join(SPLIT_OUTPUT_DIR, file_name)
            chunk['data'].to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"Saved chunk {i+1}/{len(chunks)}: {file_path} ({chunk['size'] / (1024*1024):.2f} MB, {len(chunk['data'])} rows)")
    else:
        chunks = split_by_rows(df, MAX_FILE_SIZE)
        
        for i, chunk in enumerate(chunks):
            file_name = f"contas_a_receber_parte_{i+1}.csv"
            file_path = os.path.join(SPLIT_OUTPUT_DIR, file_name)
            chunk.to_csv(file_path, index=False, encoding='utf-8-sig')
            chunk_size = os.path.getsize(file_path)
            print(f"Saved chunk {i+1}/{len(chunks)}: {file_path} ({chunk_size / (1024*1024):.2f} MB, {len(chunk)} rows)")

def process_accounts_receivable():
    print("Processing Contas a Pagar...")
    
    file_path = os.path.join(INPUT_DIR, 'contas_a_receber.csv')
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return
        
    df = pd.read_csv(file_path)
    
    if len(df) == 0:
        print("Warning: Contas a Pagar file is empty")
        return
    
    complete_file_path = os.path.join(SPLIT_OUTPUT_DIR, 'contas_a_pagar_completo.csv')
    df.to_csv(complete_file_path, index=False, encoding='utf-8-sig')
    complete_size = os.path.getsize(complete_file_path)
    print(f"Saved complete file: {complete_file_path} ({complete_size / (1024*1024):.2f} MB)")
    
    if complete_size < MAX_FILE_SIZE:
        print("Complete file is less than 2MB, no need to split.")
        return
    
    if 'Data Emissao' in df.columns:
        date_column = 'Data Emissao'
    elif 'Data emissao' in df.columns:
        date_column = 'Data emissao'
    else:
        date_column = None
        print("Warning: Date column not found for Contas a Pagar.")
    
    if date_column:
        chunks = split_by_date_range(df, date_column, MAX_FILE_SIZE)
        
        for i, chunk in enumerate(chunks):
            file_name = f"contas_a_pagar_{chunk['date_label'].replace(' ', '_').replace(':', '')}.csv"
            file_path = os.path.join(SPLIT_OUTPUT_DIR, file_name)
            chunk['data'].to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"Saved chunk {i+1}/{len(chunks)}: {file_path} ({chunk['size'] / (1024*1024):.2f} MB, {len(chunk['data'])} rows)")
    else:
        chunks = split_by_rows(df, MAX_FILE_SIZE)
        
        for i, chunk in enumerate(chunks):
            file_name = f"contas_a_pagar_parte_{i+1}.csv"
            file_path = os.path.join(SPLIT_OUTPUT_DIR, file_name)
            chunk.to_csv(file_path, index=False, encoding='utf-8-sig')
            chunk_size = os.path.getsize(file_path)
            print(f"Saved chunk {i+1}/{len(chunks)}: {file_path} ({chunk_size / (1024*1024):.2f} MB, {len(chunk)} rows)")

def process_contacts():
    print("Processing Contatos...")
    
    file_path = os.path.join(INPUT_DIR, 'contatos.csv')
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return
        
    df = pd.read_csv(file_path)
    
    if len(df) == 0:
        print("Warning: Contatos file is empty")
        return
    
    complete_file_path = os.path.join(SPLIT_OUTPUT_DIR, 'contatos_completo.csv')
    df.to_csv(complete_file_path, index=False, encoding='utf-8-sig')
    complete_size = os.path.getsize(complete_file_path)
    print(f"Saved complete file: {complete_file_path} ({complete_size / (1024*1024):.2f} MB)")
    
    if complete_size < MAX_FILE_SIZE:
        print("Complete file is less than 2MB, no need to split.")
        return
    
    chunks = split_by_rows(df, MAX_FILE_SIZE)
    
    for i, chunk in enumerate(chunks):
        file_name = f"contatos_parte_{i+1}.csv"
        file_path = os.path.join(SPLIT_OUTPUT_DIR, file_name)
        chunk.to_csv(file_path, index=False, encoding='utf-8-sig')
        chunk_size = os.path.getsize(file_path)
        print(f"Saved chunk {i+1}/{len(chunks)}: {file_path} ({chunk_size / (1024*1024):.2f} MB, {len(chunk)} rows)")

if __name__ == "__main__":
    print(f"Starting data splitting process at {datetime.now().strftime('%H:%M:%S')}")
    print(f"Files larger than {MAX_FILE_SIZE/(1024*1024)} MB will be split")
    
    if not os.path.exists(INPUT_DIR):
        print(f"Error: Input directory {INPUT_DIR} not found")
    else:
        process_accounts_payable()
        process_accounts_receivable()
        process_contacts()
    
    print(f"Data splitting completed at {datetime.now().strftime('%H:%M:%S')}") 