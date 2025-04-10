import pandas as pd
import os
import math
import sys
from datetime import datetime

# Directory setup
INPUT_DIR = 'exported_data'
OUTPUT_DIR = 'exported_data_split'
os.makedirs(OUTPUT_DIR, exist_ok=True)

MAX_FILE_SIZE = 2000 * 1024  # 2MB maximum size

def estimate_xlsx_size(df):
    """Estimate the size of a dataframe if saved as Excel"""
    temp_file = 'temp_size_estimate.xlsx'
    df.to_excel(temp_file, index=False)
    size = os.path.getsize(temp_file)
    os.remove(temp_file)  # Clean up
    return size

def format_date_columns(df):
    """Format all date columns to DD/MM/YYYY format"""
    df_copy = df.copy()
    
    # Detect and format date columns
    date_columns = []
    for col in df_copy.columns:
        # Check if column name contains date-related terms
        if any(date_term in col.lower() for date_term in ['data', 'date', 'dt_', 'venc', 'emiss', 'nasc', 'liquid']):
            date_columns.append(col)
        # Or check if it's already a datetime dtype
        elif pd.api.types.is_datetime64_dtype(df_copy[col]):
            date_columns.append(col)
    
    # Format the identified date columns
    for col in date_columns:
        try:
            if not pd.api.types.is_datetime64_dtype(df_copy[col]):
                # Use dayfirst=True for DD/MM/YYYY format
                df_copy[col] = pd.to_datetime(df_copy[col], dayfirst=True, errors='coerce')
            
            # Format to DD/MM/YYYY
            df_copy[col] = df_copy[col].dt.strftime('%d/%m/%Y')
        except Exception as e:
            print(f"Warning: Could not format column '{col}' as date: {str(e)}")
    
    # Handle specific column types
    if 'Contribuinte' in df_copy.columns:
        df_copy['Contribuinte'] = pd.to_numeric(df_copy['Contribuinte'], errors='coerce').fillna(0).astype(int)
    
    return df_copy

def optimize_file_splitting(input_file, max_file_size=MAX_FILE_SIZE):
    """
    Optimize the splitting of a large Excel file to minimize the number of output files,
    while ensuring each file is under the maximum size limit.
    
    Parameters:
    - input_file: Path to the input Excel file (can be relative to INPUT_DIR or absolute)
    - max_file_size: Maximum file size in bytes (default 2MB)
    
    Returns:
    - List of output file paths
    """
    print(f"Processing large file: {input_file}")
    
    # Determine if the path is absolute or relative to INPUT_DIR
    if os.path.isabs(input_file) or os.path.exists(input_file):
        file_path = input_file
        base_filename = os.path.splitext(os.path.basename(input_file))[0]
    else:
        file_path = os.path.join(INPUT_DIR, input_file)
        base_filename = os.path.splitext(input_file)[0]
    
    # Read the input file
    try:
        df = pd.read_excel(file_path)
        total_rows = len(df)
        print(f"Total rows: {total_rows}")
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        return []
    
    # Get file size
    original_size = os.path.getsize(file_path)
    print(f"Original file size: {original_size/1024/1024:.2f} MB")
    
    # If the file is already under the limit, just return it
    if original_size <= max_file_size:
        print("File already under size limit. No splitting required.")
        return [input_file]
    
    # Calculate optimal rows per file based on size
    bytes_per_row = original_size / total_rows if total_rows > 0 else 0
    if bytes_per_row <= 0:
        print("Warning: Cannot determine bytes per row. Using default value.")
        bytes_per_row = 500  # Default assumption
    
    rows_per_file = int(max_file_size * 0.95 / bytes_per_row)  # 5% safety margin
    rows_per_file = max(1, min(rows_per_file, total_rows))  # Ensure valid range
    
    # Calculate number of files needed
    num_files = math.ceil(total_rows / rows_per_file)
    print(f"Optimal splitting: {num_files} files with approximately {rows_per_file} rows each")
    
    output_files = []
    
    # Split the dataframe
    for i in range(num_files):
        start_idx = i * rows_per_file
        end_idx = min((i + 1) * rows_per_file, total_rows)
        
        # Extract the chunk
        chunk = df.iloc[start_idx:end_idx].copy()
        
        # Format the data for Excel
        formatted_chunk = format_date_columns(chunk)
        
        # Check the actual size
        estimated_size = estimate_xlsx_size(formatted_chunk)
        
        # If the chunk is still too large and has more than one row, optimize further
        if estimated_size > max_file_size and len(formatted_chunk) > 1:
            print(f"Chunk {i+1} still too large ({estimated_size/1024/1024:.2f} MB). Optimizing further...")
            
            # Calculate a better row count
            new_rows_per_chunk = int(len(formatted_chunk) * (max_file_size * 0.9 / estimated_size))
            new_rows_per_chunk = max(1, new_rows_per_chunk)  # Ensure at least 1 row
            sub_chunks = math.ceil(len(formatted_chunk) / new_rows_per_chunk)
            
            for j in range(sub_chunks):
                sub_start = j * new_rows_per_chunk
                sub_end = min((j + 1) * new_rows_per_chunk, len(formatted_chunk))
                
                sub_chunk = formatted_chunk.iloc[sub_start:sub_end].copy()
                
                # Save the sub-chunk
                output_filename = f"{base_filename}_parte_{i+1}_{j+1}.xlsx"
                output_path = os.path.join(OUTPUT_DIR, output_filename)
                sub_chunk.to_excel(output_path, index=False)
                
                actual_size = os.path.getsize(output_path)
                output_files.append(output_path)
                
                print(f"  Sub-chunk {j+1}/{sub_chunks}: {output_filename} - {actual_size/1024/1024:.2f} MB, {len(sub_chunk)} rows")
        else:
            # Save the chunk
            output_filename = f"{base_filename}_parte_{i+1}.xlsx"
            output_path = os.path.join(OUTPUT_DIR, output_filename)
            formatted_chunk.to_excel(output_path, index=False)
            
            actual_size = os.path.getsize(output_path)
            output_files.append(output_path)
            
            print(f"Chunk {i+1}/{num_files}: {output_filename} - {actual_size/1024/1024:.2f} MB, {len(formatted_chunk)} rows")
    
    print(f"Successfully split into {len(output_files)} files under {max_file_size/1024/1024:.2f} MB each")
    return output_files

def optimize_accounts_splitting():
    """
    Optimize the splitting of accounts files (contas_a_pagar.xlsx, contas_a_receber.xlsx, or contatos.xlsx)
    """
    accounts_files = [
        f for f in os.listdir(INPUT_DIR) 
        if f.endswith('.xlsx') and ('contas_a_pagar' in f.lower() or 'contas_a_receber' in f.lower() or 'contatos' in f.lower())
    ]
    
    if not accounts_files:
        print("No accounts or contacts files found in the input directory.")
        return
    
    total_files_created = 0
    for file in accounts_files:
        output_files = optimize_file_splitting(file)
        total_files_created += len(output_files)
    
    print(f"Total files created: {total_files_created}")

if __name__ == "__main__":
    print(f"Starting file splitting optimization at {datetime.now().strftime('%H:%M:%S')}")
    print(f"Maximum file size: {MAX_FILE_SIZE/1024/1024:.2f} MB")
    
    # Check if a specific file was provided as command line argument
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        if os.path.exists(file_path):
            optimize_file_splitting(file_path)
        else:
            print(f"Error: File '{file_path}' not found")
    else:
        optimize_accounts_splitting()
    
    print(f"File splitting optimization completed at {datetime.now().strftime('%H:%M:%S')}") 