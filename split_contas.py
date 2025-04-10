#!/usr/bin/env python3
import os
import sys
from split_large_file import optimize_file_splitting

def main():
    """
    Split accounts spreadsheet into smaller files (max 2MB each).
    Usage: python split_contas.py <path_to_excel_file>
    """
    if len(sys.argv) < 2:
        print("Usage: python split_contas.py <path_to_excel_file>")
        print("Example: python split_contas.py ./exported_data/contas_a_pagar.xlsx")
        return
    
    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return
    
    if not file_path.endswith('.xlsx'):
        print("Warning: File does not have .xlsx extension. Continuing anyway...")
    
    # Get file size in MB
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"File size: {file_size_mb:.2f} MB")
    
    # Run the optimization
    print(f"Splitting file: {file_path}")
    output_files = optimize_file_splitting(file_path)
    
    # Print summary
    print("\nSummary:")
    print(f"Input file: {file_path} ({file_size_mb:.2f} MB)")
    print(f"Files created: {len(output_files)}")
    print("All files saved to the 'exported_data_split' directory")

if __name__ == "__main__":
    main() 