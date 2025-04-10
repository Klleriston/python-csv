#!/usr/bin/env python3
import pandas as pd
import os
import glob
import sys

def verify_split_integrity(original_file, split_dir='exported_data_split'):
    """
    Verify that all data from the original file is present in the split files
    and that all columns are preserved.
    
    Parameters:
    - original_file: Path to the original Excel file
    - split_dir: Directory containing the split files
    """
    print(f"Verifying integrity of split files for {original_file}...")
    
    # Read the original file
    try:
        original_df = pd.read_excel(original_file)
        original_rows = len(original_df)
        original_cols = original_df.columns.tolist()
        print(f"Original file: {original_rows} rows, {len(original_cols)} columns")
    except Exception as e:
        print(f"Error reading original file: {str(e)}")
        return
    
    # Get base filename without extension
    base_filename = os.path.splitext(os.path.basename(original_file))[0]
    
    # Find all split files for this original file
    split_files = glob.glob(os.path.join(split_dir, f"{base_filename}_parte_*.xlsx"))
    split_files.sort()
    
    if not split_files:
        print(f"No split files found for {base_filename}")
        return
    
    print(f"Found {len(split_files)} split files")
    
    # Verify each split file
    total_split_rows = 0
    for i, split_file in enumerate(split_files):
        try:
            split_df = pd.read_excel(split_file)
            file_rows = len(split_df)
            file_cols = split_df.columns.tolist()
            total_split_rows += file_rows
            
            # Verify columns match
            if set(file_cols) != set(original_cols):
                missing_cols = set(original_cols) - set(file_cols)
                extra_cols = set(file_cols) - set(original_cols)
                print(f"WARNING - Column mismatch in {os.path.basename(split_file)}")
                if missing_cols:
                    print(f"  Missing columns: {missing_cols}")
                if extra_cols:
                    print(f"  Extra columns: {extra_cols}")
            
            # Check file size
            file_size_mb = os.path.getsize(split_file) / (1024 * 1024)
            print(f"  Split file {i+1}/{len(split_files)}: {os.path.basename(split_file)} - {file_rows} rows, {file_size_mb:.2f} MB")
            
        except Exception as e:
            print(f"Error reading split file {split_file}: {str(e)}")
    
    # Compare total rows
    if total_split_rows == original_rows:
        print(f"✓ Row count matches: Original ({original_rows}) = Sum of split files ({total_split_rows})")
    else:
        print(f"✗ Row count mismatch: Original ({original_rows}) ≠ Sum of split files ({total_split_rows})")
        print(f"  Difference: {abs(original_rows - total_split_rows)} rows")
    
    print(f"Verification complete for {base_filename}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python verify_split_integrity.py <path_to_original_excel_file>")
        sys.exit(1)
    
    original_file = sys.argv[1]
    if not os.path.exists(original_file):
        print(f"Error: File not found: {original_file}")
        sys.exit(1)
    
    verify_split_integrity(original_file) 