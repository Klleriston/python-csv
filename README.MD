# Excel File Splitter

Utility scripts to split large Excel spreadsheet files into smaller files while preserving data integrity.

## Problem

Large Excel files can be difficult to work with, especially when they exceed a few megabytes in size. This tool helps split large Excel files (particularly account spreadsheets) into smaller, more manageable files of at most 2MB each.

## Features

- Splits large Excel files into smaller files (max 2MB each)
- Preserves all columns and data formatting
- Handles date formats properly (DD/MM/YYYY)
- Intelligently divides data to minimize the number of files created
- Creates test data for demonstration purposes

## Scripts

1. `split_large_file.py` - The core library for splitting Excel files
2. `split_contas.py` - A simple wrapper script for splitting account files
3. `create_test_accounts_file.py` - Generates a test file of ~10MB for demonstration

## Usage

### Split a specific Excel file:

```bash
python split_contas.py <path_to_excel_file>
```

Example:

```bash
python split_contas.py ./exported_data/contas_a_pagar.xlsx
```

### Generate a test file (for demonstration):

```bash
python create_test_accounts_file.py
```

This will create a ~10MB test file in the `exported_data` directory.

### Run the splitter on all account files:

```bash
python split_large_file.py
```

This will search the `exported_data` directory for files containing "contas_a_pagar" or "contas_a_receber" in their names and split them into smaller files.

## Output

All split files are saved to the `exported_data_split` directory with names based on the original file:

- `original_filename_parte_1.xlsx`
- `original_filename_parte_2.xlsx`
- etc.

## Requirements

- Python 3.6+
- pandas
- numpy
- openpyxl

## Installation

```bash
pip install pandas numpy openpyxl
``` 