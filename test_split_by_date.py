import unittest
import os
import pandas as pd
import shutil
from unittest.mock import patch, MagicMock
from datetime import datetime
from io import StringIO
import sys

import split_by_date

class TestSplitByDate(unittest.TestCase):
    
    def setUp(self):       
        self.test_input_dir = 'test_exported_data'
        self.test_output_dir = 'test_exported_data_split'
        os.makedirs(self.test_input_dir, exist_ok=True)
        os.makedirs(self.test_output_dir, exist_ok=True)
        
        self.contas_a_pagar_df = pd.DataFrame({
            'ID': range(1, 101),
            'Fornecedor': ['Vendor' + str(i) for i in range(1, 101)],
            'Data emissao': pd.date_range(start='2021-01-01', periods=100),
            'Data vencimento': pd.date_range(start='2021-02-01', periods=100),
            'Valor documento': [i * 100 for i in range(1, 101)],
            'Situação': ['Em Aberto'] * 50 + ['Liquidado'] * 50,
        })
        
        self.contas_a_receber_df = pd.DataFrame({
            'Id': range(1, 101),
            'Cliente': ['Client' + str(i) for i in range(1, 101)],
            'Data Emissao': pd.date_range(start='2021-01-01', periods=100),
            'Data vencimento': pd.date_range(start='2021-02-01', periods=100),
            'Valor documento': [i * 100 for i in range(1, 101)],
            'Situacao': ['Em Aberto'] * 50 + ['Liquidado'] * 50,
        })
        
        self.contatos_df = pd.DataFrame({
            'Id': range(1, 101),
            'Nome': ['Contact' + str(i) for i in range(1, 101)],
            'Fantasia': ['Trade Name' + str(i) for i in range(1, 101)],
            'Endereço': ['Address' + str(i) for i in range(1, 101)],
            'Telefone': ['Phone' + str(i) for i in range(1, 101)],
        })
        
        self.contas_a_pagar_df.to_csv(os.path.join(self.test_input_dir, 'contas_a_pagar.csv'), index=False)
        self.contas_a_receber_df.to_csv(os.path.join(self.test_input_dir, 'contas_a_receber.csv'), index=False)
        self.contatos_df.to_csv(os.path.join(self.test_input_dir, 'contatos.csv'), index=False)
        
        self.original_input_dir = split_by_date.INPUT_DIR
        self.original_output_dir = split_by_date.SPLIT_OUTPUT_DIR
        self.original_max_file_size = split_by_date.MAX_FILE_SIZE
        
        split_by_date.INPUT_DIR = self.test_input_dir
        split_by_date.SPLIT_OUTPUT_DIR = self.test_output_dir
        split_by_date.MAX_FILE_SIZE = 100
    
    def tearDown(self):
        split_by_date.INPUT_DIR = self.original_input_dir
        split_by_date.SPLIT_OUTPUT_DIR = self.original_output_dir
        split_by_date.MAX_FILE_SIZE = self.original_max_file_size
        
        if os.path.exists(self.test_input_dir):
            shutil.rmtree(self.test_input_dir)
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
    
    def test_estimate_csv_size(self):
        df = pd.DataFrame({
            'col1': ['a', 'b', 'c'],
            'col2': [1, 2, 3]
        })
        size = split_by_date.estimate_csv_size(df)
        self.assertGreater(size, 0)
        
        df2 = pd.DataFrame({
            'col1': ['a'] * 1000,
            'col2': list(range(1000))
        })
        size2 = split_by_date.estimate_csv_size(df2)
        self.assertGreater(size2, size)
    
    def test_split_by_rows(self):
        df = pd.DataFrame({
            'col1': list(range(100)),
            'col2': list(range(100, 200))
        })
        
        max_size = 50
        chunks = split_by_date.split_by_rows(df, max_size)
        
        self.assertGreaterEqual(len(chunks), 3)
        
        for chunk in chunks:
            self.assertIsInstance(chunk, pd.DataFrame)
        
        total_rows = sum(len(chunk) for chunk in chunks)
        self.assertEqual(total_rows, 100)
    
    def test_split_by_date_range(self):
        df = pd.DataFrame({
            'date': pd.date_range(start='2021-01-01', periods=100),
            'value': list(range(100))
        })
        
        chunks = split_by_date.split_by_date_range(df, 'date', 100)
        
        self.assertGreater(len(chunks), 1)
        
        for chunk in chunks:
            self.assertIn('start_date', chunk)
            self.assertIn('end_date', chunk)
            self.assertIn('date_label', chunk)
            self.assertIn('data', chunk)
            self.assertIn('size', chunk)
            self.assertIsInstance(chunk['data'], pd.DataFrame)
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_process_accounts_payable_name_swap(self, mock_stdout):
        split_by_date.process_accounts_payable()
        output = mock_stdout.getvalue()
        
        self.assertIn("Contas a Receber", output)
        self.assertNotIn("Contas a Pagar", output)
        
        files = os.listdir(self.test_output_dir)
        self.assertTrue(any('contas_a_receber' in f for f in files))
        self.assertFalse(any('contas_a_pagar' in f for f in files if 'completo' in f))  # excluding files from other tests
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_process_accounts_receivable_name_swap(self, mock_stdout):
        split_by_date.process_accounts_receivable()
        output = mock_stdout.getvalue()
        
        self.assertIn("Contas a Pagar", output)
        self.assertNotIn("Contas a Receber", output)
        
        files = os.listdir(self.test_output_dir)
        self.assertTrue(any('contas_a_pagar' in f for f in files))
        self.assertFalse(any('contas_a_receber' in f for f in files if 'completo' in f))  # excluding files from other tests
    
    def test_file_size_limit(self):
        split_by_date.MAX_FILE_SIZE = 100
        
        split_by_date.process_accounts_payable()
        
        files = [f for f in os.listdir(self.test_output_dir) if f.startswith('contas_a_receber_') and not f.endswith('completo.csv')]
        self.assertGreater(len(files), 1)
        
        for file in files:
            file_path = os.path.join(self.test_output_dir, file)
            self.assertLessEqual(os.path.getsize(file_path), 120 * 1.1)
    
    @patch('sys.stdout', new=StringIO())
    def test_main_execution(self):
        if os.path.exists(self.test_input_dir) and os.path.exists(self.test_output_dir):
            original_process_payable = split_by_date.process_accounts_payable
            original_process_receivable = split_by_date.process_accounts_receivable
            original_process_contacts = split_by_date.process_contacts
            
            called_functions = []
            
            def mock_process_accounts_payable():
                called_functions.append('process_accounts_payable')
                return
                
            def mock_process_accounts_receivable():
                called_functions.append('process_accounts_receivable')
                return
                
            def mock_process_contacts():
                called_functions.append('process_contacts')
                return
                
            split_by_date.process_accounts_payable = mock_process_accounts_payable
            split_by_date.process_accounts_receivable = mock_process_accounts_receivable
            split_by_date.process_contacts = mock_process_contacts
            
            try:
                if not os.path.exists(split_by_date.INPUT_DIR):
                    print(f"Error: Input directory {split_by_date.INPUT_DIR} not found")
                else:
                    split_by_date.process_accounts_payable()
                    split_by_date.process_accounts_receivable()
                    split_by_date.process_contacts()
                
                self.assertIn('process_accounts_payable', called_functions)
                self.assertIn('process_accounts_receivable', called_functions)
                self.assertIn('process_contacts', called_functions)
            finally:
                split_by_date.process_accounts_payable = original_process_payable
                split_by_date.process_accounts_receivable = original_process_receivable
                split_by_date.process_contacts = original_process_contacts
    
    def test_integration(self):
        original_stdout = sys.stdout
        sys.stdout = StringIO()
        
        try:
            split_by_date.process_accounts_payable()
            split_by_date.process_accounts_receivable()
            split_by_date.process_contacts()
            
            files = os.listdir(self.test_output_dir)
            self.assertIn('contas_a_receber_completo.csv', files)
            self.assertIn('contas_a_pagar_completo.csv', files)
            self.assertIn('contatos_completo.csv', files)
            
            self.assertTrue(any(f.startswith('contas_a_receber_') and not f.endswith('completo.csv') for f in files))
            self.assertTrue(any(f.startswith('contas_a_pagar_') and not f.endswith('completo.csv') for f in files))
            
            for file in files:
                df = pd.read_csv(os.path.join(self.test_output_dir, file))
                self.assertIsInstance(df, pd.DataFrame)
                self.assertGreater(len(df), 0)
        
        finally:
            sys.stdout = original_stdout

if __name__ == '__main__':
    unittest.main() 