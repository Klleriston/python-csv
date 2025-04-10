#!/usr/bin/env python3
import pandas as pd
import os
import glob
import sys
from datetime import datetime

# Configuração de diretórios
INPUT_DIR = 'exported_data'
OUTPUT_DIR = 'exported_data_split'

def verificar_integridade(tipo_conta):
    """
    Verifica se todos os dados da planilha original estão presentes nas partes divididas.
    
    Parâmetros:
    - tipo_conta: string 'receber' ou 'pagar'
    
    Retorna:
    - True se a verificação foi bem-sucedida, False caso contrário
    """
    print(f"\n{'=' * 50}")
    print(f"Verificando integridade dos dados de contas a {tipo_conta}")
    print(f"{'=' * 50}\n")
    
    # Determinar o arquivo original e o padrão das partes
    if tipo_conta == 'receber':
        arquivo_original = os.path.join(INPUT_DIR, 'contas_a_receber.xlsx')
        padrao_partes = os.path.join(OUTPUT_DIR, 'contas_receber_parte_*.xlsx')
        coluna_id = 'Id'  # Nome da coluna de ID nas contas a receber
    else:  # pagar
        arquivo_original = os.path.join(INPUT_DIR, 'contas_a_pagar.xlsx')
        padrao_partes = os.path.join(OUTPUT_DIR, 'contas_pagar_parte_*.xlsx')
        coluna_id = 'ID'  # Nome da coluna de ID nas contas a pagar
    
    # Verificar se o arquivo original existe
    if not os.path.exists(arquivo_original):
        print(f"Erro: Arquivo original {arquivo_original} não encontrado!")
        return False
    
    print(f"Lendo arquivo original: {arquivo_original}")
    df_original = pd.read_excel(arquivo_original)
    total_registros_original = len(df_original)
    print(f"Total de registros no arquivo original: {total_registros_original}")
    
    # Listar todos os arquivos divididos
    arquivos_partes = sorted(glob.glob(padrao_partes))
    
    if not arquivos_partes:
        print(f"Erro: Nenhum arquivo dividido encontrado com o padrão {padrao_partes}")
        return False
    
    print(f"Encontrados {len(arquivos_partes)} arquivos divididos")
    
    # Criar dataframe combinado das partes
    dfs_partes = []
    total_registros_partes = 0
    
    for arquivo in arquivos_partes:
        print(f"Lendo arquivo: {arquivo}")
        df_parte = pd.read_excel(arquivo)
        total_registros_partes += len(df_parte)
        dfs_partes.append(df_parte)
    
    # Concatenar todas as partes
    df_combinado = pd.concat(dfs_partes, ignore_index=True)
    print(f"Total de registros nas partes combinadas: {total_registros_partes}")
    
    # Verificar número de registros
    if total_registros_original != total_registros_partes:
        print(f"ERRO: Número de registros não corresponde!")
        print(f"  Original: {total_registros_original}")
        print(f"  Partes combinadas: {total_registros_partes}")
        return False
    else:
        print(f"Número de registros corresponde: {total_registros_original}")
    
    # Verificar IDs únicos
    ids_original = set(df_original[coluna_id].astype(str))
    ids_combinado = set(df_combinado[coluna_id].astype(str))
    
    # Mostrar total de IDs únicos
    print(f"IDs únicos no original: {len(ids_original)}")
    print(f"IDs únicos nas partes combinadas: {len(ids_combinado)}")
    
    # Verificar IDs ausentes
    ids_ausentes = ids_original - ids_combinado
    if ids_ausentes:
        print(f"ERRO: Encontrados {len(ids_ausentes)} IDs no arquivo original que não estão nos arquivos divididos")
        if len(ids_ausentes) <= 10:
            print(f"IDs ausentes: {', '.join(str(id) for id in ids_ausentes)}")
        return False
    
    # Verificar IDs extras
    ids_extras = ids_combinado - ids_original
    if ids_extras:
        print(f"ERRO: Encontrados {len(ids_extras)} IDs nos arquivos divididos que não estão no arquivo original")
        if len(ids_extras) <= 10:
            print(f"IDs extras: {', '.join(str(id) for id in ids_extras)}")
        return False
    
    # Verificar integridade dos dados financeiros específicos
    colunas_numericas = ['Valor documento', 'Saldo']
    if tipo_conta == 'receber':
        colunas_numericas.append('Taxas')
    
    # Somar valores para verificar se batem
    for coluna in colunas_numericas:
        if coluna in df_original.columns and coluna in df_combinado.columns:
            soma_original = pd.to_numeric(df_original[coluna], errors='coerce').sum()
            soma_combinado = pd.to_numeric(df_combinado[coluna], errors='coerce').sum()
            
            # Verificar se as somas são iguais (com uma pequena margem de erro para arredondamentos)
            if abs(soma_original - soma_combinado) > 0.01:
                print(f"ERRO: Soma da coluna '{coluna}' não corresponde!")
                print(f"  Original: {soma_original}")
                print(f"  Partes combinadas: {soma_combinado}")
                print(f"  Diferença: {abs(soma_original - soma_combinado)}")
                return False
            else:
                print(f"Soma da coluna '{coluna}' corresponde: {soma_original}")
    
    print(f"\nVerificação de integridade de contas a {tipo_conta} concluída com sucesso!")
    return True

def main():
    """Função principal do script"""
    print(f"Iniciando verificação de integridade em {datetime.now().strftime('%H:%M:%S')}")
    
    # Verificar argumentos da linha de comando
    verificar_receber = True
    verificar_pagar = True
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--receber':
            verificar_pagar = False
        elif sys.argv[1] == '--pagar':
            verificar_receber = False
        elif sys.argv[1] == '--help':
            print("Uso: python verify_financeiro_integrity.py [opção]")
            print("\nOpções:")
            print("  --receber    Verifica apenas contas a receber")
            print("  --pagar      Verifica apenas contas a pagar")
            print("  --help       Mostra esta ajuda")
            return
    
    # Realizar verificações
    resultados = []
    
    if verificar_receber:
        resultados.append(verificar_integridade('receber'))
    
    if verificar_pagar:
        resultados.append(verificar_integridade('pagar'))
    
    # Resumo final
    print(f"\n{'=' * 50}")
    print("Resumo da verificação de integridade:")
    
    if verificar_receber:
        status_receber = "SUCESSO" if resultados[0] else "FALHA"
        print(f"Contas a receber: {status_receber}")
    
    if verificar_pagar:
        idx = 0 if not verificar_receber else 1
        status_pagar = "SUCESSO" if resultados[idx] else "FALHA"
        print(f"Contas a pagar: {status_pagar}")
    
    print(f"{'=' * 50}")
    
    # Retornar código de saída
    if all(resultados):
        print(f"Verificação de integridade concluída com sucesso em {datetime.now().strftime('%H:%M:%S')}")
        return 0
    else:
        print(f"Verificação de integridade concluída com falhas em {datetime.now().strftime('%H:%M:%S')}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 