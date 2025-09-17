#!/usr/bin/env python3
"""
Script para analisar a estrutura dos arquivos CSV do e-SUS e identificar problemas com o campo INE
"""

import csv
import os
from pathlib import Path

def find_data_start(lines):
    """Encontra onde começam os dados reais no arquivo CSV do e-SUS"""
    for i, line in enumerate(lines):
        # Procurar por linhas que parecem ser cabeçalho de dados
        if any(keyword in line.upper() for keyword in ['NOME', 'CPF', 'CNS', 'NASCIMENTO', 'SEXO']):
            return i
        # Ou procurar por linhas com muitas colunas separadas por ;
        if line.count(';') > 5:
            return i
    return None

def analyze_csv_structure(csv_file):
    """Analisa a estrutura de um arquivo CSV do e-SUS"""
    print(f"\n📄 ANALISANDO: {csv_file}")
    print("-" * 50)
    
    # Tentar diferentes encodings
    encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            with open(csv_file, 'r', encoding=encoding) as f:
                lines = f.readlines()
                
                print(f"✅ Encoding funcionou: {encoding}")
                print(f"📋 Total de linhas: {len(lines)}")
                
                # Mostrar as primeiras linhas para entender a estrutura
                print(f"📋 Primeiras 10 linhas:")
                for i, line in enumerate(lines[:10]):
                    print(f"   {i+1:2d}: {line.strip()[:80]}...")
                
                # Encontrar onde começam os dados
                data_start = find_data_start(lines)
                
                if data_start is None:
                    print("❌ Não foi possível encontrar o início dos dados")
                    continue
                
                print(f"🎯 Dados começam na linha: {data_start + 1}")
                
                # Analisar o cabeçalho dos dados
                header_line = lines[data_start].strip()
                print(f"📊 Linha do cabeçalho: {header_line[:100]}...")
                
                # Dividir por ponto e vírgula
                header = [col.strip() for col in header_line.split(';')]
                print(f"📊 Cabeçalho ({len(header)} colunas):")
                for i, col in enumerate(header):
                    if col:  # Só mostrar colunas não vazias
                        print(f"   {i+1:2d}. '{col}'")
                
                # Verificar se existe coluna INE
                ine_columns = []
                for i, col in enumerate(header):
                    if 'INE' in col.upper():
                        ine_columns.append((i, col))
                
                if ine_columns:
                    print(f"✅ Colunas INE encontradas:")
                    for idx, col in ine_columns:
                        print(f"   Posição {idx+1}: '{col}'")
                else:
                    print(f"❌ Nenhuma coluna INE encontrada")
                    # Procurar por colunas similares
                    similar_cols = []
                    for i, col in enumerate(header):
                        if any(keyword in col.upper() for keyword in ['EQUIPE', 'UNIDADE', 'CODIGO']):
                            similar_cols.append((i, col))
                    
                    if similar_cols:
                        print(f"🔍 Colunas relacionadas encontradas:")
                        for idx, col in similar_cols:
                            print(f"   Posição {idx+1}: '{col}'")
                
                # Analisar algumas linhas de dados
                print(f"📋 Primeiras 3 linhas de dados:")
                for line_num in range(data_start + 1, min(data_start + 4, len(lines))):
                    if line_num < len(lines):
                        data_line = lines[line_num].strip()
                        data = [col.strip() for col in data_line.split(';')]
                        
                        print(f"   Linha {line_num + 1}:")
                        
                        # Mostrar dados das colunas INE se existirem
                        for idx, col_name in ine_columns:
                            if idx < len(data):
                                print(f"     {col_name}: '{data[idx]}'")
                        
                        # Mostrar algumas outras colunas importantes
                        important_keywords = ['NOME', 'CPF', 'CNS', 'NASCIMENTO']
                        for i, col in enumerate(header):
                            if any(keyword in col.upper() for keyword in important_keywords):
                                if i < len(data):
                                    value = data[i][:30] if data[i] else ''
                                    print(f"     {col}: '{value}'")
                
                return True  # Sucesso
                
        except UnicodeDecodeError:
            print(f"❌ Encoding {encoding} falhou")
            continue
        except Exception as e:
            print(f"❌ Erro com encoding {encoding}: {e}")
            continue
    
    print(f"❌ Não foi possível ler o arquivo com nenhum encoding")
    return False

def main():
    print("🚀 ANÁLISE DA ESTRUTURA DOS ARQUIVOS CSV DO e-SUS")
    print("=" * 60)
    
    # Analisar arquivos CSV na pasta datacsv
    csv_dir = Path("datacsv")
    if not csv_dir.exists():
        print(f"❌ Diretório {csv_dir} não encontrado")
        return
    
    csv_files = list(csv_dir.glob("*.csv"))
    
    if not csv_files:
        print(f"❌ Nenhum arquivo CSV encontrado em {csv_dir}")
        return
    
    print(f"📁 Encontrados {len(csv_files)} arquivos CSV:")
    for csv_file in csv_files:
        print(f"   - {csv_file.name}")
    
    # Analisar cada arquivo
    for csv_file in csv_files:
        analyze_csv_structure(csv_file)
    
    print("\n✅ ANÁLISE CONCLUÍDA")

if __name__ == "__main__":
    main()