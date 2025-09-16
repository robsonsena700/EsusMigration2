#!/usr/bin/env python3
"""
Script para verificar a estrutura real das tabelas FAT
"""

import psycopg2
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def get_table_structure(table_name):
    """Obter estrutura de uma tabela específica"""
    try:
        # Conectar ao banco
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('POSTGRES_PORT', 5432)
        )
        
        cursor = conn.cursor()
        
        # Query para obter estrutura da tabela
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_name = %s 
        AND table_schema = 'public'
        ORDER BY ordinal_position;
        """
        
        cursor.execute(query, (table_name,))
        columns = cursor.fetchall()
        
        print(f"\n=== Estrutura da tabela {table_name} ===")
        if columns:
            print(f"Total de colunas: {len(columns)}")
            print("\nColunas:")
            for col in columns:
                column_name, data_type, is_nullable, column_default = col
                nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
                default = f" DEFAULT {column_default}" if column_default else ""
                print(f"  - {column_name} ({data_type}) {nullable}{default}")
                
            # Verificar se existe coluna relacionada a CNS
            cns_columns = [col[0] for col in columns if 'cns' in col[0].lower()]
            if cns_columns:
                print(f"\nColunas relacionadas a CNS: {cns_columns}")
            else:
                print("\nNenhuma coluna relacionada a CNS encontrada")
                
        else:
            print(f"Tabela {table_name} não encontrada ou sem colunas")
            
        cursor.close()
        conn.close()
        
        return [col[0] for col in columns]
        
    except Exception as e:
        print(f"Erro ao verificar tabela {table_name}: {e}")
        return []

def main():
    """Função principal"""
    print("=== Verificação da Estrutura das Tabelas FAT ===")
    
    # Tabelas FAT para verificar
    fat_tables = [
        'tb_fat_cad_individual',
        'tb_fat_cidadao', 
        'tb_fat_cidadao_pec'
    ]
    
    all_structures = {}
    
    for table in fat_tables:
        columns = get_table_structure(table)
        all_structures[table] = columns
    
    # Comparar estruturas
    print("\n=== Comparação de Colunas ===")
    for table, columns in all_structures.items():
        if columns:
            print(f"\n{table}: {len(columns)} colunas")
            # Mostrar primeiras 10 colunas
            for i, col in enumerate(columns[:10]):
                print(f"  {i+1}. {col}")
            if len(columns) > 10:
                print(f"  ... e mais {len(columns) - 10} colunas")

if __name__ == "__main__":
    main()