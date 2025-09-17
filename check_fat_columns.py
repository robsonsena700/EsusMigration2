#!/usr/bin/env python3
"""
Script para verificar as colunas das tabelas FAT
"""

import psycopg2
import os
from dotenv import load_dotenv

def check_fat_columns():
    """Verifica as colunas das tabelas FAT"""
    
    load_dotenv()
    
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('POSTGRES_PORT')
        )
        
        cursor = conn.cursor()
        
        # Verificar colunas das tabelas FAT
        tables = ['tb_fat_cad_individual', 'tb_fat_cidadao', 'tb_fat_cidadao_pec']
        
        for table in tables:
            print(f'\n=== {table} ===')
            try:
                # Verificar se a tabela existe
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    )
                """, (table,))
                
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    print(f'❌ Tabela {table} não existe')
                    continue
                
                # Listar todas as colunas
                cursor.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s 
                    ORDER BY ordinal_position
                """, (table,))
                
                columns = cursor.fetchall()
                
                print(f'✅ Tabela {table} existe com {len(columns)} colunas:')
                
                uuid_found = False
                for col_name, col_type in columns:
                    if 'uuid' in col_name.lower():
                        print(f'  🔑 {col_name} ({col_type}) - CAMPO UUID')
                        uuid_found = True
                    else:
                        print(f'  - {col_name} ({col_type})')
                
                if not uuid_found:
                    print('  ❌ Nenhuma coluna UUID encontrada nesta tabela')
                    
            except Exception as e:
                print(f'❌ Erro ao verificar {table}: {e}')
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f'❌ Erro de conexão: {e}')

if __name__ == '__main__':
    check_fat_columns()