#!/usr/bin/env python3
"""
Script para verificar se as colunas nu_uuid_ficha existem nas tabelas FAT
"""

import psycopg2
import os
from dotenv import load_dotenv

def check_fat_uuid_columns():
    """Verifica se as colunas UUID existem nas tabelas FAT"""
    
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
                
                # Verificar colunas UUID
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s 
                    AND column_name LIKE '%%uuid%%'
                    ORDER BY ordinal_position
                """, (table,))
                
                uuid_columns = cursor.fetchall()
                
                if uuid_columns:
                    print('✅ Colunas UUID encontradas:')
                    for col in uuid_columns:
                        print(f'  - {col[0]}')
                else:
                    print('❌ Nenhuma coluna UUID encontrada')
                
                # Verificar especificamente nu_uuid_ficha
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s 
                    AND column_name IN ('nu_uuid_ficha', 'nu_uuid_ficha_origem')
                """, (table,))
                
                specific_columns = cursor.fetchall()
                
                if specific_columns:
                    print('✅ Colunas específicas encontradas:')
                    for col in specific_columns:
                        print(f'  - {col[0]}')
                else:
                    print('❌ Colunas nu_uuid_ficha/nu_uuid_ficha_origem NÃO encontradas')
                    
            except Exception as e:
                print(f'❌ Erro ao verificar {table}: {e}')
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f'❌ Erro de conexão: {e}')

if __name__ == '__main__':
    check_fat_uuid_columns()