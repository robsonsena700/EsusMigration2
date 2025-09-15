#!/usr/bin/env python3
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def check_cns_columns():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        
        cursor = conn.cursor()
        
        # Verificar colunas CNS na tabela tb_fat_cad_individual
        print("=== Colunas CNS na tabela tb_fat_cad_individual ===")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'tb_fat_cad_individual' 
            AND column_name LIKE '%cns%'
            ORDER BY column_name;
        """)
        
        cns_columns = cursor.fetchall()
        if cns_columns:
            for row in cns_columns:
                print(f"  {row[0]} ({row[1]})")
        else:
            print("  Nenhuma coluna CNS encontrada!")
        
        # Verificar todas as colunas da tabela para entender a estrutura
        print("\n=== Todas as colunas da tabela tb_fat_cad_individual ===")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'tb_fat_cad_individual'
            ORDER BY ordinal_position;
        """)
        
        all_columns = cursor.fetchall()
        for row in all_columns:
            print(f"  {row[0]} ({row[1]})")
        
        # Testar uma query simples para ver se a tabela existe e tem dados
        print(f"\n=== Contagem de registros ===")
        cursor.execute("SELECT COUNT(*) FROM tb_fat_cad_individual;")
        count = cursor.fetchone()[0]
        print(f"  Total de registros: {count}")
        
        if count > 0:
            print("\n=== Amostra de dados (primeiras 2 linhas) ===")
            cursor.execute("SELECT * FROM tb_fat_cad_individual LIMIT 2;")
            sample_data = cursor.fetchall()
            
            # Obter nomes das colunas
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'tb_fat_cad_individual'
                ORDER BY ordinal_position;
            """)
            column_names = [row[0] for row in cursor.fetchall()]
            
            for i, row in enumerate(sample_data):
                print(f"  Registro {i+1}:")
                for j, value in enumerate(row):
                    if j < len(column_names):
                        print(f"    {column_names[j]}: {value}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    check_cns_columns()