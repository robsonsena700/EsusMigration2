#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def check_table_structure():
    """Verifica a estrutura das tabelas relacionadas ao sexo"""
    try:
        # Conectar ao banco
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', '127.0.0.1'),
            database=os.getenv('POSTGRES_DB', 'esus'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('POSTGRES_PORT', 5433)
        )
        cursor = conn.cursor()
        
        print("=== ESTRUTURA DA tb_fat_cidadao_pec ===")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'tb_fat_cidadao_pec' 
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        for col_name, data_type in columns:
            print(f"{col_name}: {data_type}")
        
        print("\n=== COLUNAS DE SEXO NA tb_fat_cidadao_pec ===")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tb_fat_cidadao_pec' 
            AND column_name LIKE '%sexo%'
        """)
        sexo_columns = cursor.fetchall()
        for col in sexo_columns:
            print(f"Coluna de sexo: {col[0]}")
        
        print("\n=== ESTRUTURA DA tb_cidadao ===")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'tb_cidadao' 
            AND column_name LIKE '%sexo%'
            ORDER BY ordinal_position
        """)
        cidadao_sexo = cursor.fetchall()
        for col_name, data_type in cidadao_sexo:
            print(f"{col_name}: {data_type}")
        
        print("\n=== VALORES ÚNICOS DE SEXO ===")
        
        # tb_cidadao
        cursor.execute("SELECT DISTINCT no_sexo, COUNT(*) FROM tb_cidadao GROUP BY no_sexo ORDER BY COUNT(*) DESC")
        cidadao_values = cursor.fetchall()
        print("tb_cidadao.no_sexo:")
        for value, count in cidadao_values:
            print(f"  '{value}': {count}")
        
        # tb_fat_cidadao_pec - verificar se tem co_dim_sexo
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tb_fat_cidadao_pec' 
            AND column_name = 'co_dim_sexo'
        """)
        has_co_dim_sexo = cursor.fetchone()
        
        if has_co_dim_sexo:
            cursor.execute("SELECT DISTINCT co_dim_sexo, COUNT(*) FROM tb_fat_cidadao_pec GROUP BY co_dim_sexo ORDER BY COUNT(*) DESC")
            fat_values = cursor.fetchall()
            print("tb_fat_cidadao_pec.co_dim_sexo:")
            for value, count in fat_values:
                print(f"  {value}: {count}")
        else:
            print("tb_fat_cidadao_pec não tem coluna co_dim_sexo")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    check_table_structure()