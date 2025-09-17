#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def check_tb_cidadao_structure():
    """Verifica a estrutura da tb_cidadao"""
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
        
        print("=== ESTRUTURA COMPLETA DA tb_cidadao ===")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'tb_cidadao' 
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        for col_name, data_type in columns:
            print(f"{col_name}: {data_type}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    check_tb_cidadao_structure()