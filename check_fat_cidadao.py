#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def check_tb_cidadao():
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
        
        print("=== ESTRUTURA DA tb_cidadao ===")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'tb_cidadao' 
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        for col_name, data_type in columns:
            print(f"{col_name}: {data_type}")
        
        print("\n=== AMOSTRA DE DADOS ===")
        cursor.execute("""
            SELECT 
                co_seq_cidadao,
                nu_cns,
                nu_cpf,
                no_cidadao,
                no_sexo,
                dt_nascimento,
                nu_telefone_celular,
                co_raca_cor,
                co_nacionalidade,
                co_pais_nascimento,
                co_unico_cidadao
            FROM tb_cidadao 
            WHERE nu_cns IS NOT NULL 
            LIMIT 5
        """)
        rows = cursor.fetchall()
        
        print("Primeiros 5 registros com CNS:")
        for row in rows:
            print(row)
        
        print("\n=== CONTAGEM DE REGISTROS ===")
        cursor.execute("SELECT COUNT(*) FROM tb_cidadao WHERE nu_cns IS NOT NULL")
        count = cursor.fetchone()[0]
        print(f"Registros com CNS: {count}")
        
        cursor.execute("SELECT COUNT(*) FROM tb_cidadao WHERE nu_cpf IS NOT NULL")
        count = cursor.fetchone()[0]
        print(f"Registros com CPF: {count}")
        
        cursor.execute("SELECT COUNT(*) FROM tb_cidadao WHERE nu_telefone_celular IS NOT NULL")
        count = cursor.fetchone()[0]
        print(f"Registros com telefone: {count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    check_tb_cidadao()