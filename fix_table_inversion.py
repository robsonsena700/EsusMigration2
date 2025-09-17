#!/usr/bin/env python3
"""
Script para corrigir a inversão de dados entre as tabelas tl_cds_cad_individual e tb_cds_cad_individual
"""

import psycopg2
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def get_db_connection():
    """Conecta ao banco de dados PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', '127.0.0.1'),
            port=os.getenv('POSTGRES_PORT', '5433'),
            database=os.getenv('POSTGRES_DB', 'esus'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres')
        )
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco: {e}")
        return None

def clear_tables(conn):
    """Limpa as tabelas para recriar os dados corretamente"""
    try:
        cur = conn.cursor()
        
        print("🧹 Limpando tabelas...")
        
        # Limpar as tabelas
        cur.execute("TRUNCATE TABLE tl_cds_cad_individual RESTART IDENTITY CASCADE;")
        cur.execute("TRUNCATE TABLE tb_cds_cad_individual RESTART IDENTITY CASCADE;")
        
        # Resetar as sequências
        cur.execute("ALTER SEQUENCE seq_tl_cds_cad_individual RESTART WITH 1;")
        cur.execute("ALTER SEQUENCE seq_tb_cds_cad_individual RESTART WITH 1;")
        
        conn.commit()
        cur.close()
        
        print("✅ Tabelas limpas com sucesso!")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao limpar tabelas: {e}")
        conn.rollback()
        return False

def verify_fix(conn):
    """Verifica se a correção foi aplicada corretamente"""
    try:
        cur = conn.cursor()
        
        # Verificar contagem das tabelas
        cur.execute("SELECT COUNT(*) FROM tl_cds_cad_individual;")
        tl_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM tb_cds_cad_individual;")
        tb_count = cur.fetchone()[0]
        
        print(f"📊 Contagem após limpeza:")
        print(f"   tl_cds_cad_individual: {tl_count} registros")
        print(f"   tb_cds_cad_individual: {tb_count} registros")
        
        cur.close()
        return True
        
    except Exception as e:
        print(f"❌ Erro ao verificar correção: {e}")
        return False

def main():
    print("🔧 Iniciando correção da inversão de dados...")
    
    # Conectar ao banco
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        # Limpar tabelas
        if clear_tables(conn):
            # Verificar resultado
            verify_fix(conn)
            print("\n✅ Correção concluída! Agora você pode executar o migrator.py novamente.")
            print("💡 Execute: python migrator.py --table-name public.tl_cds_cad_individual")
            print("💡 Execute: python migrator.py --table-name public.tb_cds_cad_individual")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()