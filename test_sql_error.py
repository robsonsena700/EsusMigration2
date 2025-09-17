#!/usr/bin/env python3
"""
Script para testar a conexão com o banco e reproduzir o erro SQL
"""

import os
import psycopg2
from psycopg2 import sql
import sys

def get_connection():
    """Estabelece conexão com o banco PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
            port=os.getenv("POSTGRES_PORT", "5433"),
            database=os.getenv("POSTGRES_DB", "esus"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco: {e}")
        return None

def test_table_exists(conn, table_name):
    """Verifica se a tabela existe no banco"""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table_name,))
        
        exists = cur.fetchone()[0]
        cur.close()
        return exists
    except Exception as e:
        print(f"❌ Erro ao verificar tabela {table_name}: {e}")
        return False

def test_insert_command(conn, table_name):
    """Testa um comando INSERT simples na tabela"""
    try:
        cur = conn.cursor()
        
        # Primeiro, vamos verificar a estrutura da tabela
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = cur.fetchall()
        print(f"📋 Estrutura da tabela {table_name}:")
        for col in columns[:5]:  # Mostra apenas as primeiras 5 colunas
            print(f"   - {col[0]} ({col[1]}) {'NULL' if col[2] == 'YES' else 'NOT NULL'}")
        
        if len(columns) > 5:
            print(f"   ... e mais {len(columns) - 5} colunas")
        
        cur.close()
        return True
        
    except Exception as e:
        print(f"❌ Erro ao testar INSERT na tabela {table_name}: {e}")
        return False

def test_problematic_command():
    """Testa especificamente o comando que está causando erro"""
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        
        # Tenta executar um comando similar ao que está causando erro
        problematic_sql = "INSERT INTO public.tb_cds_cad_individu"
        
        print(f"🧪 Testando comando problemático: {problematic_sql}")
        cur.execute(problematic_sql)
        
    except Exception as e:
        print(f"✅ Erro reproduzido: {e}")
        print(f"   Tipo do erro: {type(e).__name__}")
        return True
    finally:
        if conn:
            conn.close()
    
    return False

def main():
    print("🔍 Testando conexão e comandos SQL...")
    
    # Carrega variáveis de ambiente do .env
    env_file = "D:\\Robson\\Projetos\\Cascavel\\.env"
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value.strip('"')
    
    conn = get_connection()
    if not conn:
        print("❌ Não foi possível conectar ao banco")
        return
    
    print("✅ Conexão estabelecida com sucesso!")
    
    # Testa as tabelas relacionadas
    tables_to_test = [
        'tb_cds_cad_individual',
        'tl_cds_cad_individual'
    ]
    
    for table in tables_to_test:
        print(f"\n🔍 Testando tabela: {table}")
        if test_table_exists(conn, table):
            print(f"✅ Tabela {table} existe")
            test_insert_command(conn, table)
        else:
            print(f"❌ Tabela {table} não existe")
    
    conn.close()
    
    # Testa o comando problemático
    print(f"\n🧪 Testando comando problemático...")
    test_problematic_command()

if __name__ == "__main__":
    main()