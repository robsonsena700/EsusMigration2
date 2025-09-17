#!/usr/bin/env python3
"""
Script para analisar a inversão de dados entre as tabelas tl_cds_cad_individual e tb_cds_cad_individual
"""

import os
import psycopg2
from dotenv import load_dotenv
import json
from datetime import datetime

# Carregar variáveis de ambiente
load_dotenv()

def connect_db():
    """Conecta ao banco de dados PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco: {e}")
        return None

def analyze_table_structure(conn, table_name):
    """Analisa a estrutura de uma tabela"""
    cursor = conn.cursor()
    
    # Obter informações das colunas
    cursor.execute(f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' 
        ORDER BY ordinal_position;
    """)
    
    columns = cursor.fetchall()
    cursor.close()
    
    return columns

def analyze_table_data(conn, table_name, limit=5):
    """Analisa dados de uma tabela"""
    cursor = conn.cursor()
    
    # Contar total de registros
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_count = cursor.fetchone()[0]
    
    # Obter primeiros registros
    cursor.execute(f"SELECT * FROM {table_name} ORDER BY 1 LIMIT {limit}")
    sample_data = cursor.fetchall()
    
    # Obter nomes das colunas
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' 
        ORDER BY ordinal_position;
    """)
    column_names = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    
    return {
        'total_count': total_count,
        'sample_data': sample_data,
        'column_names': column_names
    }

def compare_key_fields(conn):
    """Compara campos-chave entre as duas tabelas"""
    cursor = conn.cursor()
    
    print("\n🔍 COMPARANDO CAMPOS-CHAVE ENTRE AS TABELAS")
    print("=" * 60)
    
    # Comparar alguns registros específicos
    queries = {
        'tl_cds_cad_individual': """
            SELECT co_seq_cds_cad_individual, no_cidadao, nu_cpf_cidadao, co_sexo, dt_nascimento 
            FROM tl_cds_cad_individual 
            ORDER BY co_seq_cds_cad_individual 
            LIMIT 3
        """,
        'tb_cds_cad_individual': """
            SELECT co_seq_cds_cad_individual, no_cidadao, nu_cpf_cidadao, co_sexo, dt_nascimento 
            FROM tb_cds_cad_individual 
            ORDER BY co_seq_cds_cad_individual 
            LIMIT 3
        """
    }
    
    results = {}
    for table, query in queries.items():
        cursor.execute(query)
        results[table] = cursor.fetchall()
        
        print(f"\n📊 {table.upper()}:")
        for row in results[table]:
            print(f"  ID: {row[0]} | Nome: {row[1]} | CPF: {row[2]} | Sexo: {row[3]} | Nascimento: {row[4]}")
    
    cursor.close()
    return results

def analyze_data_origin(conn):
    """Analisa a origem dos dados para identificar inversão"""
    cursor = conn.cursor()
    
    print("\n🔍 ANALISANDO ORIGEM DOS DADOS")
    print("=" * 60)
    
    # Verificar campos únicos que podem indicar origem
    queries = {
        'tl_cds_cad_individual': """
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT co_unico_ficha) as fichas_unicas,
                COUNT(DISTINCT no_cidadao) as nomes_unicos,
                MIN(dt_cad_individual) as primeira_data,
                MAX(dt_cad_individual) as ultima_data
            FROM tl_cds_cad_individual
        """,
        'tb_cds_cad_individual': """
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT co_unico_ficha) as fichas_unicas,
                COUNT(DISTINCT no_cidadao) as nomes_unicos,
                MIN(dt_cad_individual) as primeira_data,
                MAX(dt_cad_individual) as ultima_data
            FROM tb_cds_cad_individual
        """
    }
    
    for table, query in queries.items():
        cursor.execute(query)
        result = cursor.fetchone()
        print(f"\n📈 {table.upper()}:")
        print(f"  Total de registros: {result[0]}")
        print(f"  Fichas únicas: {result[1]}")
        print(f"  Nomes únicos: {result[2]}")
        print(f"  Primeira data: {result[3]}")
        print(f"  Última data: {result[4]}")
    
    cursor.close()

def main():
    print("🔍 ANÁLISE DE INVERSÃO DE DADOS - TABELAS CDS")
    print("=" * 60)
    
    conn = connect_db()
    if not conn:
        return
    
    try:
        # Analisar estrutura das tabelas
        print("\n📋 ESTRUTURA DAS TABELAS")
        print("-" * 40)
        
        for table in ['tl_cds_cad_individual', 'tb_cds_cad_individual']:
            print(f"\n🏗️ {table.upper()}:")
            columns = analyze_table_structure(conn, table)
            print(f"  Total de colunas: {len(columns)}")
            
            # Mostrar algumas colunas importantes
            important_cols = ['co_seq_cds_cad_individual', 'no_cidadao', 'nu_cpf_cidadao', 'co_sexo']
            for col_name, data_type, nullable, default in columns:
                if col_name in important_cols:
                    print(f"    {col_name}: {data_type} ({'NULL' if nullable == 'YES' else 'NOT NULL'})")
        
        # Analisar dados das tabelas
        print("\n📊 DADOS DAS TABELAS")
        print("-" * 40)
        
        for table in ['tl_cds_cad_individual', 'tb_cds_cad_individual']:
            data = analyze_table_data(conn, table)
            print(f"\n📈 {table.upper()}:")
            print(f"  Total de registros: {data['total_count']}")
            
            if data['sample_data']:
                print("  Primeiros registros:")
                for i, row in enumerate(data['sample_data'][:2]):
                    print(f"    Registro {i+1}:")
                    for j, col_name in enumerate(data['column_names'][:5]):  # Mostrar apenas primeiras 5 colunas
                        print(f"      {col_name}: {row[j]}")
        
        # Comparar campos-chave
        compare_key_fields(conn)
        
        # Analisar origem dos dados
        analyze_data_origin(conn)
        
        print("\n✅ ANÁLISE CONCLUÍDA")
        
    except Exception as e:
        print(f"❌ Erro durante análise: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()