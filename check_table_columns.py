#!/usr/bin/env python3
"""
Script para verificar a estrutura das tabelas FAT e identificar colunas faltantes
"""

import os
import psycopg2
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def check_table_structure():
    """Verifica a estrutura das tabelas FAT"""
    
    # Configuração do banco de dados
    db_config = {
        'host': os.getenv('POSTGRES_HOST', '127.0.0.1'),
        'database': os.getenv('POSTGRES_DB', 'esus'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', ''),
        'port': os.getenv('POSTGRES_PORT', '5433')
    }
    
    try:
        # Conectar ao banco de dados
        print("Conectando ao banco de dados...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Tabelas para verificar
        tables = [
            'tb_fat_cad_individual',
            'tb_fat_cidadao', 
            'tb_fat_cidadao_pec'
        ]
        
        for table in tables:
            print(f"\n=== Estrutura da tabela {table} ===")
            
            # Verificar se a tabela existe
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (table,))
            
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                print(f"❌ Tabela {table} NÃO EXISTE!")
                continue
                
            print(f"✅ Tabela {table} existe")
            
            # Listar todas as colunas da tabela
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position;
            """, (table,))
            
            columns = cursor.fetchall()
            
            print(f"Colunas encontradas ({len(columns)}):")
            for col_name, data_type, nullable, default in columns:
                print(f"  - {col_name} ({data_type}) {'NULL' if nullable == 'YES' else 'NOT NULL'} {f'DEFAULT {default}' if default else ''}")
            
            # Verificar especificamente se co_seq_cds_cad_individual existe
            has_co_seq = any(col[0] == 'co_seq_cds_cad_individual' for col in columns)
            if has_co_seq:
                print("✅ Coluna co_seq_cds_cad_individual EXISTE")
            else:
                print("❌ Coluna co_seq_cds_cad_individual NÃO EXISTE")
        
        cursor.close()
        conn.close()
        print("\n✅ Verificação concluída!")
        
    except Exception as e:
        print(f"❌ Erro ao verificar estrutura das tabelas: {e}")

if __name__ == "__main__":
    check_table_structure()