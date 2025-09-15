#!/usr/bin/env python3
"""
Script para testar conexão com banco e verificar tabelas FAT
"""

import psycopg2
import os
from dotenv import load_dotenv

def test_connection():
    # Carregar variáveis de ambiente
    load_dotenv()
    
    try:
        print("🔍 Testando conexão com PostgreSQL...")
        print(f"Host: {os.getenv('POSTGRES_HOST')}")
        print(f"Port: {os.getenv('POSTGRES_PORT')}")
        print(f"Database: {os.getenv('POSTGRES_DB')}")
        print(f"User: {os.getenv('POSTGRES_USER')}")
        
        # Conectar ao banco
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        
        cur = conn.cursor()
        print("✅ Conexão estabelecida com sucesso!")
        
        # Verificar tabelas FAT
        print("\n🔍 Verificando tabelas FAT...")
        cur.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public' 
            AND (tablename LIKE 'tb_fat_%' OR tablename LIKE 'fat_%')
            ORDER BY tablename;
        """)
        
        fat_tables = cur.fetchall()
        
        if fat_tables:
            print(f"✅ Encontradas {len(fat_tables)} tabelas FAT:")
            for table in fat_tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table[0]};")
                    count = cur.fetchone()[0]
                    print(f"  - {table[0]}: {count} registros")
                except Exception as e:
                    print(f"  - {table[0]}: Erro ao contar - {e}")
        else:
            print("❌ Nenhuma tabela FAT encontrada!")
        
        # Testar especificamente a tb_fat_cad_individual
        print("\n🎯 Testando tb_fat_cad_individual...")
        try:
            cur.execute("SELECT COUNT(*) FROM tb_fat_cad_individual;")
            count = cur.fetchone()[0]
            print(f"✅ tb_fat_cad_individual: {count} registros")
            
            # Mostrar algumas colunas
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'tb_fat_cad_individual' 
                ORDER BY ordinal_position 
                LIMIT 10;
            """)
            columns = cur.fetchall()
            print("📋 Primeiras 10 colunas:")
            for col in columns:
                print(f"  - {col[0]} ({col[1]})")
                
        except Exception as e:
            print(f"❌ Erro ao acessar tb_fat_cad_individual: {e}")
        
        cur.close()
        conn.close()
        print("\n✅ Teste concluído!")
        
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco: {e}")
        return False
    
    return True

if __name__ == '__main__':
    test_connection()