#!/usr/bin/env python3
import os
import psycopg2
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def check_table_columns():
    try:
        # Conectar ao banco
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        
        cursor = conn.cursor()
        
        tables = ['tl_cds_cad_individual', 'tb_cds_cad_individual']
        
        for table in tables:
            print(f"\n🔍 COLUNAS DA TABELA {table.upper()}:")
            print("=" * 60)
            
            # Verificar se a tabela existe
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (table,))
            
            if cursor.fetchone()[0]:
                # Listar todas as colunas
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                    ORDER BY ordinal_position;
                """, (table,))
                
                columns = cursor.fetchall()
                
                print(f"📊 Total de colunas: {len(columns)}")
                print("\n📋 Lista de colunas:")
                for i, (col_name, data_type, nullable) in enumerate(columns, 1):
                    null_info = "NULL" if nullable == "YES" else "NOT NULL"
                    print(f"   {i:2d}. {col_name:<30} | {data_type:<20} | {null_info}")
                    
                # Verificar se tem dados
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"\n📈 Registros na tabela: {count}")
                
                if count > 0:
                    # Mostrar uma amostra dos dados
                    cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                    sample = cursor.fetchone()
                    print(f"\n🔍 Amostra de dados (primeiro registro):")
                    for i, (col_name, _, _) in enumerate(columns):
                        value = sample[i] if sample and i < len(sample) else 'NULL'
                        print(f"   {col_name}: {value}")
            else:
                print(f"❌ Tabela {table} não encontrada!")
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ Verificação concluída!")
        
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    check_table_columns()