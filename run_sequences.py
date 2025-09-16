#!/usr/bin/env python3
"""
Script para executar a criação das sequences necessárias no banco de dados
"""

import psycopg2
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def create_sequences():
    """Cria todas as sequences necessárias no banco de dados"""
    
    # Configuração do banco de dados
    db_config = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'database': os.getenv('POSTGRES_DB', 'esus'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
        'port': os.getenv('POSTGRES_PORT', '5432')
    }
    
    # SQL para criar as sequences
    sequences_sql = """
    -- Sequence para tb_cds_cad_individual
    CREATE SEQUENCE IF NOT EXISTS seq_tb_cds_cad_individual
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;

    -- Sequence para tl_cds_cad_individual
    CREATE SEQUENCE IF NOT EXISTS seq_tl_cds_cad_individual
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;

    -- Sequence para tb_fat_cad_individual
    CREATE SEQUENCE IF NOT EXISTS seq_tb_fat_cad_individual
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;

    -- Sequence para tb_fat_cidadao
    CREATE SEQUENCE IF NOT EXISTS seq_tb_fat_cidadao
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;

    -- Sequence para tb_fat_cidadao_pec
    CREATE SEQUENCE IF NOT EXISTS seq_tb_fat_cidadao_pec
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;

    -- Sequence para tb_cidadao
    CREATE SEQUENCE IF NOT EXISTS seq_tb_cidadao
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
    """
    
    # SQL para verificar as sequences criadas (consulta simples)
    verify_sql = """
    SELECT n.nspname as schema_name, c.relname as sequence_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'S' 
    AND c.relname IN (
        'seq_tb_cds_cad_individual',
        'seq_tl_cds_cad_individual', 
        'seq_tb_fat_cad_individual',
        'seq_tb_fat_cidadao',
        'seq_tb_fat_cidadao_pec',
        'seq_tb_cidadao'
    )
    ORDER BY c.relname;
    """
    
    try:
        # Conectar ao banco de dados
        print("🔗 Conectando ao banco de dados...")
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        # Executar criação das sequences
        print("📝 Criando sequences...")
        cur.execute(sequences_sql)
        conn.commit()
        print("✅ Sequences criadas com sucesso!")
        
        # Verificar sequences criadas
        print("\n🔍 Verificando sequences criadas:")
        cur.execute(verify_sql)
        sequences = cur.fetchall()
        
        if sequences:
            print(f"📊 {len(sequences)} sequences encontradas:")
            for schema, seq_name in sequences:
                print(f"  - {schema}.{seq_name}")
        else:
            print("⚠️  Nenhuma sequence encontrada!")
        
        # Fechar conexões
        cur.close()
        conn.close()
        print("\n🎉 Processo concluído com sucesso!")
        
    except psycopg2.Error as e:
        print(f"❌ Erro ao conectar/executar no banco de dados: {e}")
        return False
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("🚀 Iniciando criação das sequences...")
    success = create_sequences()
    
    if success:
        print("\n✨ Todas as sequences foram criadas com sucesso!")
        print("   Agora você pode executar o migrator.py sem erros de sequences.")
    else:
        print("\n💥 Falha na criação das sequences. Verifique os logs acima.")