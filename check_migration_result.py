#!/usr/bin/env python3
"""
Script para verificar se a migração funcionou e se a correção INE está ativa
"""

import os
import psycopg2
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

def get_db_connection():
    """Conecta ao banco de dados"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('POSTGRES_PORT', 5432)
        )
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco: {e}")
        return None

def check_migration_results():
    """Verifica os resultados da migração"""
    print("🔍 VERIFICANDO RESULTADOS DA MIGRAÇÃO")
    print("=" * 50)
    
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Conta total de registros
        cursor.execute("SELECT COUNT(*) FROM public.tl_cds_cad_individual")
        total_records = cursor.fetchone()[0]
        print(f"📊 Total de registros: {total_records}")
        
        # Conta registros com co_unidade_saude
        cursor.execute("""
            SELECT COUNT(*) 
            FROM public.tl_cds_cad_individual 
            WHERE co_unidade_saude IS NOT NULL
        """)
        with_unidade = cursor.fetchone()[0]
        print(f"📊 Registros com co_unidade_saude: {with_unidade}")
        
        # Verifica registros recentes (últimos 100)
        cursor.execute("""
            SELECT 
                co_unidade_saude,
                COUNT(*) as count
            FROM public.tl_cds_cad_individual 
            WHERE co_unidade_saude IS NOT NULL
            GROUP BY co_unidade_saude
            ORDER BY count DESC
            LIMIT 10
        """)
        
        print(f"\n📋 Top 10 unidades com mais registros:")
        results = cursor.fetchall()
        for unidade, count in results:
            print(f"   🏥 Unidade {unidade}: {count} registros")
        
        # Verifica se há registros da equipe BRITO (que tem INE 0001496166)
        cursor.execute("""
            SELECT COUNT(*) 
            FROM public.tl_cds_cad_individual 
            WHERE co_unidade_saude = 3
        """)
        brito_count = cursor.fetchone()[0]
        print(f"\n🎯 Registros da unidade 3 (BRITO): {brito_count}")
        
        if brito_count > 0:
            print("✅ SUCESSO: Correção INE funcionando! Registros foram mapeados para a unidade correta.")
            return True
        else:
            print("⚠️ Nenhum registro encontrado para a unidade BRITO")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao verificar resultados: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_migration_results()