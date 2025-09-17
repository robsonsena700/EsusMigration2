#!/usr/bin/env python3
"""
Relatório Final de Validação da Correção INE
============================================

Este script valida se a correção INE está funcionando corretamente
em todos os aspectos do sistema.
"""

import os
import psycopg2
import requests
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

def validate_database():
    """Valida os dados no banco de dados"""
    print("🔍 VALIDAÇÃO DO BANCO DE DADOS")
    print("=" * 50)
    
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # 1. Verificar total de registros
        cursor.execute("SELECT COUNT(*) FROM public.tl_cds_cad_individual")
        total_records = cursor.fetchone()[0]
        print(f"📊 Total de registros: {total_records:,}")
        
        # 2. Verificar registros com co_unidade_saude preenchido
        cursor.execute("""
            SELECT COUNT(*) 
            FROM public.tl_cds_cad_individual 
            WHERE co_unidade_saude IS NOT NULL
        """)
        with_unidade = cursor.fetchone()[0]
        print(f"📊 Registros com co_unidade_saude: {with_unidade:,}")
        
        # 3. Verificar distribuição por unidade
        cursor.execute("""
            SELECT 
                u.no_unidade_saude,
                t.co_unidade_saude,
                COUNT(*) as count
            FROM public.tl_cds_cad_individual t
            LEFT JOIN public.tb_unidade_saude u ON t.co_unidade_saude = u.co_seq_unidade_saude
            WHERE t.co_unidade_saude IS NOT NULL
            GROUP BY u.no_unidade_saude, t.co_unidade_saude
            ORDER BY count DESC
        """)
        
        print(f"\n📋 Distribuição por unidade de saúde:")
        results = cursor.fetchall()
        for unidade_nome, unidade_id, count in results:
            print(f"   🏥 {unidade_nome} (ID: {unidade_id}): {count:,} registros")
        
        # 4. Verificar se a correção INE específica funcionou
        cursor.execute("""
            SELECT COUNT(*) 
            FROM public.tl_cds_cad_individual 
            WHERE co_unidade_saude = 3
        """)
        brito_count = cursor.fetchone()[0]
        
        success = brito_count > 0 and with_unidade > 0
        
        if success:
            print(f"\n✅ BANCO DE DADOS: Correção INE funcionando!")
            print(f"   - Unidade BRITO (ID: 3): {brito_count:,} registros")
        else:
            print(f"\n❌ BANCO DE DADOS: Problema na correção INE")
            
        return success
        
    except Exception as e:
        print(f"❌ Erro na validação do banco: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def validate_backend():
    """Valida se o backend está funcionando"""
    print("\n🔍 VALIDAÇÃO DO BACKEND")
    print("=" * 50)
    
    try:
        # 1. Testar health check
        response = requests.get("http://localhost:3000/api/health", timeout=5)
        if response.status_code == 200:
            print("✅ Health check: OK")
        else:
            print(f"❌ Health check: Status {response.status_code}")
            return False
        
        # 2. Testar endpoint de dados
        response = requests.get("http://localhost:3000/api/tables/tl_cds_cad_individual?page=1&limit=5", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Endpoint de dados: {len(data.get('data', []))} registros retornados")
            
            # Verificar se há registros com co_unidade_saude
            records_with_unidade = [r for r in data.get('data', []) if r.get('co_unidade_saude')]
            if records_with_unidade:
                print(f"✅ Registros com co_unidade_saude: {len(records_with_unidade)}")
            else:
                print("⚠️ Nenhum registro com co_unidade_saude nos primeiros 5")
                
        else:
            print(f"❌ Endpoint de dados: Status {response.status_code}")
            return False
            
        print("✅ BACKEND: Funcionando corretamente!")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"❌ BACKEND: Erro de conexão - {e}")
        return False
    except Exception as e:
        print(f"❌ BACKEND: Erro inesperado - {e}")
        return False

def validate_frontend():
    """Valida se o frontend está funcionando"""
    print("\n🔍 VALIDAÇÃO DO FRONTEND")
    print("=" * 50)
    
    try:
        response = requests.get("http://localhost:3001", timeout=5)
        if response.status_code == 200:
            print("✅ FRONTEND: Respondendo na porta 3001")
            return True
        else:
            print(f"❌ FRONTEND: Status {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ FRONTEND: Erro de conexão - {e}")
        return False

def generate_final_report():
    """Gera o relatório final"""
    print("\n" + "=" * 60)
    print("📋 RELATÓRIO FINAL DE VALIDAÇÃO")
    print("=" * 60)
    
    db_ok = validate_database()
    backend_ok = validate_backend()
    frontend_ok = validate_frontend()
    
    print("\n" + "=" * 60)
    print("🎯 RESUMO EXECUTIVO")
    print("=" * 60)
    
    print(f"{'✅' if db_ok else '❌'} Banco de Dados: {'OK' if db_ok else 'FALHA'}")
    print(f"{'✅' if backend_ok else '❌'} Backend API: {'OK' if backend_ok else 'FALHA'}")
    print(f"{'✅' if frontend_ok else '❌'} Frontend: {'OK' if frontend_ok else 'FALHA'}")
    
    all_ok = db_ok and backend_ok and frontend_ok
    
    print(f"\n{'🎉' if all_ok else '⚠️'} STATUS GERAL: {'SUCESSO COMPLETO' if all_ok else 'REQUER ATENÇÃO'}")
    
    if all_ok:
        print("\n✅ A correção INE foi implementada com sucesso!")
        print("✅ Todos os componentes estão funcionando corretamente!")
        print("✅ O sistema está pronto para uso em produção!")
    else:
        print("\n⚠️ Alguns componentes precisam de atenção.")
        print("⚠️ Verifique os logs acima para detalhes.")
    
    return all_ok

if __name__ == "__main__":
    print("🚀 INICIANDO VALIDAÇÃO FINAL DO SISTEMA")
    print("🎯 Verificando correção INE e funcionalidade geral")
    print()
    
    success = generate_final_report()
    
    print("\n" + "=" * 60)
    exit(0 if success else 1)