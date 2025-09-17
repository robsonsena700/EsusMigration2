#!/usr/bin/env python3
"""
Script para testar a correção do mapeamento INE -> co_unidade_saude
"""

import psycopg2
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
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

def get_unidade_saude_by_ine_corrected(conn, ine_equipe):
    """
    Versão corrigida da função que busca o código da unidade de saúde pelo INE da equipe
    Tenta diferentes formatos de INE para garantir compatibilidade
    """
    if not conn or not ine_equipe:
        return None
    
    try:
        cur = conn.cursor()
        
        # Limpar o INE removendo aspas e espaços
        ine_clean = str(ine_equipe).strip().strip('"').strip("'")
        
        # Lista de formatos de INE para tentar
        ine_formats = [
            ine_clean,                    # Formato original
            ine_clean.zfill(10),         # Com zeros à esquerda (10 dígitos)
            ine_clean.lstrip('0'),       # Sem zeros à esquerda
        ]
        
        # Remover duplicatas mantendo a ordem
        ine_formats = list(dict.fromkeys(ine_formats))
        
        print(f"🔍 Testando INE '{ine_equipe}' nos formatos: {ine_formats}")
        
        for ine_format in ine_formats:
            cur.execute("""
                SELECT co_unidade_saude, no_equipe
                FROM tb_equipe 
                WHERE nu_ine = %s
            """, (ine_format,))
            
            result = cur.fetchone()
            
            if result:
                cur.close()
                print(f"✅ INE '{ine_equipe}' encontrado como '{ine_format}' -> Unidade: {result[0]}, Equipe: {result[1]}")
                return result[0]
        
        cur.close()
        print(f"❌ INE equipe '{ine_equipe}' não encontrado na tabela tb_equipe")
        return None
        
    except Exception as e:
        print(f"❌ Erro ao buscar unidade de saúde pelo INE {ine_equipe}: {e}")
        return None

def test_problematic_ines():
    """Testa os INEs problemáticos identificados nos logs"""
    
    print("🚀 TESTE DA CORREÇÃO DO MAPEAMENTO INE")
    print("=" * 50)
    
    conn = get_db_connection()
    if not conn:
        return
    
    # INEs problemáticos identificados nos logs
    problematic_ines = [
        "83364",
        "1913433",
        '"83364"',
        '"1913433"',
    ]
    
    # INEs dos CSVs analisados
    csv_ines = [
        '"0001482254"',  # Barra Nova
        '"0001496166"',  # Brito
    ]
    
    all_test_ines = problematic_ines + csv_ines
    
    print(f"📋 Testando {len(all_test_ines)} INEs:")
    
    success_count = 0
    
    for ine in all_test_ines:
        print(f"\n🔍 Testando INE: {ine}")
        result = get_unidade_saude_by_ine_corrected(conn, ine)
        
        if result:
            success_count += 1
            print(f"   ✅ Sucesso: Unidade {result}")
        else:
            print(f"   ❌ Falhou")
    
    print(f"\n📊 RESULTADO:")
    print(f"   ✅ Sucessos: {success_count}/{len(all_test_ines)}")
    print(f"   ❌ Falhas: {len(all_test_ines) - success_count}/{len(all_test_ines)}")
    
    if success_count > 0:
        print(f"   🎯 Taxa de sucesso: {(success_count/len(all_test_ines)*100):.1f}%")
    
    conn.close()

def test_csv_ines():
    """Testa especificamente os INEs encontrados nos CSVs"""
    
    print("\n🚀 TESTE ESPECÍFICO DOS INEs DOS CSVs")
    print("=" * 50)
    
    conn = get_db_connection()
    if not conn:
        return
    
    # INEs específicos dos CSVs analisados
    csv_test_cases = [
        {
            "arquivo": "Alto Luminoso",
            "ines": []  # Não encontramos INEs neste arquivo
        },
        {
            "arquivo": "Barra Nova", 
            "ines": ['"0001482254"']
        },
        {
            "arquivo": "Brito",
            "ines": ['"0001496166"']
        }
    ]
    
    for test_case in csv_test_cases:
        print(f"\n📄 Arquivo: {test_case['arquivo']}")
        
        if not test_case['ines']:
            print("   ⚠️  Nenhum INE encontrado neste arquivo")
            continue
        
        for ine in test_case['ines']:
            print(f"   🔍 Testando INE: {ine}")
            result = get_unidade_saude_by_ine_corrected(conn, ine)
            
            if result:
                print(f"      ✅ Mapeado para unidade: {result}")
            else:
                print(f"      ❌ Não foi possível mapear")
    
    conn.close()

if __name__ == "__main__":
    test_problematic_ines()
    test_csv_ines()
    print("\n✅ TESTE CONCLUÍDO")