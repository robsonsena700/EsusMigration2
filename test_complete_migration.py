#!/usr/bin/env python3
"""
Script para testar a migração completa com arquivo CSV real
Testa se a correção do mapeamento INE está funcionando na prática
"""

import os
import sys
import psycopg2
import subprocess
from dotenv import load_dotenv
from pathlib import Path

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

def count_records_before():
    """Conta registros na tabela antes da migração"""
    conn = get_db_connection()
    if not conn:
        return 0
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM public.tl_cds_cad_individual")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception as e:
        print(f"❌ Erro ao contar registros: {e}")
        return 0

def get_sample_csv_file():
    """Encontra um arquivo CSV de exemplo para teste"""
    # Procura em múltiplos diretórios
    search_dirs = [
        Path("D:/Robson/Projetos/Cascavel/datacsv"),
        Path("D:/Robson/Projetos/Cascavel"),
    ]
    
    csv_files = []
    
    for csv_dir in search_dirs:
        if csv_dir.exists():
            files = list(csv_dir.glob("*.csv"))
            csv_files.extend(files)
            print(f"📁 Encontrados {len(files)} arquivos em {csv_dir}")
    
    if not csv_files:
        print("❌ Nenhum arquivo CSV encontrado")
        return None
    
    # Prefere arquivos que sabemos que têm INEs válidos
    preferred_files = ["Brito", "Barra Nova"]
    
    for preferred in preferred_files:
        for csv_file in csv_files:
            if preferred.lower() in csv_file.name.lower():
                return csv_file
    
    # Se não encontrar os preferidos, usa o primeiro
    return csv_files[0]

def run_migration_test(csv_file):
    """Executa teste de migração com arquivo CSV"""
    print(f"🚀 TESTE DE MIGRAÇÃO COMPLETA")
    print(f"=" * 50)
    print(f"📄 Arquivo CSV: {csv_file.name}")
    
    # Conta registros antes
    records_before = count_records_before()
    print(f"📊 Registros antes da migração: {records_before}")
    
    # Prepara comando de migração
    python_cmd = os.getenv('PYTHON_COMMAND', 'python')
    migrator_script = "migrator.py"
    
    # Comando para executar migração
    cmd = [
        python_cmd,
        migrator_script,
        "--file", str(csv_file),
        "--table-name", "public.tl_cds_cad_individual"
    ]
    
    print(f"🔧 Executando: {' '.join(cmd)}")
    print(f"-" * 50)
    
    try:
        # Executa migração
        result = subprocess.run(
            cmd,
            cwd=os.getcwd(),
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos timeout
        )
        
        print("📋 SAÍDA DA MIGRAÇÃO:")
        print(result.stdout)
        
        if result.stderr:
            print("⚠️ ERROS/WARNINGS:")
            print(result.stderr)
        
        # Conta registros depois
        records_after = count_records_before()
        records_added = records_after - records_before
        
        print(f"-" * 50)
        print(f"📊 RESULTADO:")
        print(f"   📊 Registros antes: {records_before}")
        print(f"   📊 Registros depois: {records_after}")
        print(f"   ➕ Registros adicionados: {records_added}")
        
        if result.returncode == 0:
            print(f"   ✅ Migração concluída com sucesso!")
            
            if records_added > 0:
                print(f"   🎯 {records_added} novos registros inseridos")
                return True
            else:
                print(f"   ⚠️ Nenhum registro foi inserido")
                return False
        else:
            print(f"   ❌ Migração falhou (código: {result.returncode})")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Timeout: Migração demorou mais de 5 minutos")
        return False
    except Exception as e:
        print(f"❌ Erro ao executar migração: {e}")
        return False

def check_ine_mapping_in_db():
    """Verifica se os INEs foram mapeados corretamente no banco"""
    print(f"\n🔍 VERIFICANDO MAPEAMENTO INE NO BANCO")
    print(f"=" * 50)
    
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Verifica registros recentes com co_unidade_saude válido
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(co_unidade_saude) as com_unidade,
                COUNT(DISTINCT co_unidade_saude) as unidades_distintas
            FROM public.tl_cds_cad_individual 
            WHERE co_unidade_saude IS NOT NULL
        """)
        
        result = cursor.fetchone()
        total, com_unidade, unidades_distintas = result
        
        print(f"📊 Registros com co_unidade_saude: {com_unidade}/{total}")
        print(f"📊 Unidades distintas: {unidades_distintas}")
        
        if com_unidade > 0:
            # Mostra algumas unidades mapeadas
            cursor.execute("""
                SELECT 
                    co_unidade_saude,
                    COUNT(*) as registros
                FROM public.tl_cds_cad_individual 
                WHERE co_unidade_saude IS NOT NULL
                GROUP BY co_unidade_saude
                ORDER BY registros DESC
                LIMIT 5
            """)
            
            print(f"\n📋 Top 5 unidades com mais registros:")
            for row in cursor.fetchall():
                unidade, count = row
                print(f"   🏥 Unidade {unidade}: {count} registros")
            
            cursor.close()
            conn.close()
            return True
        else:
            print(f"❌ Nenhum registro com co_unidade_saude encontrado")
            cursor.close()
            conn.close()
            return False
            
    except Exception as e:
        print(f"❌ Erro ao verificar mapeamento: {e}")
        return False

def main():
    """Função principal"""
    print(f"🧪 TESTE COMPLETO DE MIGRAÇÃO COM CORREÇÃO INE")
    print(f"=" * 60)
    
    # Verifica conexão com banco
    conn = get_db_connection()
    if not conn:
        print("❌ Não foi possível conectar ao banco de dados")
        return False
    conn.close()
    print("✅ Conexão com banco OK")
    
    # Encontra arquivo CSV
    csv_file = get_sample_csv_file()
    if not csv_file:
        print("❌ Nenhum arquivo CSV encontrado para teste")
        return False
    
    print(f"✅ Arquivo CSV encontrado: {csv_file}")
    
    # Executa teste de migração
    migration_success = run_migration_test(csv_file)
    
    if migration_success:
        # Verifica mapeamento no banco
        mapping_success = check_ine_mapping_in_db()
        
        if mapping_success:
            print(f"\n🎉 TESTE COMPLETO: SUCESSO!")
            print(f"✅ Migração executada com sucesso")
            print(f"✅ Mapeamento INE -> co_unidade_saude funcionando")
            return True
        else:
            print(f"\n⚠️ TESTE PARCIAL: Migração OK, mas mapeamento com problemas")
            return False
    else:
        print(f"\n❌ TESTE FALHOU: Problemas na migração")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)