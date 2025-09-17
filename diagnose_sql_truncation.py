#!/usr/bin/env python3
"""
Script para diagnosticar a origem do comando SQL truncado
"""

import os
import psycopg2
import sys
import re
from pathlib import Path

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

def test_table_name_limits():
    """Testa limites de nomes de tabela no PostgreSQL"""
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        
        # Testa o limite de caracteres para nomes de tabela no PostgreSQL
        print("🔍 Testando limites de nomes de tabela no PostgreSQL...")
        
        # PostgreSQL tem limite de 63 caracteres para identificadores
        test_names = [
            "tb_cds_cad_individual",  # 20 caracteres
            "tb_cds_cad_individu",    # 18 caracteres (truncado)
            "a" * 63,                 # 63 caracteres (limite máximo)
            "a" * 64,                 # 64 caracteres (deve ser truncado)
        ]
        
        for name in test_names:
            try:
                # Testa se o nome é válido
                cur.execute(f"SELECT '{name}'::regclass::text")
                result = cur.fetchone()
                print(f"✅ Nome '{name}' ({len(name)} chars): {result[0] if result else 'NULL'}")
            except Exception as e:
                print(f"❌ Nome '{name}' ({len(name)} chars): {e}")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Erro ao testar limites: {e}")
        return False

def check_environment_variables():
    """Verifica variáveis de ambiente que podem estar causando truncagem"""
    print("\n🔍 Verificando variáveis de ambiente...")
    
    env_vars = [
        "TABLE_NAME",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_HOST",
        "POSTGRES_PORT"
    ]
    
    for var in env_vars:
        value = os.getenv(var, "NÃO DEFINIDA")
        print(f"   {var}: {value}")
    
    # Verifica se há alguma variável que possa estar truncando
    table_name = os.getenv("TABLE_NAME", "")
    if "tb_cds_cad_individu" in table_name and not table_name.endswith("individual"):
        print(f"⚠️  PROBLEMA ENCONTRADO: TABLE_NAME está truncado: {table_name}")
        return False
    
    return True

def check_code_for_truncation():
    """Verifica código Python que pode estar truncando nomes de tabela"""
    print("\n🔍 Verificando código Python para truncagem...")
    
    files_to_check = [
        "migrator.py",
        "csv_adjuster.py",
        "server.js"
    ]
    
    base_dir = Path("D:\\Robson\\Projetos\\Cascavel")
    
    for file_name in files_to_check:
        file_path = base_dir / file_name
        if not file_path.exists():
            continue
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Procura por padrões que podem truncar strings
            patterns = [
                r'\.substring\(\d+,\s*\d+\)',
                r'\[:\d+\]',
                r'\.slice\(\d+,\s*\d+\)',
                r'text\[:\d+\]',
                r'truncate.*\(',
                r'tb_cds_cad_individu[^a-l]'
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                if matches:
                    print(f"⚠️  Padrão suspeito em {file_name}: {matches}")
                    
        except Exception as e:
            print(f"❌ Erro ao verificar {file_name}: {e}")

def test_sql_generation():
    """Testa geração de comandos SQL"""
    print("\n🔍 Testando geração de comandos SQL...")
    
    # Simula diferentes cenários de geração de SQL
    table_names = [
        "public.tb_cds_cad_individual",
        "tb_cds_cad_individual",
        "public.tb_cds_cad_individu",  # truncado
        "tb_cds_cad_individu"          # truncado
    ]
    
    for table_name in table_names:
        sql = f"INSERT INTO {table_name} (co_seq_cds_cad_individual) VALUES (1);"
        print(f"   SQL: {sql}")
        
        # Testa se o SQL é válido
        if "tb_cds_cad_individu" in table_name and not table_name.endswith("individual"):
            print(f"   ❌ PROBLEMA: Nome da tabela truncado detectado!")

def check_postgresql_logs():
    """Verifica logs do PostgreSQL para erros relacionados"""
    print("\n🔍 Verificando logs do PostgreSQL...")
    
    # Locais comuns de logs do PostgreSQL no Windows
    possible_log_dirs = [
        "C:\\Program Files\\PostgreSQL\\16\\data\\log",
        "C:\\Program Files\\PostgreSQL\\15\\data\\log",
        "C:\\Program Files\\PostgreSQL\\14\\data\\log",
        "C:\\PostgreSQL\\data\\log"
    ]
    
    for log_dir in possible_log_dirs:
        if os.path.exists(log_dir):
            print(f"   📁 Diretório de logs encontrado: {log_dir}")
            
            # Lista arquivos de log recentes
            try:
                log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
                if log_files:
                    latest_log = sorted(log_files)[-1]
                    print(f"   📄 Log mais recente: {latest_log}")
                    
                    # Verifica se há erros relacionados ao nosso problema
                    log_path = os.path.join(log_dir, latest_log)
                    try:
                        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                            # Lê apenas as últimas 100 linhas
                            lines = f.readlines()[-100:]
                            
                        for line in lines:
                            if "tb_cds_cad_individu" in line and "syntax error" in line:
                                print(f"   ❌ ERRO ENCONTRADO NO LOG: {line.strip()}")
                                
                    except Exception as e:
                        print(f"   ⚠️  Erro ao ler log: {e}")
                        
            except Exception as e:
                print(f"   ⚠️  Erro ao listar logs: {e}")
            
            break
    else:
        print("   ℹ️  Nenhum diretório de logs do PostgreSQL encontrado")

def main():
    print("🔍 DIAGNÓSTICO DE TRUNCAGEM SQL")
    print("=" * 50)
    
    # Carrega variáveis de ambiente do .env
    env_file = "D:\\Robson\\Projetos\\Cascavel\\.env"
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value.strip('"')
    
    # Executa todos os testes
    tests = [
        ("Variáveis de Ambiente", check_environment_variables),
        ("Limites de Nomes de Tabela", test_table_name_limits),
        ("Código Python", check_code_for_truncation),
        ("Geração de SQL", test_sql_generation),
        ("Logs do PostgreSQL", check_postgresql_logs)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Erro durante teste {test_name}: {e}")
            results.append((test_name, False))
    
    # Resumo
    print(f"\n{'='*20} RESUMO {'='*20}")
    for test_name, result in results:
        status = "✅ PASSOU" if result else "❌ FALHOU"
        print(f"{test_name}: {status}")
    
    print("\n🔍 RECOMENDAÇÕES:")
    print("1. Verifique se há processos em execução que possam estar gerando SQL truncado")
    print("2. Examine logs de aplicação para comandos SQL malformados")
    print("3. Verifique se há limitações de buffer ou configurações de rede")
    print("4. Considere reiniciar serviços PostgreSQL se necessário")

if __name__ == "__main__":
    main()