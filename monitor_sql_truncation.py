#!/usr/bin/env python3
"""
Script para monitorar e interceptar comandos SQL truncados em tempo real
"""

import os
import psycopg2
import sys
import time
import threading
from pathlib import Path
import subprocess
import re

def monitor_postgresql_logs():
    """Monitora logs do PostgreSQL em tempo real"""
    print("🔍 Monitorando logs do PostgreSQL...")
    
    # Locais comuns de logs do PostgreSQL no Windows
    possible_log_dirs = [
        "C:\\Program Files\\PostgreSQL\\16\\data\\log",
        "C:\\Program Files\\PostgreSQL\\15\\data\\log",
        "C:\\Program Files\\PostgreSQL\\14\\data\\log",
        "C:\\PostgreSQL\\data\\log"
    ]
    
    for log_dir in possible_log_dirs:
        if os.path.exists(log_dir):
            print(f"   📁 Monitorando: {log_dir}")
            
            try:
                # Usa PowerShell para monitorar o log em tempo real
                cmd = f'Get-Content "{log_dir}\\*.log" -Wait -Tail 10 | Where-Object {{$_ -match "tb_cds_cad_individu"}}'
                
                process = subprocess.Popen(
                    ["powershell", "-Command", cmd],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
                
                print("   ⏳ Aguardando comandos SQL truncados...")
                
                for line in iter(process.stdout.readline, ''):
                    if line.strip():
                        print(f"   🚨 COMANDO TRUNCADO DETECTADO: {line.strip()}")
                        
                        # Analisa o comando
                        if "syntax error" in line.lower():
                            print(f"   ❌ ERRO DE SINTAXE CONFIRMADO!")
                        
                        if "tb_cds_cad_individu" in line and not "tb_cds_cad_individual" in line:
                            print(f"   ⚠️  TRUNCAGEM CONFIRMADA: Nome da tabela está truncado!")
                
            except Exception as e:
                print(f"   ❌ Erro ao monitorar logs: {e}")
            
            break
    else:
        print("   ℹ️  Nenhum diretório de logs do PostgreSQL encontrado")

def monitor_application_logs():
    """Monitora logs da aplicação em tempo real"""
    print("\n🔍 Monitorando logs da aplicação...")
    
    app_log = "D:\\Robson\\Projetos\\Cascavel\\backend\\logs\\app.log"
    
    if not os.path.exists(app_log):
        print("   ❌ Arquivo de log da aplicação não encontrado")
        return
    
    try:
        # Usa PowerShell para monitorar o log em tempo real
        cmd = f'Get-Content "{app_log}" -Wait -Tail 10 | Where-Object {{$_ -match "tb_cds_cad_individu"}}'
        
        process = subprocess.Popen(
            ["powershell", "-Command", cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        print("   ⏳ Aguardando comandos SQL truncados na aplicação...")
        
        for line in iter(process.stdout.readline, ''):
            if line.strip():
                print(f"   🚨 COMANDO TRUNCADO NA APLICAÇÃO: {line.strip()}")
                
                # Analisa o comando
                if "INSERT INTO" in line and "tb_cds_cad_individu" in line:
                    print(f"   ❌ INSERT TRUNCADO DETECTADO!")
                
    except Exception as e:
        print(f"   ❌ Erro ao monitorar logs da aplicação: {e}")

def test_direct_sql_execution():
    """Testa execução direta de SQL para reproduzir o erro"""
    print("\n🔍 Testando execução direta de SQL...")
    
    # Carrega variáveis de ambiente
    env_file = "D:\\Robson\\Projetos\\Cascavel\\.env"
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value.strip('"')
    
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
            port=os.getenv("POSTGRES_PORT", "5433"),
            database=os.getenv("POSTGRES_DB", "esus"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        
        cur = conn.cursor()
        
        # Testa diferentes variações do comando
        test_commands = [
            "INSERT INTO public.tb_cds_cad_individual (co_seq_cds_cad_individual) VALUES (1);",
            "INSERT INTO tb_cds_cad_individual (co_seq_cds_cad_individual) VALUES (1);",
            "INSERT INTO public.tb_cds_cad_individu (co_seq_cds_cad_individual) VALUES (1);",  # truncado
            "INSERT INTO tb_cds_cad_individu (co_seq_cds_cad_individual) VALUES (1);"          # truncado
        ]
        
        for i, cmd in enumerate(test_commands, 1):
            print(f"\n   Teste {i}: {cmd}")
            try:
                cur.execute(cmd)
                conn.rollback()  # Desfaz a inserção
                print(f"   ✅ Comando executado com sucesso")
            except Exception as e:
                print(f"   ❌ ERRO: {e}")
                conn.rollback()
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"   ❌ Erro ao conectar ao banco: {e}")

def check_running_processes():
    """Verifica processos em execução que podem estar causando o problema"""
    print("\n🔍 Verificando processos em execução...")
    
    try:
        # Lista processos Python
        result = subprocess.run(
            ["powershell", "-Command", "Get-Process | Where-Object {$_.ProcessName -like '*python*'} | Select-Object Id, ProcessName, CommandLine"],
            capture_output=True,
            text=True
        )
        
        if result.stdout.strip():
            print("   📋 Processos Python encontrados:")
            print(result.stdout)
        else:
            print("   ℹ️  Nenhum processo Python em execução")
        
        # Lista processos Node.js
        result = subprocess.run(
            ["powershell", "-Command", "Get-Process | Where-Object {$_.ProcessName -like '*node*'} | Select-Object Id, ProcessName, CommandLine"],
            capture_output=True,
            text=True
        )
        
        if result.stdout.strip():
            print("   📋 Processos Node.js encontrados:")
            print(result.stdout)
        else:
            print("   ℹ️  Nenhum processo Node.js em execução")
            
    except Exception as e:
        print(f"   ❌ Erro ao verificar processos: {e}")

def check_network_connections():
    """Verifica conexões de rede ativas para o PostgreSQL"""
    print("\n🔍 Verificando conexões de rede para PostgreSQL...")
    
    try:
        # Lista conexões na porta do PostgreSQL
        result = subprocess.run(
            ["powershell", "-Command", "Get-NetTCPConnection | Where-Object {$_.LocalPort -eq 5433 -or $_.RemotePort -eq 5433} | Select-Object LocalAddress, LocalPort, RemoteAddress, RemotePort, State"],
            capture_output=True,
            text=True
        )
        
        if result.stdout.strip():
            print("   📋 Conexões PostgreSQL ativas:")
            print(result.stdout)
        else:
            print("   ℹ️  Nenhuma conexão PostgreSQL ativa na porta 5433")
            
    except Exception as e:
        print(f"   ❌ Erro ao verificar conexões: {e}")

def main():
    print("🔍 MONITOR DE TRUNCAGEM SQL")
    print("=" * 50)
    print("Este script irá monitorar em tempo real onde o comando SQL está sendo truncado.")
    print("Pressione Ctrl+C para parar o monitoramento.\n")
    
    # Executa verificações iniciais
    check_running_processes()
    check_network_connections()
    test_direct_sql_execution()
    
    print("\n" + "=" * 50)
    print("🚀 INICIANDO MONITORAMENTO EM TEMPO REAL")
    print("=" * 50)
    
    # Inicia threads de monitoramento
    threads = []
    
    # Thread para monitorar logs do PostgreSQL
    pg_thread = threading.Thread(target=monitor_postgresql_logs, daemon=True)
    pg_thread.start()
    threads.append(pg_thread)
    
    # Thread para monitorar logs da aplicação
    app_thread = threading.Thread(target=monitor_application_logs, daemon=True)
    app_thread.start()
    threads.append(app_thread)
    
    try:
        print("\n⏳ Monitoramento ativo. Execute operações que possam gerar o erro SQL...")
        print("💡 Sugestão: Execute o migrator.py ou acesse a aplicação web")
        print("🛑 Pressione Ctrl+C para parar\n")
        
        # Mantém o script rodando
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Monitoramento interrompido pelo usuário")
        print("📊 Resumo: Se nenhum comando truncado foi detectado, o problema pode estar:")
        print("   1. Em um buffer de rede ou driver de banco")
        print("   2. Em uma configuração específica do PostgreSQL")
        print("   3. Em um middleware ou proxy entre a aplicação e o banco")
        print("   4. Em uma limitação de caracteres em alguma biblioteca")

if __name__ == "__main__":
    main()