#!/usr/bin/env python3
# test_db_connection.py
# Script para testar conexão com PostgreSQL usando credenciais do .env

import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from datetime import datetime

def load_env_config(env_file=".env"):
    """Carrega configurações do arquivo .env"""
    if not os.path.exists(env_file):
        print(f"❌ Erro: Arquivo {env_file} não encontrado.")
        return False
    
    load_dotenv(env_file)
    print(f"✅ Arquivo {env_file} carregado com sucesso.")
    return True

def get_db_config():
    """Extrai configurações do banco de dados das variáveis de ambiente"""
    config = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432')
    }
    
    # Verificar se todas as configurações necessárias estão presentes
    missing_configs = [key for key, value in config.items() if not value]
    if missing_configs:
        print(f"❌ Erro: Configurações ausentes no .env: {', '.join(missing_configs)}")
        return None
    
    return config

def test_connection(config):
    """Testa a conexão com o banco PostgreSQL"""
    print("\n🔄 Iniciando teste de conexão...")
    print(f"📍 Host: {config['host']}:{config['port']}")
    print(f"🗄️  Database: {config['dbname']}")
    print(f"👤 User: {config['user']}")
    
    try:
        # Tentativa de conexão
        print("\n⏳ Conectando ao PostgreSQL...")
        conn = psycopg2.connect(**config)
        
        # Teste básico com cursor
        cursor = conn.cursor()
        
        # Verificar versão do PostgreSQL
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ Conexão estabelecida com sucesso!")
        print(f"📊 Versão do PostgreSQL: {version.split(',')[0]}")
        
        # Verificar se a tabela destino existe
        table_name = os.getenv('TABLE_NAME', 'public.tl_cds_cad_individual')
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            );
        """, (table_name.split('.')[0], table_name.split('.')[1]))
        
        table_exists = cursor.fetchone()[0]
        if table_exists:
            print(f"✅ Tabela {table_name} encontrada.")
            
            # Contar registros na tabela
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            print(f"📈 Registros na tabela: {count:,}")
        else:
            print(f"⚠️  Tabela {table_name} não encontrada.")
        
        # Testar permissões básicas
        cursor.execute("SELECT current_user, session_user;")
        current_user, session_user = cursor.fetchone()
        print(f"👤 Usuário atual: {current_user}")
        print(f"🔐 Usuário da sessão: {session_user}")
        
        cursor.close()
        conn.close()
        
        print("\n🎉 Teste de conexão concluído com SUCESSO!")
        return True
        
    except psycopg2.OperationalError as e:
        print(f"\n❌ Erro de conexão: {e}")
        print("\n🔍 Possíveis causas:")
        print("   • PostgreSQL não está rodando")
        print("   • Host ou porta incorretos")
        print("   • Credenciais inválidas")
        print("   • Firewall bloqueando a conexão")
        return False
        
    except psycopg2.Error as e:
        print(f"\n❌ Erro do PostgreSQL: {e}")
        return False
        
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
        return False

def main():
    """Função principal"""
    print("🔧 TESTE DE CONEXÃO POSTGRESQL")
    print("=" * 40)
    print(f"⏰ Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Carregar configurações do .env
    if not load_env_config():
        sys.exit(1)
    
    # Obter configurações do banco
    config = get_db_config()
    if not config:
        sys.exit(1)
    
    # Executar teste de conexão
    success = test_connection(config)
    
    print("\n" + "=" * 40)
    if success:
        print("🎯 RESULTADO: CONEXÃO VÁLIDA ✅")
        sys.exit(0)
    else:
        print("🎯 RESULTADO: FALHA NA CONEXÃO ❌")
        sys.exit(1)

if __name__ == "__main__":
    main()