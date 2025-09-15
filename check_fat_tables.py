#!/usr/bin/env python3
"""
Script para verificar tabelas FAT no banco de dados
"""

import psycopg2
import os
from dotenv import load_dotenv

def main():
    # Carregar variáveis de ambiente
    load_dotenv()
    
    try:
        # Conectar ao banco
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        
        cur = conn.cursor()
        
        # Buscar todas as tabelas
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;")
        tables = cur.fetchall()
        
        print('🗄️  TABELAS DISPONÍVEIS NO BANCO:')
        print('=' * 50)
        for table in tables:
            print(f'  - {table[0]}')
        
        print('\n🎯 TABELAS FAT (fat_ ou tb_fat):')
        print('=' * 50)
        fat_tables = [t[0] for t in tables if t[0].startswith('fat_') or t[0].startswith('tb_fat')]
        
        if fat_tables:
            for table in fat_tables:
                # Contar registros em cada tabela
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table};")
                    count = cur.fetchone()[0]
                    print(f'  ✅ {table} - {count} registros')
                except Exception as e:
                    print(f'  ❌ {table} - Erro: {e}')
        else:
            print('  ❌ Nenhuma tabela FAT encontrada!')
        
        print('\n🔍 TABELAS QUE CONTÊM "CDS" OU "INDIVIDUAL":')
        print('=' * 50)
        cds_tables = [t[0] for t in tables if 'cds' in t[0].lower() or 'individual' in t[0].lower()]
        
        for table in cds_tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                count = cur.fetchone()[0]
                print(f'  ✅ {table} - {count} registros')
            except Exception as e:
                print(f'  ❌ {table} - Erro: {e}')
        
        cur.close()
        conn.close()
        
        print('\n✅ Verificação concluída!')
        
    except Exception as e:
        print(f'❌ Erro ao conectar ao banco: {e}')

if __name__ == '__main__':
    main()