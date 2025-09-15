#!/usr/bin/env python3
"""
Script para buscar estatísticas das tabelas CDS/FAT
"""

import json
import sys
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

def get_fat_stats():
    """Busca estatísticas das tabelas CDS/FAT"""
    
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
        
        # Buscar todas as tabelas CDS disponíveis dinamicamente
        tables_query = """
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public' 
            AND (tablename LIKE 'tb_cds_%' OR tablename LIKE 'tl_cds_%')
            ORDER BY tablename
        """
        
        cur.execute(tables_query)
        tables = [row[0] for row in cur.fetchall()]
        
        stats = {}
        total_records = 0
        
        # Buscar estatísticas para cada tabela
        for table in tables:
            try:
                cur.execute(f"SELECT COUNT(*) as total FROM {table}")
                count = cur.fetchone()[0]
                stats[table] = {
                    'total_records': int(count)
                }
                total_records += int(count)
            except Exception as table_error:
                stats[table] = {
                    'total_records': 0,
                    'error': str(table_error)
                }
        
        # Preparar resposta
        response = {
            'success': True,
            'tables': stats,
            'summary': {
                'total_tables': len(tables),
                'total_records': total_records,
                'timestamp': datetime.now().isoformat()
            }
        }
        
        cur.close()
        conn.close()
        
        return response
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

def main():
    """Função principal"""
    try:
        result = get_fat_stats()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        sys.exit(0 if result['success'] else 1)
    except Exception as e:
        error_result = {
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }
        print(json.dumps(error_result, ensure_ascii=False, indent=2))
        sys.exit(1)

if __name__ == '__main__':
    main()