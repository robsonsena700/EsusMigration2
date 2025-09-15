#!/usr/bin/env python3
"""
Script para verificar se as tabelas TL e TB Cadastro Individual existem no banco
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
        
        # Tabelas que queremos verificar
        target_tables = [
            'tl_cds_cad_individual',
            'tb_cds_cad_individual'
        ]
        
        print('🔍 VERIFICANDO TABELAS TL E TB CADASTRO INDIVIDUAL:')
        print('=' * 60)
        
        for table in target_tables:
            try:
                # Verificar se a tabela existe
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    );
                """, (table,))
                
                exists = cur.fetchone()[0]
                
                if exists:
                    # Contar registros
                    cur.execute(f"SELECT COUNT(*) FROM {table};")
                    count = cur.fetchone()[0]
                    
                    # Verificar estrutura (primeiras 5 colunas)
                    cur.execute("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        ORDER BY ordinal_position 
                        LIMIT 5;
                    """, (table,))
                    
                    columns = cur.fetchall()
                    
                    print(f'✅ {table}:')
                    print(f'   📊 Registros: {count}')
                    print(f'   🏗️  Estrutura (primeiras 5 colunas):')
                    for col_name, col_type in columns:
                        print(f'      - {col_name}: {col_type}')
                    print()
                    
                else:
                    print(f'❌ {table}: Tabela não encontrada')
                    print()
                    
            except Exception as e:
                print(f'❌ {table}: Erro ao verificar - {e}')
                print()
        
        # Verificar se existem outras tabelas similares
        print('🔍 OUTRAS TABELAS RELACIONADAS A CADASTRO INDIVIDUAL:')
        print('=' * 60)
        
        cur.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public' 
            AND (tablename LIKE '%cad_individual%' OR tablename LIKE '%cadastro%individual%')
            ORDER BY tablename;
        """)
        
        related_tables = cur.fetchall()
        
        if related_tables:
            for table_row in related_tables:
                table_name = table_row[0]
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                    count = cur.fetchone()[0]
                    print(f'  📋 {table_name}: {count} registros')
                except Exception as e:
                    print(f'  ❌ {table_name}: Erro - {e}')
        else:
            print('  ❌ Nenhuma tabela relacionada encontrada')
        
        cur.close()
        conn.close()
        
        print('\n✅ Verificação concluída!')
        
    except Exception as e:
        print(f'❌ Erro ao conectar ao banco: {e}')

if __name__ == '__main__':
    main()