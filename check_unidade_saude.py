import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    
    cur = conn.cursor()
    
    # Verificar estrutura da tabela TB_EQUIPE
    print('Estrutura da tabela TB_EQUIPE:')
    cur.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = 'tb_equipe' 
        AND table_schema = 'public' 
        ORDER BY ordinal_position
    """)
    
    cols = cur.fetchall()
    for col in cols:
        print(f'{col[0]} - {col[1]} - Nullable: {col[2]} - Default: {col[3]}')
    
    # Buscar registros de exemplo na TB_EQUIPE
    print('\nExemplos de registros na TB_EQUIPE:')
    cur.execute("""
        SELECT co_seq_equipe, nu_ine, no_equipe, co_unidade_saude 
        FROM tb_equipe 
        LIMIT 5
    """)
    
    equipes = cur.fetchall()
    for equipe in equipes:
        print(f'ID: {equipe[0]}, INE: {equipe[1]}, Nome: {equipe[2]}, Unidade: {equipe[3]}')
    
    # Verificar se existe algum registro com INE = '0001609114'
    print('\nBuscando equipe com INE 0001609114:')
    cur.execute("""
        SELECT co_seq_equipe, nu_ine, no_equipe, co_unidade_saude 
        FROM tb_equipe 
        WHERE nu_ine = '0001609114'
    """)
    
    equipe_especifica = cur.fetchall()
    if equipe_especifica:
        for equipe in equipe_especifica:
            print(f'Encontrada - ID: {equipe[0]}, INE: {equipe[1]}, Nome: {equipe[2]}, Unidade: {equipe[3]}')
    else:
        print('Equipe com INE 0001609114 não encontrada')
        
        # Buscar INEs similares
        cur.execute("""
            SELECT co_seq_equipe, nu_ine, no_equipe, co_unidade_saude 
            FROM tb_equipe 
            WHERE nu_ine LIKE '%0001609114%' OR nu_ine LIKE '%609114%'
        """)
        
        similares = cur.fetchall()
        if similares:
            print('INEs similares encontrados:')
            for equipe in similares:
                print(f'ID: {equipe[0]}, INE: {equipe[1]}, Nome: {equipe[2]}, Unidade: {equipe[3]}')
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f'Erro: {e}')