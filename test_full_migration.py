import psycopg2
from dotenv import load_dotenv
import os
import csv

load_dotenv()

def get_unidade_saude_by_ine(conn, ine_equipe):
    """
    Busca o código da unidade de saúde pelo INE da equipe
    """
    if not conn or not ine_equipe:
        return None
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT co_unidade_saude 
            FROM public.tb_equipe 
            WHERE nu_ine = %s
        """, (ine_equipe,))
        
        result = cur.fetchone()
        if result:
            return result[0]
        else:
            print(f"Aviso: INE {ine_equipe} não encontrado na tabela tb_equipe")
            return None
    except Exception as e:
        print(f"Erro ao buscar unidade de saúde por INE {ine_equipe}: {e}")
        return None

# Conectar ao banco
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    database=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    port=os.getenv('POSTGRES_PORT')
)

# Testar com diferentes INEs
test_cases = [
    {'nome': 'Caponga', 'ine': '0000083275'},
    {'nome': 'Águas Belas', 'ine': '0001609114'}
]

print('=== TESTANDO DIFERENTES INEs ===')

for case in test_cases:
    print(f'\n--- Testando {case["nome"]} ---')
    ine_equipe = case['ine']
    
    # Buscar co_unidade_saude pelo INE equipe
    co_unidade_saude_from_ine = get_unidade_saude_by_ine(conn, ine_equipe)
    
    if co_unidade_saude_from_ine:
        print(f'INE {ine_equipe} -> co_unidade_saude: {co_unidade_saude_from_ine}')
        
        # Verificar qual unidade de saúde corresponde
        cur = conn.cursor()
        cur.execute('SELECT co_seq_unidade_saude, no_unidade_saude, nu_cnes FROM public.tb_unidade_saude WHERE co_seq_unidade_saude = %s', (co_unidade_saude_from_ine,))
        unidade = cur.fetchone()
        if unidade:
            print(f'Unidade: {unidade[1]} (CNES: {unidade[2]})')
    else:
        print(f'INE {ine_equipe} não encontrado')

# Verificar se o INE de Águas Belas existe na tabela tb_equipe
print('\n=== VERIFICANDO SE INE ÁGUAS BELAS EXISTE ===')
cur = conn.cursor()
cur.execute('SELECT nu_ine, co_unidade_saude, no_equipe FROM public.tb_equipe WHERE nu_ine LIKE %s', ('%1609114%',))
results = cur.fetchall()
if results:
    for result in results:
        print(f'Encontrado: INE={result[0]}, co_unidade_saude={result[1]}, equipe={result[2]}')
else:
    print('INE de Águas Belas não encontrado na tabela tb_equipe')

conn.close()