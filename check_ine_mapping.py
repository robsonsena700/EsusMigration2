import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    database=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    port=os.getenv('POSTGRES_PORT')
)

cur = conn.cursor()

print('=== VERIFICANDO PROBLEMA DO INE 0000083127 ===')

# 1. Verificar o INE na tabela tb_equipe
cur.execute('SELECT nu_ine, co_unidade_saude, no_equipe FROM public.tb_equipe WHERE nu_ine = %s;', ('0000083127',))
result = cur.fetchone()
if result:
    print(f'INE 0000083127 na tb_equipe: co_unidade_saude={result[1]}, equipe={result[2]}')
else:
    print('INE 0000083127 NÃO ENCONTRADO na tabela tb_equipe')

# 2. Verificar qual unidade tem CNES 9017364 (padrão do código)
cur.execute('SELECT co_seq_unidade_saude, no_unidade_saude, nu_cnes FROM public.tb_unidade_saude WHERE nu_cnes = %s;', ('9017364',))
result = cur.fetchone()
if result:
    print(f'Unidade com CNES 9017364: ID={result[0]}, Nome={result[1]}')
else:
    print('Unidade com CNES 9017364 NÃO ENCONTRADA')

# 3. Verificar a unidade Sede
cur.execute('SELECT co_seq_unidade_saude, no_unidade_saude, nu_cnes FROM public.tb_unidade_saude WHERE co_seq_unidade_saude = 2;')
result = cur.fetchone()
if result:
    print(f'Unidade ID=2 (Sede): Nome={result[1]}, CNES={result[2]}')

# 4. Verificar a unidade Águas Belas
cur.execute("SELECT co_seq_unidade_saude, no_unidade_saude, nu_cnes FROM public.tb_unidade_saude WHERE UPPER(no_unidade_saude) LIKE '%AGUAS%';")
result = cur.fetchone()
if result:
    print(f'Unidade Águas Belas: ID={result[0]}, Nome={result[1]}, CNES={result[2]}')

print('\n=== SIMULANDO A LÓGICA DO MIGRATOR ===')
# Simular a lógica do get_esus_ledi_data
cur.execute("SELECT co_seq_unidade_saude, nu_cnes FROM tb_unidade_saude WHERE nu_cnes = '9017364' LIMIT 1")
unidade_result = cur.fetchone()
if unidade_result:
    co_unidade_saude_padrao = unidade_result[0]
    print(f'Valor padrão do migrator (CNES 9017364): co_unidade_saude={co_unidade_saude_padrao}')
else:
    print('CNES 9017364 não encontrado - usando fallback')
    cur.execute("SELECT co_seq_unidade_saude, nu_cnes FROM tb_unidade_saude LIMIT 1")
    fallback_result = cur.fetchone()
    co_unidade_saude_padrao = fallback_result[0] if fallback_result else 2
    print(f'Valor fallback: co_unidade_saude={co_unidade_saude_padrao}')

# Simular get_unidade_saude_by_ine para INE 0000083127
cur.execute("SELECT co_unidade_saude FROM tb_equipe WHERE nu_ine = %s", ('0000083127',))
ine_result = cur.fetchone()
co_unidade_saude_from_ine = ine_result[0] if ine_result else None

print(f'INE 0000083127 retorna: co_unidade_saude={co_unidade_saude_from_ine}')

# Lógica final do migrator
if co_unidade_saude_from_ine:
    final_co_unidade_saude = co_unidade_saude_from_ine
    print(f'RESULTADO FINAL: Usando INE - co_unidade_saude={final_co_unidade_saude}')
else:
    final_co_unidade_saude = co_unidade_saude_padrao
    print(f'RESULTADO FINAL: Usando padrão - co_unidade_saude={final_co_unidade_saude}')

# Verificar qual unidade é essa
cur.execute('SELECT no_unidade_saude FROM public.tb_unidade_saude WHERE co_seq_unidade_saude = %s;', (final_co_unidade_saude,))
nome_result = cur.fetchone()
if nome_result:
    print(f'Unidade final: {nome_result[0]}')

conn.close()