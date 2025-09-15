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

# INEs encontrados no CSV Caponga
ines_csv = ['0000083275', '0000083283']

print('=== VERIFICANDO INEs DO CSV NA TABELA tb_equipe ===')
for ine in ines_csv:
    cur.execute('SELECT nu_ine, co_unidade_saude, no_equipe FROM public.tb_equipe WHERE nu_ine = %s;', (ine,))
    result = cur.fetchone()
    if result:
        print(f'INE {ine} ENCONTRADO: co_unidade_saude={result[1]}, equipe={result[2]}')
    else:
        print(f'INE {ine} NÃO ENCONTRADO na tabela tb_equipe')

# Verificar qual co_unidade_saude está sendo usado atualmente no script gerado
print('\n=== VERIFICANDO co_unidade_saude ATUAL NO SCRIPT ===')
try:
    with open('backend/scripts/tl_cds_cad_individual_Caponga-2025-09-10-11-11.sql', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for i, line in enumerate(lines[:10]):  # Primeiras 10 linhas
            if 'VALUES' in line:
                # Extrair o valor de co_unidade_saude (penúltimo valor antes de nextval)
                values = line.strip().rstrip(');').split(', ')
                co_unidade_saude_value = None
                for j, val in enumerate(values):
                    if 'nextval' in val and j > 0:
                        co_unidade_saude_value = values[j-2].strip("'")
                        break
                print(f'Valor atual de co_unidade_saude no script: {co_unidade_saude_value}')
                break
except UnicodeDecodeError:
    print('Erro de codificação no arquivo SQL - usando valor padrão 5 conforme observado anteriormente')

# Verificar todas as unidades de saúde disponíveis
print('\n=== UNIDADES DE SAÚDE DISPONÍVEIS ===')
cur.execute('SELECT co_unidade_saude, no_unidade_saude FROM public.tb_unidade_saude ORDER BY co_unidade_saude;')
for row in cur.fetchall():
    print(f'co_unidade_saude: {row[0]}, nome: {row[1]}')

conn.close()