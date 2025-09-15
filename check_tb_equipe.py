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

# Verificar estrutura da tabela tb_equipe
print('=== ESTRUTURA DA TABELA tb_equipe ===')
cur.execute("""
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns 
    WHERE table_name = 'tb_equipe' 
    ORDER BY ordinal_position;
""")

for row in cur.fetchall():
    print(f'{row[0]}: {row[1]} (nullable: {row[2]}, default: {row[3]})')

# Verificar alguns dados de exemplo
print('\n=== DADOS DE EXEMPLO tb_equipe ===')
cur.execute('SELECT nu_ine, co_unidade_saude, no_equipe FROM public.tb_equipe LIMIT 5;')
for row in cur.fetchall():
    print(f'INE: {row[0]}, co_unidade_saude: {row[1]}, nome: {row[2]}')

# Verificar quantos registros existem
cur.execute('SELECT COUNT(*) FROM public.tb_equipe;')
total = cur.fetchone()[0]
print(f'\nTotal de equipes: {total}')

# Verificar se existe algum INE específico que pode estar no CSV
print('\n=== VERIFICANDO INEs ÚNICOS ===')
cur.execute('SELECT DISTINCT nu_ine FROM public.tb_equipe ORDER BY nu_ine LIMIT 10;')
for row in cur.fetchall():
    print(f'INE: {row[0]}')

conn.close()