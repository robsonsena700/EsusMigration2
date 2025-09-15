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

# Verificar estrutura da tabela tb_unidade_saude
print('=== ESTRUTURA DA TABELA tb_unidade_saude ===')
cur.execute("""
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns 
    WHERE table_name = 'tb_unidade_saude' 
    ORDER BY ordinal_position;
""")

for row in cur.fetchall():
    print(f'{row[0]}: {row[1]} (nullable: {row[2]}, default: {row[3]})')

# Verificar alguns dados de exemplo
print('\n=== DADOS DE EXEMPLO tb_unidade_saude ===')
cur.execute('SELECT * FROM public.tb_unidade_saude LIMIT 5;')
columns = [desc[0] for desc in cur.description]
print(f'Colunas: {columns}')

for row in cur.fetchall():
    print(f'Dados: {row}')

conn.close()