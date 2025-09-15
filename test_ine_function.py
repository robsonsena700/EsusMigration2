import psycopg2
from dotenv import load_dotenv
import os

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

# Testar com os INEs do CSV Caponga
ines_teste = ['0000083275', '0000083283']

print('=== TESTANDO FUNÇÃO get_unidade_saude_by_ine ===')
for ine in ines_teste:
    resultado = get_unidade_saude_by_ine(conn, ine)
    print(f'INE: {ine} -> co_unidade_saude: {resultado}')

# Verificar se o resultado está correto comparando com consulta direta
print('\n=== VERIFICAÇÃO DIRETA ===')
cur = conn.cursor()
for ine in ines_teste:
    cur.execute('SELECT co_unidade_saude FROM public.tb_equipe WHERE nu_ine = %s', (ine,))
    result = cur.fetchone()
    print(f'INE: {ine} -> co_unidade_saude (direto): {result[0] if result else None}')

conn.close()