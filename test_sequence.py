#!/usr/bin/env python3
"""
Script para testar se as sequências estão funcionando corretamente
"""

import psycopg2
import os
from dotenv import load_dotenv

def test_sequences():
    # Carregar variáveis de ambiente
    load_dotenv()
    
    try:
        # Conectar ao banco
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('POSTGRES_PORT')
        )
        
        cur = conn.cursor()
        
        # Testar seq_tb_cds_cad_individual
        print("🧪 Testando seq_tb_cds_cad_individual...")
        cur.execute("SELECT nextval('seq_tb_cds_cad_individual') as next_value;")
        result = cur.fetchone()
        print(f"✅ Próximo valor: {result[0]}")
        
        # Testar seq_tl_cds_cad_individual
        print("\n🧪 Testando seq_tl_cds_cad_individual...")
        cur.execute("SELECT nextval('seq_tl_cds_cad_individual') as next_value;")
        result = cur.fetchone()
        print(f"✅ Próximo valor: {result[0]}")
        
        # Testar um INSERT simulado (sem executar)
        print("\n🧪 Testando sintaxe de INSERT...")
        test_query = """
        SELECT 
            nextval('seq_tb_cds_cad_individual') as co_seq_cds_cad_individual,
            'Teste' as no_cidadao
        """
        cur.execute(test_query)
        result = cur.fetchone()
        print(f"✅ INSERT simulado funcionou: ID={result[0]}, Nome={result[1]}")
        
        cur.close()
        conn.close()
        
        print("\n🎉 TODAS AS SEQUÊNCIAS ESTÃO FUNCIONANDO CORRETAMENTE!")
        print("✅ O erro 'relation seq_tb_cds_cad_individual does not exist' foi resolvido!")
        
    except Exception as e:
        print(f"❌ Erro ao testar sequências: {e}")
        return False
    
    return True

if __name__ == "__main__":
    test_sequences()