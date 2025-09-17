#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def test_sexo_mapping():
    """Testa se o mapeamento de sexo está correto"""
    try:
        # Conectar ao banco
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', '127.0.0.1'),
            database=os.getenv('POSTGRES_DB', 'esus'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('POSTGRES_PORT', 5433)
        )
        cursor = conn.cursor()
        
        print("=== TESTE DE MAPEAMENTO DE SEXO ===\n")
        
        # 1. Verificar valores únicos na tb_sexo
        print("1. Valores válidos na tb_sexo:")
        cursor.execute("SELECT co_sexo, no_sexo, sg_sexo FROM tb_sexo ORDER BY co_sexo")
        sexo_values = cursor.fetchall()
        for co_sexo, no_sexo, sg_sexo in sexo_values:
            print(f"   ID {co_sexo}: {no_sexo} ({sg_sexo})")
        
        # 2. Verificar se existem valores 'N/I' nas tabelas migradas
        print("\n2. Verificando valores 'N/I' nas tabelas:")
        
        # Verificar tb_cidadao
        cursor.execute("SELECT COUNT(*) FROM tb_cidadao WHERE no_sexo = 'N/I'")
        count_ni_cidadao = cursor.fetchone()[0]
        print(f"   tb_cidadao com 'N/I': {count_ni_cidadao}")
        
        # Verificar tb_fat_cidadao_pec (usa co_dim_sexo, não no_sexo)
        cursor.execute("SELECT COUNT(*) FROM tb_fat_cidadao_pec WHERE co_dim_sexo NOT IN (SELECT co_sexo FROM tb_sexo)")
        count_invalid_fat = cursor.fetchone()[0]
        print(f"   tb_fat_cidadao_pec com co_dim_sexo inválido: {count_invalid_fat}")
        
        # 3. Verificar distribuição de valores de sexo
        print("\n3. Distribuição de valores de sexo:")
        
        # tb_cidadao
        cursor.execute("SELECT no_sexo, COUNT(*) FROM tb_cidadao GROUP BY no_sexo ORDER BY COUNT(*) DESC")
        cidadao_dist = cursor.fetchall()
        print("   tb_cidadao:")
        for sexo, count in cidadao_dist:
            print(f"     {sexo}: {count}")
        
        # tb_fat_cidadao_pec
        cursor.execute("SELECT co_dim_sexo, COUNT(*) FROM tb_fat_cidadao_pec GROUP BY co_dim_sexo ORDER BY COUNT(*) DESC")
        fat_dist = cursor.fetchall()
        print("   tb_fat_cidadao_pec:")
        for sexo, count in fat_dist:
            print(f"     {sexo}: {count}")
        
        # 4. Verificar co_dim_sexo inválidos
        print("\n4. Verificando co_dim_sexo inválidos:")
        cursor.execute("""
            SELECT DISTINCT co_dim_sexo 
            FROM tb_fat_cidadao_pec 
            WHERE co_dim_sexo NOT IN (SELECT co_sexo FROM tb_sexo)
        """)
        invalid_codes = cursor.fetchall()
        if invalid_codes:
            print("   Códigos inválidos encontrados:")
            for code in invalid_codes:
                print(f"     {code[0]}")
        else:
            print("   ✓ Nenhum código inválido encontrado")
        
        # 5. Teste de inserção simulada
        print("\n5. Teste de valores que seriam mapeados:")
        test_values = [
            {'sexo': 'Masculino', 'expected_co': 0, 'expected_no': 'MASCULINO'},
            {'sexo': 'Feminino', 'expected_co': 1, 'expected_no': 'FEMININO'},
            {'sexo': 'M', 'expected_co': 0, 'expected_no': 'MASCULINO'},
            {'sexo': 'F', 'expected_co': 1, 'expected_no': 'FEMININO'},
            {'sexo': 'Inválido', 'expected_co': 0, 'expected_no': 'MASCULINO'},
            {'sexo': None, 'expected_co': 0, 'expected_no': 'MASCULINO'}
        ]
        
        for test in test_values:
            sexo_map = {'Masculino': 0, 'Feminino': 1, 'M': 0, 'F': 1}
            co_sexo = sexo_map.get(test['sexo'], 0)
            
            sexo_map_text = {'Masculino': 'MASCULINO', 'Feminino': 'FEMININO', 'M': 'MASCULINO', 'F': 'FEMININO'}
            no_sexo = sexo_map_text.get(test['sexo'], 'MASCULINO')
            
            status = "✓" if (co_sexo == test['expected_co'] and no_sexo == test['expected_no']) else "✗"
            print(f"   {status} '{test['sexo']}' -> co_sexo: {co_sexo}, no_sexo: '{no_sexo}'")
        
        print("\n=== RESULTADO DO TESTE ===")
        if count_ni_cidadao == 0 and count_invalid_fat == 0 and not invalid_codes:
            print("✓ SUCESSO: Mapeamento de sexo corrigido com sucesso!")
            print("  - Nenhum valor 'N/I' encontrado")
            print("  - Todos os códigos de sexo são válidos")
        else:
            print("✗ ATENÇÃO: Ainda existem problemas:")
            if count_ni_cidadao > 0:
                print(f"  - {count_ni_cidadao} valores 'N/I' na tb_cidadao")
            if count_invalid_fat > 0:
                print(f"  - {count_invalid_fat} códigos inválidos na tb_fat_cidadao_pec")
            if invalid_codes:
                print(f"  - Códigos inválidos encontrados: {invalid_codes}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro durante o teste: {e}")

if __name__ == "__main__":
    test_sexo_mapping()