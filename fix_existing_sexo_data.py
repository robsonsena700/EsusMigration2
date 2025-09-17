#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def fix_existing_sexo_data():
    """Corrige os dados existentes com 'N/I' na tb_cidadao"""
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
        
        print("=== CORREÇÃO DE DADOS EXISTENTES ===\n")
        
        # 1. Verificar situação atual
        cursor.execute("SELECT COUNT(*) FROM tb_cidadao WHERE no_sexo = 'N/I'")
        count_before = cursor.fetchone()[0]
        print(f"1. Registros com 'N/I' antes da correção: {count_before}")
        
        # 2. Verificar distribuição de valores de sexo
        print("\n2. Distribuição de valores de sexo:")
        cursor.execute("""
            SELECT DISTINCT no_sexo, COUNT(*)
            FROM tb_cidadao 
            GROUP BY no_sexo
            ORDER BY no_sexo
        """)
        sexo_dist = cursor.fetchall()
        for sexo, count in sexo_dist:
            print(f"   {sexo}: {count} registros")
        
        # 3. Atualizar registros com 'N/I' para usar o valor correto
        print("\n3. Atualizando registros com 'N/I'...")
        cursor.execute("""
            UPDATE tb_cidadao 
            SET no_sexo = 'NÃO INFORMADO'
            WHERE no_sexo = 'N/I'
        """)
        updated_count = cursor.rowcount
        print(f"   ✓ {updated_count} registros atualizados para 'NÃO INFORMADO'")
        
        # Commit das alterações
        conn.commit()
        
        # 4. Verificar situação após correção
        cursor.execute("SELECT COUNT(*) FROM tb_cidadao WHERE no_sexo = 'N/I'")
        count_after = cursor.fetchone()[0]
        print(f"\n4. Registros com 'N/I' após a correção: {count_after}")
        
        # 5. Verificar nova distribuição
        cursor.execute("SELECT no_sexo, COUNT(*) FROM tb_cidadao GROUP BY no_sexo ORDER BY COUNT(*) DESC")
        new_dist = cursor.fetchall()
        print("\n5. Nova distribuição de sexo na tb_cidadao:")
        for sexo, count in new_dist:
            print(f"   '{sexo}': {count}")
        
        # 6. Resultado final
        print(f"\n=== RESULTADO DA CORREÇÃO ===")
        if count_after == 0:
            print("✅ SUCESSO: Todos os valores 'N/I' foram corrigidos!")
            print(f"   Total de registros atualizados: {updated_count}")
        else:
            print(f"⚠️ ATENÇÃO: Ainda restam {count_after} registros com 'N/I'")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro durante a correção: {e}")
        if 'conn' in locals():
            conn.rollback()

if __name__ == "__main__":
    fix_existing_sexo_data()