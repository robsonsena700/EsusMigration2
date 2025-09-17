#!/usr/bin/env python3
"""
Script para verificar se a correção da inversão de tabelas foi bem-sucedida.
Verifica se as sequências estão sendo usadas corretamente e se os dados estão nas tabelas certas.
"""

import psycopg2
from dotenv import load_dotenv
import os

def main():
    # Carrega variáveis de ambiente
    load_dotenv()
    
    # Conecta ao banco
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', '127.0.0.1'),
        database=os.getenv('POSTGRES_DB', 'esus'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD'),
        port=os.getenv('POSTGRES_PORT', '5433')
    )
    
    cur = conn.cursor()
    
    print("=== VERIFICAÇÃO DA CORREÇÃO DA INVERSÃO DE TABELAS ===\n")
    
    # 1. Contagem de registros
    print("1. CONTAGEM DE REGISTROS:")
    cur.execute("SELECT COUNT(*) FROM tb_cds_cad_individual")
    tb_count = cur.fetchone()[0]
    print(f"   tb_cds_cad_individual: {tb_count:,} registros")
    
    cur.execute("SELECT COUNT(*) FROM tl_cds_cad_individual")
    tl_count = cur.fetchone()[0]
    print(f"   tl_cds_cad_individual: {tl_count:,} registros")
    
    # 2. Verificação das sequências atuais
    print("\n2. VALORES ATUAIS DAS SEQUÊNCIAS:")
    cur.execute("SELECT last_value FROM seq_tb_cds_cad_individual")
    seq_tb = cur.fetchone()[0]
    print(f"   seq_tb_cds_cad_individual: {seq_tb:,}")
    
    cur.execute("SELECT last_value FROM seq_tl_cds_cad_individual")
    seq_tl = cur.fetchone()[0]
    print(f"   seq_tl_cds_cad_individual: {seq_tl:,}")
    
    # 3. Verificação dos IDs máximos nas tabelas
    print("\n3. IDs MÁXIMOS NAS TABELAS:")
    cur.execute("SELECT MAX(co_seq_cds_cad_individual) FROM tb_cds_cad_individual")
    max_tb = cur.fetchone()[0]
    print(f"   MAX(co_seq_cds_cad_individual) em tb_cds_cad_individual: {max_tb:,}")
    
    cur.execute("SELECT MAX(co_seq_cds_cad_individual) FROM tl_cds_cad_individual")
    max_tl = cur.fetchone()[0]
    print(f"   MAX(co_seq_cds_cad_individual) em tl_cds_cad_individual: {max_tl:,}")
    
    # 4. Verificação de alguns registros para confirmar estrutura
    print("\n4. AMOSTRA DE REGISTROS (primeiros 3 de cada tabela):")
    
    print("\n   tb_cds_cad_individual:")
    cur.execute("""
        SELECT co_seq_cds_cad_individual, no_cidadao, nu_cpf_cidadao 
        FROM tb_cds_cad_individual 
        ORDER BY co_seq_cds_cad_individual 
        LIMIT 3
    """)
    for row in cur.fetchall():
        print(f"     ID: {row[0]}, Nome: {row[1]}, CPF: {row[2]}")
    
    print("\n   tl_cds_cad_individual:")
    cur.execute("""
        SELECT co_seq_cds_cad_individual, no_cidadao, nu_cpf_cidadao 
        FROM tl_cds_cad_individual 
        ORDER BY co_seq_cds_cad_individual 
        LIMIT 3
    """)
    for row in cur.fetchall():
        print(f"     ID: {row[0]}, Nome: {row[1]}, CPF: {row[2]}")
    
    # 5. Análise da correção
    print("\n5. ANÁLISE DA CORREÇÃO:")
    
    # Verifica se as sequências estão alinhadas com os dados
    tb_sequence_ok = seq_tb >= max_tb if max_tb else True
    tl_sequence_ok = seq_tl >= max_tl if max_tl else True
    
    print(f"   ✓ Sequência tb_cds_cad_individual alinhada: {'SIM' if tb_sequence_ok else 'NÃO'}")
    print(f"   ✓ Sequência tl_cds_cad_individual alinhada: {'SIM' if tl_sequence_ok else 'NÃO'}")
    
    # Verifica se há dados nas duas tabelas
    both_tables_populated = tb_count > 0 and tl_count > 0
    print(f"   ✓ Ambas as tabelas populadas: {'SIM' if both_tables_populated else 'NÃO'}")
    
    # Verifica se a proporção está correta (tb deve ter ~2x mais registros que tl)
    ratio_ok = False
    if tl_count > 0:
        ratio = tb_count / tl_count
        ratio_ok = 1.8 <= ratio <= 2.2  # Margem de tolerância
        print(f"   ✓ Proporção tb/tl (~2:1): {ratio:.2f} {'SIM' if ratio_ok else 'NÃO'}")
    
    # Resultado final
    print(f"\n6. RESULTADO FINAL:")
    if tb_sequence_ok and tl_sequence_ok and both_tables_populated and ratio_ok:
        print("   🎉 CORREÇÃO BEM-SUCEDIDA! As tabelas estão corretas.")
    else:
        print("   ⚠️  ATENÇÃO: Ainda há problemas que precisam ser verificados.")
    
    conn.close()

if __name__ == "__main__":
    main()