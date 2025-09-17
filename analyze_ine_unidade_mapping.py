#!/usr/bin/env python3
"""
Script para analisar o mapeamento entre INE, tb_equipe e tb_unidade_saude
"""

import psycopg2
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def connect_db():
    """Conecta ao banco PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5433'),
            database=os.getenv('POSTGRES_DB', 'cascavel_esus'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres')
        )
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        return None

def analyze_table_structures():
    """Analisa as estruturas das tabelas tb_equipe e tb_unidade_saude"""
    conn = connect_db()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        print("🔍 ANÁLISE DAS ESTRUTURAS DAS TABELAS")
        print("=" * 60)
        
        # Estrutura da tb_equipe
        print("\n📋 ESTRUTURA DA TB_EQUIPE:")
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'tb_equipe' 
            ORDER BY ordinal_position;
        """)
        
        equipe_columns = cur.fetchall()
        for col in equipe_columns:
            print(f"   {col[0]} ({col[1]}) - Nullable: {col[2]} - Default: {col[3]}")
        
        # Estrutura da tb_unidade_saude
        print("\n🏥 ESTRUTURA DA TB_UNIDADE_SAUDE:")
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'tb_unidade_saude' 
            ORDER BY ordinal_position;
        """)
        
        unidade_columns = cur.fetchall()
        for col in unidade_columns:
            print(f"   {col[0]} ({col[1]}) - Nullable: {col[2]} - Default: {col[3]}")
        
        # Verificar dados de exemplo
        print("\n📊 DADOS DE EXEMPLO DA TB_EQUIPE:")
        cur.execute("""
            SELECT co_seq_equipe, nu_ine, no_equipe, co_unidade_saude 
            FROM tb_equipe 
            LIMIT 10;
        """)
        
        equipe_data = cur.fetchall()
        for row in equipe_data:
            print(f"   ID: {row[0]}, INE: {row[1]}, Nome: {row[2]}, Unidade: {row[3]}")
        
        print("\n🏥 DADOS DE EXEMPLO DA TB_UNIDADE_SAUDE:")
        # Primeiro vamos ver quais colunas existem realmente
        cur.execute("""
            SELECT * FROM tb_unidade_saude LIMIT 1;
        """)
        sample_row = cur.fetchone()
        
        # Pegar nomes das colunas
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tb_unidade_saude' 
            ORDER BY ordinal_position;
        """)
        column_names = [row[0] for row in cur.fetchall()]
        
        print(f"   Colunas disponíveis: {column_names}")
        
        # Agora fazer query com colunas que existem
        cur.execute("""
            SELECT co_seq_unidade_saude, no_unidade_saude, nu_cnes 
            FROM tb_unidade_saude 
            LIMIT 10;
        """)
        
        unidade_data = cur.fetchall()
        for row in unidade_data:
            print(f"   ID: {row[0]}, Nome: {row[1]}, CNES: {row[2]}")
        
        # Verificar INEs problemáticos dos logs
        print("\n⚠️  VERIFICANDO INEs PROBLEMÁTICOS DOS LOGS:")
        problematic_ines = ['83364', '1913433', '0000083364', '0001913433']
        
        for ine in problematic_ines:
            cur.execute("SELECT nu_ine, co_unidade_saude, no_equipe FROM tb_equipe WHERE nu_ine = %s;", (ine,))
            result = cur.fetchone()
            if result:
                print(f"   ✅ INE {ine} encontrado: Unidade={result[1]}, Equipe={result[2]}")
            else:
                print(f"   ❌ INE {ine} NÃO encontrado na tb_equipe")
        
        # Verificar se há INEs que começam com esses números
        print("\n🔍 VERIFICANDO INEs SIMILARES:")
        for ine in ['83364', '1913433']:
            cur.execute("SELECT nu_ine, co_unidade_saude, no_equipe FROM tb_equipe WHERE nu_ine LIKE %s;", (f'%{ine}%',))
            results = cur.fetchall()
            if results:
                print(f"   INEs contendo '{ine}':")
                for result in results[:3]:  # Mostrar apenas os primeiros 3
                    print(f"     {result[0]} -> Unidade: {result[1]}, Equipe: {result[2]}")
            else:
                print(f"   ❌ Nenhum INE contendo '{ine}' encontrado")
        
        # Verificar quantos INEs únicos existem
        print("\n📈 ESTATÍSTICAS:")
        cur.execute("SELECT COUNT(*) FROM tb_equipe;")
        total_equipes = cur.fetchone()[0]
        print(f"   Total de equipes: {total_equipes}")
        
        cur.execute("SELECT COUNT(DISTINCT nu_ine) FROM tb_equipe;")
        ines_unicos = cur.fetchone()[0]
        print(f"   INEs únicos: {ines_unicos}")
        
        cur.execute("SELECT COUNT(*) FROM tb_unidade_saude;")
        total_unidades = cur.fetchone()[0]
        print(f"   Total de unidades de saúde: {total_unidades}")
        
        # Verificar relacionamento entre tb_equipe e tb_unidade_saude
        print("\n🔗 VERIFICANDO RELACIONAMENTO:")
        cur.execute("""
            SELECT DISTINCT e.co_unidade_saude, u.no_unidade_saude, u.nu_cnes
            FROM tb_equipe e
            LEFT JOIN tb_unidade_saude u ON e.co_unidade_saude = u.co_seq_unidade_saude
            ORDER BY e.co_unidade_saude
            LIMIT 10;
        """)
        
        relacionamentos = cur.fetchall()
        for rel in relacionamentos:
            print(f"   Unidade ID: {rel[0]} -> Nome: {rel[1]}, CNES: {rel[2]}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro na análise: {e}")
        if conn:
            conn.close()

def analyze_csv_ines():
    """Analisa os INEs presentes nos arquivos CSV"""
    import csv
    import glob
    
    print("\n🔍 ANÁLISE DOS INEs NOS ARQUIVOS CSV")
    print("=" * 60)
    
    csv_files = glob.glob("datacsv/*.csv")
    
    for csv_file in csv_files:
        print(f"\n📄 Arquivo: {csv_file}")
        try:
            # Tentar diferentes encodings
            encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
            
            for encoding in encodings:
                try:
                    with open(csv_file, 'r', encoding=encoding) as f:
                        # Tentar detectar o delimitador
                        sample = f.read(1024)
                        f.seek(0)
                        
                        sniffer = csv.Sniffer()
                        delimiter = sniffer.sniff(sample).delimiter
                        
                        reader = csv.DictReader(f, delimiter=delimiter)
                        
                        ines_found = set()
                        row_count = 0
                        
                        for row in reader:
                            row_count += 1
                            if 'INE equipe' in row and row['INE equipe']:
                                ine = str(row['INE equipe']).strip().strip('"')
                                if ine:
                                    ines_found.add(ine)
                            
                            # Limitar para não processar arquivos muito grandes
                            if row_count > 100:
                                break
                        
                        print(f"   Encoding usado: {encoding}")
                        print(f"   Registros analisados: {row_count}")
                        print(f"   INEs únicos encontrados: {len(ines_found)}")
                        if ines_found:
                            print(f"   Exemplos de INEs: {list(ines_found)[:5]}")
                        break  # Se conseguiu ler, sair do loop de encodings
                        
                except UnicodeDecodeError:
                    continue  # Tentar próximo encoding
                    
        except Exception as e:
            print(f"   ❌ Erro ao processar {csv_file}: {e}")

if __name__ == "__main__":
    print("🚀 INICIANDO ANÁLISE DO MAPEAMENTO INE -> UNIDADE DE SAÚDE")
    print("=" * 70)
    
    analyze_table_structures()
    analyze_csv_ines()
    
    print("\n✅ ANÁLISE CONCLUÍDA")