#!/usr/bin/env python3
# migrator.py
import os
import csv
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from datetime import datetime
import json
import argparse
import sys

# ======================
# Configurações principais (podem vir do .env)
# ======================
DEFAULT_BASE_DIR = r"D:\Robson\Projetos\Cascavel"
ENV_FILE_DEFAULT = os.path.join(DEFAULT_BASE_DIR, ".env")

def emit_event(obj):
    """Imprime evento JSON na stdout (uma linha). O Node vai ler e parsear."""
    sys.stdout.write("EVENT:" + json.dumps(obj, ensure_ascii=False) + "\n")
    sys.stdout.flush()

def load_env(env_file):
    if os.path.exists(env_file):
        load_dotenv(env_file)
    else:
        raise FileNotFoundError(f"Arquivo de configuração {env_file} não encontrado.")

def get_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
            port=os.getenv("POSTGRES_PORT", "5432"),
        )
        return conn
    except Exception as e:
        raise ConnectionError(f"Erro ao conectar no PostgreSQL: {e}")

def get_table_columns(conn, table_name):
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
    """
    if "." not in table_name:
        raise ValueError("TABLE_NAME deve estar no formato schema.table")
    schema, table = table_name.split(".", 1)
    cur = conn.cursor()
    cur.execute(query, (schema, table))
    cols = [row[0] for row in cur.fetchall()]
    cur.close()
    return cols

def detect_encoding(file_path):
    """Detecta a codificação do arquivo CSV"""
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read(1024)  # Tenta ler os primeiros 1024 bytes
            return encoding
        except UnicodeDecodeError:
            continue
    
    # Se nenhuma codificação funcionar, usa utf-8 com ignore
    return 'utf-8'

def map_csv_columns_to_db(csv_columns):
    """Mapeia colunas do CSV para colunas da tabela do banco"""
    column_mapping = {
        'Nome': 'no_cidadao',
        'CPF/CNS': 'nu_cpf_cidadao',
        'Data de nascimento': 'dt_nascimento',
        'Sexo': 'co_sexo',
        'Telefone celular': 'nu_celular_cidadao',
        'Endereço': 'ds_endereco',
        'Nome equipe': 'no_equipe',
        'INE equipe': 'ine_equipe',
        'Microárea': 'nu_micro_area',
        'Idade': 'idade_calculada',
        'Identidade de gênero': 'identidade_genero',
        'Telefone residencial': 'telefone_residencial',
        'Telefone de contato': 'telefone_contato',
        'Última atualização cadastral': 'dt_ultima_atualizacao',
        'Origem': 'origem_cadastro'
    }
    
    mapped_columns = {}
    for csv_col in csv_columns:
        if csv_col in column_mapping:
            mapped_columns[csv_col] = column_mapping[csv_col]
        else:
            # Manter coluna original se não houver mapeamento
            mapped_columns[csv_col] = csv_col
    
    return mapped_columns

def csv_to_insert(csv_file, sql_file, table_name, skip_rows, conn=None):
    start_time = datetime.now()
    total_rows = 0
    success_rows = 0
    error_rows = 0

    emit_event({"type":"file_start", "file": csv_file})

    table_columns = get_table_columns(conn, table_name) if conn else []
    emit_event({"type":"table_columns", "columns": table_columns})

    # Detectar codificação do arquivo
    file_encoding = detect_encoding(csv_file)
    emit_event({"type":"encoding_detected", "encoding": file_encoding})

    try:
        with open(csv_file, newline='', encoding=file_encoding, errors='replace') as f:
            for _ in range(skip_rows):
                next(f, None)

            reader = csv.DictReader(f, delimiter=';')
            csv_columns = reader.fieldnames or []
            emit_event({"type":"csv_columns", "columns": csv_columns})

            # Mapear colunas do CSV para colunas do banco
            column_mapping = map_csv_columns_to_db(csv_columns)
            
            # Verificar quais colunas mapeadas existem na tabela
            valid_columns = []
            mapped_columns = {}
            ignored_columns = []
            
            for csv_col, db_col in column_mapping.items():
                if db_col in table_columns:
                    valid_columns.append(csv_col)
                    mapped_columns[csv_col] = db_col
                else:
                    ignored_columns.append(csv_col)

            emit_event({"type":"columns_validated", "valid_columns": valid_columns, "ignored_columns": ignored_columns})

            if not valid_columns:
                emit_event({"type":"error", "message":"Nenhuma coluna válida encontrada; abortando."})
                return {"total":0,"success":0,"error":0}

            cur = conn.cursor() if conn else None
            inserts = []

            for row in reader:
                total_rows += 1
                values = []
                for col in valid_columns:
                    raw = row.get(col, "")
                    val = raw.strip() if raw is not None else None
                    db_col = mapped_columns[col]
                    
                    if val is None or val == "":
                        values.append("NULL")
                    else:
                        # Conversões específicas por tipo de coluna
                        if db_col == 'co_sexo':
                            # Converter texto para código numérico (sem aspas para inteiro)
                            if val.lower() in ['feminino', 'f']:
                                values.append("1")
                            elif val.lower() in ['masculino', 'm']:
                                values.append("2")
                            else:
                                values.append("NULL")
                        elif db_col == 'nu_micro_area':
                            # Limitar micro área a 3 caracteres
                            if len(val) > 3:
                                truncated_val = val[:3]
                                values.append(f"'{truncated_val}'")
                            else:
                                values.append(f"'{val}'")
                        elif db_col in ['nu_cpf_cidadao', 'nu_cpf_responsavel']:
                            # Limitar CPF a 11 caracteres
                            if len(val) > 11:
                                truncated_val = val[:11]
                                values.append(f"'{truncated_val}'")
                            else:
                                values.append(f"'{val}'")
                        elif db_col == 'nu_declaracao_obito':
                            # Limitar declaração de óbito a 9 caracteres
                            if len(val) > 9:
                                truncated_val = val[:9]
                                values.append(f"'{truncated_val}'")
                            else:
                                values.append(f"'{val}'")
                        elif db_col in ['dt_nascimento']:
                            # Converter formato de data se necessário
                            try:
                                # Assumindo formato dd/mm/yyyy
                                if '/' in val:
                                    parts = val.split('/')
                                    if len(parts) == 3:
                                        formatted_date = f"{parts[2]}-{parts[1]:0>2}-{parts[0]:0>2}"
                                        values.append(f"'{formatted_date}'")
                                    else:
                                        values.append("NULL")
                                else:
                                    escaped = val.replace("'", "''")
                                    values.append(f"'{escaped}'")
                            except:
                                values.append("NULL")
                        else:
                            # Valor padrão - escapar aspas
                            escaped = val.replace("'", "''")
                            values.append(f"'{escaped}'")

                # Definir todas as colunas conforme modelo
                all_columns = [
                    'co_tipo_revisao', 'nu_cns_cidadao', 'nu_micro_area', 'nu_cpf_cidadao',
                    'nu_cpf_responsavel', 'dt_cad_individual', 'dt_entrada_brasil', 'dt_nascimento',
                    'dt_nascimento_responsavel', 'dt_naturalizacao', 'dt_obito', 'st_desconhece_nome_mae',
                    'st_desconhece_nome_pai', 'ds_email_cidadao', 'st_atualizacao', 'no_cidadao',
                    'no_cidadao_filtro', 'no_mae_cidadao', 'no_pai_cidadao', 'no_social_cidadao',
                    'nu_cartao_sus_responsavel', 'nu_pis_pasep', 'nu_celular_cidadao', 'nu_declaracao_obito',
                    'ds_portaria_naturalizacao', 'ds_rg_recusa_cad', 'co_sexo', 'st_ficha', 'st_versao_atual',
                    'st_erro_inativacao', 'st_fora_area', 'st_gerado_automaticamente', 'st_ficha_inativa',
                    'st_recusa_cad', 'st_responsavel_familiar', 'tp_cds_origem', 'co_unico_ficha',
                    'co_unico_grupo', 'co_unico_ficha_origem', 'ds_versao_ficha', 'co_cbo', 'co_etnia',
                    'co_localidade_origem', 'co_municipio', 'co_nacionalidade', 'co_pais',
                    'co_cds_prof_cadastrante', 'co_raca_cor', 'co_unidade_saude',
                    'co_revisao'
                ]
                
                # Criar valores para todas as colunas
                all_values = []
                for col in all_columns:
                    # Verificar se esta coluna tem dados do CSV
                    csv_col = None
                    for csv_c, db_c in mapped_columns.items():
                        if db_c == col:
                            csv_col = csv_c
                            break
                    
                    if csv_col and csv_col in valid_columns:
                        # Usar valor do CSV se disponível
                        idx = valid_columns.index(csv_col)
                        val = values[idx] if idx < len(values) else 'NULL'
                        all_values.append(val)
                    else:
                        # Valores padrão conforme modelo
                        if col == 'co_tipo_revisao':
                            all_values.append("'0'")
                        elif col == 'dt_cad_individual':
                            all_values.append(f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'")
                        elif col in ['st_desconhece_nome_mae', 'st_desconhece_nome_pai', 'st_atualizacao', 
                                   'st_ficha', 'st_versao_atual', 'st_erro_inativacao', 'st_fora_area', 
                                   'st_gerado_automaticamente', 'st_ficha_inativa', 'st_recusa_cad']:
                            all_values.append("'0'")
                        elif col == 'tp_cds_origem':
                            all_values.append("'1'")
                        elif col == 'co_etnia':
                            all_values.append("'1407'")
                        elif col == 'co_municipio':
                            all_values.append("'1531'")
                        elif col == 'co_nacionalidade':
                            all_values.append("'1'")
                        elif col == 'co_pais':
                            all_values.append("'31'")
                        elif col == 'co_raca_cor':
                            all_values.append("'4'")
                        elif col == 'co_unidade_saude':
                            all_values.append("'2'")
                        elif col == 'co_revisao':
                            all_values.append("'5'")
                        elif col == 'ds_versao_ficha':
                            all_values.append("'7.2.3'")
                        else:
                            all_values.append('NULL')
                
                insert = f"INSERT INTO {table_name} (\n    {', '.join(all_columns)}\n) VALUES (\n    {', '.join(all_values)}\n);"
                inserts.append(insert)

                if cur:
                    try:
                        cur.execute(sql.SQL(insert))
                        conn.commit()
                        success_rows += 1
                        emit_event({"type":"row_success", "row": total_rows})
                    except Exception as e:
                        error_rows += 1
                        conn.rollback()
                        emit_event({"type":"row_error", "row": total_rows, "error": str(e)})
                else:
                    # if no connection, just generate SQL
                    emit_event({"type":"row_generated", "row": total_rows})

                # periodically emit progress
                if total_rows % 100 == 0:
                    emit_event({"type":"progress", "total": total_rows, "success": success_rows, "error": error_rows})

            # salva em arquivo
            if sql_file:
                with open(sql_file, 'w', encoding='utf-8') as f:
                    f.write("\n".join(inserts))
                emit_event({"type":"sql_saved", "file": sql_file})

            if cur:
                cur.close()

    except UnicodeDecodeError as e:
        emit_event({"type":"fatal", "error": f"Erro de codificação no arquivo {csv_file}: {str(e)}"})
        return {"total": 0, "success": 0, "error": 1}
    except Exception as e:
        emit_event({"type":"fatal", "error": f"Erro ao processar arquivo {csv_file}: {str(e)}"})
        return {"total": 0, "success": 0, "error": 1}

    duration = (datetime.now() - start_time).total_seconds()
    summary = {"type":"file_end", "file": csv_file, "total": total_rows, "success": success_rows, "error": error_rows, "duration_s": duration}
    emit_event(summary)
    return {"total": total_rows, "success": success_rows, "error": error_rows}

def main():
    parser = argparse.ArgumentParser(description="CSV -> SQL migrator")
    parser.add_argument("--env-file", default=ENV_FILE_DEFAULT, help="caminho para o .env")
    parser.add_argument("--file", default=None, help="opcional: processar apenas 1 CSV (caminho relativo ao CSV_DIR ou absoluto)")
    args = parser.parse_args()

    try:
        load_env(args.env_file)
    except Exception as e:
        emit_event({"type":"fatal", "error": str(e)})
        return

    BASE_DIR = os.getenv("BASE_DIR", DEFAULT_BASE_DIR)
    CSV_DIR = os.path.join(BASE_DIR, "datacsv")
    SQL_DIR = os.path.join(BASE_DIR, "backend", "scripts")
    os.makedirs(SQL_DIR, exist_ok=True)

    table_name = os.getenv("TABLE_NAME", "public.tl_cds_cad_individual")
    skip_rows = int(os.getenv("CSV_SKIP_ROWS", "18"))

    try:
        conn = get_connection()
        emit_event({"type":"db_connected"})
    except Exception as e:
        emit_event({"type":"fatal", "error": str(e)})
        return

    files_to_process = []
    if args.file:
        candidate = args.file
        if not os.path.isabs(candidate):
            candidate = os.path.join(CSV_DIR, candidate)
        if os.path.exists(candidate):
            files_to_process = [candidate]
        else:
            emit_event({"type":"fatal", "error": f"Arquivo não encontrado: {candidate}"})
            return
    else:
        files = [f for f in os.listdir(CSV_DIR) if f.lower().endswith(".csv")]
        files_to_process = [os.path.join(CSV_DIR, f) for f in files]

    if not files_to_process:
        emit_event({"type":"info", "message":"Nenhum CSV encontrado para processar."})
    else:
        for csv_file in files_to_process:
            sql_file = os.path.join(SQL_DIR, os.path.basename(csv_file).rsplit(".",1)[0] + ".sql")
            csv_to_insert(csv_file, sql_file, table_name, skip_rows, conn)

    if conn:
        conn.close()
        emit_event({"type":"db_disconnected"})

    emit_event({"type":"done"})

if __name__ == "__main__":
    main()
