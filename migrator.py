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
import uuid

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

def get_esus_ledi_data(conn):
    """
    Busca dados necessários para conformidade e-SUS LEDI
    """
    cur = conn.cursor()
    
    # Buscar profissional específico (co_seq_cds_prof = 1)
    cur.execute("SELECT co_seq_cds_prof FROM tb_cds_prof WHERE co_seq_cds_prof = 1 LIMIT 1")
    prof_result = cur.fetchone()
    if not prof_result:
        # Fallback para primeiro disponível
        cur.execute("SELECT co_seq_cds_prof FROM tb_cds_prof LIMIT 1")
        prof_result = cur.fetchone()
    co_cds_prof_cadastrante = prof_result[0] if prof_result else 1
    
    # Buscar unidade de saúde UBASF Águas Belas (CNES 9017364)
    cur.execute("SELECT co_seq_unidade_saude, nu_cnes FROM tb_unidade_saude WHERE nu_cnes = '9017364' LIMIT 1")
    unidade_result = cur.fetchone()
    if unidade_result:
        co_unidade_saude = unidade_result[0]
        nu_cnes = unidade_result[1]
    else:
        # Fallback para primeira unidade disponível
        cur.execute("SELECT co_seq_unidade_saude, nu_cnes FROM tb_unidade_saude LIMIT 1")
        fallback_result = cur.fetchone()
        co_unidade_saude = fallback_result[0] if fallback_result else 2
        nu_cnes = fallback_result[1] if fallback_result else '0000000'
    
    # Buscar localidade origem específica
    cur.execute("SELECT co_localidade FROM tb_localidade WHERE co_localidade IS NOT NULL LIMIT 1")
    localidade_result = cur.fetchone()
    co_localidade_origem = localidade_result[0] if localidade_result else None
    
    cur.close()
    
    return {
        'co_cds_prof_cadastrante': co_cds_prof_cadastrante,
        'co_unidade_saude': co_unidade_saude,
        'nu_cnes': nu_cnes,
        'co_localidade_origem': co_localidade_origem
    }

def generate_co_unico_ficha(nu_cnes):
    """
    Gera co_unico_ficha no formato e-SUS LEDI: CNES + UUID
    Exemplo: 9017364-31ba4fb1-9fe3-4cb7-9585-891fdd6721ee
    """
    unique_uuid = str(uuid.uuid4())
    return f"{nu_cnes}-{unique_uuid}"

def validate_cpf(cpf):
    """
    Valida CPF brasileiro
    """
    if not cpf or len(cpf) != 11 or not cpf.isdigit():
        return False
    
    # Verifica se todos os dígitos são iguais
    if cpf == cpf[0] * 11:
        return False
    
    # Calcula primeiro dígito verificador
    sum1 = sum(int(cpf[i]) * (10 - i) for i in range(9))
    digit1 = 11 - (sum1 % 11)
    if digit1 >= 10:
        digit1 = 0
    
    # Calcula segundo dígito verificador
    sum2 = sum(int(cpf[i]) * (11 - i) for i in range(10))
    digit2 = 11 - (sum2 % 11)
    if digit2 >= 10:
        digit2 = 0
    
    return cpf[-2:] == f"{digit1}{digit2}"

def validate_cns(cns):
    """
    Valida CNS (Cartão Nacional de Saúde)
    Baseado no algoritmo oficial do e-SUS
    """
    if not cns or len(cns) != 15 or not cns.isdigit():
        return False
    
    # CNS provisório (começa com 7, 8 ou 9)
    if cns[0] in ['7', '8', '9']:
        # Validação para CNS provisório
        sum_val = sum(int(cns[i]) * (15 - i) for i in range(15))
        return sum_val % 11 == 0
    
    # CNS definitivo (começa com 1 ou 2)
    elif cns[0] in ['1', '2']:
        # Validação para CNS definitivo
        sum_val = sum(int(cns[i]) * (15 - i) for i in range(11))
        rest = sum_val % 11
        
        if rest < 2:
            dv = 0
        else:
            dv = 11 - rest
        
        return cns[11:15] == f"{dv:04d}"
    
    return False

def format_phone_number(phone):
    """
    Formata número de telefone removendo caracteres especiais
    Exemplo: (85) 99108-2143 -> 85991082143
    """
    if not phone:
        return None
    
    # Remove todos os caracteres não numéricos
    clean_phone = ''.join(filter(str.isdigit, phone))
    
    # Verifica se tem pelo menos 10 dígitos (DDD + número)
    if len(clean_phone) >= 10:
        return clean_phone
    
    return None

def validate_name(name):
    """
    Valida nome conforme regras e-SUS
    """
    if not name or len(name.strip()) < 2:
        return False
    
    # Remove espaços extras e converte para maiúsculo
    clean_name = ' '.join(name.strip().split()).upper()
    
    # Verifica se contém apenas letras, espaços e alguns caracteres especiais
    allowed_chars = set('ABCDEFGHIJKLMNOPQRSTUVWXYZÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞŸ ')
    if not all(c in allowed_chars for c in clean_name):
        return False
    
    return clean_name

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

def map_csv_columns_to_db(csv_columns, table_name):
    """Mapeia colunas do CSV para colunas da tabela do banco"""
    # Mapeamento para tabela tl_cds_cad_individual (nova)
    if 'tl_cds_cad_individual' in table_name:
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
    else:
        # Mapeamento original para outras tabelas
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
            FROM tb_equipe 
            WHERE nu_ine = %s
        """, (ine_equipe,))
        
        result = cur.fetchone()
        cur.close()
        
        if result:
            return result[0]
        else:
            emit_event({"type": "warning", "message": f"INE equipe '{ine_equipe}' não encontrado na tabela tb_equipe"})
            return None
    except Exception as e:
        emit_event({"type": "error", "message": f"Erro ao buscar unidade de saúde pelo INE {ine_equipe}: {e}"})
        return None

def csv_to_insert(csv_file, sql_file, table_name, skip_rows, conn=None, co_municipio=None):
    start_time = datetime.now()
    total_rows = 0
    success_rows = 0
    error_rows = 0

    emit_event({"type":"file_start", "file": csv_file})

    # Buscar dados e-SUS LEDI
    esus_data = get_esus_ledi_data(conn) if conn else {
        'co_cds_prof_cadastrante': 1,
        'co_unidade_saude': 2,
        'nu_cnes': '0000000',
        'co_localidade_origem': None
    }

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
            column_mapping = map_csv_columns_to_db(csv_columns, table_name)
            
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

            for index, row in enumerate(reader):
                total_rows += 1
                
                # Buscar INE equipe da coluna B para determinar co_unidade_saude
                ine_equipe = None
                if 'INE equipe' in row:
                    ine_equipe = str(row['INE equipe']).strip().strip('"') if row['INE equipe'] else None
                
                # Buscar co_unidade_saude pelo INE equipe
                co_unidade_saude_from_ine = None
                if ine_equipe:
                    co_unidade_saude_from_ine = get_unidade_saude_by_ine(conn, ine_equipe)
                
                # Mapear dados do CSV para campos do banco
                mapped_data = {}
                for col in valid_columns:
                    raw = row.get(col, "")
                    val = raw.strip() if raw is not None else None
                    db_col = mapped_columns[col]
                    mapped_data[db_col] = val
                
                # Adicionar co_municipio se fornecido como parâmetro
                if co_municipio:
                    mapped_data['co_municipio'] = co_municipio
                
                # Validações e conversões específicas
                # Sexo: converter texto para código baseado em tb_sexo
                if 'co_sexo' in mapped_data:
                    sexo_text = str(mapped_data['co_sexo']).upper().strip() if mapped_data['co_sexo'] else ''
                    if sexo_text in ['MASCULINO', 'M']:
                        mapped_data['co_sexo'] = 0
                    elif sexo_text in ['FEMININO', 'F']:
                        mapped_data['co_sexo'] = 1
                    else:
                        mapped_data['co_sexo'] = None
                
                # Micro área: sempre null conforme solicitado
                mapped_data['nu_micro_area'] = None
                
                # CPF e CNS: validação rigorosa com regras específicas
                cpf_value = None
                cns_value = None
                
                # Processar CPF primeiro
                if 'nu_cpf_cidadao' in mapped_data:
                    cpf = str(mapped_data['nu_cpf_cidadao']) if mapped_data['nu_cpf_cidadao'] else ''
                    cpf_clean = ''.join(filter(str.isdigit, cpf))
                    if len(cpf_clean) == 11:
                        if validate_cpf(cpf_clean):
                            cpf_value = cpf_clean
                        else:
                            emit_event(f"Aviso: CPF inválido '{cpf}' na linha {index + 1}")
                    elif len(cpf_clean) == 15:
                        # Se tem 15 dígitos, tratar como CNS
                        if validate_cns(cpf_clean):
                            cns_value = cpf_clean
                            emit_event(f"Info: Valor de 15 dígitos tratado como CNS na linha {index + 1}")
                        else:
                            emit_event(f"Aviso: CNS inválido '{cpf}' na linha {index + 1}")
                    elif cpf_clean:  # Se tem dígitos mas não são 11 nem 15
                        emit_event(f"Aviso: CPF/CNS com formato incorreto '{cpf}' na linha {index + 1}")
                    mapped_data['nu_cpf_cidadao'] = cpf_value
                
                # Processar CNS (apenas se não foi processado como CPF de 15 dígitos)
                if 'nu_cns_cidadao' in mapped_data and not cns_value:
                    cns = str(mapped_data['nu_cns_cidadao']) if mapped_data['nu_cns_cidadao'] else ''
                    cns_clean = ''.join(filter(str.isdigit, cns))
                    
                    if len(cns_clean) == 15:
                        if validate_cns(cns_clean):
                            # CNS só pode ser preenchido se CPF não estiver preenchido
                            if not cpf_value:
                                cns_value = cns_clean
                            else:
                                emit_event(f"Aviso: CNS ignorado pois CPF está preenchido na linha {index + 1}")
                        else:
                            emit_event(f"Aviso: CNS inválido '{cns}' na linha {index + 1}")
                    elif cns_clean:  # Se tem dígitos mas não são 15
                        emit_event(f"Aviso: CNS com formato incorreto '{cns}' na linha {index + 1}")
                
                mapped_data['nu_cns_cidadao'] = cns_value
                
                # Validar obrigatoriedade: CPF ou CNS deve estar preenchido
                if not cpf_value and not cns_value:
                    emit_event(f"Aviso: CPF ou CNS obrigatório na linha {index + 1}")
                
                # Se CPF está preenchido, garantir que CNS seja None
                if cpf_value and 'nu_cns_cidadao' in mapped_data:
                    mapped_data['nu_cns_cidadao'] = None
                
                # Celular: formatação específica e validação de obrigatoriedade
                if 'nu_celular_cidadao' in mapped_data:
                    phone = mapped_data['nu_celular_cidadao']
                    formatted_phone = format_phone_number(phone)
                    
                    # Verificar nacionalidade para obrigatoriedade
                    nacionalidade = mapped_data.get('nacionalidade_cidadao', '').lower().strip()
                    if nacionalidade in ['brasileira', 'naturalizado']:
                        # Campo obrigatório para brasileiros e naturalizados
                        if not formatted_phone:
                            emit_event(f"Aviso: Celular obrigatório para nacionalidade '{nacionalidade}' na linha {index + 1}")
                    
                    mapped_data['nu_celular_cidadao'] = formatted_phone
                
                # Nome do pai: validação específica
                if 'no_pai_cidadao' in mapped_data:
                    nome_pai = mapped_data['no_pai_cidadao']
                    desconhece_pai = mapped_data.get('st_desconhece_nome_pai', 0)
                    if desconhece_pai == 1:
                        mapped_data['no_pai_cidadao'] = None
                    else:
                        validated_name = validate_name(nome_pai) if nome_pai else None
                        mapped_data['no_pai_cidadao'] = validated_name
                
                # Nome da mãe: validação específica
                if 'no_mae_cidadao' in mapped_data:
                    nome_mae = mapped_data['no_mae_cidadao']
                    desconhece_mae = mapped_data.get('st_desconhece_nome_mae', 0)
                    if desconhece_mae == 1:
                        mapped_data['no_mae_cidadao'] = None
                    else:
                        validated_name = validate_name(nome_mae) if nome_mae else None
                        mapped_data['no_mae_cidadao'] = validated_name
                
                # Raça/Cor e Etnia: regras específicas
                raca_cor = mapped_data.get('co_raca_cor')
                if raca_cor == 5:  # INDÍGENA
                    # co_etnia é obrigatório quando raca_cor = 5
                    mapped_data['co_etnia'] = 1407  # Valor padrão conforme solicitado
                else:
                    mapped_data['co_etnia'] = None
                
                # Converter dados mapeados para valores SQL
                values = []
                for col in valid_columns:
                    db_col = mapped_columns[col]
                    val = mapped_data.get(db_col)
                    
                    if val is None or val == "":
                        values.append("NULL")
                    else:
                        if db_col == 'co_sexo':
                            # Valor numérico sem aspas
                            values.append(str(val))
                        elif db_col in ['dt_nascimento']:
                            # Converter formato de data se necessário
                            try:
                                # Assumindo formato dd/mm/yyyy
                                if '/' in str(val):
                                    parts = str(val).split('/')
                                    if len(parts) == 3:
                                        formatted_date = f"{parts[2]}-{parts[1]:0>2}-{parts[0]:0>2}"
                                        values.append(f"'{formatted_date}'")
                                    else:
                                        values.append("NULL")
                                else:
                                    escaped = str(val).replace("'", "''")
                                    values.append(f"'{escaped}'")
                            except:
                                values.append("NULL")
                        else:
                             # Valor padrão - escapar aspas
                             escaped = str(val).replace("'", "''")
                             values.append(f"'{escaped}'")

                # Definir colunas conforme a tabela específica
                if 'tl_cds_cad_individual' in table_name:
                    # Colunas para a nova tabela tl_cds_cad_individual
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
                        'co_cds_prof_cadastrante', 'co_raca_cor', 'co_unidade_saude', 'co_seq_cds_cad_individual',
                        'co_revisao'
                    ]
                else:
                    # Colunas para tabela tb_cds_cad_individual (baseado na estrutura real)
                    all_columns = [
                        'co_seq_cds_cad_individual', 'nu_cns_cidadao', 'nu_micro_area', 'nu_cpf_cidadao',
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
                        'co_cds_prof_cadastrante', 'co_raca_cor', 'co_unidade_saude'
                    ]
                
                # Criar valores para todas as colunas com regras específicas
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
                        # Valores padrão conforme modelo com regras específicas
                        if col == 'co_seq_cds_cad_individual':
                            # Usar sequência para gerar valor único
                            all_values.append("nextval('seq_tb_cds_cad_individual')")
                        elif col == 'dt_cad_individual':
                            all_values.append(f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'")
                        elif col == 'st_desconhece_nome_mae':
                            # Regra específica: 1 se não tem nome da mãe, 0 se tem
                            if mapped_data.get('no_mae_cidadao'):
                                all_values.append("'0'")
                            else:
                                all_values.append("'1'")
                        elif col == 'st_desconhece_nome_pai':
                            # Regra específica: 1 se não tem nome do pai, 0 se tem
                            if mapped_data.get('no_pai_cidadao'):
                                all_values.append("'0'")
                            else:
                                all_values.append("'1'")
                        elif col in ['st_atualizacao', 'st_ficha', 'st_versao_atual', 'st_erro_inativacao', 
                                   'st_fora_area', 'st_gerado_automaticamente', 'st_ficha_inativa', 'st_recusa_cad']:
                            all_values.append("'0'")
                        elif col == 'tp_cds_origem':
                            all_values.append("'1'")
                        elif col == 'co_etnia':
                            # Regra específica: obrigatório quando raca_cor = 5 (INDÍGENA)
                            if mapped_data.get('co_raca_cor') == 5:
                                all_values.append("'1407'")
                            else:
                                all_values.append('NULL')
                        elif col == 'co_municipio':
                            # Usar valor configurado ou NULL se não fornecido
                            co_municipio_value = mapped_data.get('co_municipio')
                            if co_municipio_value:
                                all_values.append(f"'{co_municipio_value}'")
                            else:
                                all_values.append('NULL')
                        elif col == 'co_nacionalidade':
                            all_values.append("'1'")
                        elif col == 'co_pais':
                            all_values.append("'31'")
                        elif col == 'co_raca_cor':
                            # Usar valor do CSV se disponível, senão padrão
                            if mapped_data.get('co_raca_cor') is not None:
                                all_values.append(f"'{mapped_data['co_raca_cor']}'")
                            else:
                                all_values.append("'4'")
                        elif col == 'co_sexo':
                            # Usar valor do CSV se disponível
                            if mapped_data.get('co_sexo') is not None:
                                all_values.append(f"'{mapped_data['co_sexo']}'")
                            else:
                                all_values.append('NULL')
                        elif col == 'nu_cpf_cidadao':
                            # Usar CPF validado
                            if mapped_data.get('nu_cpf_cidadao'):
                                all_values.append(f"'{mapped_data['nu_cpf_cidadao']}'")
                            else:
                                all_values.append('NULL')
                        elif col == 'nu_cns_cidadao':
                            # Usar CNS validado (só se CPF não estiver preenchido)
                            if mapped_data.get('nu_cns_cidadao'):
                                all_values.append(f"'{mapped_data['nu_cns_cidadao']}'")
                            else:
                                all_values.append('NULL')
                        elif col == 'nu_celular_cidadao':
                            # Usar celular formatado
                            if mapped_data.get('nu_celular_cidadao'):
                                all_values.append(f"'{mapped_data['nu_celular_cidadao']}'")
                            else:
                                all_values.append('NULL')
                        elif col == 'no_cidadao':
                            # Usar nome validado
                            if mapped_data.get('no_cidadao'):
                                all_values.append(f"'{mapped_data['no_cidadao']}'")
                            else:
                                all_values.append('NULL')
                        elif col == 'no_mae_cidadao':
                            # Usar nome da mãe validado
                            if mapped_data.get('no_mae_cidadao'):
                                all_values.append(f"'{mapped_data['no_mae_cidadao']}'")
                            else:
                                all_values.append('NULL')
                        elif col == 'no_pai_cidadao':
                            # Usar nome do pai validado
                            if mapped_data.get('no_pai_cidadao'):
                                all_values.append(f"'{mapped_data['no_pai_cidadao']}'")
                            else:
                                all_values.append('NULL')
                        elif col == 'dt_nascimento':
                            # Usar data de nascimento formatada com horário 12:00:00
                            if mapped_data.get('dt_nascimento'):
                                date_val = mapped_data['dt_nascimento']
                                # Adicionar horário 12:00:00 se não estiver presente
                                if ' ' not in date_val:
                                    date_val += ' 12:00:00'
                                all_values.append(f"'{date_val}'")
                            else:
                                all_values.append('NULL')
                        elif col == 'nu_micro_area':
                            # Sempre NULL conforme solicitado
                            all_values.append('NULL')
                        elif col == 'co_cds_prof_cadastrante':
                            all_values.append(f"'{esus_data['co_cds_prof_cadastrante']}'")
                        elif col == 'co_unidade_saude':
                            # Usar co_unidade_saude encontrado pelo INE equipe, senão usar padrão
                            if co_unidade_saude_from_ine:
                                all_values.append(f"'{co_unidade_saude_from_ine}'")
                            else:
                                all_values.append(f"'{esus_data['co_unidade_saude']}'")
                        elif col == 'co_localidade_origem':
                            # Sempre usar 1407 (Cascavel)
                            all_values.append("'1407'")
                        elif col == 'co_unico_ficha':
                            co_unico_ficha = generate_co_unico_ficha(esus_data['nu_cnes'])
                            all_values.append(f"'{co_unico_ficha}'")
                        elif col == 'co_unico_ficha_origem':
                            # Usar a mesma lógica do co_unico_ficha
                            co_unico_ficha_origem = generate_co_unico_ficha(esus_data['nu_cnes'])
                            all_values.append(f"'{co_unico_ficha_origem}'")
                        elif col == 'no_cidadao_filtro':
                            # Usar nome do cidadão em minúsculas
                            if mapped_data.get('no_cidadao'):
                                nome_filtro = mapped_data['no_cidadao'].lower()
                                all_values.append(f"'{nome_filtro}'")
                            else:
                                all_values.append('NULL')
                        elif col == 'nu_cartao_sus_responsavel':
                            # Sempre NULL conforme solicitado
                            all_values.append('NULL')
                        elif col == 'co_unico_grupo':
                            all_values.append(f"'{str(uuid.uuid4())}'")
                        elif col == 'co_revisao':
                            all_values.append("'5'")
                        elif col == 'ds_versao_ficha':
                            all_values.append("'7.2.3'")
                        elif col == 'co_seq_cds_cad_individual':
                            # Usar a sequência apropriada baseada na tabela
                            if 'tl_cds_cad_individual' in table_name:
                                all_values.append("nextval('seq_tl_cds_cad_individual')")
                            else:
                                all_values.append("nextval('seq_tb_cds_cad_individual')")
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
        import traceback
        emit_event({"type":"fatal", "error": f"Erro ao processar arquivo {csv_file}: {str(e)}"})
        emit_event({"type":"debug", "traceback": traceback.format_exc()})
        return {"total": 0, "success": 0, "error": 1}

    duration = (datetime.now() - start_time).total_seconds()
    summary = {"type":"file_end", "file": csv_file, "total": total_rows, "success": success_rows, "error": error_rows, "duration_s": duration}
    emit_event(summary)
    return {"total": total_rows, "success": success_rows, "error": error_rows}

def main():
    parser = argparse.ArgumentParser(description="CSV -> SQL migrator")
    parser.add_argument("--env-file", default=ENV_FILE_DEFAULT, help="caminho para o .env")
    parser.add_argument("--file", default=None, help="opcional: processar apenas 1 CSV (caminho relativo ao CSV_DIR ou absoluto)")
    parser.add_argument("--co-municipio", default=None, help="código do município onde nasceu o cidadão (opcional)")
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
            # Gerar nome do arquivo SQL baseado na tabela de destino
            base_name = os.path.basename(csv_file).rsplit(".",1)[0]
            table_suffix = table_name.replace("public.", "").replace(".", "_")
            sql_file = os.path.join(SQL_DIR, f"{table_suffix}_{base_name}.sql")
            csv_to_insert(csv_file, sql_file, table_name, skip_rows, conn, getattr(args, 'co_municipio', None))

    if conn:
        conn.close()
        emit_event({"type":"db_disconnected"})

    emit_event({"type":"done"})

if __name__ == "__main__":
    main()
