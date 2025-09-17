#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Migrator especializado para tabelas FAT (Fato) do e-SUS
Responsável pela inserção criteriosa de dados nas tabelas:
- tb_fat_cad_individual
- tb_fat_cidadao  
- tb_fat_cidadao_pec
- tb_cidadao

Autor: Assistente IA
Data: 2025-01-15
"""

import psycopg2
import pandas as pd
import hashlib
import re
import uuid
from datetime import datetime
from dotenv import load_dotenv
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FATTablesMigrator:
    def __init__(self):
        load_dotenv()
        self.conn = None
        self.cur = None
        self.connect_db()
        
    def connect_db(self):
        """Conecta ao banco de dados PostgreSQL"""
        try:
            self.conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST'),
                port=os.getenv('POSTGRES_PORT'),
                database=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD')
            )
            self.cur = self.conn.cursor()
            logger.info("Conexão com banco estabelecida")
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco: {e}")
            raise
    
    def generate_uuid_with_unidade(self, co_unidade_saude):
        """
        Gera UUID baseado no código da unidade de saúde
        Formato: {co_unidade_saude}{uuid}
        """
        unique_uuid = str(uuid.uuid4()).replace('-', '')
        return f"{co_unidade_saude}{unique_uuid}"
    
    def validate_cns(self, cns_value):
        """
        Valida e normaliza CNS (Cartão Nacional de Saúde)
        Retorna CNS válido ou None
        """
        if not cns_value or pd.isna(cns_value):
            return None
            
        # Remove caracteres não numéricos
        cns_clean = re.sub(r'[^\d]', '', str(cns_value))
        
        # CNS deve ter 15 dígitos
        if len(cns_clean) == 15:
            return cns_clean
        elif len(cns_clean) == 11:
            # Pode ser CPF, gerar CNS fictício baseado no CPF
            return self.generate_cns_from_cpf(cns_clean)
        
        return None
    
    def generate_cns_from_cpf(self, cpf):
        """
        Gera CNS fictício baseado no CPF para manter consistência
        """
        if not cpf or len(cpf) != 11:
            return None
            
        # Gera hash do CPF e usa os primeiros 15 dígitos
        hash_obj = hashlib.md5(cpf.encode())
        hash_hex = hash_obj.hexdigest()
        # Converte para números e pega 15 dígitos
        cns_generated = ''.join([str(ord(c) % 10) for c in hash_hex])[:15]
        return cns_generated
    
    def get_unidade_saude_id(self, ine_equipe):
        """
        Mapeia INE da equipe para ID da unidade de saúde
        """
        try:
            # Garantir que INE seja string e tenha zeros à esquerda
            if ine_equipe is None or str(ine_equipe).strip() == '':
                logger.warning("INE vazio ou nulo fornecido")
                return None
                
            # Limpar e formatar INE
            ine_clean = str(ine_equipe).strip().strip('"\'')
            if ine_clean.isdigit():
                ine_clean = ine_clean.zfill(10)  # Garantir 10 dígitos com zeros à esquerda
            
            self.cur.execute("""
                SELECT co_unidade_saude 
                FROM tb_equipe 
                WHERE nu_ine = %s
                LIMIT 1
            """, (ine_clean,))
            
            result = self.cur.fetchone()
            if result:
                return result[0]
            else:
                logger.warning(f"Unidade não encontrada para INE: {ine_clean}")
                return None
        except Exception as e:
            logger.error(f"Erro ao buscar unidade para INE {ine_equipe}: {e}")
            return None
    
    def parse_csv_data(self, csv_file):
        """
        Processa CSV e extrai dados relevantes
        """
        try:
            # Tentar diferentes encodings
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            df = None
            
            for encoding in encodings:
                try:
                    # Especificar dtype para manter INE como string
                    dtype_dict = {'INE equipe': str}
                    df = pd.read_csv(csv_file, sep=';', skiprows=16, encoding=encoding, dtype=dtype_dict)
                    logger.info(f"CSV lido com encoding: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                raise Exception("Não foi possível ler o CSV com nenhum encoding testado")
            
            # Verificar se temos colunas suficientes
            if len(df.columns) < 15:
                logger.warning(f"CSV tem apenas {len(df.columns)} colunas, esperado 15+")
                return None
            
            # Renomear colunas para facilitar acesso (16 colunas, última pode ser vazia)
            column_names = [
                'nome_equipe', 'ine_equipe', 'micro_area', 'endereco', 
                'cpf_cns', 'nome', 'idade', 'sexo', 'identidade_genero',
                'data_nascimento', 'telefone_celular', 'telefone_residencial',
                'telefone_contato', 'ultima_atualizacao', 'origem', 'extra'
            ]
            
            # Ajustar para o número real de colunas
            df.columns = column_names[:len(df.columns)]
            
            # Garantir que INE seja string e remover aspas se existirem
            if 'ine_equipe' in df.columns:
                df['ine_equipe'] = df['ine_equipe'].astype(str).str.strip().str.strip('"\'')
                # Garantir que tenha 10 dígitos com zeros à esquerda
                df['ine_equipe'] = df['ine_equipe'].apply(lambda x: x.zfill(10) if x.isdigit() else x)
            
            # Limpar dados
            df = df.dropna(subset=['nome', 'cpf_cns'])
            df = df[df['nome'].str.strip() != '']  # Remove linhas com nome vazio
            
            logger.info(f"CSV processado: {len(df)} registros válidos")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao processar CSV {csv_file}: {e}")
            return None
    
    def extract_cpf_cns(self, cpf_cns_field):
        """
        Extrai CPF e CNS do campo combinado
        """
        if not cpf_cns_field or pd.isna(cpf_cns_field):
            return None, None
            
        # Remove caracteres especiais
        clean_value = re.sub(r'[^\d]', '', str(cpf_cns_field))
        
        if len(clean_value) == 11:
            # É CPF
            return clean_value, self.generate_cns_from_cpf(clean_value)
        elif len(clean_value) == 15:
            # É CNS
            return None, clean_value
        
        return None, None
    
    def insert_tb_cidadao(self, row_data):
        """
        Insere dados na tabela tb_cidadao com novos campos específicos
        """
        try:
            cpf, cns = self.extract_cpf_cns(row_data['cpf_cns'])
            unidade_id = self.get_unidade_saude_id(row_data['ine_equipe'])
            
            if (not cns and not cpf) or not unidade_id:
                logger.warning(f"CNS/CPF ou unidade inválida para {row_data['nome']}")
                return None
            
            # Verificar se já existe (por CNS ou CPF)
            existing = None
            if cns:
                self.cur.execute("SELECT co_seq_cidadao FROM tb_cidadao WHERE nu_cns = %s", (cns,))
                existing = self.cur.fetchone()
            
            if not existing and cpf:
                self.cur.execute("SELECT co_seq_cidadao FROM tb_cidadao WHERE nu_cpf = %s", (cpf,))
                existing = self.cur.fetchone()
            
            if existing:
                logger.info(f"Cidadão já existe: {row_data['nome']} (CNS: {cns}, CPF: {cpf})")
                return existing[0]
            
            # Gerar co_unico_cidadao baseado na unidade de saúde + UUID
            co_unico_cidadao = self.generate_uuid_with_unidade(unidade_id)
            co_unico_ultima_ficha = co_unico_cidadao  # Mesmo valor conforme especificação
            
            # Data atual no formato AAAAMMDD para dt_ultima_ficha
            dt_ultima_ficha = datetime.now().strftime('%Y%m%d')
            
            # Inserir novo cidadão com todos os campos específicos
            insert_query = """
                INSERT INTO tb_cidadao (
                    co_seq_cidadao, co_localidade, co_unico_cidadao, co_pais_nascimento,
                    co_unico_ultima_ficha, dt_ultima_ficha, co_nacionalidade, st_unificado,
                    nu_cns, nu_cpf, no_cidadao, no_cidadao_filtro, 
                    dt_nascimento, no_sexo, nu_telefone_celular,
                    st_desconhece_nome_mae, dt_atualizado
                ) VALUES (nextval('sq_cidadao_coseqcidadao'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING co_seq_cidadao
            """
            
            # Processar dados
            nome_filtro = row_data['nome'].lower().strip()
            sexo_map = {'Masculino': 'MASCULINO', 'Feminino': 'FEMININO', 'M': 'MASCULINO', 'F': 'FEMININO'}
            no_sexo = sexo_map.get(row_data.get('sexo'), 'MASCULINO')
            
            # Converter data de nascimento
            dt_nascimento = None
            if row_data.get('data_nascimento'):
                try:
                    dt_nascimento = datetime.strptime(row_data['data_nascimento'], '%d/%m/%Y').date()
                except:
                    logger.warning(f"Data de nascimento inválida: {row_data['data_nascimento']}")
            
            # Processar telefone celular (somente números)
            nu_telefone_celular = None
            if row_data.get('telefone_celular'):
                nu_telefone_celular = re.sub(r'[^\d]', '', str(row_data['telefone_celular']))
            
            # TODO: co_localidade deve ser obtido da tabela TB_LOCALIDADE
            # Por enquanto, usando None até implementar a busca correta
            co_localidade = None
            
            self.cur.execute(insert_query, (
                co_localidade,              # co_localidade (chave estrangeira TB_LOCALIDADE)
                co_unico_cidadao,           # co_unico_cidadao (unidade + UUID)
                31,                         # co_pais_nascimento (31 = Brasil)
                co_unico_ultima_ficha,      # co_unico_ultima_ficha (mesmo valor)
                dt_ultima_ficha,            # dt_ultima_ficha (data atual DDMMAAAA)
                1,                          # co_nacionalidade (1 = Brasil)
                0,                          # st_unificado
                cns,                        # nu_cns (15 dígitos)
                cpf,                        # nu_cpf (11 dígitos)
                row_data['nome'],           # no_cidadao
                nome_filtro,                # no_cidadao_filtro
                dt_nascimento,              # dt_nascimento
                no_sexo,                    # no_sexo
                nu_telefone_celular,        # nu_telefone_celular (somente números)
                0,                          # st_desconhece_nome_mae
                datetime.now()              # dt_atualizado
            ))
            
            cidadao_id = self.cur.fetchone()[0]
            logger.info(f"Cidadão inserido: {row_data['nome']} (ID: {cidadao_id})")
            return cidadao_id
            
        except Exception as e:
            logger.error(f"Erro ao inserir tb_cidadao: {e}")
            return None
    
    def insert_tb_fat_cidadao_pec(self, row_data, cidadao_id):
        """
        Insere dados na tabela tb_fat_cidadao_pec com novos campos específicos
        """
        try:
            cpf, cns = self.extract_cpf_cns(row_data['cpf_cns'])
            unidade_id = self.get_unidade_saude_id(row_data['ine_equipe'])
            
            if not cns or not unidade_id:
                return None
            
            # Verificar se já existe
            self.cur.execute("""
                SELECT co_seq_fat_cidadao_pec 
                FROM tb_fat_cidadao_pec 
                WHERE co_cidadao = %s AND nu_cns = %s
            """, (cidadao_id, cns))
            
            existing = self.cur.fetchone()
            if existing:
                return existing[0]
            
            # Obter próximo ID
            self.cur.execute("SELECT COALESCE(MAX(co_seq_fat_cidadao_pec), 0) + 1 FROM tb_fat_cidadao_pec")
            next_id = self.cur.fetchone()[0]
            
            # Processar data de nascimento para co_dim_tempo_nascimento (YYYYMMDD)
            co_dim_tempo_nascimento = None
            if row_data.get('data_nascimento'):
                try:
                    dt_nasc = datetime.strptime(row_data['data_nascimento'], '%d/%m/%Y')
                    co_dim_tempo_nascimento = int(dt_nasc.strftime('%Y%m%d'))
                except:
                    pass
            
            # Processar sexo (0=Masculino, 1=Feminino conforme tb_sexo)
            sexo_map = {'Masculino': 0, 'Feminino': 1, 'M': 0, 'F': 1}
            co_dim_sexo = sexo_map.get(row_data.get('sexo'), 0)  # Default masculino
            
            # Processar telefone celular (somente números)
            nu_telefone_celular = None
            if row_data.get('telefone_celular'):
                nu_telefone_celular = re.sub(r'[^\d]', '', str(row_data['telefone_celular']))
            
            # Inserir novo registro com todos os campos específicos
            insert_query = """
                INSERT INTO tb_fat_cidadao_pec (
                    co_seq_fat_cidadao_pec, co_cidadao, nu_cns, no_cidadao, no_social_cidadao,
                    co_dim_tempo_nascimento, co_dim_sexo, co_dim_identidade_genero, nu_telefone_celular,
                    st_faleceu, st_lookup_etl, nu_cpf_cidadao, co_dim_unidade_saude_vinc, co_dim_equipe_vinc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING co_seq_fat_cidadao_pec
            """
            
            self.cur.execute(insert_query, (
                next_id,                      # co_seq_fat_cidadao_pec
                cidadao_id,                   # co_cidadao
                cns,                          # nu_cns
                row_data['nome'],             # no_cidadao
                None,                         # no_social_cidadao (nulo conforme especificação)
                co_dim_tempo_nascimento,      # co_dim_tempo_nascimento (YYYYMMDD)
                co_dim_sexo,                  # co_dim_sexo (0=Masculino, 1=Feminino)
                5,                            # co_dim_identidade_genero
                nu_telefone_celular,          # nu_telefone_celular (somente números)
                0,                            # st_faleceu
                0,                            # st_lookup_etl
                cpf,                          # nu_cpf_cidadao (11 dígitos)
                unidade_id,                   # co_dim_unidade_saude_vinc
                row_data['ine_equipe']        # co_dim_equipe_vinc (INE)
            ))
            
            pec_id = self.cur.fetchone()[0]
            logger.info(f"FAT Cidadão PEC inserido: {row_data['nome']} (ID: {pec_id})")
            return pec_id
            
        except Exception as e:
            logger.error(f"Erro ao inserir tb_fat_cidadao_pec: {e}")
            return None
    
    def insert_tb_fat_cidadao(self, row_data):
        """
        Insere dados na tabela tb_fat_cidadao com os mesmos campos da tb_fat_cad_individual
        """
        try:
            cpf, cns = self.extract_cpf_cns(row_data['cpf_cns'])
            unidade_id = self.get_unidade_saude_id(row_data['ine_equipe'])
            
            if not cns or not unidade_id:
                return None
            
            # Verificar se já existe
            self.cur.execute("SELECT co_seq_fat_cidadao FROM tb_fat_cidadao WHERE nu_cns = %s", (cns,))
            existing = self.cur.fetchone()
            
            if existing:
                return existing[0]
            
            # Obter próximo ID
            self.cur.execute("SELECT COALESCE(MAX(co_seq_fat_cidadao), 0) + 1 FROM tb_fat_cidadao")
            next_id = self.cur.fetchone()[0]
            
            # Gerar UUIDs baseados na unidade de saúde
            nu_uuid_ficha = self.generate_uuid_with_unidade(unidade_id)
            nu_uuid_ficha_origem = nu_uuid_ficha  # Mesmo valor conforme especificação
            nu_uuid_dado_transp = nu_uuid_ficha  # Mesmo valor conforme especificação
            
            # Processar dados adicionais
            sexo_map = {'Masculino': 0, 'Feminino': 1}
            co_sexo = sexo_map.get(row_data.get('sexo'), 0)  # Default masculino
            
            # Converter data de nascimento
            dt_nascimento = None
            if row_data.get('data_nascimento'):
                try:
                    dt_nascimento = datetime.strptime(row_data['data_nascimento'], '%d/%m/%Y').date()
                except:
                    pass
            
            # Inserir novo registro com todos os campos obrigatórios
            insert_query = """
                INSERT INTO tb_fat_cidadao (
                    co_seq_fat_cidadao, nu_uuid_ficha, nu_uuid_ficha_origem, st_recusa_cadastro,
                    st_desconhece_mae, co_dim_profissional, co_dim_tipo_ficha, co_dim_municipio,
                    co_dim_unidade_saude, co_dim_equipe, co_dim_tempo, co_dim_validade,
                    co_dim_validade_recusa, co_dim_raca_cor, co_dim_nacionalidade, co_dim_pais_nascimento,
                    nu_uuid_dado_transp, nu_cpf_cidadao, nu_cns, co_dim_sexo, dt_nascimento
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING co_seq_fat_cidadao
            """
            
            self.cur.execute(insert_query, (
                next_id,                    # co_seq_fat_cidadao
                nu_uuid_ficha,             # nu_uuid_ficha
                nu_uuid_ficha_origem,      # nu_uuid_ficha_origem
                0,                         # st_recusa_cadastro
                0,                         # st_desconhece_mae
                2,                         # co_dim_profissional
                2,                         # co_dim_tipo_ficha
                2,                         # co_dim_municipio
                unidade_id,                # co_dim_unidade_saude
                row_data['ine_equipe'],    # co_dim_equipe
                20250915,                  # co_dim_tempo
                30001231,                  # co_dim_validade
                30001231,                  # co_dim_validade_recusa
                4,                         # co_dim_raca_cor
                1,                         # co_dim_nacionalidade
                31,                        # co_dim_pais_nascimento
                nu_uuid_dado_transp,       # nu_uuid_dado_transp
                cpf,                       # nu_cpf_cidadao
                cns,                       # nu_cns
                co_sexo,                   # co_dim_sexo
                dt_nascimento              # dt_nascimento
            ))
            
            fat_cidadao_id = self.cur.fetchone()[0]
            logger.info(f"FAT Cidadão inserido: {row_data['nome']} (ID: {fat_cidadao_id})")
            return fat_cidadao_id
            
        except Exception as e:
            logger.error(f"Erro ao inserir tb_fat_cidadao: {e}")
            return None
    
    def insert_tb_fat_cad_individual(self, row_data, fat_cidadao_pec_id):
        """
        Insere dados na tabela tb_fat_cad_individual com todos os campos obrigatórios
        """
        try:
            cpf, cns = self.extract_cpf_cns(row_data['cpf_cns'])
            unidade_id = self.get_unidade_saude_id(row_data['ine_equipe'])
            
            if not cns or not fat_cidadao_pec_id or not unidade_id:
                return None
            
            # Verificar se já existe
            self.cur.execute("""
                SELECT co_seq_fat_cad_individual 
                FROM tb_fat_cad_individual 
                WHERE nu_cns = %s AND co_fat_cidadao_pec = %s
            """, (cns, fat_cidadao_pec_id))
            
            existing = self.cur.fetchone()
            if existing:
                return existing[0]
            
            # Obter próximo ID
            self.cur.execute("SELECT COALESCE(MAX(co_seq_fat_cad_individual), 0) + 1 FROM tb_fat_cad_individual")
            next_id = self.cur.fetchone()[0]
            
            # Gerar UUIDs baseados na unidade de saúde
            nu_uuid_ficha = self.generate_uuid_with_unidade(unidade_id)
            nu_uuid_ficha_origem = nu_uuid_ficha  # Mesmo valor conforme especificação
            nu_uuid_dado_transp = nu_uuid_ficha  # Mesmo valor conforme especificação
            
            # Processar dados adicionais
            sexo_map = {'Masculino': 0, 'Feminino': 1}
            co_sexo = sexo_map.get(row_data.get('sexo'), 0)  # Default masculino
            
            # Converter data de nascimento
            dt_nascimento = None
            if row_data.get('data_nascimento'):
                try:
                    dt_nascimento = datetime.strptime(row_data['data_nascimento'], '%d/%m/%Y').date()
                except:
                    pass
            
            # Inserir novo registro com todos os campos obrigatórios
            insert_query = """
                INSERT INTO tb_fat_cad_individual (
                    co_seq_fat_cad_individual, nu_uuid_ficha, nu_uuid_ficha_origem, st_recusa_cadastro,
                    st_desconhece_mae, co_dim_profissional, co_dim_tipo_ficha, co_dim_municipio,
                    co_dim_unidade_saude, co_dim_equipe, co_dim_tempo, co_dim_validade,
                    co_dim_validade_recusa, co_dim_raca_cor, co_dim_nacionalidade, co_dim_pais_nascimento,
                    nu_uuid_dado_transp, nu_cpf_cidadao, nu_cns, co_fat_cidadao_pec,
                    co_dim_sexo, dt_nascimento
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING co_seq_fat_cad_individual
            """
            
            self.cur.execute(insert_query, (
                next_id,                    # co_seq_fat_cad_individual
                nu_uuid_ficha,             # nu_uuid_ficha
                nu_uuid_ficha_origem,      # nu_uuid_ficha_origem
                0,                         # st_recusa_cadastro
                0,                         # st_desconhece_mae
                2,                         # co_dim_profissional
                2,                         # co_dim_tipo_ficha
                2,                         # co_dim_municipio
                unidade_id,                # co_dim_unidade_saude
                row_data['ine_equipe'],    # co_dim_equipe
                20250915,                  # co_dim_tempo
                30001231,                  # co_dim_validade
                30001231,                  # co_dim_validade_recusa (corrigido)
                4,                         # co_dim_raca_cor
                1,                         # co_dim_nacionalidade
                31,                        # co_dim_pais_nascimento
                nu_uuid_dado_transp,       # nu_uuid_dado_transp
                cpf,                       # nu_cpf_cidadao
                cns,                       # nu_cns
                fat_cidadao_pec_id,        # co_fat_cidadao_pec
                co_sexo,                   # co_dim_sexo
                dt_nascimento              # dt_nascimento
            ))
            
            fat_cad_id = self.cur.fetchone()[0]
            logger.info(f"FAT Cad Individual inserido: {row_data['nome']} (ID: {fat_cad_id})")
            return fat_cad_id
            
        except Exception as e:
            logger.error(f"Erro ao inserir tb_fat_cad_individual: {e}")
            return None
    
    def process_csv_file(self, csv_file, co_municipio):
        """
        Processa arquivo CSV completo inserindo dados em todas as tabelas
        """
        print(f"Iniciando processamento do arquivo: {csv_file}")
        logger.info(f"Iniciando processamento do arquivo: {csv_file}")
        
        df = self.parse_csv_data(csv_file)
        if df is None:
            print("Erro: Falha ao processar CSV")
            return False
        
        print(f"CSV processado com sucesso: {len(df)} registros")
        
        success_count = 0
        error_count = 0
        
        try:
            for index, row in df.iterrows():
                try:
                    print(f"Processando registro {index + 1}/{len(df)}: {row['nome']}")
                    
                    # 1. Inserir tb_cidadao (base)
                    cidadao_id = self.insert_tb_cidadao(row)
                    if not cidadao_id:
                        print(f"  [ERRO] Falha ao inserir tb_cidadao")
                        error_count += 1
                        continue
                    print(f"  [OK] tb_cidadao inserido (ID: {cidadao_id})")
                    
                    # 2. Inserir tb_fat_cidadao_pec (intermediária)
                    pec_id = self.insert_tb_fat_cidadao_pec(row, cidadao_id)
                    if not pec_id:
                        print(f"  [ERRO] Falha ao inserir tb_fat_cidadao_pec")
                        error_count += 1
                        continue
                    print(f"  [OK] tb_fat_cidadao_pec inserido (ID: {pec_id})")
                    
                    # 3. Inserir tb_fat_cidadao (independente)
                    fat_cidadao_id = self.insert_tb_fat_cidadao(row)
                    if fat_cidadao_id:
                        print(f"  [OK] tb_fat_cidadao inserido (ID: {fat_cidadao_id})")
                    
                    # 4. Inserir tb_fat_cad_individual (principal)
                    fat_cad_id = self.insert_tb_fat_cad_individual(row, pec_id)
                    
                    if fat_cad_id:
                        print(f"  [OK] tb_fat_cad_individual inserido (ID: {fat_cad_id})")
                        success_count += 1
                    else:
                        print(f"  [ERRO] Falha ao inserir tb_fat_cad_individual")
                        error_count += 1
                    
                    # Commit a cada 50 registros
                    if (index + 1) % 50 == 0:
                        self.conn.commit()
                        print(f"Commit realizado - {index + 1} registros processados")
                        logger.info(f"Processados {index + 1} registros...")
                
                except Exception as e:
                    print(f"  [ERRO] Erro ao processar linha {index}: {e}")
                    logger.error(f"Erro ao processar linha {index}: {e}")
                    error_count += 1
                    continue
            
            # Commit final
            self.conn.commit()
            
            print(f"Processamento concluído: {success_count} sucessos, {error_count} erros")
            logger.info(f"Processamento concluído: {success_count} sucessos, {error_count} erros")
            return True
            
        except Exception as e:
            print(f"Erro durante processamento: {e}")
            logger.error(f"Erro durante processamento: {e}")
            self.conn.rollback()
            return False
    
    def close_connection(self):
        """Fecha conexão com banco"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logger.info("Conexão fechada")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrator para tabelas FAT do e-SUS')
    parser.add_argument('--file', required=True, help='Arquivo CSV para processar')
    parser.add_argument('--co-municipio', required=True, help='Código do município')
    
    args = parser.parse_args()
    
    migrator = FATTablesMigrator()
    try:
        success = migrator.process_csv_file(args.file, args.co_municipio)
        if success:
            logger.info("Migração concluída com sucesso!")
        else:
            logger.error("Migração falhou!")
    finally:
        migrator.close_connection()