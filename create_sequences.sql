-- Script para criar as sequências necessárias para as tabelas cds_cad_individual
-- Este script resolve o-- Script para resolver erro de sequences inexistentes
-- Criando todas as sequences necessárias para o sistema de migração

-- Sequence para tb_cds_cad_individual
CREATE SEQUENCE IF NOT EXISTS seq_tb_cds_cad_individual
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Sequence para tl_cds_cad_individual
CREATE SEQUENCE IF NOT EXISTS seq_tl_cds_cad_individual
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Sequence para tb_fat_cad_individual
CREATE SEQUENCE IF NOT EXISTS seq_tb_fat_cad_individual
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Sequence para tb_fat_cidadao
CREATE SEQUENCE IF NOT EXISTS seq_tb_fat_cidadao
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Sequence para tb_fat_cidadao_pec
CREATE SEQUENCE IF NOT EXISTS seq_tb_fat_cidadao_pec
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Sequence para tb_cidadao
CREATE SEQUENCE IF NOT EXISTS seq_tb_cidadao
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Verificar se as sequences foram criadas corretamente
SELECT schemaname, sequencename 
FROM pg_sequences 
WHERE sequencename IN (
    'seq_tb_cds_cad_individual',
    'seq_tl_cds_cad_individual', 
    'seq_tb_fat_cad_individual',
    'seq_tb_fat_cidadao',
    'seq_tb_fat_cidadao_pec',
    'seq_tb_cidadao'
)
ORDER BY sequencename;