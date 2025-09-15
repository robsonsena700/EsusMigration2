-- Script para criar as sequências necessárias para as tabelas cds_cad_individual
-- Este script resolve o erro: relation "seq_tb_cds_cad_individual" does not exist

-- Criar sequência para tb_cds_cad_individual
CREATE SEQUENCE IF NOT EXISTS seq_tb_cds_cad_individual
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Criar sequência para tl_cds_cad_individual  
CREATE SEQUENCE IF NOT EXISTS seq_tl_cds_cad_individual
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;