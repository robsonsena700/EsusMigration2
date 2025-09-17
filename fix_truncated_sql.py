#!/usr/bin/env python3
"""
Script para verificar e corrigir comandos INSERT truncados na tabela tb_cds_cad_individual
"""

import os
import re
import sys
from pathlib import Path

def find_truncated_inserts(directory):
    """Encontra arquivos SQL com comandos INSERT truncados"""
    truncated_files = []
    sql_files = Path(directory).rglob("*.sql")
    
    for sql_file in sql_files:
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Procura por INSERT INTO com nome de tabela truncado
            pattern = r'INSERT\s+INTO\s+public\.tb_cds_cad_individu(?![a-l])'
            matches = re.findall(pattern, content, re.IGNORECASE)
            
            if matches:
                truncated_files.append({
                    'file': sql_file,
                    'matches': len(matches)
                })
                print(f"❌ Encontrado comando truncado em: {sql_file}")
                print(f"   Número de ocorrências: {len(matches)}")
                
        except Exception as e:
            print(f"⚠️  Erro ao ler arquivo {sql_file}: {e}")
    
    return truncated_files

def fix_truncated_inserts(file_path):
    """Corrige comandos INSERT truncados em um arquivo específico"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Backup do arquivo original
        backup_path = f"{file_path}.backup"
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Backup criado: {backup_path}")
        
        # Corrige o nome da tabela truncado
        pattern = r'INSERT\s+INTO\s+public\.tb_cds_cad_individu(?![a-l])'
        fixed_content = re.sub(pattern, 'INSERT INTO public.tb_cds_cad_individual', content, flags=re.IGNORECASE)
        
        # Verifica se houve mudanças
        if fixed_content != content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(fixed_content)
            print(f"✅ Arquivo corrigido: {file_path}")
            return True
        else:
            print(f"ℹ️  Nenhuma correção necessária em: {file_path}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao corrigir arquivo {file_path}: {e}")
        return False

def validate_sql_syntax(file_path):
    """Valida a sintaxe SQL básica do arquivo"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Verifica se há comandos INSERT incompletos
        lines = content.split('\n')
        for i, line in enumerate(lines, 1):
            line = line.strip()
            if line.startswith('INSERT INTO') and not line.endswith(';'):
                # Verifica se é realmente incompleto (não é apenas quebra de linha)
                remaining_lines = lines[i:]
                complete_statement = line
                for next_line in remaining_lines:
                    complete_statement += ' ' + next_line.strip()
                    if next_line.strip().endswith(';'):
                        break
                else:
                    print(f"❌ Comando INSERT incompleto na linha {i}: {line}")
                    return False
        
        print(f"✅ Sintaxe SQL válida: {file_path}")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao validar arquivo {file_path}: {e}")
        return False

def main():
    base_dir = "D:\\Robson\\Projetos\\Cascavel"
    
    print("🔍 Procurando por comandos INSERT truncados...")
    truncated_files = find_truncated_inserts(base_dir)
    
    if not truncated_files:
        print("✅ Nenhum comando INSERT truncado encontrado!")
        return
    
    print(f"\n📋 Encontrados {len(truncated_files)} arquivos com problemas:")
    for file_info in truncated_files:
        print(f"   - {file_info['file']} ({file_info['matches']} ocorrências)")
    
    # Pergunta se deve corrigir
    response = input("\n🔧 Deseja corrigir os arquivos automaticamente? (s/n): ")
    if response.lower() in ['s', 'sim', 'y', 'yes']:
        print("\n🔧 Corrigindo arquivos...")
        for file_info in truncated_files:
            if fix_truncated_inserts(file_info['file']):
                validate_sql_syntax(file_info['file'])
        print("\n✅ Correção concluída!")
    else:
        print("ℹ️  Correção cancelada pelo usuário.")

if __name__ == "__main__":
    main()