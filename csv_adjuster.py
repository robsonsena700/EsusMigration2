import os
import csv
import json
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from analise_table import TableAnalyzer
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

class CSVAdjuster:
    def __init__(self):
        self.table_analyzer = TableAnalyzer()
        self.table_structure = {}
        self.column_mapping = {
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
        
    def load_table_structure(self, table_name: str, schema: str = 'public') -> bool:
        """Carrega a estrutura da tabela usando o TableAnalyzer"""
        try:
            analysis = self.table_analyzer.analyze_table(table_name, schema)
            if analysis and 'structure' in analysis:
                # Converter para formato mais fácil de usar
                self.table_structure = {}
                for col in analysis['structure']:
                    self.table_structure[col['field']] = {
                        'type': col['type'],
                        'max_length': col['max_length'],
                        'not_null': col['not_null'],
                        'default': col['default'],
                        'description': col['description']
                    }
                print(f"✅ Estrutura da tabela {schema}.{table_name} carregada com {len(self.table_structure)} colunas")
                return True
            else:
                print(f"❌ Falha ao carregar estrutura da tabela {schema}.{table_name}")
                return False
        except Exception as e:
            print(f"❌ Erro ao carregar estrutura da tabela: {e}")
            return False
    
    def detect_csv_encoding(self, file_path: str) -> str:
        """Detecta a codificação do arquivo CSV"""
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    f.read(1024)
                return encoding
            except UnicodeDecodeError:
                continue
        
        return 'utf-8'
    
    def find_csv_header_row(self, file_path: str, encoding: str) -> int:
        """Encontra a linha que contém o cabeçalho do CSV"""
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                for i, line in enumerate(f):
                    # Procura por linha que contém separadores típicos de CSV
                    if ';' in line and any(keyword in line.lower() for keyword in 
                                         ['nome', 'cpf', 'data', 'telefone', 'equipe']):
                        return i
            return 0  # Fallback para primeira linha
        except Exception as e:
            print(f"❌ Erro ao encontrar cabeçalho: {e}")
            return 0
    
    def clean_phone_number(self, phone: str) -> str:
        """Limpa e formata número de telefone"""
        if not phone or phone.strip() in ['-', '']:
            return ''
        
        # Remove caracteres não numéricos
        cleaned = re.sub(r'[^0-9]', '', phone)
        
        # Limita a 11 dígitos (padrão brasileiro)
        if len(cleaned) > 11:
            cleaned = cleaned[-11:]
        
        return cleaned
    
    def clean_cpf(self, cpf: str) -> str:
        """Limpa e valida CPF"""
        if not cpf or cpf.strip() in ['-', '']:
            return ''
        
        # Remove caracteres não numéricos
        cleaned = re.sub(r'[^0-9]', '', cpf)
        
        # CPF deve ter exatamente 11 dígitos
        if len(cleaned) == 11:
            return cleaned
        
        return ''
    
    def parse_date(self, date_str: str) -> str:
        """Converte data para formato PostgreSQL"""
        if not date_str or date_str.strip() in ['-', '']:
            return ''
        
        # Formatos possíveis: dd/mm/yyyy, dd/mm/yy
        date_patterns = [
            r'(\d{2})/(\d{2})/(\d{4})',
            r'(\d{2})/(\d{2})/(\d{2})'
        ]
        
        for pattern in date_patterns:
            match = re.match(pattern, date_str.strip())
            if match:
                day, month, year = match.groups()
                
                # Ajustar ano de 2 dígitos
                if len(year) == 2:
                    year_int = int(year)
                    if year_int > 50:  # Assumir 1900s
                        year = f"19{year}"
                    else:  # Assumir 2000s
                        year = f"20{year}"
                
                try:
                    # Validar data
                    datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
                    return f"{year}-{month}-{day}"
                except ValueError:
                    continue
        
        return ''
    
    def convert_sex_to_code(self, sex: str) -> str:
        """Converte sexo para código numérico"""
        if not sex or sex.strip() in ['-', '']:
            return ''
        
        sex_mapping = {
            'masculino': '1',
            'feminino': '2',
            'male': '1',
            'female': '2',
            'm': '1',
            'f': '2'
        }
        
        return sex_mapping.get(sex.lower().strip(), '')
    
    def truncate_text(self, text: str, max_length: int) -> str:
        """Trunca texto para o tamanho máximo permitido"""
        if not text or not max_length:
            return text or ''
        
        if len(text) > max_length:
            return text[:max_length]
        
        return text
    
    def validate_and_adjust_value(self, value: str, column_name: str, field_info: Dict) -> str:
        """Valida e ajusta valor baseado na estrutura da tabela"""
        if not value or value.strip() in ['-', '']:
            return ''
        
        field_type = field_info.get('type', '')
        max_length = field_info.get('max_length')
        
        # Ajustes específicos por tipo de campo
        if 'character varying' in field_type or 'varchar' in field_type:
            adjusted = self.truncate_text(value.strip(), max_length)
            
            # Ajustes específicos por nome do campo
            if 'cpf' in column_name.lower():
                adjusted = self.clean_cpf(adjusted)
            elif 'telefone' in column_name.lower() or 'celular' in column_name.lower():
                adjusted = self.clean_phone_number(adjusted)
            elif 'sexo' in column_name.lower():
                adjusted = self.convert_sex_to_code(adjusted)
            
            return adjusted
            
        elif field_type in ['date', 'timestamp']:
            if 'nascimento' in column_name.lower() or 'data' in column_name.lower():
                return self.parse_date(value)
            return value.strip()
            
        elif field_type in ['integer', 'bigint']:
            # Extrair apenas números
            numeric = re.sub(r'[^0-9]', '', value)
            return numeric if numeric else ''
            
        else:
            return value.strip()
    
    def adjust_csv_file(self, input_file: str, output_file: str, table_name: str, 
                       schema: str = 'public', skip_rows: int = None) -> Dict[str, Any]:
        """Ajusta arquivo CSV baseado na estrutura da tabela"""
        
        # Carregar estrutura da tabela
        if not self.load_table_structure(table_name, schema):
            return {'success': False, 'error': 'Falha ao carregar estrutura da tabela'}
        
        # Detectar codificação
        encoding = self.detect_csv_encoding(input_file)
        print(f"📄 Codificação detectada: {encoding}")
        
        # Encontrar linha do cabeçalho se skip_rows não foi especificado
        if skip_rows is None:
            skip_rows = self.find_csv_header_row(input_file, encoding)
            print(f"📍 Cabeçalho encontrado na linha: {skip_rows + 1}")
        
        try:
            adjusted_rows = []
            errors = []
            total_rows = 0
            adjusted_count = 0
            
            with open(input_file, 'r', encoding=encoding, errors='replace') as f:
                # Pular linhas até o cabeçalho
                for _ in range(skip_rows):
                    next(f, None)
                
                reader = csv.DictReader(f, delimiter=';')
                csv_columns = reader.fieldnames or []
                
                print(f"📋 Colunas encontradas no CSV: {len(csv_columns)}")
                
                # Mapear colunas CSV para colunas da tabela
                mapped_columns = {}
                valid_columns = []
                
                for csv_col in csv_columns:
                    if csv_col in self.column_mapping:
                        db_col = self.column_mapping[csv_col]
                        if db_col in self.table_structure:
                            mapped_columns[csv_col] = db_col
                            valid_columns.append(csv_col)
                
                print(f"✅ Colunas mapeadas: {len(mapped_columns)}")
                
                # Processar cada linha
                for row_num, row in enumerate(reader, 1):
                    total_rows += 1
                    adjusted_row = {}
                    row_errors = []
                    row_adjusted = False
                    
                    for csv_col, db_col in mapped_columns.items():
                        original_value = row.get(csv_col, '')
                        field_info = self.table_structure[db_col]
                        
                        adjusted_value = self.validate_and_adjust_value(
                            original_value, db_col, field_info
                        )
                        
                        adjusted_row[csv_col] = adjusted_value
                        
                        # Verificar se houve ajuste
                        if original_value != adjusted_value:
                            row_adjusted = True
                        
                        # Verificar se campo obrigatório está vazio
                        if field_info.get('not_null') and not adjusted_value:
                            row_errors.append(f"Campo obrigatório '{csv_col}' está vazio")
                    
                    if row_adjusted:
                        adjusted_count += 1
                    
                    if row_errors:
                        errors.append({
                            'row': row_num,
                            'errors': row_errors
                        })
                    
                    adjusted_rows.append(adjusted_row)
            
            # Salvar arquivo ajustado
            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                if adjusted_rows:
                    writer = csv.DictWriter(f, fieldnames=valid_columns, delimiter=';')
                    writer.writeheader()
                    writer.writerows(adjusted_rows)
            
            result = {
                'success': True,
                'input_file': input_file,
                'output_file': output_file,
                'total_rows': total_rows,
                'adjusted_rows': adjusted_count,
                'errors': errors,
                'columns_mapped': len(mapped_columns),
                'encoding': encoding,
                'skip_rows': skip_rows
            }
            
            print(f"\n✅ Ajuste concluído:")
            print(f"   📄 Arquivo de entrada: {input_file}")
            print(f"   📄 Arquivo de saída: {output_file}")
            print(f"   📊 Total de linhas: {total_rows}")
            print(f"   🔧 Linhas ajustadas: {adjusted_count}")
            print(f"   ❌ Erros encontrados: {len(errors)}")
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Erro ao processar arquivo: {str(e)}"
            }
    
    def generate_adjustment_report(self, result: Dict[str, Any], report_file: str = None):
        """Gera relatório detalhado do ajuste"""
        if not result.get('success'):
            print(f"❌ Não é possível gerar relatório: {result.get('error')}")
            return
        
        if not report_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = f"csv_adjustment_report_{timestamp}.json"
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"\n📋 Relatório salvo em: {report_file}")
            
        except Exception as e:
            print(f"❌ Erro ao salvar relatório: {e}")

def main():
    """Função principal para teste"""
    adjuster = CSVAdjuster()
    
    # Configurações
    input_file = "d:\\Robson\\Projetos\\Cascavel\\datacsv\\Aguas Belas-2025-09-10-11-07.csv"
    output_file = "d:\\Robson\\Projetos\\Cascavel\\datacsv\\Aguas_Belas_adjusted.csv"
    table_name = "tl_cds_cad_individual"
    schema = "public"
    
    print(f"🔧 Iniciando ajuste do arquivo CSV...")
    print(f"📄 Entrada: {input_file}")
    print(f"📄 Saída: {output_file}")
    print(f"🗃️ Tabela: {schema}.{table_name}")
    
    # Executar ajuste
    result = adjuster.adjust_csv_file(input_file, output_file, table_name, schema)
    
    # Gerar relatório
    if result.get('success'):
        adjuster.generate_adjustment_report(result)
    else:
        print(f"❌ Falha no ajuste: {result.get('error')}")

if __name__ == "__main__":
    main()