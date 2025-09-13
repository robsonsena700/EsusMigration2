import os
import psycopg2
import json
from dotenv import load_dotenv
from typing import Dict, List, Tuple, Optional

# Carregar variáveis de ambiente
load_dotenv()

class TableAnalyzer:
    def __init__(self):
        self.connection = None
        self.cursor = None
        
    def connect_database(self) -> bool:
        """Conecta ao banco de dados PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST'),
                port=os.getenv('POSTGRES_PORT'),
                database=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD')
            )
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            print(f"❌ Erro ao conectar ao banco: {e}")
            return False
    
    def disconnect_database(self):
        """Desconecta do banco de dados"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def get_table_structure(self, table_name: str, schema: str = 'public') -> List[Dict]:
        """Obtém a estrutura completa de uma tabela"""
        if not self.cursor:
            return []
        
        query = """
        SELECT 
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            c.ordinal_position,
            pgd.description
        FROM information_schema.columns c
        LEFT JOIN pg_catalog.pg_statio_all_tables st 
            ON c.table_schema = st.schemaname 
            AND c.table_name = st.relname
        LEFT JOIN pg_catalog.pg_description pgd 
            ON pgd.objoid = st.relid 
            AND pgd.objsubid = c.ordinal_position
        WHERE c.table_schema = %s 
        AND c.table_name = %s
        ORDER BY c.ordinal_position;
        """
        
        try:
            self.cursor.execute(query, (schema, table_name))
            columns = self.cursor.fetchall()
            
            structure = []
            for col in columns:
                column_info = {
                    'field': col[0],
                    'type': col[1],
                    'max_length': col[2],
                    'numeric_precision': col[3],
                    'numeric_scale': col[4],
                    'not_null': col[5] == 'NO',
                    'default': col[6],
                    'position': col[7],
                    'description': col[8] or ''
                }
                structure.append(column_info)
            
            return structure
            
        except Exception as e:
            print(f"❌ Erro ao obter estrutura da tabela {schema}.{table_name}: {e}")
            return []
    
    def get_table_constraints(self, table_name: str, schema: str = 'public') -> List[Dict]:
        """Obtém as constraints de uma tabela"""
        if not self.cursor:
            return []
        
        query = """
        SELECT 
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints tc
        LEFT JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
        LEFT JOIN information_schema.constraint_column_usage ccu 
            ON tc.constraint_name = ccu.constraint_name
        WHERE tc.table_schema = %s 
        AND tc.table_name = %s;
        """
        
        try:
            self.cursor.execute(query, (schema, table_name))
            constraints = self.cursor.fetchall()
            
            constraint_list = []
            for constraint in constraints:
                constraint_info = {
                    'name': constraint[0],
                    'type': constraint[1],
                    'column': constraint[2],
                    'foreign_table': constraint[3],
                    'foreign_column': constraint[4]
                }
                constraint_list.append(constraint_info)
            
            return constraint_list
            
        except Exception as e:
            print(f"❌ Erro ao obter constraints da tabela {schema}.{table_name}: {e}")
            return []
    
    def get_foreign_key_references(self, table_name: str, schema: str = 'public') -> List[Dict]:
        """Obtém as tabelas de referência (foreign keys)"""
        if not self.cursor:
            return []
        
        query = """
        SELECT
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = %s
        AND tc.table_name = %s;
        """
        
        try:
            self.cursor.execute(query, (schema, table_name))
            references = self.cursor.fetchall()
            
            reference_list = []
            for ref in references:
                reference_info = {
                    'column': ref[0],
                    'foreign_schema': ref[1],
                    'foreign_table': ref[2],
                    'foreign_column': ref[3]
                }
                reference_list.append(reference_info)
            
            return reference_list
            
        except Exception as e:
            print(f"❌ Erro ao obter referências da tabela {schema}.{table_name}: {e}")
            return []
    
    def analyze_table(self, table_name: str, schema: str = 'public') -> Dict:
        """Análise completa de uma tabela"""
        if not self.connect_database():
            return {}
        
        try:
            # Obter estrutura da tabela
            structure = self.get_table_structure(table_name, schema)
            
            # Obter constraints
            constraints = self.get_table_constraints(table_name, schema)
            
            # Obter referências de foreign keys
            references = self.get_foreign_key_references(table_name, schema)
            
            analysis = {
                'table_name': table_name,
                'schema': schema,
                'structure': structure,
                'constraints': constraints,
                'foreign_key_references': references,
                'analysis_summary': self._generate_summary(structure, constraints, references)
            }
            
            return analysis
            
        finally:
            self.disconnect_database()
    
    def _generate_summary(self, structure: List[Dict], constraints: List[Dict], references: List[Dict]) -> Dict:
        """Gera um resumo da análise"""
        summary = {
            'total_columns': len(structure),
            'nullable_columns': len([col for col in structure if not col['not_null']]),
            'columns_with_default': len([col for col in structure if col['default']]),
            'varchar_fields': [],
            'numeric_fields': [],
            'primary_keys': [],
            'foreign_keys': [],
            'unique_constraints': []
        }
        
        # Analisar tipos de campos
        for col in structure:
            if 'character varying' in col['type'] or 'varchar' in col['type']:
                summary['varchar_fields'].append({
                    'field': col['field'],
                    'max_length': col['max_length']
                })
            elif col['type'] in ['integer', 'bigint', 'numeric', 'decimal']:
                summary['numeric_fields'].append({
                    'field': col['field'],
                    'type': col['type'],
                    'precision': col['numeric_precision'],
                    'scale': col['numeric_scale']
                })
        
        # Analisar constraints
        for constraint in constraints:
            if constraint['type'] == 'PRIMARY KEY':
                summary['primary_keys'].append(constraint['column'])
            elif constraint['type'] == 'FOREIGN KEY':
                summary['foreign_keys'].append({
                    'column': constraint['column'],
                    'references': f"{constraint['foreign_table']}.{constraint['foreign_column']}"
                })
            elif constraint['type'] == 'UNIQUE':
                summary['unique_constraints'].append(constraint['column'])
        
        return summary
    
    def print_analysis(self, analysis: Dict):
        """Imprime a análise de forma formatada"""
        if not analysis:
            print("❌ Nenhuma análise disponível")
            return
        
        table_name = analysis['table_name']
        schema = analysis['schema']
        structure = analysis['structure']
        summary = analysis['analysis_summary']
        
        print("\n" + "="*80)
        print(f"🔍 ANÁLISE DA TABELA {schema}.{table_name}")
        print("="*80)
        
        print(f"\n📊 RESUMO:")
        print(f"   Total de colunas: {summary['total_columns']}")
        print(f"   Colunas que permitem NULL: {summary['nullable_columns']}")
        print(f"   Colunas com valor padrão: {summary['columns_with_default']}")
        
        print(f"\n🔑 CHAVES:")
        if summary['primary_keys']:
            print(f"   Primary Keys: {', '.join(summary['primary_keys'])}")
        if summary['foreign_keys']:
            print(f"   Foreign Keys:")
            for fk in summary['foreign_keys']:
                print(f"     • {fk['column']} → {fk['references']}")
        
        print(f"\n📋 ESTRUTURA DETALHADA:")
        print("-"*80)
        print(f"{'CAMPO':<30} {'TIPO':<20} {'TAMANHO':<10} {'NULL':<8} {'PADRÃO':<15}")
        print("-"*80)
        
        for col in structure:
            field = col['field']
            data_type = col['type']
            max_length = str(col['max_length']) if col['max_length'] else 'N/A'
            nullable = 'NÃO' if col['not_null'] else 'SIM'
            default = str(col['default'])[:15] if col['default'] else 'N/A'
            
            print(f"{field:<30} {data_type:<20} {max_length:<10} {nullable:<8} {default:<15}")
            
            if col['description']:
                print(f"{'':>30} 💬 {col['description']}")
        
        if summary['varchar_fields']:
            print(f"\n📝 CAMPOS VARCHAR:")
            for field in summary['varchar_fields']:
                print(f"   • {field['field']}: máximo {field['max_length']} caracteres")
    
    def save_analysis_to_file(self, analysis: Dict, filename: str = None):
        """Salva a análise em um arquivo JSON"""
        if not analysis:
            return
        
        if not filename:
            table_name = analysis['table_name']
            schema = analysis['schema']
            filename = f"analysis_{schema}_{table_name}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(analysis, f, indent=2, ensure_ascii=False, default=str)
            print(f"\n💾 Análise salva em: {filename}")
        except Exception as e:
            print(f"❌ Erro ao salvar análise: {e}")

def main():
    """Função principal para teste"""
    analyzer = TableAnalyzer()
    
    # Exemplo de uso
    table_name = "tl_cds_cad_individual"
    schema = "public"
    
    print(f"🔍 Analisando tabela {schema}.{table_name}...")
    
    analysis = analyzer.analyze_table(table_name, schema)
    
    if analysis:
        analyzer.print_analysis(analysis)
        analyzer.save_analysis_to_file(analysis)
    else:
        print("❌ Falha na análise da tabela")

if __name__ == "__main__":
    main()