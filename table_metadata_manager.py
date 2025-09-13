import os
import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Any
from analise_table import TableAnalyzer
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

class TableMetadataManager:
    def __init__(self, db_path: str = None):
        if not db_path:
            db_path = os.path.join(os.getcwd(), 'table_metadata.db')
        
        self.db_path = db_path
        self.table_analyzer = TableAnalyzer()
        self.init_database()
    
    def init_database(self):
        """Inicializa o banco de dados SQLite para persistir metadados"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Tabela para armazenar informações das tabelas
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS table_info (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        schema_name TEXT NOT NULL,
                        table_name TEXT NOT NULL,
                        total_columns INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(schema_name, table_name)
                    )
                ''')
                
                # Tabela para armazenar informações das colunas
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS column_info (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        table_id INTEGER,
                        column_name TEXT NOT NULL,
                        data_type TEXT NOT NULL,
                        max_length INTEGER,
                        numeric_precision INTEGER,
                        numeric_scale INTEGER,
                        is_nullable BOOLEAN,
                        column_default TEXT,
                        ordinal_position INTEGER,
                        description TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (table_id) REFERENCES table_info (id),
                        UNIQUE(table_id, column_name)
                    )
                ''')
                
                # Tabela para armazenar constraints
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS constraint_info (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        table_id INTEGER,
                        constraint_name TEXT NOT NULL,
                        constraint_type TEXT NOT NULL,
                        column_name TEXT,
                        foreign_table_name TEXT,
                        foreign_column_name TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (table_id) REFERENCES table_info (id)
                    )
                ''')
                
                # Tabela para armazenar referências de foreign keys
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS foreign_key_references (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        table_id INTEGER,
                        column_name TEXT NOT NULL,
                        foreign_schema TEXT NOT NULL,
                        foreign_table TEXT NOT NULL,
                        foreign_column TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (table_id) REFERENCES table_info (id)
                    )
                ''')
                
                # Tabela para armazenar análises completas (JSON)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS table_analysis (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        table_id INTEGER,
                        analysis_data TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (table_id) REFERENCES table_info (id)
                    )
                ''')
                
                conn.commit()
                print(f"✅ Banco de metadados inicializado: {self.db_path}")
                
        except Exception as e:
            print(f"❌ Erro ao inicializar banco de metadados: {e}")
    
    def save_table_analysis(self, schema: str, table_name: str, force_update: bool = False) -> bool:
        """Salva análise completa de uma tabela"""
        try:
            # Obter análise da tabela
            analysis = self.table_analyzer.analyze_table(table_name, schema)
            if not analysis:
                print(f"❌ Falha ao analisar tabela {schema}.{table_name}")
                return False
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Verificar se tabela já existe
                cursor.execute(
                    'SELECT id FROM table_info WHERE schema_name = ? AND table_name = ?',
                    (schema, table_name)
                )
                existing = cursor.fetchone()
                
                if existing and not force_update:
                    print(f"⚠️ Tabela {schema}.{table_name} já existe no banco de metadados")
                    return True
                
                if existing:
                    table_id = existing[0]
                    # Atualizar informações da tabela
                    cursor.execute('''
                        UPDATE table_info 
                        SET total_columns = ?, updated_at = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    ''', (len(analysis['structure']), table_id))
                    
                    # Limpar dados antigos
                    cursor.execute('DELETE FROM column_info WHERE table_id = ?', (table_id,))
                    cursor.execute('DELETE FROM constraint_info WHERE table_id = ?', (table_id,))
                    cursor.execute('DELETE FROM foreign_key_references WHERE table_id = ?', (table_id,))
                    cursor.execute('DELETE FROM table_analysis WHERE table_id = ?', (table_id,))
                    
                else:
                    # Inserir nova tabela
                    cursor.execute('''
                        INSERT INTO table_info (schema_name, table_name, total_columns)
                        VALUES (?, ?, ?)
                    ''', (schema, table_name, len(analysis['structure'])))
                    table_id = cursor.lastrowid
                
                # Inserir informações das colunas
                for col in analysis['structure']:
                    cursor.execute('''
                        INSERT INTO column_info (
                            table_id, column_name, data_type, max_length, 
                            numeric_precision, numeric_scale, is_nullable, 
                            column_default, ordinal_position, description
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        table_id, col['field'], col['type'], col['max_length'],
                        col['numeric_precision'], col['numeric_scale'], 
                        not col['not_null'], col['default'], col['position'], 
                        col['description']
                    ))
                
                # Inserir constraints
                for constraint in analysis['constraints']:
                    cursor.execute('''
                        INSERT INTO constraint_info (
                            table_id, constraint_name, constraint_type, 
                            column_name, foreign_table_name, foreign_column_name
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        table_id, constraint['name'], constraint['type'],
                        constraint['column'], constraint['foreign_table'],
                        constraint['foreign_column']
                    ))
                
                # Inserir referências de foreign keys
                for ref in analysis['foreign_key_references']:
                    cursor.execute('''
                        INSERT INTO foreign_key_references (
                            table_id, column_name, foreign_schema, 
                            foreign_table, foreign_column
                        ) VALUES (?, ?, ?, ?, ?)
                    ''', (
                        table_id, ref['column'], ref['foreign_schema'],
                        ref['foreign_table'], ref['foreign_column']
                    ))
                
                # Inserir análise completa como JSON
                cursor.execute('''
                    INSERT INTO table_analysis (table_id, analysis_data)
                    VALUES (?, ?)
                ''', (table_id, json.dumps(analysis, default=str)))
                
                conn.commit()
                print(f"✅ Análise da tabela {schema}.{table_name} salva com sucesso")
                return True
                
        except Exception as e:
            print(f"❌ Erro ao salvar análise da tabela: {e}")
            return False
    
    def get_table_metadata(self, schema: str, table_name: str) -> Optional[Dict[str, Any]]:
        """Recupera metadados de uma tabela"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Buscar informações da tabela
                cursor.execute('''
                    SELECT id, total_columns, created_at, updated_at
                    FROM table_info 
                    WHERE schema_name = ? AND table_name = ?
                ''', (schema, table_name))
                
                table_info = cursor.fetchone()
                if not table_info:
                    return None
                
                table_id, total_columns, created_at, updated_at = table_info
                
                # Buscar informações das colunas
                cursor.execute('''
                    SELECT column_name, data_type, max_length, numeric_precision,
                           numeric_scale, is_nullable, column_default, 
                           ordinal_position, description
                    FROM column_info 
                    WHERE table_id = ?
                    ORDER BY ordinal_position
                ''', (table_id,))
                
                columns = []
                for row in cursor.fetchall():
                    columns.append({
                        'field': row[0],
                        'type': row[1],
                        'max_length': row[2],
                        'numeric_precision': row[3],
                        'numeric_scale': row[4],
                        'not_null': not row[5],
                        'default': row[6],
                        'position': row[7],
                        'description': row[8] or ''
                    })
                
                # Buscar constraints
                cursor.execute('''
                    SELECT constraint_name, constraint_type, column_name,
                           foreign_table_name, foreign_column_name
                    FROM constraint_info 
                    WHERE table_id = ?
                ''', (table_id,))
                
                constraints = []
                for row in cursor.fetchall():
                    constraints.append({
                        'name': row[0],
                        'type': row[1],
                        'column': row[2],
                        'foreign_table': row[3],
                        'foreign_column': row[4]
                    })
                
                # Buscar referências de foreign keys
                cursor.execute('''
                    SELECT column_name, foreign_schema, foreign_table, foreign_column
                    FROM foreign_key_references 
                    WHERE table_id = ?
                ''', (table_id,))
                
                references = []
                for row in cursor.fetchall():
                    references.append({
                        'column': row[0],
                        'foreign_schema': row[1],
                        'foreign_table': row[2],
                        'foreign_column': row[3]
                    })
                
                return {
                    'table_name': table_name,
                    'schema': schema,
                    'total_columns': total_columns,
                    'created_at': created_at,
                    'updated_at': updated_at,
                    'structure': columns,
                    'constraints': constraints,
                    'foreign_key_references': references
                }
                
        except Exception as e:
            print(f"❌ Erro ao recuperar metadados da tabela: {e}")
            return None
    
    def list_stored_tables(self) -> List[Dict[str, Any]]:
        """Lista todas as tabelas armazenadas"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT schema_name, table_name, total_columns, 
                           created_at, updated_at
                    FROM table_info 
                    ORDER BY schema_name, table_name
                ''')
                
                tables = []
                for row in cursor.fetchall():
                    tables.append({
                        'schema': row[0],
                        'table_name': row[1],
                        'total_columns': row[2],
                        'created_at': row[3],
                        'updated_at': row[4]
                    })
                
                return tables
                
        except Exception as e:
            print(f"❌ Erro ao listar tabelas: {e}")
            return []
    
    def get_foreign_key_references_for_table(self, schema: str, table_name: str) -> List[Dict[str, Any]]:
        """Obtém todas as referências de foreign keys para uma tabela específica"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT fkr.column_name, fkr.foreign_schema, 
                           fkr.foreign_table, fkr.foreign_column,
                           ci.description
                    FROM foreign_key_references fkr
                    JOIN table_info ti ON fkr.table_id = ti.id
                    LEFT JOIN column_info ci ON fkr.table_id = ci.table_id 
                                              AND fkr.column_name = ci.column_name
                    WHERE ti.schema_name = ? AND ti.table_name = ?
                ''', (schema, table_name))
                
                references = []
                for row in cursor.fetchall():
                    references.append({
                        'column': row[0],
                        'foreign_schema': row[1],
                        'foreign_table': row[2],
                        'foreign_column': row[3],
                        'description': row[4] or ''
                    })
                
                return references
                
        except Exception as e:
            print(f"❌ Erro ao buscar referências de foreign keys: {e}")
            return []
    
    def export_metadata_to_json(self, output_file: str = None) -> bool:
        """Exporta todos os metadados para um arquivo JSON"""
        try:
            tables = self.list_stored_tables()
            export_data = {
                'export_timestamp': datetime.now().isoformat(),
                'total_tables': len(tables),
                'tables': []
            }
            
            for table_info in tables:
                schema = table_info['schema']
                table_name = table_info['table_name']
                
                metadata = self.get_table_metadata(schema, table_name)
                if metadata:
                    export_data['tables'].append(metadata)
            
            if not output_file:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = f"table_metadata_export_{timestamp}.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"✅ Metadados exportados para: {output_file}")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao exportar metadados: {e}")
            return False
    
    def analyze_and_store_multiple_tables(self, table_list: List[Tuple[str, str]]) -> Dict[str, bool]:
        """Analisa e armazena múltiplas tabelas"""
        results = {}
        
        for schema, table_name in table_list:
            print(f"\n🔍 Analisando {schema}.{table_name}...")
            success = self.save_table_analysis(schema, table_name)
            results[f"{schema}.{table_name}"] = success
        
        return results

def main():
    """Função principal para teste"""
    manager = TableMetadataManager()
    
    # Exemplo: analisar e armazenar tabela principal
    schema = "public"
    table_name = "tl_cds_cad_individual"
    
    print(f"🔍 Analisando e armazenando metadados da tabela {schema}.{table_name}...")
    
    success = manager.save_table_analysis(schema, table_name, force_update=True)
    
    if success:
        print(f"\n📋 Recuperando metadados armazenados...")
        metadata = manager.get_table_metadata(schema, table_name)
        
        if metadata:
            print(f"✅ Metadados recuperados:")
            print(f"   📊 Total de colunas: {metadata['total_columns']}")
            print(f"   🔑 Foreign keys: {len(metadata['foreign_key_references'])}")
            print(f"   🔒 Constraints: {len(metadata['constraints'])}")
        
        # Listar todas as tabelas armazenadas
        print(f"\n📚 Tabelas armazenadas:")
        tables = manager.list_stored_tables()
        for table in tables:
            print(f"   • {table['schema']}.{table['table_name']} ({table['total_columns']} colunas)")
        
        # Exportar metadados
        print(f"\n💾 Exportando metadados...")
        manager.export_metadata_to_json()
    
    else:
        print(f"❌ Falha ao analisar tabela")

if __name__ == "__main__":
    main()