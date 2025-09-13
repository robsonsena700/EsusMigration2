import os
import psycopg2
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

def check_table_structure():
    try:
        # Conectar ao banco
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        
        cur = conn.cursor()
        
        # Buscar informações detalhadas sobre co_seq_cds_cad_individual
        cur.execute("""
            SELECT 
                column_name, 
                data_type, 
                character_maximum_length, 
                is_nullable, 
                column_default,
                ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'tl_cds_cad_individual' 
            AND table_schema = 'public'
            AND column_name = 'co_seq_cds_cad_individual'
            ORDER BY ordinal_position;
        """)
        
        seq_field = cur.fetchall()
        
        print("\n" + "="*80)
        print("🔍 INFORMAÇÕES DO CAMPO co_seq_cds_cad_individual:")
        print("="*80)
        
        if seq_field:
            for col in seq_field:
                column_name, data_type, max_length, nullable, default_val, position = col
                max_len_str = str(max_length) if max_length else 'N/A'
                nullable_str = "SIM" if nullable == 'YES' else "NÃO"
                default_str = str(default_val) if default_val else 'N/A'
                
                print(f"📋 Campo: {column_name}")
                print(f"   Tipo: {data_type}")
                print(f"   Tamanho máximo: {max_len_str}")
                print(f"   Permite NULL: {nullable_str}")
                print(f"   Valor padrão: {default_str}")
                print(f"   Posição: {position}")
        else:
            print("❌ Campo co_seq_cds_cad_individual não encontrado!")
        
        # Verificar se há sequências associadas
        cur.execute("""
            SELECT 
                s.sequence_name,
                s.data_type,
                s.start_value,
                s.increment,
                s.maximum_value,
                s.cycle_option
            FROM information_schema.sequences s
            WHERE s.sequence_schema = 'public'
            AND s.sequence_name LIKE '%cad_individual%';
        """)
        
        sequences = cur.fetchall()
        
        print("\n🔄 SEQUÊNCIAS RELACIONADAS:")
        print("-"*50)
        
        if sequences:
            for seq in sequences:
                seq_name, data_type, start_val, increment, max_val, cycle = seq
                print(f"📊 Sequência: {seq_name}")
                print(f"   Tipo: {data_type}")
                print(f"   Valor inicial: {start_val}")
                print(f"   Incremento: {increment}")
                print(f"   Valor máximo: {max_val}")
                print(f"   Ciclo: {cycle}")
        else:
            print("❌ Nenhuma sequência encontrada para cad_individual")
        
        # Verificar constraints da tabela
        cur.execute("""
            SELECT 
                tc.constraint_name,
                tc.constraint_type,
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = 'tl_cds_cad_individual' 
            AND tc.table_schema = 'public'
            AND kcu.column_name = 'co_seq_cds_cad_individual';
        """)
        
        constraints = cur.fetchall()
        
        print("\n🔒 CONSTRAINTS DO CAMPO:")
        print("-"*50)
        
        if constraints:
            for constraint in constraints:
                constraint_name, constraint_type, column_name = constraint
                print(f"🔐 Constraint: {constraint_name}")
                print(f"   Tipo: {constraint_type}")
                print(f"   Coluna: {column_name}")
        else:
            print("❌ Nenhuma constraint específica encontrada para co_seq_cds_cad_individual")
        
        # Consultar estrutura da tabela
        query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'tl_cds_cad_individual'
        ORDER BY ordinal_position;
        """
        
        cur.execute(query)
        columns = cur.fetchall()
        
        print("🔍 ESTRUTURA DA TABELA tl_cds_cad_individual")
        print("=" * 80)
        print(f"{'COLUNA':<35} {'TIPO':<20} {'TAMANHO':<10} {'NULO':<8} {'PADRÃO':<15}")
        print("-" * 80)
        
        varchar_11_fields = []
        varchar_3_fields = []
        varchar_9_fields = []
        
        for col in columns:
            column_name, data_type, max_length, nullable, default = col
            
            # Identificar campos com limitação específica
            if data_type == 'character varying':
                if max_length == 11:
                    varchar_11_fields.append(column_name)
                elif max_length == 3:
                    varchar_3_fields.append(column_name)
                elif max_length == 9:
                    varchar_9_fields.append(column_name)
            
            max_len_str = str(max_length) if max_length else 'N/A'
            nullable_str = 'SIM' if nullable == 'YES' else 'NÃO'
            default_str = str(default)[:15] if default else 'N/A'
            
            print(f"{column_name:<35} {data_type:<20} {max_len_str:<10} {nullable_str:<8} {default_str:<15}")
        
        print("\n" + "=" * 80)
        print("📋 CAMPOS COM LIMITAÇÃO DE CARACTERES:")
        
        if varchar_11_fields:
            print(f"\n🔴 VARCHAR(11) - {len(varchar_11_fields)} campos:")
            for field in varchar_11_fields:
                print(f"   • {field}")
        
        if varchar_3_fields:
            print(f"\n🟡 VARCHAR(3) - {len(varchar_3_fields)} campos:")
            for field in varchar_3_fields:
                print(f"   • {field}")
        
        if varchar_9_fields:
            print(f"\n🟠 VARCHAR(9) - {len(varchar_9_fields)} campos:")
            for field in varchar_9_fields:
                print(f"   • {field}")
        
        cur.close()
        conn.close()
        
        return varchar_11_fields, varchar_3_fields, varchar_9_fields
        
    except Exception as e:
        print(f"❌ Erro ao verificar estrutura: {e}")
        return [], [], []

if __name__ == "__main__":
    check_table_structure()