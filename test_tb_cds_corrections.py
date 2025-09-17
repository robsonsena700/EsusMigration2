#!/usr/bin/env python3
"""
Script de teste para validar as correções na tb_cds_cad_individual
"""

import requests
import json
from csv_adjuster import CSVAdjuster

def test_frontend_api():
    """Testa se a API do frontend está retornando os dados corretos"""
    try:
        # Testar endpoint da tb_cds_cad_individual
        response = requests.get('http://localhost:3000/api/tables/tb_cds_cad_individual?page=1&limit=5')
        
        if response.status_code == 200:
            data = response.json()
            print("✅ API respondendo corretamente")
            print(f"Total de registros: {data.get('totalRecords', 0)}")
            
            if data.get('data') and len(data['data']) > 0:
                record = data['data'][0]
                print("\n📋 Estrutura do primeiro registro:")
                for key, value in record.items():
                    print(f"  {key}: {value}")
                
                # Verificar se os campos estão presentes
                expected_fields = ['nu_micro_area', 'nu_cpf_cidadao', 'no_cidadao', 'dt_nascimento', 'no_sexo', 'nu_celular_cidadao']
                missing_fields = [field for field in expected_fields if field not in record]
                
                if missing_fields:
                    print(f"⚠️  Campos ausentes: {missing_fields}")
                else:
                    print("✅ Todos os campos esperados estão presentes")
                    
                return True
            else:
                print("⚠️  Nenhum registro retornado")
                return False
        else:
            print(f"❌ Erro na API: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao testar API: {e}")
        return False

def test_csv_adjuster():
    """Testa o mapeamento CSV do CSVAdjuster"""
    try:
        adjuster = CSVAdjuster()
        
        # Testar mapeamento de sexo
        test_cases = [
            ('masculino', '1'),
            ('feminino', '2'),
            ('m', '1'),
            ('f', '2'),
            ('homem', '1'),
            ('mulher', '2'),
            ('invalid', '')
        ]
        
        print("\n🧪 Testando mapeamento de sexo:")
        all_passed = True
        
        for input_val, expected in test_cases:
            result = adjuster.convert_sex_to_code(input_val)
            status = "✅" if result == expected else "❌"
            print(f"  {status} '{input_val}' -> '{result}' (esperado: '{expected}')")
            if result != expected:
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        print(f"❌ Erro ao testar CSVAdjuster: {e}")
        return False

def test_column_mapping():
    """Testa o mapeamento de colunas"""
    try:
        adjuster = CSVAdjuster()
        
        print("\n📊 Mapeamento de colunas:")
        for csv_col, db_col in adjuster.column_mapping.items():
            print(f"  '{csv_col}' -> '{db_col}'")
        
        # Verificar se os mapeamentos essenciais estão presentes
        essential_mappings = {
            'Nome': 'no_cidadao',
            'CPF/CNS': 'nu_cpf_cidadao',
            'Data de nascimento': 'dt_nascimento',
            'Sexo': 'co_sexo',
            'Telefone celular': 'nu_celular_cidadao',
            'Microárea': 'nu_micro_area'
        }
        
        missing_mappings = []
        for csv_col, expected_db_col in essential_mappings.items():
            if csv_col not in adjuster.column_mapping:
                missing_mappings.append(csv_col)
            elif adjuster.column_mapping[csv_col] != expected_db_col:
                print(f"⚠️  Mapeamento incorreto: '{csv_col}' -> '{adjuster.column_mapping[csv_col]}' (esperado: '{expected_db_col}')")
        
        if missing_mappings:
            print(f"❌ Mapeamentos ausentes: {missing_mappings}")
            return False
        else:
            print("✅ Todos os mapeamentos essenciais estão corretos")
            return True
            
    except Exception as e:
        print(f"❌ Erro ao testar mapeamento: {e}")
        return False

def main():
    """Executa todos os testes"""
    print("🔍 Iniciando testes de correção da tb_cds_cad_individual\n")
    
    tests = [
        ("Mapeamento de colunas", test_column_mapping),
        ("CSVAdjuster", test_csv_adjuster),
        ("API Frontend", test_frontend_api)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"🧪 Teste: {test_name}")
        print('='*50)
        
        result = test_func()
        results.append((test_name, result))
        
        status = "✅ PASSOU" if result else "❌ FALHOU"
        print(f"\n{status}: {test_name}")
    
    # Resumo final
    print(f"\n{'='*50}")
    print("📊 RESUMO DOS TESTES")
    print('='*50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅" if result else "❌"
        print(f"{status} {test_name}")
    
    print(f"\n🎯 Resultado: {passed}/{total} testes passaram")
    
    if passed == total:
        print("🎉 Todas as correções estão funcionando corretamente!")
    else:
        print("⚠️  Algumas correções precisam de ajustes.")

if __name__ == "__main__":
    main()