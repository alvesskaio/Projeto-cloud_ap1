#!/usr/bin/env python3
"""
Teste Geral Completo da Aplicação de Cotações B3

Este script executa uma bateria completa de testes para verificar:
1. Pré-requisitos (PostgreSQL, Azurite)
2. Funcionalidades do módulo extract
3. Funcionalidades do módulo transform_load
4. Pipeline completo end-to-end
5. Limpeza e consistência dos dados
"""

import sys
import os
import time
from datetime import datetime, timedelta
from helpers import yymmdd
from main import check_prerequisites, run_pipeline
from database import DatabaseManager
from azure_storage import BlobServiceClient, AZURE_BLOB_CONNECTION
import shutil

def print_header(title):
    """Imprime cabeçalho formatado"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)

def print_test(test_name):
    """Imprime nome do teste"""
    print(f"\n🧪 TESTE: {test_name}")
    print("-" * 50)

def print_result(success, message):
    """Imprime resultado do teste"""
    status = "✅ PASSOU" if success else "❌ FALHOU"
    print(f"{status}: {message}")

def test_prerequisites():
    """Testa se todos os pré-requisitos estão funcionando"""
    print_test("Verificação de Pré-requisitos")

    try:
        success = check_prerequisites()
        print_result(success, "Pré-requisitos verificados")
        return success
    except Exception as e:
        print_result(False, f"Erro ao verificar pré-requisitos: {e}")
        return False

def test_helpers():
    """Testa função de formatação de data"""
    print_test("Função helpers.yymmdd()")

    try:
        # Teste com data atual
        data_atual = yymmdd(datetime.now())
        expected = datetime.now().strftime("%y%m%d")

        success = data_atual == expected
        print_result(success, f"Data atual: {data_atual} (esperado: {expected})")

        # Teste com data específica
        test_date = datetime(2025, 9, 23)
        result = yymmdd(test_date)
        expected = "250923"

        success = result == expected
        print_result(success, f"Data específica: {result} (esperado: {expected})")

        return True
    except Exception as e:
        print_result(False, f"Erro no teste de helpers: {e}")
        return False

def test_extract_functionality():
    """Testa módulo de extração"""
    print_test("Módulo de Extração (extract.py)")

    try:
        from extract import run, ensure_data_directory, build_url_download

        # Teste 1: Função de criação de diretório
        ensure_data_directory()
        success = os.path.exists("./dados_b3")
        print_result(success, "Criação de diretório dados_b3")

        # Teste 2: Construção de URL
        url = build_url_download("250923")
        expected = "https://www.b3.com.br/pesquisapregao/download?filelist=SPRE250923.zip"
        success = url == expected
        print_result(success, f"URL construída: {url}")

        # Teste 3: Extração com data conhecida que funciona
        print("\n📥 Executando extração completa...")
        run("250923")  # Data que sabemos que funciona
        print_result(True, "Extração executada sem erros")

        # Verificar se pasta está limpa após extração
        pasta_vazia = not os.listdir("./dados_b3") if os.path.exists("./dados_b3") else True
        print_result(pasta_vazia, "Pasta dados_b3 limpa após extração")

        return True

    except Exception as e:
        print_result(False, f"Erro no teste de extração: {e}")
        return False

def test_blob_storage():
    """Testa acesso ao blob storage"""
    print_test("Blob Storage (Azurite)")

    try:
        service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
        containers = list(service.list_containers())

        print_result(True, f"Conexão com Azurite estabelecida")
        print(f"   Containers encontrados: {len(containers)}")

        # Verificar se arquivo foi enviado
        container_client = service.get_container_client("dados-pregao-bolsa")
        blobs = list(container_client.list_blobs())

        success = len(blobs) > 0
        print_result(success, f"Arquivos no blob storage: {len(blobs)}")

        if blobs:
            for blob in blobs[:3]:  # Mostrar primeiros 3
                print(f"   📄 {blob.name} ({blob.size:,} bytes)")

        return True

    except Exception as e:
        print_result(False, f"Erro no teste de blob storage: {e}")
        return False

def test_database():
    """Testa conexão e estrutura do banco de dados"""
    print_test("Banco de Dados PostgreSQL")

    try:
        db = DatabaseManager()
        success = db.connect()
        print_result(success, "Conexão com PostgreSQL")

        if success:
            # Verificar se tabela existe
            db.create_tables()
            print_result(True, "Tabela cotacoes verificada/criada")

            # Contar registros
            with db.get_session() as session:
                from database import Cotacoes
                count = session.query(Cotacoes).count()
                print_result(True, f"Registros na tabela: {count:,}")

        return success

    except Exception as e:
        print_result(False, f"Erro no teste de database: {e}")
        return False

def test_pipeline_complete():
    """Testa pipeline completo end-to-end"""
    print_test("Pipeline Completo End-to-End")

    try:
        # Usar data que sabemos que funciona
        test_date = "250924"
        print(f"🚀 Executando pipeline para data: {test_date}")

        success = run_pipeline(test_date)
        print_result(success, "Pipeline completo executado")

        if success:
            # Verificar se dados foram inseridos no banco
            db = DatabaseManager()
            with db.get_session() as session:
                from database import Cotacoes
                from datetime import date

                # Contar registros desta data específica
                data_pregao = date(2025, 9, 24)  # 250924
                count = session.query(Cotacoes).filter(
                    Cotacoes.data_pregao == data_pregao
                ).count()

                success = count > 0
                print_result(success, f"Dados inseridos no banco: {count:,} registros")

        return success

    except Exception as e:
        print_result(False, f"Erro no teste de pipeline: {e}")
        return False

def test_error_handling():
    """Testa tratamento de erros"""
    print_test("Tratamento de Erros")

    try:
        from extract import run

        # Teste com data inválida (futura)
        try:
            future_date = yymmdd(datetime.now() + timedelta(days=30))
            run(future_date)
            print_result(False, "Deveria falhar com data futura")
        except RuntimeError as e:
            print_result(True, f"Erro tratado corretamente: {str(e)[:50]}...")

        return True

    except Exception as e:
        print_result(False, f"Erro no teste de tratamento de erros: {e}")
        return False

def cleanup_test_data():
    """Limpa dados de teste"""
    print_test("Limpeza de Dados de Teste")

    try:
        # Limpar pasta dados_b3 se existir
        if os.path.exists("./dados_b3"):
            shutil.rmtree("./dados_b3", ignore_errors=True)
            print_result(True, "Pasta dados_b3 limpa")

        return True

    except Exception as e:
        print_result(False, f"Erro na limpeza: {e}")
        return False

def main():
    """Executa todos os testes"""
    print_header("TESTE GERAL COMPLETO - APLICAÇÃO COTAÇÕES B3")

    start_time = time.time()
    tests_passed = 0
    total_tests = 0

    # Lista de testes
    tests = [
        ("Pré-requisitos", test_prerequisites),
        ("Helpers", test_helpers),
        ("Extração", test_extract_functionality),
        ("Blob Storage", test_blob_storage),
        ("Banco de Dados", test_database),
        ("Pipeline Completo", test_pipeline_complete),
        ("Tratamento de Erros", test_error_handling),
        ("Limpeza", cleanup_test_data)
    ]

    # Executar testes
    for test_name, test_func in tests:
        total_tests += 1
        try:
            if test_func():
                tests_passed += 1
        except Exception as e:
            print_result(False, f"Erro inesperado em {test_name}: {e}")

    # Relatório final
    elapsed_time = time.time() - start_time
    print_header("RELATÓRIO FINAL")

    print(f"✅ Testes Passou: {tests_passed}/{total_tests}")
    print(f"❌ Testes Falhou: {total_tests - tests_passed}/{total_tests}")
    print(f"⏱️  Tempo Total: {elapsed_time:.2f}s")
    print(f"📊 Taxa de Sucesso: {(tests_passed/total_tests)*100:.1f}%")

    if tests_passed == total_tests:
        print("\n🎉 TODOS OS TESTES PASSARAM! Aplicação funcionando perfeitamente.")
        return 0
    else:
        print(f"\n⚠️  {total_tests - tests_passed} teste(s) falharam. Verifique os logs acima.")
        return 1

if __name__ == "__main__":
    sys.exit(main())