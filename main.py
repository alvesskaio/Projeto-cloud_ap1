#!/usr/bin/env python3
"""
Pipeline Principal para Processamento de Cotações B3

Este script orquestra o pipeline completo:
1. Extração de dados da B3 (download e armazenamento no blob local)
2. Processamento do XML e inserção no PostgreSQL local

Requisitos:
- PostgreSQL executando em localhost:5432
- Azurite (emulador blob storage) executando em localhost:10000
- Dependências Python instaladas (requirements.txt)
"""

import sys
import os
import time
from datetime import datetime
from helpers import yymmdd
from extract import run as extract_run
from transform_load import transform_and_load
from database import DatabaseManager

def print_timestamp():
    """Retorna timestamp formatado para logs"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def print_section_header(title):
    """Imprime cabeçalho de seção formatado"""
    print("\n" + "=" * 60)
    print(f"[{print_timestamp()}] {title}")
    print("=" * 60)

def check_prerequisites():
    """
    Verifica se os pré-requisitos estão disponíveis
    """
    print(f"[{print_timestamp()}] [INFO] Verificando pré-requisitos...")
    
    start_time = time.time()
    prereq_status = []

    # Verificar se conseguimos conectar ao banco
    print(f"[{print_timestamp()}] [INFO] Testando conexão com PostgreSQL...")
    try:
        db = DatabaseManager()
        if not db.connect():
            print(f"[{print_timestamp()}] [ERROR] PostgreSQL não está disponível em localhost:5433")
            print("        Execute: docker-compose up -d")
            prereq_status.append(("PostgreSQL", False, "Conexão falhou"))
        else:
            print(f"[{print_timestamp()}] [OK] PostgreSQL disponível e conectado")
            prereq_status.append(("PostgreSQL", True, "Conectado"))
    except Exception as e:
        print(f"[{print_timestamp()}] [ERROR] Erro ao testar PostgreSQL: {e}")
        prereq_status.append(("PostgreSQL", False, str(e)))

    # Verificar se o Azurite está rodando
    print(f"[{print_timestamp()}] [INFO] Testando conexão com Azurite (Blob Storage)...")
    try:
        from azure_storage import BlobServiceClient, AZURE_BLOB_CONNECTION
        service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
        # Tentativa simples de listar containers
        container_list = list(service.list_containers())
        container_count = len(container_list)
        print(f"[{print_timestamp()}] [OK] Azurite disponível ({container_count} containers encontrados)")
        prereq_status.append(("Azurite", True, f"{container_count} containers"))
    except Exception as e:
        print(f"[{print_timestamp()}] [ERROR] Azurite não está disponível em localhost:10003")
        print(f"        Erro: {e}")
        print("        Execute: docker-compose up -d")
        prereq_status.append(("Azurite", False, str(e)))

    # Resumo dos pré-requisitos
    elapsed_time = time.time() - start_time
    print(f"\n[{print_timestamp()}] [INFO] Verificação de pré-requisitos concluída em {elapsed_time:.2f}s")
    print("📋 Status dos serviços:")
    
    all_ok = True
    for service, status, details in prereq_status:
        status_icon = "✅" if status else "❌"
        print(f"   {status_icon} {service}: {details}")
        if not status:
            all_ok = False
    
    return all_ok

def run_pipeline(date_str=None, file_name=None):
    """
    Executa o pipeline completo de processamento

    Args:
        date_str: Data no formato YYMMDD (ex: "250923"). Se None, usa data atual
        file_name: Nome específico do arquivo. Se None, usa padrão baseado na data
    """
    pipeline_start_time = time.time()
    
    # Define data e nome do arquivo
    if not date_str:
        # Usa data do dia anterior por padrão (dados mais prováveis de estar disponíveis)
        from datetime import timedelta
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yymmdd(yesterday)
        print(f"[{print_timestamp()}] [INFO] Usando data do dia anterior: {date_str}")

    if not file_name:
        file_name = f"BVBG186_{date_str}.xml"

    print_section_header("INICIANDO PIPELINE DE PROCESSAMENTO")
    print(f"📅 Data do pregão: {date_str}")
    print(f"📁 Arquivo alvo: {file_name}")
    print(f"⏰ Horário de início: {print_timestamp()}")

    try:
        # Etapa 1: Extração (download da B3 e upload para blob local)
        print_section_header("ETAPA 1: EXTRAÇÃO DE DADOS DA B3")
        step1_start = time.time()
        
        try:
            extract_run(date_str)  # Passa a data para a função de extração
            step1_time = time.time() - step1_start
            print(f"[{print_timestamp()}] [OK] ✅ Extração concluída com sucesso em {step1_time:.2f}s")
        except Exception as e:
            step1_time = time.time() - step1_start
            print(f"[{print_timestamp()}] [ERROR] ❌ Falha na extração após {step1_time:.2f}s: {e}")
            return False

        # Etapa 2: Transformação e Carga (processamento XML e inserção no PostgreSQL)
        print_section_header("ETAPA 2: TRANSFORMAÇÃO E CARGA NO POSTGRESQL")
        step2_start = time.time()
        
        try:
            success = transform_and_load(file_name)
            step2_time = time.time() - step2_start
            
            if success:
                print(f"[{print_timestamp()}] [OK] ✅ Transformação e carga concluídas com sucesso em {step2_time:.2f}s")
            else:
                print(f"[{print_timestamp()}] [ERROR] ❌ Falha na transformação e carga após {step2_time:.2f}s")
                return False
        except Exception as e:
            step2_time = time.time() - step2_start
            print(f"[{print_timestamp()}] [ERROR] ❌ Falha na transformação e carga após {step2_time:.2f}s: {e}")
            return False

        # Resumo final
        total_time = time.time() - pipeline_start_time
        print_section_header("PIPELINE CONCLUÍDO COM SUCESSO")
        print(f"🎉 Status: SUCESSO")
        print(f"📊 Dados processados: {date_str}")
        print(f"⏱️  Tempo total: {total_time:.2f}s")
        print(f"⏰ Concluído em: {print_timestamp()}")
        print(f"💾 Dados inseridos no PostgreSQL")
        
        return True

    except KeyboardInterrupt:
        total_time = time.time() - pipeline_start_time
        print(f"\n[{print_timestamp()}] [INFO] ⚠️  Pipeline interrompido pelo usuário após {total_time:.2f}s")
        return False
    except Exception as e:
        total_time = time.time() - pipeline_start_time
        print(f"\n[{print_timestamp()}] [ERROR] ❌ Erro inesperado no pipeline após {total_time:.2f}s: {e}")
        return False

def show_help():
    """Exibe ajuda sobre como usar o script"""
    help_text = """
Pipeline de Processamento de Cotações B3

USO:
    python main.py                    # Executa com data do dia anterior
    python main.py YYMMDD            # Executa com data específica
    python main.py --check          # Verifica pré-requisitos
    python main.py --help           # Exibe esta ajuda

EXEMPLOS:
    python main.py                   # Processa cotações de ontem (mais provável de estar disponível)
    python main.py 250923           # Processa cotações de 23/09/2025
    python main.py --check          # Verifica se PostgreSQL e Azurite estão rodando

PRÉ-REQUISITOS:
    1. Docker Compose executando PostgreSQL e Azurite:
       docker-compose up -d

    2. Dependências Python instaladas:
       pip install -r requirements.txt

ESTRUTURA DO PIPELINE:
    1. Extração: Download de dados da B3 → Blob Storage local (Azurite)
    2. Transformação: Processamento XML → Dados estruturados
    3. Carga: Inserção no PostgreSQL local
    """
    print(help_text)

def main():
    """Função principal"""

    # Verificar argumentos da linha de comando
    if len(sys.argv) > 1:
        arg = sys.argv[1]

        if arg in ['--help', '-h']:
            show_help()
            return
        elif arg in ['--check', '-c']:
            if check_prerequisites():
                print("[OK] Todos os pré-requisitos estão disponíveis")
                sys.exit(0)
            else:
                print("[ERROR] Alguns pré-requisitos não estão disponíveis")
                sys.exit(1)
        elif arg.isdigit() and len(arg) == 6:
            # Data fornecida no formato YYMMDD
            date_str = arg
        else:
            print(f"[ERROR] Argumento inválido: {arg}")
            print("Use --help para ver opções disponíveis")
            sys.exit(1)
    else:
        date_str = None

    # Verificar pré-requisitos
    print_section_header("VERIFICAÇÃO DE PRÉ-REQUISITOS")
    if not check_prerequisites():
        print(f"\n[{print_timestamp()}] [ERROR] ❌ Pré-requisitos não atendidos. Pipeline não pode continuar.")
        print("💡 Use 'python main.py --help' para mais informações")
        sys.exit(1)

    # Executar pipeline
    success = run_pipeline(date_str)

    if success:
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()