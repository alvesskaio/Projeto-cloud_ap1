from azure_storage import get_file_from_blob
from database import DatabaseManager, Cotacoes
import xml.etree.ElementTree as ET
from datetime import datetime, date
from decimal import Decimal
import re
import time
from tqdm import tqdm

# Namespace usado no XML da B3 (seção de cotações)
NAMESPACE = {"ns": "urn:bvmf.217.01.xsd"}

def print_timestamp():
    """Retorna timestamp formatado para logs"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def format_file_size(size_bytes):
    """Formata tamanho de arquivo em unidades legíveis"""
    if size_bytes == 0:
        return "0 B"
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024
        i += 1
    return f"{size_bytes:.1f} {size_names[i]}"

def extract_date_from_xml(xml_date_text):
    """
    Extrai data do formato XML da B3 e converte para objeto date
    Suporta formatos: 2025-10-06 e 2025-10-06T23:12:53Z
    """
    try:
        if xml_date_text:
            date_str = xml_date_text.strip()
            # Tenta primeiro o formato de data simples
            if len(date_str) == 10:  # YYYY-MM-DD
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            # Tenta formato de timestamp ISO
            elif 'T' in date_str:
                # Remove timezone se presente
                if date_str.endswith('Z'):
                    date_str = date_str[:-1]
                # Extrai apenas a parte da data
                date_part = date_str.split('T')[0]
                return datetime.strptime(date_part, "%Y-%m-%d").date()
    except ValueError:
        print(f"[WARN] Formato de data inválido: {xml_date_text}")
    return None

def extract_datetime_from_xml(xml_date_text):
    """
    Extrai data e horário completos do formato XML da B3
    Suporta formatos: 2025-10-06T23:12:53Z, 2025-10-06T23:12:53
    """
    try:
        if xml_date_text:
            date_str = xml_date_text.strip()
            # Tenta formato de timestamp ISO com horário
            if 'T' in date_str:
                # Remove timezone se presente
                if date_str.endswith('Z'):
                    date_str = date_str[:-1]
                return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
            # Se não tem horário, assume meia-noite
            elif len(date_str) == 10:  # YYYY-MM-DD
                return datetime.strptime(date_str + "T00:00:00", "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        print(f"[WARN] Formato de data/hora inválido: {xml_date_text}")
    return None

def clean_price_value(price_text):
    """
    Limpa e converte valores de preço em texto para Decimal
    """
    try:
        if price_text:
            # Remove espaços e converte para decimal
            clean_text = price_text.strip()
            return Decimal(clean_text)
    except (ValueError, TypeError):
        print(f"[WARN] Valor de preço inválido: {price_text}")
    return None

def process_xml_cotacoes(file_name):
    """
    Processa arquivo XML da B3 e extrai dados de cotações

    Args:
        file_name: Nome do arquivo no blob storage

    Returns:
        Lista de dicionários com dados das cotações
    """
    processing_start = time.time()
    print(f"[{print_timestamp()}] [INFO] 📥 Iniciando processamento do arquivo: {file_name}")

    # Obtém o conteúdo do arquivo do blob storage
    download_start = time.time()
    xml_content = get_file_from_blob(file_name)
    download_time = time.time() - download_start
    
    if not xml_content:
        print(f"[{print_timestamp()}] [ERROR] ❌ Não foi possível obter o arquivo {file_name} do blob storage")
        return []

    xml_size = len(xml_content.encode('utf-8'))
    print(f"[{print_timestamp()}] [OK] ✅ Arquivo baixado do blob storage em {download_time:.2f}s ({format_file_size(xml_size)})")

    cotacoes_data = []
    stats = {
        'total_elements': 0,
        'valid_quotes': 0,
        'missing_date': 0,
        'missing_ticker': 0,
        'missing_prices': 0,
        'unique_tickers': set(),
        'date_range': {'min': None, 'max': None}
    }

    try:
        # Parse do XML completo (arquivo é grande, mas ElementTree é eficiente)
        parse_start = time.time()
        print(f"[{print_timestamp()}] [INFO] 🔄 Fazendo parse do XML...")
        
        root = ET.fromstring(xml_content)
        parse_time = time.time() - parse_start
        print(f"[{print_timestamp()}] [OK] ✅ Parse XML concluído em {parse_time:.2f}s")

        # Encontrar todos os PricRpt no namespace correto
        search_start = time.time()
        pric_rpts = root.findall(".//{urn:bvmf.217.01.xsd}PricRpt")
        search_time = time.time() - search_start
        
        stats['total_elements'] = len(pric_rpts)
        print(f"[{print_timestamp()}] [INFO] 📊 Encontrados {len(pric_rpts)} elementos PricRpt para processar em {search_time:.2f}s")

        # Processamento com barra de progresso
        process_start = time.time()
        
        progress_bar = tqdm(
            pric_rpts,
            desc="🔄 Processando XML",
            unit="elementos",
            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
        )
        
        for pric_rpt in progress_bar:
            # Buscar data
            trad_dt = pric_rpt.find(".//{urn:bvmf.217.01.xsd}TradDt")
            data_pregao = None
            if trad_dt is not None:
                dt_elem = trad_dt.find(".//{urn:bvmf.217.01.xsd}Dt")
                if dt_elem is not None and dt_elem.text:
                    data_pregao = extract_date_from_xml(dt_elem.text)
            
            if not data_pregao:
                stats['missing_date'] += 1
                continue

            # Buscar ativo
            scty_id = pric_rpt.find(".//{urn:bvmf.217.01.xsd}SctyId")
            ativo = None
            if scty_id is not None:
                tckr_elem = scty_id.find(".//{urn:bvmf.217.01.xsd}TckrSymb")
                if tckr_elem is not None and tckr_elem.text:
                    ativo = tckr_elem.text.strip()

            if not ativo:
                stats['missing_ticker'] += 1
                continue

            # Buscar atributos financeiros
            fin_attrs = pric_rpt.find(".//{urn:bvmf.217.01.xsd}FinInstrmAttrbts")
            if fin_attrs is not None:
                # Primeiro preço (abertura)
                frst_pric = fin_attrs.find(".//{urn:bvmf.217.01.xsd}FrstPric")
                abertura = None
                if frst_pric is not None and frst_pric.text:
                    abertura = clean_price_value(frst_pric.text)

                # Último preço (fechamento)
                last_pric = fin_attrs.find(".//{urn:bvmf.217.01.xsd}LastPric")
                fechamento = None
                if last_pric is not None and last_pric.text:
                    fechamento = clean_price_value(last_pric.text)

                # Volume (quantidade de transações)
                rglr_txs = fin_attrs.find(".//{urn:bvmf.217.01.xsd}RglrTxsQty")
                volume = None
                if rglr_txs is not None and rglr_txs.text:
                    volume = clean_price_value(rglr_txs.text)

                # Se temos dados válidos, adiciona à lista
                if abertura or fechamento:
                    cotacao = {
                        'ativo': ativo[:10],  # Limita a 10 caracteres conforme schema
                        'data_pregao': data_pregao,
                        'abertura': abertura,
                        'fechamento': fechamento,
                        'volume': volume
                    }
                    cotacoes_data.append(cotacao)
                    stats['valid_quotes'] += 1
                    stats['unique_tickers'].add(ativo[:10])
                    
                    # Atualizar range de datas
                    if stats['date_range']['min'] is None or data_pregao < stats['date_range']['min']:
                        stats['date_range']['min'] = data_pregao
                    if stats['date_range']['max'] is None or data_pregao > stats['date_range']['max']:
                        stats['date_range']['max'] = data_pregao
                else:
                    stats['missing_prices'] += 1

        progress_bar.close()
        process_time = time.time() - process_start
        print(f"[{print_timestamp()}] [OK] ✅ Processamento de elementos concluído em {process_time:.2f}s")

    except Exception as e:
        processing_time = time.time() - processing_start
        print(f"[{print_timestamp()}] [ERROR] ❌ Erro ao processar XML após {processing_time:.2f}s: {e}")
        return []

    total_time = time.time() - processing_start
    
    # Relatório detalhado de estatísticas
    print(f"[{print_timestamp()}] [INFO] 📊 RELATÓRIO DE PROCESSAMENTO:")
    print(f"[{print_timestamp()}] [INFO]   ⏱️  Tempo total: {total_time:.2f}s")
    print(f"[{print_timestamp()}] [INFO]   📥 Arquivo: {format_file_size(xml_size)}")
    print(f"[{print_timestamp()}] [INFO]   🔍 Elementos analisados: {stats['total_elements']:,}")
    print(f"[{print_timestamp()}] [INFO]   ✅ Cotações válidas: {stats['valid_quotes']:,}")
    print(f"[{print_timestamp()}] [INFO]   📈 Ativos únicos: {len(stats['unique_tickers']):,}")
    
    if stats['date_range']['min'] and stats['date_range']['max']:
        print(f"[{print_timestamp()}] [INFO]   📅 Período: {stats['date_range']['min']} até {stats['date_range']['max']}")
    
    if stats['missing_date'] + stats['missing_ticker'] + stats['missing_prices'] > 0:
        print(f"[{print_timestamp()}] [INFO]   ⚠️  Elementos ignorados:")
        if stats['missing_date'] > 0:
            print(f"[{print_timestamp()}] [INFO]     🚫 Sem data: {stats['missing_date']:,}")
        if stats['missing_ticker'] > 0:
            print(f"[{print_timestamp()}] [INFO]     🚫 Sem ticker: {stats['missing_ticker']:,}")
        if stats['missing_prices'] > 0:
            print(f"[{print_timestamp()}] [INFO]     🚫 Sem preços: {stats['missing_prices']:,}")
    
    # Sample de alguns ativos processados
    if stats['unique_tickers']:
        sample_tickers = list(stats['unique_tickers'])[:10]
        print(f"[{print_timestamp()}] [INFO]   📋 Amostra de ativos: {', '.join(sample_tickers)}")
    
    print(f"[{print_timestamp()}] [OK] ✅ Processamento XML concluído com sucesso!")
    return cotacoes_data

def transform_and_load(file_name):
    """
    Função principal que executa o pipeline de transformação e carga

    Args:
        file_name: Nome do arquivo XML no blob storage
    """
    pipeline_start = time.time()
    print(f"[{print_timestamp()}] [INFO] 🚀 Iniciando pipeline de transformação e carga para: {file_name}")

    # 1. Processar XML e extrair dados
    print(f"[{print_timestamp()}] [INFO] 📊 ETAPA 1: Processamento de dados XML")
    cotacoes_data = process_xml_cotacoes(file_name)

    if not cotacoes_data:
        pipeline_time = time.time() - pipeline_start
        print(f"[{print_timestamp()}] [WARN] ⚠️ Nenhuma cotação foi extraída do arquivo após {pipeline_time:.2f}s")
        return False

    # 2. Preparar dados para inserção (manter todos os registros)
    prep_start = time.time()
    print(f"[{print_timestamp()}] [INFO] 📊 ETAPA 2: Preparação dos dados")
    print(f"[{print_timestamp()}] [INFO] 📋 Mantendo todos os {len(cotacoes_data):,} registros (sem remoção de duplicatas)")
    
    cotacoes_unique = cotacoes_data  # Usar todos os dados sem filtrar
    
    # Análise rápida dos dados antes da inserção
    ativos_count = len(set(c['ativo'] for c in cotacoes_unique))
    dates_count = len(set(c['data_pregao'] for c in cotacoes_unique))
    with_abertura = sum(1 for c in cotacoes_unique if c['abertura'] is not None)
    with_fechamento = sum(1 for c in cotacoes_unique if c['fechamento'] is not None)
    with_volume = sum(1 for c in cotacoes_unique if c['volume'] is not None)
    
    prep_time = time.time() - prep_start
    print(f"[{print_timestamp()}] [INFO] 📈 Análise dos dados preparados em {prep_time:.2f}s:")
    print(f"[{print_timestamp()}] [INFO]   📊 Registros: {len(cotacoes_unique):,}")
    print(f"[{print_timestamp()}] [INFO]   🏢 Ativos únicos: {ativos_count:,}")
    print(f"[{print_timestamp()}] [INFO]   📅 Datas únicas: {dates_count:,}")
    print(f"[{print_timestamp()}] [INFO]   💰 Com preço abertura: {with_abertura:,} ({with_abertura/len(cotacoes_unique)*100:.1f}%)")
    print(f"[{print_timestamp()}] [INFO]   💰 Com preço fechamento: {with_fechamento:,} ({with_fechamento/len(cotacoes_unique)*100:.1f}%)")
    print(f"[{print_timestamp()}] [INFO]   📊 Com volume: {with_volume:,} ({with_volume/len(cotacoes_unique)*100:.1f}%)")

    # 3. Conectar ao banco e inserir dados
    print(f"[{print_timestamp()}] [INFO] 📊 ETAPA 3: Conexão com banco de dados")
    db_start = time.time()
    
    db = DatabaseManager()
    if not db.connect():
        pipeline_time = time.time() - pipeline_start
        print(f"[{print_timestamp()}] [ERROR] ❌ Falha ao conectar ao banco de dados após {pipeline_time:.2f}s")
        return False

    db_connect_time = time.time() - db_start
    print(f"[{print_timestamp()}] [OK] ✅ Conexão estabelecida em {db_connect_time:.2f}s")

    # 4. Criar tabelas se não existirem
    print(f"[{print_timestamp()}] [INFO] 🗄️ Verificando/criando estrutura de tabelas...")
    table_start = time.time()
    
    if not db.create_tables():
        pipeline_time = time.time() - pipeline_start
        print(f"[{print_timestamp()}] [ERROR] ❌ Falha ao criar/verificar tabelas após {pipeline_time:.2f}s")
        return False
        
    table_time = time.time() - table_start
    print(f"[{print_timestamp()}] [OK] ✅ Estrutura de tabelas verificada em {table_time:.2f}s")

    # 5. Inserir dados em lote
    print(f"[{print_timestamp()}] [INFO] 📊 ETAPA 4: Inserção de dados no PostgreSQL")
    insert_start = time.time()
    
    inserted_count = db.insert_cotacoes_batch(cotacoes_unique)
    insert_time = time.time() - insert_start

    pipeline_total_time = time.time() - pipeline_start

    if inserted_count > 0:
        rate = inserted_count / insert_time if insert_time > 0 else 0
        print(f"[{print_timestamp()}] [SUCCESS] 🎉 Pipeline concluído com sucesso!")
        print(f"[{print_timestamp()}] [INFO] 📊 RESUMO FINAL:")
        print(f"[{print_timestamp()}] [INFO]   ✅ Cotações inseridas: {inserted_count:,}")
        print(f"[{print_timestamp()}] [INFO]   ⏱️  Tempo de inserção: {insert_time:.2f}s")
        print(f"[{print_timestamp()}] [INFO]   📈 Taxa de inserção: {rate:.0f} registros/s")
        print(f"[{print_timestamp()}] [INFO]   ⏱️  Tempo total do pipeline: {pipeline_total_time:.2f}s")
        return True
    else:
        print(f"[{print_timestamp()}] [ERROR] ❌ Falha ao inserir dados no banco após {pipeline_total_time:.2f}s")
        return False

if __name__ == "__main__":
    # Configuração padrão - pode ser alterada conforme necessário
    DATA_FILE = "250923"
    FILE_NAME = f"BVBG186_{DATA_FILE}.xml"

    # Executa o pipeline
    success = transform_and_load(FILE_NAME)

    if success:
        print("[SUCCESS] Pipeline executado com sucesso!")
    else:
        print("[FAILURE] Pipeline falhou durante a execução")

