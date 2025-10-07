from azure_storage import get_file_from_blob
from database import DatabaseManager, Cotacoes
import xml.etree.ElementTree as ET
from datetime import datetime, date
from decimal import Decimal
import re

# Namespace usado no XML da B3 (seção de cotações)
NAMESPACE = {"ns": "urn:bvmf.217.01.xsd"}

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
    print(f"[INFO] Iniciando processamento do arquivo: {file_name}")

    # Obtém o conteúdo do arquivo do blob storage
    xml_content = get_file_from_blob(file_name)
    if not xml_content:
        print(f"[ERROR] Não foi possível obter o arquivo {file_name} do blob storage")
        return []

    cotacoes_data = []

    try:
        # Parse do XML completo (arquivo é grande, mas ElementTree é eficiente)
        root = ET.fromstring(xml_content)

        # Encontrar todos os PricRpt no namespace correto
        pric_rpts = root.findall(".//{urn:bvmf.217.01.xsd}PricRpt")
        print(f"[INFO] Encontrados {len(pric_rpts)} elementos PricRpt para processar")

        for i, pric_rpt in enumerate(pric_rpts):
            # Buscar data
            trad_dt = pric_rpt.find(".//{urn:bvmf.217.01.xsd}TradDt")
            data_pregao = None
            if trad_dt is not None:
                dt_elem = trad_dt.find(".//{urn:bvmf.217.01.xsd}Dt")
                if dt_elem is not None and dt_elem.text:
                    data_pregao = extract_date_from_xml(dt_elem.text)

            # Buscar ativo
            scty_id = pric_rpt.find(".//{urn:bvmf.217.01.xsd}SctyId")
            ativo = None
            if scty_id is not None:
                tckr_elem = scty_id.find(".//{urn:bvmf.217.01.xsd}TckrSymb")
                if tckr_elem is not None and tckr_elem.text:
                    ativo = tckr_elem.text.strip()

            # Buscar atributos financeiros
            fin_attrs = pric_rpt.find(".//{urn:bvmf.217.01.xsd}FinInstrmAttrbts")
            if fin_attrs is not None and ativo and data_pregao:
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
                if ativo and data_pregao and (abertura or fechamento):
                    cotacao = {
                        'ativo': ativo[:10],  # Limita a 10 caracteres conforme schema
                        'data_pregao': data_pregao,
                        'abertura': abertura,
                        'fechamento': fechamento,
                        'volume': volume
                    }
                    cotacoes_data.append(cotacao)

                    if len(cotacoes_data) % 1000 == 0:
                        print(f"[INFO] Processadas {len(cotacoes_data)} cotações...")

    except Exception as e:
        print(f"[ERROR] Erro ao processar XML: {e}")
        return []

    print(f"[OK] Processamento concluído. Total de cotações extraídas: {len(cotacoes_data)}")
    return cotacoes_data

def transform_and_load(file_name):
    """
    Função principal que executa o pipeline de transformação e carga

    Args:
        file_name: Nome do arquivo XML no blob storage
    """
    print(f"[INFO] Iniciando pipeline de transformação e carga para: {file_name}")

    # 1. Processar XML e extrair dados
    cotacoes_data = process_xml_cotacoes(file_name)

    if not cotacoes_data:
        print("[WARN] Nenhuma cotação foi extraída do arquivo")
        return False

    # 2. Manter todos os registros (incluindo duplicatas diferenciadas por horário)
    print(f"[INFO] Mantendo todos os {len(cotacoes_data)} registros (sem remoção de duplicatas)")
    cotacoes_unique = cotacoes_data  # Usar todos os dados sem filtrar

    # 3. Conectar ao banco e inserir dados
    db = DatabaseManager()
    if not db.connect():
        print("[ERROR] Falha ao conectar ao banco de dados")
        return False

    # 4. Criar tabelas se não existirem
    if not db.create_tables():
        print("[ERROR] Falha ao criar/verificar tabelas")
        return False

    # 5. Inserir dados em lote
    inserted_count = db.insert_cotacoes_batch(cotacoes_unique)

    if inserted_count > 0:
        print(f"[OK] Pipeline concluído com sucesso. {inserted_count} cotações inseridas no banco")
        return True
    else:
        print("[ERROR] Falha ao inserir dados no banco")
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

