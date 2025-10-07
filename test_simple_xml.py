#!/usr/bin/env python3
"""
Teste simples de processamento XML usando ElementTree
"""

from azure_storage import get_file_from_blob
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal

def test_simple_xml_processing():
    """Teste simples usando ElementTree padrão"""

    file_name = "BVBG186_250923.xml"
    print(f"[INFO] Testando processamento simples do arquivo: {file_name}")

    xml_content = get_file_from_blob(file_name)
    if not xml_content:
        print("[ERROR] Não foi possível obter o arquivo do blob storage")
        return

    try:
        # Parse do XML completo
        root = ET.fromstring(xml_content)
        print(f"[INFO] XML parseado com sucesso. Root tag: {root.tag}")

        # Tentar encontrar PricRpt em diferentes namespaces
        pric_rpts_052 = root.findall(".//{urn:bvmf.052.01.xsd}PricRpt")
        pric_rpts_217 = root.findall(".//{urn:bvmf.217.01.xsd}PricRpt")
        pric_rpts = pric_rpts_052 + pric_rpts_217

        print(f"[INFO] Encontrados {len(pric_rpts_052)} PricRpt em namespace 052.01")
        print(f"[INFO] Encontrados {len(pric_rpts_217)} PricRpt em namespace 217.01")
        print(f"[INFO] Total de {len(pric_rpts)} elementos PricRpt")

        cotacoes_encontradas = 0

        for i, pric_rpt in enumerate(pric_rpts[:5]):  # Processa apenas os primeiros 5
            print(f"\n[DEBUG] === Processando PricRpt #{i+1} ===")

            # Detectar namespace deste PricRpt
            ns = ""
            if "{urn:bvmf.052.01.xsd}" in pric_rpt.tag:
                ns = "{urn:bvmf.052.01.xsd}"
            elif "{urn:bvmf.217.01.xsd}" in pric_rpt.tag:
                ns = "{urn:bvmf.217.01.xsd}"

            print(f"  Namespace: {ns}")

            # Buscar data
            trad_dt = pric_rpt.find(f".//{ns}TradDt")
            data_pregao = None
            if trad_dt is not None:
                dt_elem = trad_dt.find(f".//{ns}Dt")
                if dt_elem is not None and dt_elem.text:
                    data_pregao = datetime.strptime(dt_elem.text.strip(), "%Y-%m-%d").date()
                    print(f"  Data encontrada: {data_pregao}")

            # Buscar ativo
            scty_id = pric_rpt.find(f".//{ns}SctyId")
            ativo = None
            if scty_id is not None:
                tckr_elem = scty_id.find(f".//{ns}TckrSymb")
                if tckr_elem is not None and tckr_elem.text:
                    ativo = tckr_elem.text.strip()
                    print(f"  Ativo encontrado: {ativo}")

            # Buscar atributos financeiros
            fin_attrs = pric_rpt.find(f".//{ns}FinInstrmAttrbts")
            if fin_attrs is not None:
                print("  FinInstrmAttrbts encontrado:")

                # Primeiro preço (abertura)
                frst_pric = fin_attrs.find(f".//{ns}FrstPric")
                abertura = None
                if frst_pric is not None and frst_pric.text:
                    abertura = Decimal(frst_pric.text.strip())
                    print(f"    Abertura: {abertura}")

                # Último preço (fechamento)
                last_pric = fin_attrs.find(f".//{ns}LastPric")
                fechamento = None
                if last_pric is not None and last_pric.text:
                    fechamento = Decimal(last_pric.text.strip())
                    print(f"    Fechamento: {fechamento}")

                # Volume (quantidade de transações)
                rglr_txs = fin_attrs.find(f".//{ns}RglrTxsQty")
                volume = None
                if rglr_txs is not None and rglr_txs.text:
                    volume = Decimal(rglr_txs.text.strip())
                    print(f"    Volume: {volume}")

                # Se temos dados completos
                if ativo and data_pregao and (abertura or fechamento):
                    cotacoes_encontradas += 1
                    print(f"  [OK] Cotação válida encontrada!")
                else:
                    print(f"  [SKIP] Dados incompletos: ativo={bool(ativo)}, data={bool(data_pregao)}, precos={bool(abertura or fechamento)}")

        print(f"\n[RESULTADO] {cotacoes_encontradas} cotações válidas encontradas de {min(5, len(pric_rpts))} analisadas")

    except Exception as e:
        print(f"[ERROR] Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simple_xml_processing()