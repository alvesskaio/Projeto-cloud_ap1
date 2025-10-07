#!/usr/bin/env python3
"""
Script de debug para analisar a estrutura do XML da B3
"""

from azure_storage import get_file_from_blob
from lxml import etree
import io

def debug_xml_structure():
    """Analisa a estrutura do XML para entender como extrair os dados"""

    file_name = "BVBG186_250923.xml"
    print(f"[DEBUG] Analisando estrutura do arquivo: {file_name}")

    xml_content = get_file_from_blob(file_name)
    if not xml_content:
        print("[ERROR] Não foi possível obter o arquivo do blob storage")
        return

    xml_bytes = io.BytesIO(xml_content.encode('utf-8'))

    # Contadores
    pric_rpt_count = 0

    try:
        context = etree.iterparse(xml_bytes, events=('start', 'end'))
        context = iter(context)

        for event, elem in context:
            if event == 'start':
                continue

            # evento 'end'
            tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag

            if tag_name == 'PricRpt':
                pric_rpt_count += 1
                if pric_rpt_count <= 3:  # Analisa apenas os primeiros 3 PricRpt
                    print(f"\n[DEBUG] === PricRpt #{pric_rpt_count} ===")

                    ativo = None
                    data_pregao = None
                    attrs_found = False

                    # Percorre os elementos filhos do PricRpt
                    for child in elem:
                        child_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag

                        if child_tag == 'TradDt':
                            for dt_elem in child:
                                dt_tag = dt_elem.tag.split('}')[-1] if '}' in dt_elem.tag else dt_elem.tag
                                if dt_tag == 'Dt':
                                    data_pregao = dt_elem.text
                                    print(f"  Data: {data_pregao}")
                                    break

                        elif child_tag == 'SctyId':
                            for ticker_elem in child:
                                ticker_tag = ticker_elem.tag.split('}')[-1] if '}' in ticker_elem.tag else ticker_elem.tag
                                if ticker_tag == 'TckrSymb':
                                    ativo = ticker_elem.text
                                    print(f"  Ativo: {ativo}")
                                    break

                        elif child_tag == 'FinInstrmAttrbts':
                            attrs_found = True
                            print(f"  FinInstrmAttrbts encontrado:")
                            # Mostra alguns atributos
                            for attr_child in child:
                                attr_tag = attr_child.tag.split('}')[-1] if '}' in attr_child.tag else attr_child.tag
                                if attr_child.text and attr_child.text.strip():
                                    print(f"    {attr_tag}: {attr_child.text.strip()}")

                    print(f"  Resultado: ativo={ativo}, data={data_pregao}, attrs={attrs_found}")

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

            # Para após processar algumas amostras
            if pric_rpt_count >= 3:
                break

    except Exception as e:
        print(f"[ERROR] Erro durante análise: {e}")
        import traceback
        traceback.print_exc()

    print(f"\n[SUMMARY] Encontrados: {pric_rpt_count} PricRpt")

if __name__ == "__main__":
    debug_xml_structure()