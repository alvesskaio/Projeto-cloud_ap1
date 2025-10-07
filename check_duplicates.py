from transform_load import process_xml_cotacoes
from collections import Counter

# Processar XML
print("Processando XML...")
cotacoes = process_xml_cotacoes('BVBG186_250923.xml')

# Analisar duplicatas
keys = [(c['ativo'], c['data_pregao']) for c in cotacoes]
duplicados = [(key, count) for key, count in Counter(keys).items() if count > 1]

print(f'Total de registros: {len(cotacoes)}')
print(f'Registros únicos: {len(set(keys))}')
print(f'Duplicados encontrados: {len(duplicados)}')

if duplicados:
    print('\nPrimeiros 5 duplicados:')
    for i, (key, count) in enumerate(duplicados[:5]):
        print(f'  {key[0]} - {key[1]}: {count} vezes')

    # Mostrar detalhes de um duplicado
    if duplicados:
        ativo_dup, data_dup = duplicados[0][0]
        registros_dup = [c for c in cotacoes if c['ativo'] == ativo_dup and c['data_pregao'] == data_dup]
        print(f'\nDetalhes do duplicado {ativo_dup}:')
        for reg in registros_dup[:3]:  # Mostrar apenas os primeiros 3
            print(f'  Abertura: {reg["abertura"]}, Fechamento: {reg["fechamento"]}, Volume: {reg["volume"]}')