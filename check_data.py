import psycopg2

try:
    conn = psycopg2.connect('postgresql://postgres:postgres@localhost:5433/cotacoes_b3')
    cur = conn.cursor()

    # Total de registros
    cur.execute('SELECT COUNT(*) FROM cotacoes;')
    count = cur.fetchone()[0]
    print(f'Total de registros na tabela: {count}')

    # Ativos únicos
    cur.execute('SELECT COUNT(DISTINCT ativo) FROM cotacoes;')
    unique_ativos = cur.fetchone()[0]
    print(f'Ativos únicos: {unique_ativos}')

    # Verificar duplicatas por ativo
    cur.execute("""
        SELECT ativo, COUNT(*)
        FROM cotacoes
        GROUP BY ativo
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
        LIMIT 5;
    """)
    duplicates = cur.fetchall()
    print('\nAtivos com duplicatas:')
    for row in duplicates:
        print(f'  {row[0]}: {row[1]} registros')

    # Verificar duplicatas por ativo + data
    cur.execute("""
        SELECT ativo, datapregao, COUNT(*)
        FROM cotacoes
        GROUP BY ativo, datapregao
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
        LIMIT 5;
    """)
    date_duplicates = cur.fetchall()
    print('\nCombinações ativo+data com duplicatas:')
    for row in date_duplicates:
        print(f'  {row[0]} - {row[1]}: {row[2]} registros')

    cur.close()
    conn.close()

except Exception as e:
    print(f"Erro: {e}")