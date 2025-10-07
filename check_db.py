#!/usr/bin/env python3
"""
Script para verificar a estrutura da tabela cotacoes
"""

import psycopg2

def check_table_structure():
    """Verifica a estrutura da tabela cotacoes"""
    try:
        conn = psycopg2.connect('postgresql://postgres:postgres@localhost:5433/cotacoes_b3')
        cur = conn.cursor()

        # Verificar se a tabela existe
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'cotacoes'
            );
        """)
        table_exists = cur.fetchone()[0]
        print(f"Tabela 'cotacoes' existe: {table_exists}")

        if table_exists:
            # Verificar colunas
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = 'cotacoes'
                ORDER BY ordinal_position;
            """)

            print("\nColunas da tabela cotacoes:")
            for row in cur.fetchall():
                print(f"  {row[0]} ({row[1]}) nullable={row[2]} default={row[3]}")

        # Verificar se a tabela com nome no SQL original existe
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name ILIKE '%cotac%';
        """)

        print(f"\nTabelas relacionadas a cotações:")
        for row in cur.fetchall():
            print(f"  {row[0]}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Erro ao verificar estrutura: {e}")

if __name__ == "__main__":
    check_table_structure()