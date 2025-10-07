#!/usr/bin/env python3
"""
Script para acessar e visualizar dados do PostgreSQL

Fornece informações sobre o banco de dados e permite executar
consultas básicas na tabela de cotações.
"""

from database import DatabaseManager
from sqlalchemy import text

def show_database_info():
    """
    Mostra informações gerais do banco de dados
    """
    print("=" * 60)
    print("🗄️  INFORMAÇÕES DO BANCO POSTGRESQL")
    print("=" * 60)

    db = DatabaseManager()
    if not db.connect():
        print("❌ Erro: Não foi possível conectar ao banco")
        print("   Verifique se o PostgreSQL está rodando: docker-compose up -d")
        return False

    try:
        session = db.get_session()

        # Informações de conexão
        print(f"🔗 URL de Conexão: {db.database_url}")
        print(f"📊 Status: Conectado com sucesso")

        # Contar total de registros
        total_records = session.execute(text("SELECT COUNT(*) FROM cotacoes")).scalar()
        print(f"📈 Total de cotações: {total_records:,}")

        # Contar ativos únicos
        unique_assets = session.execute(text("SELECT COUNT(DISTINCT ativo) FROM cotacoes")).scalar()
        print(f"🏢 Ativos únicos: {unique_assets}")

        # Data mais antiga e mais recente
        date_range = session.execute(text("""
            SELECT MIN(datapregao) as min_date, MAX(datapregao) as max_date
            FROM cotacoes
        """)).fetchone()

        if date_range.min_date and date_range.max_date:
            print(f"📅 Período: {date_range.min_date} até {date_range.max_date}")

        # Top 5 ativos com mais registros
        print("\n📊 TOP 5 ATIVOS COM MAIS REGISTROS:")
        top_assets = session.execute(text("""
            SELECT ativo, COUNT(*) as total
            FROM cotacoes
            GROUP BY ativo
            ORDER BY total DESC
            LIMIT 5
        """)).fetchall()

        for i, asset in enumerate(top_assets, 1):
            print(f"   {i}. {asset.ativo}: {asset.total:,} registros")

        session.close()
        return True

    except Exception as e:
        print(f"❌ Erro ao consultar banco: {e}")
        return False

def show_recent_data():
    """
    Mostra dados recentes da tabela
    """
    print("\n" + "=" * 60)
    print("📈 DADOS RECENTES (ÚLTIMOS 10 REGISTROS)")
    print("=" * 60)

    db = DatabaseManager()
    if not db.connect():
        return

    try:
        session = db.get_session()

        recent_data = session.execute(text("""
            SELECT id, ativo, datapregao, abertura, fechamento, volume
            FROM cotacoes
            ORDER BY id DESC
            LIMIT 10
        """)).fetchall()

        print(f"{'ID':<8} {'ATIVO':<8} {'DATA':<12} {'ABERTURA':<10} {'FECHAMENTO':<12} {'VOLUME':<15}")
        print("-" * 75)

        for row in recent_data:
            abertura = f"{row.abertura:.2f}" if row.abertura else "N/A"
            fechamento = f"{row.fechamento:.2f}" if row.fechamento else "N/A"
            volume = f"{row.volume:,.0f}" if row.volume else "N/A"

            print(f"{row.id:<8} {row.ativo:<8} {row.datapregao} {abertura:<10} {fechamento:<12} {volume:<15}")

        session.close()

    except Exception as e:
        print(f"❌ Erro ao consultar dados: {e}")

def show_connection_commands():
    """
    Mostra comandos para conectar ao banco via diferentes ferramentas
    """
    print("\n" + "=" * 60)
    print("🔧 COMANDOS PARA ACESSAR O BANCO")
    print("=" * 60)

    print("1️⃣ Via Docker (psql dentro do container):")
    print("   docker exec -it cotacoes_postgres psql -U postgres -d cotacoes_b3")

    print("\n2️⃣ Via psql local (se instalado):")
    print("   psql -h localhost -p 5433 -U postgres -d cotacoes_b3")

    print("\n3️⃣ Via pgAdmin4 (interface gráfica):")
    print("   Host: localhost")
    print("   Porta: 5433")
    print("   Banco: cotacoes_b3")
    print("   Usuário: postgres")
    print("   Senha: postgres")

    print("\n4️⃣ String de conexão completa:")
    print("   postgresql://postgres:postgres@localhost:5433/cotacoes_b3")

    print("\n5️⃣ Via DBeaver/DataGrip:")
    print("   Driver: PostgreSQL")
    print("   Host: localhost, Port: 5433")
    print("   Database: cotacoes_b3")
    print("   User: postgres, Password: postgres")

def interactive_query():
    """
    Permite executar consultas interativas
    """
    print("\n" + "=" * 60)
    print("💻 CONSULTA INTERATIVA")
    print("=" * 60)
    print("Digite uma consulta SQL (ou 'quit' para sair):")

    db = DatabaseManager()
    if not db.connect():
        return

    session = db.get_session()

    while True:
        try:
            query = input("\nSQL> ").strip()
            if query.lower() in ['quit', 'exit', 'q']:
                break

            if not query:
                continue

            result = session.execute(text(query))

            if query.lower().startswith('select'):
                rows = result.fetchall()
                if rows:
                    # Mostra header
                    if hasattr(rows[0], '_fields'):
                        print(" | ".join(rows[0]._fields))
                        print("-" * 50)

                    # Mostra dados
                    for row in rows[:20]:  # Limita a 20 linhas
                        print(" | ".join(str(col) for col in row))

                    if len(rows) > 20:
                        print(f"... e mais {len(rows) - 20} linhas")
                else:
                    print("Nenhum resultado encontrado")
            else:
                print("Consulta executada com sucesso")

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Erro na consulta: {e}")

    session.close()
    print("\n👋 Saindo da consulta interativa")

def main():
    """Função principal"""
    import sys

    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg in ['--query', '-q']:
            interactive_query()
            return
        elif arg in ['--help', '-h']:
            print("Uso: python show_db.py [opção]")
            print("Opções:")
            print("  --query, -q    Modo consulta interativa")
            print("  --help, -h     Mostra esta ajuda")
            return

    # Mostra informações padrão
    if show_database_info():
        show_recent_data()

    show_connection_commands()

if __name__ == "__main__":
    main()