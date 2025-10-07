#!/usr/bin/env python3
"""
Relatório rápido do banco de dados
"""

from database import DatabaseManager, Cotacoes
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

def relatorio_banco():
    db = DatabaseManager()
    db.connect()

    Session = sessionmaker(bind=db.engine)
    session = Session()

    # Total de registros
    total = session.query(Cotacoes).count()
    print(f"📊 Total de cotações no banco: {total:,}")

    # Últimas datas
    dates = session.query(
        Cotacoes.data_pregao,
        func.count(Cotacoes.id)
    ).group_by(
        Cotacoes.data_pregao
    ).order_by(
        Cotacoes.data_pregao.desc()
    ).limit(5).all()

    print("\n📅 Últimas 5 datas processadas:")
    for date, count in dates:
        print(f"  {date.strftime('%d/%m/%Y')}: {count:,} cotações")

    session.close()

if __name__ == "__main__":
    relatorio_banco()