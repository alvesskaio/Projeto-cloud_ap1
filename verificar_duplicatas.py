#!/usr/bin/env python3
"""
Verificar duplicatas diferenciadas por horário
"""

from database import DatabaseManager, Cotacoes
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

def verificar_duplicatas():
    db = DatabaseManager()
    db.connect()

    Session = sessionmaker(bind=db.engine)
    session = Session()

    # Verificar duplicatas por ativo+data
    duplicatas = session.query(
        Cotacoes.ativo,
        Cotacoes.data_pregao,
        func.count(Cotacoes.id).label('count')
    ).group_by(
        Cotacoes.ativo,
        Cotacoes.data_pregao
    ).having(
        func.count(Cotacoes.id) > 1
    ).order_by(
        func.count(Cotacoes.id).desc()
    ).limit(10).all()

    print("🔍 TOP 10 ativos com mais registros na mesma data:")
    if duplicatas:
        for ativo, data, count in duplicatas:
            print(f"  {ativo} em {data.strftime('%d/%m/%Y')}: {count} registros")

            # Mostrar diferentes registros para este ativo
            registros = session.query(
                Cotacoes.id,
                Cotacoes.abertura,
                Cotacoes.fechamento,
                Cotacoes.volume
            ).filter(
                Cotacoes.ativo == ativo,
                Cotacoes.data_pregao == data
            ).limit(5).all()

            for id_reg, abertura, fechamento, volume in registros:
                print(f"    ID={id_reg}: Abertura={abertura}, Fechamento={fechamento}, Volume={volume}")
            print()
    else:
        print("  Nenhuma duplicata encontrada (todos registros únicos por ativo+data)")

    # Verificar variação de preços para um ativo específico
    if duplicatas:
        primeiro_ativo = duplicatas[0][0]
        primeira_data = duplicatas[0][1]

        print(f"\n💰 Variação de preços para {primeiro_ativo} em {primeira_data.strftime('%d/%m/%Y')}:")
        precos = session.query(
            Cotacoes.abertura,
            Cotacoes.fechamento,
            Cotacoes.volume
        ).filter(
            Cotacoes.ativo == primeiro_ativo,
            Cotacoes.data_pregao == primeira_data
        ).all()

        for abertura, fechamento, volume in precos:
            print(f"  Abertura: {abertura}, Fechamento: {fechamento}, Volume: {volume}")

    session.close()

if __name__ == "__main__":
    verificar_duplicatas()