from sqlalchemy import create_engine, Column, Integer, String, Date, Numeric, DECIMAL, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

# Configuração da conexão com PostgreSQL local
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5433/cotacoes_b3"
)

Base = declarative_base()

class Cotacoes(Base):
    """
    Modelo SQLAlchemy para a tabela Cotacoes
    Permite múltiplos registros do mesmo ativo/data (diferentes transações)
    """
    __tablename__ = 'cotacoes'

    id = Column(Integer, primary_key=True, autoincrement=True)  # PK única - permite duplicatas de ativo+data
    ativo = Column(String(10), nullable=False)
    data_pregao = Column('datapregao', Date, nullable=False)
    abertura = Column(DECIMAL(10, 2))
    fechamento = Column(DECIMAL(10, 2))
    volume = Column(DECIMAL(18, 2))

    # Sem constraints únicas - cada registro é uma transação separada

    def __repr__(self):
        return f"<Cotacoes(id={self.id}, ativo='{self.ativo}', data_pregao='{self.data_pregao}', fechamento='{self.fechamento}')>"

class DatabaseManager:
    """Gerenciador da conexão com o banco PostgreSQL"""

    def __init__(self, database_url=None):
        self.database_url = database_url or DATABASE_URL
        self.engine = None
        self.SessionLocal = None

    def connect(self):
        """Estabelece conexão com o banco de dados"""
        try:
            self.engine = create_engine(self.database_url, echo=False)
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            print(f"[OK] Conectado ao banco PostgreSQL: {self.database_url}")
            return True
        except Exception as e:
            print(f"[ERROR] Falha ao conectar ao banco: {e}")
            return False

    def create_tables(self):
        """Cria as tabelas no banco se não existirem"""
        try:
            Base.metadata.create_all(bind=self.engine)
            print("[OK] Tabelas criadas/verificadas no banco")
            return True
        except Exception as e:
            print(f"[ERROR] Falha ao criar tabelas: {e}")
            return False

    def get_session(self):
        """Retorna uma nova sessão do banco"""
        if not self.SessionLocal:
            raise RuntimeError("Banco não conectado. Execute connect() primeiro.")
        return self.SessionLocal()

    def insert_cotacoes_batch(self, cotacoes_list):
        """
        Insere todas as cotações no banco (permite múltiplos registros do mesmo ativo/data)

        Args:
            cotacoes_list: Lista de dicionários com dados das cotações
                          Formato: {'ativo': str, 'data_pregao': date, 'abertura': float, 'fechamento': float, 'volume': float}
        """
        if not cotacoes_list:
            print("[WARNING] Lista de cotações vazia")
            return 0

        session = self.get_session()
        try:
            inserted_count = 0

            for cotacao_data in cotacoes_list:
                # Inserir cada registro - PK auto-incremento garante unicidade
                nova_cotacao = Cotacoes(
                    ativo=cotacao_data['ativo'],
                    data_pregao=cotacao_data['data_pregao'],
                    abertura=cotacao_data.get('abertura'),
                    fechamento=cotacao_data.get('fechamento'),
                    volume=cotacao_data.get('volume')
                )
                session.add(nova_cotacao)
                inserted_count += 1

            session.commit()
            print(f"[OK] Inseridas {inserted_count} cotações no banco (todos os registros mantidos)")
            return inserted_count

        except Exception as e:
            session.rollback()
            print(f"[ERROR] Falha ao inserir cotações: {e}")
            print(f"[DEBUG] Exemplo de dados: {cotacoes_list[:2] if cotacoes_list else 'Lista vazia'}")
            return 0
        finally:
            session.close()

if __name__ == "__main__":
    # Teste da conexão
    db = DatabaseManager()
    if db.connect():
        db.create_tables()