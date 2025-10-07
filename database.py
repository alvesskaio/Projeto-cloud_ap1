from sqlalchemy import create_engine, Column, Integer, String, Date, Numeric, DECIMAL, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
import time
from tqdm import tqdm

def print_timestamp():
    """Retorna timestamp formatado para logs"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Configuração da conexão com PostgreSQL local
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5433/cotacoes_b3"
)

Base = declarative_base()

def print_timestamp():
    """Retorna timestamp formatado para logs"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
        connect_start = time.time()
        
        try:
            print(f"[{print_timestamp()}] [INFO] 🔌 Iniciando conexão com PostgreSQL...")
            print(f"[{print_timestamp()}] [INFO] 🎯 URL: {self.database_url.split('@')[1] if '@' in self.database_url else self.database_url}")
            
            self.engine = create_engine(self.database_url, echo=False)
            
            # Teste a conexão fazendo uma query simples
            connection = self.engine.connect()
            result = connection.execute(text("SELECT version()"))
            version_info = result.fetchone()[0]
            connection.close()
            
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            
            connect_time = time.time() - connect_start
            print(f"[{print_timestamp()}] [OK] ✅ Conectado ao PostgreSQL em {connect_time:.2f}s")
            print(f"[{print_timestamp()}] [INFO] 🗄️ Servidor: {version_info.split(' on ')[0]}")
            
            return True
            
        except Exception as e:
            connect_time = time.time() - connect_start
            print(f"[{print_timestamp()}] [ERROR] ❌ Falha ao conectar ao banco após {connect_time:.2f}s: {e}")
            print(f"[{print_timestamp()}] [INFO] 💡 Dica: Verifique se o PostgreSQL está rodando com 'docker-compose up -d'")
            return False

    def create_tables(self):
        """Cria as tabelas no banco se não existirem"""
        create_start = time.time()
        
        try:
            print(f"[{print_timestamp()}] [INFO] 🗄️ Verificando estrutura de tabelas...")
            
            # Verificar se as tabelas já existem
            from sqlalchemy import inspect
            inspector = inspect(self.engine)
            existing_tables = inspector.get_table_names()
            
            if 'cotacoes' in existing_tables:
                print(f"[{print_timestamp()}] [INFO] ✅ Tabela 'cotacoes' já existe")
                
                # Verificar estrutura da tabela
                columns = inspector.get_columns('cotacoes')
                column_names = [col['name'] for col in columns]
                print(f"[{print_timestamp()}] [INFO] 📋 Colunas encontradas: {', '.join(column_names)}")
                
                # Contar registros existentes
                try:
                    with self.engine.connect() as conn:
                        result = conn.execute(text("SELECT COUNT(*) FROM cotacoes"))
                        count = result.fetchone()[0]
                        print(f"[{print_timestamp()}] [INFO] 📊 Registros existentes: {count:,}")
                except Exception as e:
                    print(f"[{print_timestamp()}] [WARN] ⚠️ Não foi possível contar registros: {e}")
            else:
                print(f"[{print_timestamp()}] [INFO] 🏗️ Tabela 'cotacoes' não existe, será criada")
            
            Base.metadata.create_all(bind=self.engine)
            
            create_time = time.time() - create_start
            print(f"[{print_timestamp()}] [OK] ✅ Estrutura de tabelas verificada/criada em {create_time:.2f}s")
            return True
            
        except Exception as e:
            create_time = time.time() - create_start
            print(f"[{print_timestamp()}] [ERROR] ❌ Falha ao criar tabelas após {create_time:.2f}s: {e}")
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
            print(f"[{print_timestamp()}] [WARNING] ⚠️ Lista de cotações vazia")
            return 0

        insert_start = time.time()
        batch_size = 1000  # Inserir em batches para melhor performance
        total_records = len(cotacoes_list)
        
        print(f"[{print_timestamp()}] [INFO] 💾 Iniciando inserção de {total_records:,} cotações em batches de {batch_size}")

        session = self.get_session()
        try:
            inserted_count = 0
            
            # Barra de progresso para inserção
            progress_bar = tqdm(
                cotacoes_list,
                desc="💾 Inserindo no DB",
                unit="registros",
                bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
            )
            
            for i, cotacao_data in enumerate(progress_bar):
                # Commit em batches para melhor performance
                if i % batch_size == 0 and i > 0:
                    session.commit()
                
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

            # Commit final
            progress_bar.close()
            session.commit()
            
            insert_time = time.time() - insert_start
            rate = inserted_count / insert_time if insert_time > 0 else 0
            
            print(f"[{print_timestamp()}] [OK] ✅ Inserção concluída com sucesso!")
            print(f"[{print_timestamp()}] [INFO] 📊 ESTATÍSTICAS DE INSERÇÃO:")
            print(f"[{print_timestamp()}] [INFO]   📥 Registros inseridos: {inserted_count:,}")
            print(f"[{print_timestamp()}] [INFO]   ⏱️  Tempo total: {insert_time:.2f}s")
            print(f"[{print_timestamp()}] [INFO]   📈 Taxa de inserção: {rate:.0f} registros/s")
            print(f"[{print_timestamp()}] [INFO]   💾 Todos os registros mantidos (sem remoção de duplicatas)")
            
            return inserted_count

        except Exception as e:
            session.rollback()
            insert_time = time.time() - insert_start
            
            print(f"[{print_timestamp()}] [ERROR] ❌ Falha ao inserir cotações após {insert_time:.2f}s: {e}")
            print(f"[{print_timestamp()}] [DEBUG] 🔍 Exemplo de dados:")
            
            if cotacoes_list:
                for i, exemplo in enumerate(cotacoes_list[:3]):
                    print(f"[{print_timestamp()}] [DEBUG]   [{i+1}] {exemplo}")
            else:
                print(f"[{print_timestamp()}] [DEBUG]   Lista vazia")
            
            # Informações adicionais de debug
            try:
                # Verificar se conseguimos ao menos conectar ao banco
                result = session.execute(text("SELECT 1"))
                print(f"[{print_timestamp()}] [DEBUG] ✅ Conexão com banco ainda ativa")
            except Exception as conn_error:
                print(f"[{print_timestamp()}] [DEBUG] ❌ Conexão com banco perdida: {conn_error}")
            
            return 0
            
        finally:
            session.close()

if __name__ == "__main__":
    # Teste da conexão
    db = DatabaseManager()
    if db.connect():
        db.create_tables()