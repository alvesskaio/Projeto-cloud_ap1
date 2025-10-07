# Pipeline de Processamento de Cotações B3

Este projeto implementa um pipeline local para processar arquivos XML de cotações da B3 (Bolsa de Valores do Brasil) usando:

- **Blob Storage local**: Azurite (emulador do Azure Blob Storage)
- **Banco de dados**: PostgreSQL local
- **Processamento**: Python com SQLAlchemy e lxml

## 📋 Pré-requisitos

### Software necessário:
- Docker e Docker Compose
- Python 3.8+
- pip (gerenciador de pacotes Python)

### Serviços que serão executados:
- PostgreSQL (porta 5432)
- Azurite - Azure Storage Emulator (porta 10000)
- pgAdmin (opcional, porta 8080)

## 🚀 Instalação e Configuração

### 1. Clone o repositório e instale dependências Python:

```bash
cd Projeto-cloud_ap1
pip install -r requirements.txt
```

### 2. Inicie os serviços com Docker Compose:

```bash
# Inicia PostgreSQL e Azurite
docker-compose up -d

# Para incluir pgAdmin (interface web):
docker-compose --profile tools up -d
```

### 3. Verifique se os serviços estão rodando:

```bash
python main.py --check
```

## 📊 Estrutura do Banco de Dados

```sql
CREATE TABLE Cotacoes (
    Id SERIAL PRIMARY KEY,
    Ativo VARCHAR(10),
    DataPregao DATE,
    Abertura DECIMAL(10,2),
    Fechamento DECIMAL(10,2),
    Volume DECIMAL(18,2)
);
```

## 🔄 Como Usar o Pipeline

### Execução básica:
```bash
# Processa cotações da data atual
python main.py

# Processa cotações de uma data específica (formato YYMMDD)
python main.py 250923

# Verifica pré-requisitos
python main.py --check

# Exibe ajuda
python main.py --help
```

### O pipeline executa as seguintes etapas:

1. **Extração**:
   - Download do arquivo ZIP da B3
   - Extração dos arquivos XML
   - Upload para o Blob Storage local (Azurite)

2. **Transformação**:
   - Leitura do XML do Blob Storage
   - Parse dos dados usando lxml
   - Extração de: Ativo, Data Pregão, Abertura, Fechamento, Volume

3. **Carga**:
   - Inserção dos dados no PostgreSQL usando SQLAlchemy
   - Processamento em lotes para otimizar performance

## 📁 Estrutura do Projeto

```
Projeto-cloud_ap1/
├── main.py              # Pipeline principal
├── extract.py           # Módulo de extração (download B3)
├── transform_load.py    # Módulo de transformação e carga
├── azure_storage.py     # Módulo de integração com Blob Storage
├── database.py          # Modelos SQLAlchemy e conexão com PostgreSQL
├── helpers.py           # Funções auxiliares
├── requirements.txt     # Dependências Python
├── docker-compose.yml   # Configuração dos serviços Docker
├── init-db.sql         # Script de inicialização PostgreSQL
└── README.md           # Este arquivo
```

## 🔍 Monitoramento e Depuração

### Ver logs dos containers:
```bash
docker-compose logs postgres
docker-compose logs azurite
```

### Acessar PostgreSQL diretamente:
```bash
docker exec -it cotacoes_postgres psql -U postgres -d cotacoes_b3
```

### Acessar pgAdmin (se habilitado):
- URL: http://localhost:8080
- Email: admin@cotacoes.com
- Senha: admin123

### Verificar dados inseridos:
```sql
-- No PostgreSQL
SELECT COUNT(*) FROM cotacoes;
SELECT * FROM cotacoes LIMIT 10;
SELECT ativo, COUNT(*) FROM cotacoes GROUP BY ativo ORDER BY COUNT(*) DESC LIMIT 10;
```

## ⚙️ Configurações

### Variáveis de ambiente (opcionais):

```bash
# String de conexão com PostgreSQL
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/cotacoes_b3"
```

### Conexão Azurite:
- Endpoint: http://localhost:10000/devstoreaccount1
- Container: dados-pregao-bolsa
- Connection String: (configurada em azure_storage.py)

## 🛠️ Desenvolvimento

### Executar apenas um módulo:
```bash
# Só extração
python extract.py

# Só transformação e carga
python transform_load.py

# Testar conexão com banco
python database.py
```

### Parar todos os serviços:
```bash
docker-compose down

# Para remover também os volumes (dados):
docker-compose down -v
```

## ❗ Solução de Problemas

### Erro de conexão PostgreSQL:
```bash
# Verificar se o container está rodando
docker ps
docker-compose logs postgres
```

### Erro de conexão Azurite:
```bash
# Verificar se o container está rodando
docker ps | grep azurite
docker-compose logs azurite
```

### Erro de dependências Python:
```bash
pip install --upgrade -r requirements.txt
```

### Limpar dados e recomeçar:
```bash
docker-compose down -v
docker-compose up -d
```

## 📈 Performance

- O processamento de arquivos XML grandes (70MB+) é otimizado usando `iterparse` para economizar memória
- Inserções no banco são feitas em lotes para melhorar performance
- Índices foram criados nas colunas mais consultadas (Ativo, DataPregao)

## 🔒 Segurança

Este projeto foi desenvolvido para execução **local apenas**. Para uso em produção, considere:

- Configurar senhas seguras para PostgreSQL
- Usar SSL/TLS para conexões com banco
- Implementar autenticação adequada
- Configurar backup dos dados

## 📝 Logs

O pipeline gera logs detalhados mostrando:
- Status de cada etapa
- Número de registros processados
- Erros e avisos
- Tempo de execução