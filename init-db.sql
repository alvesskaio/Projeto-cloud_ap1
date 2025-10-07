-- Script de inicialização do banco PostgreSQL
-- Este arquivo será executado automaticamente quando o container PostgreSQL for iniciado

-- Criar a tabela Cotacoes conforme especificado
CREATE TABLE IF NOT EXISTS Cotacoes (
    Id SERIAL PRIMARY KEY,
    Ativo VARCHAR(10) NOT NULL,
    DataPregao DATE NOT NULL,
    Abertura DECIMAL(10,2),
    Fechamento DECIMAL(10,2),
    Volume DECIMAL(18,2),

    -- Índices para melhorar performance das consultas
    CONSTRAINT idx_ativo_data UNIQUE (Ativo, DataPregao)
);

-- Criar índices para otimizar consultas
CREATE INDEX IF NOT EXISTS idx_cotacoes_ativo ON Cotacoes (Ativo);
CREATE INDEX IF NOT EXISTS idx_cotacoes_data_pregao ON Cotacoes (DataPregao);
CREATE INDEX IF NOT EXISTS idx_cotacoes_ativo_data ON Cotacoes (Ativo, DataPregao);

-- Inserir alguns dados de exemplo (opcional)
-- INSERT INTO Cotacoes (Ativo, DataPregao, Abertura, Fechamento, Volume)
-- VALUES
--     ('PETR4', '2025-10-06', 35.50, 36.20, 1500000.00),
--     ('VALE3', '2025-10-06', 65.80, 66.15, 2300000.00)
-- ON CONFLICT (Ativo, DataPregao) DO NOTHING;

-- Mensagem de confirmação
DO $$
BEGIN
    RAISE NOTICE 'Banco de dados cotacoes_b3 inicializado com sucesso!';
    RAISE NOTICE 'Tabela Cotacoes criada com os seguintes campos:';
    RAISE NOTICE '  - Id (SERIAL PRIMARY KEY)';
    RAISE NOTICE '  - Ativo (VARCHAR(10))';
    RAISE NOTICE '  - DataPregao (DATE)';
    RAISE NOTICE '  - Abertura (DECIMAL(10,2))';
    RAISE NOTICE '  - Fechamento (DECIMAL(10,2))';
    RAISE NOTICE '  - Volume (DECIMAL(18,2))';
END $$;