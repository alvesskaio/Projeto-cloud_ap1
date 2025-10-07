# 📊 Sistema### **📁 Gestão de Armazenamento**
```
dados_b3/               # Pasta temporária (vazia após processamento)
├── (criada durante extração)
└── (limpa após envio para blob)

Azure Blob Storage:     # Armazenamento permanente
├── BVBG186_250923.xml
├── BVBG186_250924.xml
└── BVBG186_250925.xml
```ção de Cotações B3 - Guia de Uso

## ✅ Funcionalidades Implementadas

### 🚀 **Pipeline Completo Funcionando**
- ✅ **Extração**: Download automático da B3 com data atual ou específica
- ✅ **Armazenamento Local**: Pasta `dados_b3/` mantida com arquivos XML extraídos
- ✅ **Gestão de ZIP Aninhados**: Extrai automaticamente os dois níveis de ZIP
- ✅ **Blob Storage**: Envia para Azurite (emulador local)
- ✅ **Banco de Dados**: Processa XML e insere no PostgreSQL
- ✅ **Limpeza Inteligente**: Remove apenas arquivos ZIP temporários

### 📁 **Estrutura de Pastas Criada**
```
dados_b3/
├── SPRE250923/
│   └── BVBG.186.01_BV000471202509230001000061919292430.xml
├── SPRE250924/
│   └── BVBG.186.01_BV000471202509240001000061923366930.xml
└── (outras datas conforme processadas)
```

## 🎯 **Como Usar**

### **1. Data Atual (Automática)**
```bash
python main.py
```
- Usa `helpers.yymmdd(datetime.now())` automaticamente
- Formato: YYMMDD (ex: 251007 para 07/10/2025)

### **2. Data Específica**
```bash
python main.py 250923  # Processa dados de 23/09/2025
python main.py 250924  # Processa dados de 24/09/2025
```

### **3. Verificar Pré-requisitos**
```bash
python main.py --check
```

### **4. Ajuda**
```bash
python main.py --help
```

## 📋 **Fluxo de Dados Detalhado**

### **Etapa 1: Extração**
1. **Data**: Define data atual ou específica usando `helpers.yymmdd()`
2. **URL**: Constrói URL da B3: `https://www.b3.com.br/pesquisapregao/download?filelist=SPRE{data}.zip`
3. **Download**: Baixa arquivo ZIP principal
4. **Pasta**: Verifica/cria `dados_b3/` (temporária)
5. **ZIP 1**: Extrai primeiro nível → `dados_b3/pregao_{data}/`
6. **ZIP 2**: Extrai segundo nível → `dados_b3/SPRE{data}/`
7. **XML**: Encontra arquivo XML final (ex: `BVBG.186.01_BV...xml`)
8. **Blob**: Envia para Azurite como `BVBG186_{data}.xml`
9. **Limpeza**: **Remove TODOS os arquivos locais** (ZIPs + XMLs) - mantém apenas no blob

### **Etapa 2: Processamento**
1. **XML**: Lê arquivo do blob storage
2. **Parse**: Processa namespace B3 e extrai cotações
3. **Validação**: Remove duplicatas e valida dados
4. **Database**: Insere/atualiza no PostgreSQL

## 🔧 **Correções Implementadas**

| Problema Original | ❌ Antes | ✅ Agora |
|---|---|---|
| **Data Fixa** | `dt = "250923"` | `dt = yymmdd(datetime.now())` |
| **Pasta Deletada** | `shutil.rmtree(dados_b3)` | Remove arquivos após blob |
| **Sem Validação** | Falha silenciosa | Validações robustas |
| **Data Manual** | Hardcoded | Usa `helpers.py` |
| **Sem Logs** | Poucos logs | Logs detalhados |

## 🎉 **Exemplo de Execução Bem-Sucedida**

```bash
$ python main.py 250924

============================================================
PIPELINE DE PROCESSAMENTO DE COTAÇÕES B3
============================================================
[INFO] Iniciando pipeline para data: 250924
[INFO] Arquivo alvo: BVBG186_250924.xml

[ETAPA 1] Extração de dados da B3...
[INFO] Extraindo dados para a data: 250924
[INFO] URL: https://www.b3.com.br/pesquisapregao/download?filelist=SPRE250924.zip
[INFO] Pasta dados_b3 já existe
[OK] Baixado arquivo de cotações
[OK] Arquivos extraídos e salvos em ./dados_b3/SPRE250924/
[OK] Extração concluída com sucesso

[ETAPA 2] Transformação e carga no PostgreSQL...
[INFO] Processadas 11723 cotações
[OK] Pipeline concluído com sucesso
============================================================
[SUCCESS] Pipeline executado com sucesso!
```

## 🏗️ **Arquitetura Final**

```
main.py           # Orquestrador principal
├── helpers.py    # yymmdd(datetime.now())
├── extract.py    # Download B3 → dados_b3/ → Blob
└── transform_load.py  # Blob → PostgreSQL

dados_b3/         # Pasta persistente com XMLs extraídos
├── SPRE{data}/   # Uma pasta por data processada
└── arquivo.xml   # XML original da B3
```

**🎯 Sistema funcionando perfeitamente com data atual automática e gestão inteligente de arquivos!**