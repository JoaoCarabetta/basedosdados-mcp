# Base dos Dados MCP

A Model Context Protocol (MCP) server that provides AI-optimized access to Base dos Dados, Brazil's largest open data platform.

## 💡 Create Reports with Real Brazilian Datasets with Claude Desktop

#### Ask it to replicate a news article:

```
User:
https://www.gov.br/secom/pt-br/assuntos/noticias/2025/05/no-melhor-abril-do-novo-caged-brasil-gera-257-mil-vagas-com-carteira-assinada

consegue replicar a materia e checar os numeros?

Claude: https://claude.ai/public/artifacts/15d7f5c5-f017-4a96-9380-a93e535001fd
```

#### Or a research question

```
User:
https://claude.ai/share/f8dbbe5b-1c34-4804-9462-1bfb9008558d

Claude: https://claude.ai/share/f8dbbe5b-1c34-4804-9462-1bfb9008558d
```

#### 


## ✨ Features

- **🔍 Smart Search**: Portuguese language support with accent normalization
- **📊 BigQuery Integration**: Direct SQL execution and table references
- **🇧🇷 Brazilian Data Focus**: IBGE, RAIS, TSE, INEP datasets
- **🤖 AI-Optimized**: Single-call comprehensive data retrieval

## 🚀 Quick Install to Claude Desktop

Just run this line in your terminal
```bash
bash -i <(curl -LsSf https://raw.githubusercontent.com/JoaoCarabetta/basedosdados-mcp/refs/heads/main/install.sh)
```

Add your BigQuery project_id, location and service account.

Restart Claude Desktop and you are good to go!

⚠️ I just tested it in my Mac M-Series

## 🛠️ Tools

| Tool | Description |
|------|-------------|
| `search_datasets` | Search datasets with Portuguese support |
| `get_dataset_overview` | Get complete dataset overview with tables |
| `get_table_details` | Get table details with columns and SQL samples |
| `execute_bigquery_sql` | Execute SQL queries directly |
| `check_bigquery_status` | Check BigQuery authentication |







## 🔧 Development

```bash
git clone https://github.com/JoaoCarabetta/basedosdados-mcp
cd basedosdados-mcp
uv sync
uv run basedosdados-mcp
```

## 🧪 Testing

Install development dependencies and run the comprehensive test suite:

```bash
# Install dev dependencies (includes pytest)
uv sync --extra dev

# Run all encoding tests
pytest tests/ -v

# Run specific test categories
pytest tests/ -k "encoding" -v        # Encoding tests only
pytest tests/ -k "live" -v            # Live server tests only
pytest tests/ -m "not live" -v        # Skip live tests (faster)

# Run with coverage
pytest tests/ --cov=basedosdados_mcp --cov-report=html
```

### Test Structure

The pytest-based test suite includes:

- **`test_encoding_pytest.py`**: Core Portuguese character encoding tests
  - UTF-8 encoding/decoding validation
  - JSON serialization integrity
  - MCP protocol compatibility
  - Parametrized tests for individual Portuguese words

- **`test_live_mcp_pytest.py`**: Live MCP server endpoint tests
  - Real API response validation
  - Backend API encoding verification
  - Large response handling
  - End-to-end encoding flow

- **`conftest.py`**: Shared fixtures and pytest configuration
  - Common test data and utilities
  - Automatic test marking and organization

### Encoding Validation

**Portuguese characters tested**: `população`, `educação`, `saúde`, `região`, `satélites`, `políticas`, `públicas`, and 30+ more common Brazilian dataset terms.

**Corruption patterns detected**: `Ã©`, `Ã¡`, `Ã­`, `Ã³`, `Ãº`, `Ã£`, `Ã§`, `Ã´`, `Ãª`, `Ã `, `Ãµ` (common UTF-8 corruption)

If you experience encoding issues in Claude Desktop (e.g., seeing `satÃ©lites` instead of `satélites`), run the tests to verify the issue is in the display layer, not the MCP server itself.

## 📚 About

[Base dos Dados](https://basedosdados.org) is Brazil's largest open data platform, providing standardized access to Brazilian public datasets through BigQuery.

**Data Coverage**: Demographics, Economics, Education, Politics, Health, Environment

## 📄 License

MIT License