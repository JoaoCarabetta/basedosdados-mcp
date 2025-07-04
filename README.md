# Base dos Dados MCP Server

A Model Context Protocol (MCP) server that provides AI-optimized access to Base dos Dados, Brazil's largest open data platform.

## ✨ Features

- **🔍 Smart Search**: Portuguese language support with accent normalization
- **📊 BigQuery Integration**: Direct SQL execution and table references
- **🇧🇷 Brazilian Data Focus**: IBGE, RAIS, TSE, INEP datasets
- **🤖 AI-Optimized**: Single-call comprehensive data retrieval

## 🚀 Quick Install to Claude Desktop

```bash
bash -i <(curl -LsSf https://raw.githubusercontent.com/JoaoCarabetta/basedosdados-mcp/refs/heads/main/install.sh)
```

## 🛠️ Tools

| Tool | Description |
|------|-------------|
| `search_datasets` | Search datasets with Portuguese support |
| `get_dataset_overview` | Get complete dataset overview with tables |
| `get_table_details` | Get table details with columns and SQL samples |
| `execute_bigquery_sql` | Execute SQL queries directly |
| `check_bigquery_status` | Check BigQuery authentication |

## 💡 Usage

```python
# Search for data
search_datasets(query="população brasileira")

# Explore dataset
get_dataset_overview(dataset_id="DatasetNode:br_ibge_populacao_id")

# Get table details
get_table_details(table_id="TableNode:municipio_id")

# Execute SQL
execute_bigquery_sql(query="SELECT * FROM basedosdados.br_ibge_populacao.municipio LIMIT 10")
```

## 🔧 Development

```bash
git clone https://github.com/JoaoCarabetta/basedosdados-mcp
cd basedosdados-mcp
uv sync
uv run basedosdados-mcp
```

## 📚 About

[Base dos Dados](https://basedosdados.org) is Brazil's largest open data platform, providing standardized access to Brazilian public datasets through BigQuery.

**Data Coverage**: Demographics, Economics, Education, Politics, Health, Environment

## 📄 License

MIT License