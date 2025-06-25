# Base dos Dados MCP Server

A Model Context Protocol (MCP) server that provides access to Base dos Dados, Brazil's open data platform.

## Features

- Search and browse datasets from Base dos Dados
- Get detailed information about datasets, tables, and columns
- Generate SQL queries for BigQuery access
- Access metadata and documentation

## Installation

```bash
uv install -r requirements.txt
```

## Usage

### As MCP Server

Add to your MCP client configuration:

```json
{
  "mcpServers": {
    "basedosdados": {
      "command": "python",
      "args": ["server.py"],
      "cwd": "/path/to/basedosdados_mcp"
    }
  }
}
```

### Available Tools

- **search_datasets**: Search for datasets by name, theme, or organization
- **get_dataset_info**: Get detailed information about a specific dataset
- **list_tables**: List tables in a dataset
- **get_table_info**: Get detailed information about a specific table
- **list_columns**: List columns in a table
- **get_column_info**: Get detailed information about a specific column
- **generate_sql_query**: Generate SQL query for a table

### Available Resources

- **basedosdados://datasets**: List available datasets
- **basedosdados://help**: Help information

## Development

### Setup

```bash
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv install -r requirements.txt
```

### Testing

```bash
python server.py
```

## About Base dos Dados

Base dos Dados is Brazil's open data platform that provides access to public datasets through BigQuery. Visit [basedosdados.org](https://basedosdados.org) for more information.