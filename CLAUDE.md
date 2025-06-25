# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Model Context Protocol (MCP) server** for Base dos Dados, Brazil's open data platform. The server provides tools and resources for accessing datasets, tables, columns, and generating SQL queries through the MCP protocol.

## Architecture

- **server.py**: Main MCP server implementation with GraphQL client
- **requirements.txt**: Python dependencies (mcp, httpx, pydantic)
- **pyproject.toml**: Project configuration and build settings
- **README.md**: Project documentation

## Key Components

### MCP Server (`server.py`)
- GraphQL client for Base dos Dados API (https://api.basedosdados.org/api/v1/graphql)
- Tools for dataset search, information retrieval, and SQL generation
- Resources for help and dataset listing
- Pydantic models for data validation

### Available Tools
- `search_datasets`: Search datasets by name/theme/organization
- `get_dataset_info`: Get detailed dataset information
- `list_tables`: List tables in a dataset
- `get_table_info`: Get detailed table information
- `list_columns`: List columns in a table
- `get_column_info`: Get detailed column information  
- `generate_sql_query`: Generate BigQuery SQL for tables

## Development Commands

```bash
# Install dependencies
uv install -r requirements.txt

# Run server (for testing)
python server.py

# Format code
black server.py

# Lint code  
ruff server.py

# Test (if tests exist)
pytest
```

## Base dos Dados API

The server connects to Base dos Dados GraphQL API and provides access to:
- Datasets with themes, tags, and organization metadata
- Tables with column schemas and descriptions
- SQL query generation for BigQuery access
- Coverage and metadata information

## Notes

This server is based on analysis of the Base dos Dados backend Django application, which uses GraphQL, Django ORM, and integrates with BigQuery for data access.