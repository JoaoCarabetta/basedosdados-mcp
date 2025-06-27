# GEMINI.md

This file provides guidance to Gemini when working with code in this repository.

## Project Overview

This is a **Model Context Protocol (MCP) server** for Base dos Dados, Brazil's open data platform. The server provides AI-optimized tools and resources for accessing datasets, tables, columns, and generating SQL queries through the MCP protocol.

## Architecture

The project follows a modular structure to separate concerns and improve maintainability.

- `src/basedosdados_mcp/main.py`: Main application entry point for the MCP server.
- `src/basedosdados_mcp/server.py`: Initializes the core MCP server instance.
- `src/basedosdados_mcp/config.py`: Stores configuration, like the GraphQL API endpoint.
- `src/basedosdados_mcp/models.py`: Contains Pydantic data models for API objects.
- `src/basedosdados_mcp/utils.py`: Includes helper functions for data processing and search ranking.
- `src/basedosdados_mcp/graphql_client.py`: Manages communication with the Base dos Dados GraphQL API.
- `src/basedosdados_mcp/resources.py`: Defines and handles MCP resources (e.g., help text).
- `src/basedosdados_mcp/tools.py`: Implements the core MCP tools exposed to the client.
- `pyproject.toml`: Project configuration, dependencies, and entry points.
- `README.md`: High-level project documentation.

## Key Components

### Modular MCP Server (`src/basedosdados_mcp/`)
- **GraphQL Client**: Connects to the Base dos Dados API (`https://backend.basedosdados.org/graphql`).
- **AI-Optimized Tools**: Designed for single-call, comprehensive data retrieval with ready-to-use BigQuery references.
- **Smart Search**: Features Portuguese accent normalization, acronym prioritization, and intelligent result ranking.
- **Enhanced Resources**: Provides context-aware help and guidance formatted for LLM consumption.

### AI-Optimized Tools

1.  **`search_datasets`**: Performs an enhanced search with table/column counts and BigQuery references. It includes Portuguese accent normalization (`populacao` → `população`), prioritizes common acronyms (RAIS, IBGE), and provides a comprehensive structure preview.
2.  **`get_dataset_overview`**: Returns a complete dataset view in a single call, including all tables, column counts, and full BigQuery references (e.g., `basedosdados.br_ibge_populacao.municipio`).
3.  **`get_table_details`**: Provides comprehensive table information, including all columns with their types and descriptions, plus multiple sample SQL queries for analysis.
4.  **`explore_data`**: Enables multi-level data exploration with different modes (`overview`, `detailed`) for either a quick summary or a deep dive into the data structure.

### Resources
- **`basedosdados://help`**: AI-optimized help with workflow guidance.
- **`basedosdados://datasets`**: Guidance on how to discover datasets using the available tools.

## Development Commands

```bash
# Install dependencies
pip install -e .[dev]

# Run the MCP server using the script defined in pyproject.toml
basedosdados-mcp

# Format code
black src/

# Lint code
ruff check src/
```

## Enhanced Features

### Portuguese Language Support
- **Accent Normalization**: Handles cases where users type without accents (`saude` → `saúde`).
- **Acronym Recognition**: Prioritizes key Brazilian acronyms like RAIS, IBGE, IPEA, INEP, TSE, and SUS.

### Smart Search Ranking
- **Exact Matches**: Prioritizes exact slug and name matches.
- **Acronym Bonus**: Boosts relevance for important data sources.
- **Source Prioritization**: Gives preference to official government organizations.

### AI-Friendly Output
- **Single-Call Efficiency**: Retrieves comprehensive information in one go to minimize round-trips.
- **Structured Responses**: Formats output consistently for reliable LLM consumption.
- **Ready-to-Use References**: Includes direct BigQuery table paths.
- **Next-Step Guidance**: Offers clear instructions for follow-up actions.

## Recent Changes

### 2025-06-26: Code Refactoring
- **Modular Structure**: Refactored the monolithic `server.py` into a modular architecture within the `src/basedosdados_mcp` directory.
- **Clear Separation of Concerns**: Each module now has a specific responsibility (e.g., `graphql_client.py` for API calls, `tools.py` for tool implementations).
- **Updated Entry Point**: The server is now launched via `basedosdados_mcp.main:main`, as defined in `pyproject.toml`.

## Notes

This server provides comprehensive, AI-optimized access to Base dos Dados through the MCP protocol. It is designed for efficient LLM interaction with Brazilian public data, offering single-call information retrieval and context-aware guidance for data exploration and analysis.
