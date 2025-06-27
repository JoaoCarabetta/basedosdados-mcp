# Base dos Dados MCP Server
[![smithery badge](https://smithery.ai/badge/@JoaoCarabetta/basedosdados-mcp)](https://smithery.ai/server/@JoaoCarabetta/basedosdados-mcp)

A Model Context Protocol (MCP) server that provides access to Base dos Dados, Brazil's open data platform.

## Features

- Search and browse datasets from Base dos Dados
- Get detailed information about datasets, tables, and columns
- Generate SQL queries for BigQuery access
- Access metadata and documentation

## Usage

### As MCP Server

To run the server locally and connect with Claude Desktop:

1.  **Install dependencies**:
    ```bash
    uv init
    ```
2.  **Run the MCP server**:
    ```bash
    bash run_server.sh
    ```
    This will start the server, typically on `http://127.0.0.1:8000`.

3.  **Configure Claude Desktop**:
    In Claude Desktop, add a new MCP server configuration pointing to your local server. The configuration should look similar to this:

    ```json
    {
      "mcpServers": {
        /// BigQuery MCP
        "bigquery": {
          "command": "npx",
          "args": [
            "-y",
            "@ergut/mcp-bigquery-server",
            "--project-id",
            "<replcae with project id>",
            "--location",
            "<replace with location>",
            "--key-file",
            "<replace with service account path>"
          ]
        },
        "basedosdados": {
          "command": "/Users/joaoc/Documents/projects/basedosdados_mcp/run_server.sh"
        }
      }
    }
    ```
    **Note**: This MCP server provides access to Base dos Dados metadata and tools. To query data directly from BigQuery, you will need a separate MCP server configured for BigQuery access.


### Installing via Smithery

To install basedosdados-mcp for Claude Desktop automatically via [Smithery](https://smithery.ai/server/@JoaoCarabetta/basedosdados-mcp):

```bash
npx -y @smithery/cli install @JoaoCarabetta/basedosdados-mcp --client claude
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
uv init
```

### Testing

#### Run MCP Server
```bash
uv run basedosdados-mcp
```

#### Debug and Test API Endpoints

Use the comprehensive endpoint testing script (ensure dependencies are installed first):

```bash
# Install dependencies if not already done
uv init

# Test default endpoint with full test suite
uv run debug_endpoints.py

# Test custom endpoint
uv run debug_endpoints.py --endpoint https://custom-api-url.com/graphql

# Quick connectivity test only
uv run debug_endpoints.py --quick

# Save detailed results to JSON file
uv run debug_endpoints.py --output test_results.json
```

**Debug Features:**
- ✅ Configurable endpoint URL testing
- ✅ Comprehensive GraphQL query testing
- ✅ Detailed error reporting and timing
- ✅ Progressive testing (searches → datasets → tables → columns)
- ✅ JSON export of test results
- ✅ Quick connectivity verification mode

## About Base dos Dados

Base dos Dados is Brazil's open data platform that provides access to public datasets through BigQuery. Visit [basedosdados.org](https://basedosdados.org) for more information.