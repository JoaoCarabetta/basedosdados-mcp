# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Model Context Protocol (MCP) server** for Base dos Dados, Brazil's open data platform. The server provides AI-optimized tools and resources for accessing datasets, tables, columns, and generating SQL queries through the MCP protocol.

## Architecture

- **server.py**: Main MCP server implementation with enhanced GraphQL client and AI-friendly tools
- **run_server.sh**: Wrapper script for reliable Claude Desktop integration
- **pyproject.toml**: Project configuration with all dependencies (mcp, httpx, pydantic, google-cloud-bigquery)
- **test_*.py**: Comprehensive testing and debugging scripts
- **README.md**: Project documentation

## Key Components

### Enhanced MCP Server (`server.py`)
- **GraphQL Client**: Connects to Base dos Dados API (https://backend.basedosdados.org/graphql)
- **AI-Optimized Tools**: Single-call comprehensive data retrieval with BigQuery references
- **Smart Search**: Portuguese accent normalization, acronym prioritization, intelligent ranking
- **Enhanced Resources**: Context-aware help and guidance for LLM consumption
- **Error Handling**: Robust error handling with detailed debugging information

### AI-Optimized Tools (4 enhanced tools)

1. **`search_datasets`**: Enhanced search with table/column counts and BigQuery references
   - Portuguese accent normalization (`populacao` → `população`)
   - Acronym prioritization (RAIS, IBGE, IPEA get top results)
   - Comprehensive structure preview with sample BigQuery paths
   - Intelligent ranking based on relevance scores

2. **`get_dataset_overview`**: Complete dataset view in single call
   - All tables with column counts and BigQuery references
   - Ready-to-use SQL paths like `basedosdados.br_ibge_populacao.municipio`
   - Sample columns for quick structure understanding
   - Next-step guidance for further exploration

3. **`get_table_details`**: Comprehensive table information
   - All columns with types, descriptions, and IDs
   - Multiple sample SQL queries (basic select, schema info, access patterns)
   - BigQuery reference and Python package instructions
   - Optimization tips and best practices

4. **`explore_data`**: Multi-level data exploration
   - Overview mode: Quick summary with top tables
   - Detailed mode: Complete dataset structure
   - Related mode: Find similar datasets
   - Context-aware responses based on exploration level


### Resources
- **basedosdados://help**: AI-optimized help with workflow guidance
- **basedosdados://datasets**: Dataset discovery guidance

## Development Commands

```bash
# Install dependencies with BigQuery support
uv sync

# Run server for testing
uv run server.py

# Test MCP connection and all tools
uv run test_mcp_fixed.py

# Test BigQuery service account permissions
uv run test_bigquery_permissions.py

# Test specific tool functionality
uv run test_claude_tools.py

# Format code
black server.py

# Lint code  
ruff server.py
```

## Claude Desktop Integration

### Configuration (`claude_desktop_config.json`)
```json
{
  "mcpServers": {
    "basedosdados": {
      "command": "/Users/joaoc/Documents/projects/basedosdados_mcp/run_server.sh"
    }
  }
}
```

### Wrapper Script (`run_server.sh`)
- Ensures correct working directory and environment
- Provides debug logging to stderr for troubleshooting
- Uses `uv run` for proper virtual environment activation
- Handles all dependency resolution automatically

### Debugging
- **Logs**: `~/Library/Logs/Claude/mcp-server-basedosdados.log`
- **Connection test**: `uv run test_mcp_fixed.py`
- **BigQuery permissions**: `uv run test_bigquery_permissions.py`
- **Tool functionality**: `uv run test_claude_tools.py`

## Enhanced Features

### Portuguese Language Support
- **Accent Normalization**: Handles common cases where users type without accents
- **Common Patterns**: `populacao` → `população`, `educacao` → `educação`, `saude` → `saúde`
- **Acronym Recognition**: RAIS, IBGE, IPEA, INEP, TSE, SUS, PNAD prioritized correctly

### Smart Search Ranking
- **Exact matches**: Slug and name matches get highest priority
- **Acronym bonus**: Important Brazilian data acronyms get relevance boost
- **Position weighting**: Earlier word positions in names get higher scores
- **Official sources**: Government organizations get priority ranking

### AI-Friendly Output
- **Single-call efficiency**: Comprehensive information without multiple API calls
- **Structured responses**: Consistent formatting for LLM consumption
- **Ready-to-use references**: Direct BigQuery table paths included
- **Next-step guidance**: Clear instructions for follow-up actions
- **Debug information**: Search strategies and query processing details

### BigQuery Integration
- **Service Account Support**: Tested with `google-cloud-bigquery`
- **Access Verification**: Automated permission testing
- **Query Generation**: Context-aware SQL with optimization tips
- **Table References**: Full BigQuery paths like `basedosdados.br_ibge_populacao.municipio`

## Base dos Dados API

The server connects to Base dos Dados GraphQL API and provides enhanced access to:
- **Datasets**: With themes, tags, organization metadata, and structure previews
- **Tables**: With column schemas, descriptions, and BigQuery references
- **SQL Generation**: Context-aware query creation with optimization guidance
- **Metadata**: Coverage information and usage instructions

## Usage in Claude Desktop

### Effective Prompts
- "Search for IBGE datasets in Base dos Dados"
- "What Brazilian education data is available?"
- "Get complete overview of dataset [ID] with all tables"
- "Show me table details for RAIS employment data with SQL examples"
- "Explore Brazilian census data structure"

### Expected Workflow
1. **Discover**: Use `search_datasets` with keywords like "população", "RAIS", "educação"
2. **Explore**: Use `get_dataset_overview` to see complete dataset structure
3. **Analyze**: Use `get_table_details` for specific table information with sample SQL
4. **Query**: Use the provided BigQuery references and sample SQL from table details

## Testing and Verification

### Connection Tests
- **MCP Protocol**: `uv run test_mcp_fixed.py` - Tests all tools end-to-end
- **BigQuery Access**: `uv run test_bigquery_permissions.py` - Verifies data access
- **Tool Functionality**: `uv run test_claude_tools.py` - Tests search and tool calls

### Debug Information
- **Search Debug**: Shows which search strategies were used and their results
- **Query Processing**: Shows how user queries are normalized and enhanced
- **Fallback Keywords**: Shows alternative search terms used

### Error Handling
- **GraphQL Errors**: Detailed error messages with suggestions
- **Network Issues**: Timeout and connection error handling
- **Data Not Found**: Clear messages when datasets/tables don't exist
- **Permission Issues**: BigQuery access troubleshooting guidance

## Recent Changes

### 2025-06-27: Smithery Publication Ready
- **✅ Smithery Configuration**: Created `smithery.yaml` with complete server metadata and tool documentation
- **✅ Docker Support**: Added optimized `Dockerfile` with proper Python environment and dependencies
- **✅ Package Metadata**: Enhanced `pyproject.toml` with license, authors, classifiers, and project URLs
- **✅ Build Optimization**: Added `.dockerignore` and resolved build dependencies
- **✅ Publication Ready**: Successfully tested Docker build and container execution
- **📦 Available on Smithery**: Ready for installation via Smithery registry

### 2025-06-26: Removed `generate_queries` Tool
- Removed the `generate_queries` tool to simplify the interface
- SQL generation functionality is now integrated into `get_table_details`
- Users get sample SQL queries directly in table details responses
- Maintains all functionality while reducing complexity

## Smithery Integration

### Installation via Smithery
```bash
# Install from Smithery registry
smithery install basedosdados-mcp
```

### Claude Desktop Configuration (Smithery)
```json
{
  "mcpServers": {
    "basedosdados": {
      "command": "basedosdados-mcp"
    }
  }
}
```

### Docker Deployment
- **Container Ready**: Optimized Dockerfile for containerized deployment
- **Smithery Compatible**: Meets all Smithery registry requirements
- **Lightweight**: Minimal Python 3.11-slim base with required dependencies only

## Publication Status

✅ **Ready for Publication**: All Smithery requirements met
- Configuration schema defined
- Docker containerization working
- Package metadata complete
- Tool documentation comprehensive
- Build process validated

## Notes

This server provides comprehensive, AI-optimized access to Base dos Dados through the MCP protocol. It's designed for efficient LLM interaction with Brazilian public data, offering single-call comprehensive information retrieval and context-aware guidance for data exploration and analysis.

The implementation is based on analysis of the Base dos Dados backend Django application and is optimized for both Claude Desktop integration and Smithery registry distribution with robust error handling and debugging capabilities.

**Smithery Features:**
- Zero-configuration installation
- Automatic dependency management
- Containerized deployment support
- Comprehensive tool documentation
- Portuguese language intelligence