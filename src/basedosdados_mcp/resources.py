from typing import List
from mcp.types import Resource, TextResourceContents, ReadResourceResult
from .server import server

# =============================================================================
# MCP Resource Handlers
# =============================================================================

@server.list_resources()
async def handle_list_resources() -> List[Resource]:
    """
    List available MCP resources.
    
    Resources provide static information and documentation about the server.
    """
    return [
        Resource(
            uri="basedosdados://datasets",
            name="Available Datasets",
            description="Information about accessing Base dos Dados datasets",
            mimeType="application/json",
        ),
        Resource(
            uri="basedosdados://help",
            name="Base dos Dados Help",
            description="Comprehensive help and usage information",
            mimeType="text/plain",
        ),
    ]

@server.read_resource()
async def handle_read_resource(uri: str) -> ReadResourceResult:
    """
    Read the content of a specific resource.
    
    Args:
        uri: Resource URI to read
        
    Returns:
        String content of the requested resource
    """
    if uri == "basedosdados://help":
        return ReadResourceResult(contents=[
            TextResourceContents(type="text", text="""Base dos Dados MCP Server Help

This server provides comprehensive metadata access to Base dos Dados, Brazil's open data platform.

🔧 **Enhanced Tools for AI:**
- **search_datasets**: Search with rich info including table/column counts and BigQuery references
- **get_dataset_overview**: Complete dataset view with all tables, columns, and ready-to-use SQL
- **get_table_details**: Comprehensive table info with column types, descriptions, and sample queries
- **explore_data**: Multi-level exploration for quick discovery or detailed analysis

📊 **What is Base dos Dados?**
Base dos Dados is Brazil's public data platform that standardizes and provides
access to Brazilian public datasets through Google BigQuery with references like:
`basedosdados.br_ibge_censo_demografico.municipio`

🚀 **AI-Optimized Workflow:**
1. **Discover**: Use `search_datasets` to find relevant data with structure preview
2. **Explore**: Use `get_dataset_overview` to see complete dataset structure in one call
3. **Analyze**: Use `get_table_details` for full column information and sample queries
4. **Query**: Use the provided BigQuery references and sample SQL from table details

📝 **Key Features:**
- **Single-call efficiency**: Get comprehensive info without multiple API calls
- **Ready-to-use BigQuery references**: Direct table access paths included
- **AI-friendly responses**: Structured for LLM consumption and decision making
- **Smart search ranking**: Acronyms like "RAIS", "IBGE" prioritized correctly

🌐 **More Information:**
- Website: https://basedosdados.org
- Documentation: https://docs.basedosdados.org  
- Python Package: pip install basedosdados
""")
        ])
    elif uri == "basedosdados://datasets":
        return ReadResourceResult(contents=[
            TextResourceContents(type="text", text='{"message": "Use the search_datasets tool to discover available datasets", "endpoint": "https://backend.basedosdados.org/graphql"}')
        ])
    else:
        # Return an error response for unknown resources
        return ReadResourceResult(contents=[
            TextResourceContents(type="text", text=f"Error: Unknown resource: {uri}")
        ])