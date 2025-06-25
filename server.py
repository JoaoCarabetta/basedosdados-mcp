#!/usr/bin/env python3
"""
Base dos Dados MCP Server

A Model Context Protocol (MCP) server that provides access to Base dos Dados, Brazil's open data platform.

This server connects to the Base dos Dados GraphQL API to provide metadata about Brazilian public datasets,
including information about datasets, tables, columns, and their relationships. It enables users to:

- Search for datasets by name, theme, or organization
- Get detailed information about specific datasets and tables  
- Generate BigQuery SQL queries for data access
- Browse the complete metadata catalog

GraphQL API Endpoint: https://backend.basedosdados.org/graphql
Base dos Dados Website: https://basedosdados.org

Usage Example:
    # Search for population datasets
    search_datasets(query="população", theme="Demografia")
    
    # Get dataset details
    get_dataset_info(dataset_id="br_ibge_populacao")
    
    # Generate SQL for a table
    generate_sql_query(table_id="municipio", limit=100)

Note: This server provides metadata access only. To query actual data, use the generated
BigQuery SQL statements with appropriate credentials.
"""

# Standard library imports
import asyncio
from typing import Any, Dict, List, Optional

# Third-party imports
import httpx

# MCP server imports
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import ServerCapabilities, Resource, Tool, TextContent

# Pydantic for data models
from pydantic import BaseModel

# =============================================================================
# API Configuration
# =============================================================================

# Base dos Dados GraphQL API endpoint
# This is the backend API that provides metadata about all datasets, tables, and columns
BASE_URL = "https://backend.basedosdados.org"
GRAPHQL_ENDPOINT = f"{BASE_URL}/graphql"

# =============================================================================
# Data Models
# =============================================================================

class DatasetInfo(BaseModel):
    """
    Dataset information model for Base dos Dados.
    
    Represents a collection of related tables containing Brazilian public data,
    typically organized by source organization and theme.
    """
    id: str  # UUID identifier
    name: str  # Human-readable name
    description: Optional[str] = None  # Detailed description
    organization: Optional[str] = None  # Source organization
    themes: List[str] = []  # Thematic categories
    tags: List[str] = []  # Keywords for discovery
    
class TableInfo(BaseModel):
    """
    Table information model for Base dos Dados.
    
    Represents a specific data table within a dataset, containing columns
    and accessible via BigQuery.
    """
    id: str  # UUID identifier
    name: str  # Human-readable name
    description: Optional[str] = None  # Table description
    dataset_id: str  # Parent dataset UUID
    columns: List[str] = []  # Column names

class ColumnInfo(BaseModel):
    """
    Column information model for Base dos Dados.
    
    Represents a data field within a table, with type information
    for BigQuery compatibility.
    """
    id: str  # UUID identifier
    name: str  # Column name
    description: Optional[str] = None  # Column description
    bigquery_type: Optional[str] = None  # BigQuery data type
    table_id: str  # Parent table UUID

# =============================================================================
# MCP Server Initialization
# =============================================================================

# Initialize the MCP server with a descriptive name
server = Server("basedosdados-mcp")


# =============================================================================
# Utility Functions
# =============================================================================

def clean_graphql_id(graphql_id: str) -> str:
    """
    Clean GraphQL node IDs to extract pure UUIDs.
    
    The API returns IDs like 'DatasetNode:uuid', 'TableNode:uuid', 'ColumnNode:uuid'
    but expects pure UUIDs for queries.
    
    Args:
        graphql_id: GraphQL node ID (e.g., 'DatasetNode:d30222ad-7a5c-4778-a1ec-f0785371d1ca')
        
    Returns:
        Pure UUID string (e.g., 'd30222ad-7a5c-4778-a1ec-f0785371d1ca')
    """
    if ':' in graphql_id:
        return graphql_id.split(':', 1)[1]
    return graphql_id

# =============================================================================
# GraphQL API Client
# =============================================================================

async def make_graphql_request(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Make a GraphQL request to the Base dos Dados API.
    
    This function handles communication with the Base dos Dados GraphQL endpoint,
    including error handling for common issues like network timeouts and GraphQL errors.
    
    Args:
        query: GraphQL query string
        variables: Optional variables for the GraphQL query
        
    Returns:
        Dict containing the GraphQL response data
        
    Raises:
        Exception: For various error conditions including:
            - GraphQL validation errors (400 status)
            - Network timeouts (30 second limit)
            - Connection errors
            - Unexpected API responses
            
    Note:
        The API uses Django GraphQL auto-generation, so filter arguments use
        single underscores (e.g., name_Icontains) not double underscores.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                GRAPHQL_ENDPOINT,
                json={"query": query, "variables": variables or {}},
                headers={"Content-Type": "application/json"}
            )
            
            # Handle GraphQL validation errors (common with wrong filter syntax)
            if response.status_code == 400:
                error_data = response.json()
                if "errors" in error_data:
                    error_messages = [err.get("message", "Unknown error") for err in error_data["errors"]]
                    raise Exception(f"GraphQL errors: {'; '.join(error_messages)}")
                else:
                    raise Exception(f"Bad Request (400): {error_data}")
            
            # Raise for other HTTP errors
            response.raise_for_status()
            result = response.json()
            
            # Check for GraphQL errors in successful responses
            if "errors" in result:
                error_messages = [err.get("message", "Unknown error") for err in result["errors"]]
                raise Exception(f"GraphQL errors: {'; '.join(error_messages)}")
                
            return result
            
    except httpx.TimeoutException:
        raise Exception("Request timeout - the API is taking too long to respond")
    except httpx.RequestError as e:
        raise Exception(f"Network error: {str(e)}")
    except Exception as e:
        # Re-raise our custom exceptions without modification
        if "GraphQL errors" in str(e) or "Request timeout" in str(e) or "Network error" in str(e):
            raise
        else:
            raise Exception(f"Unexpected error: {str(e)}")

# =============================================================================
# MCP Tool Definitions
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
async def handle_read_resource(uri: str) -> str:
    """
    Read the content of a specific resource.
    
    Args:
        uri: Resource URI to read
        
    Returns:
        String content of the requested resource
    """
    if uri == "basedosdados://help":
        return """Base dos Dados MCP Server Help

This server provides metadata access to Base dos Dados, Brazil's open data platform.

🔧 Available Tools:
- search_datasets: Search for datasets by name, theme, or organization
- get_dataset_info: Get detailed information about a specific dataset
- list_tables: List all tables in a dataset
- get_table_info: Get detailed information about a specific table
- list_columns: List all columns in a table
- get_column_info: Get detailed information about a specific column
- generate_sql_query: Generate BigQuery SQL for a table

📊 What is Base dos Dados?
Base dos Dados is Brazil's public data platform that standardizes and provides
access to Brazilian public datasets through Google BigQuery.

🚀 Getting Started:
1. Use search_datasets to find datasets of interest
2. Use get_dataset_info to explore dataset structure
3. Use list_tables and get_table_info to explore table structure
4. Use generate_sql_query to create BigQuery SQL for data access

📝 Important Notes:
- This server provides metadata only (no actual data)
- Use generated SQL queries in BigQuery for data access
- Filter syntax uses single underscores (name_Icontains)

🌐 More Information:
- Website: https://basedosdados.org
- Documentation: https://docs.basedosdados.org
- Python Package: pip install basedosdados
"""
    elif uri == "basedosdados://datasets":
        return '{"message": "Use the search_datasets tool to discover available datasets", "endpoint": "https://backend.basedosdados.org/graphql"}'
    else:
        raise ValueError(f"Unknown resource: {uri}")

# =============================================================================
# MCP Resource Handlers
# =============================================================================

@server.list_tools()
async def handle_list_tools() -> List[Tool]:
    """
    List all available MCP tools.
    
    Returns a list of tools that can be called by MCP clients.
    Each tool includes its name, description, and input schema.
    """
    return [
        Tool(
            name="search_datasets",
            description="Search for datasets by name, theme, or organization",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query for dataset name or description",
                    },
                    "theme": {
                        "type": "string",
                        "description": "Filter by theme (optional)",
                    },
                    "organization": {
                        "type": "string", 
                        "description": "Filter by organization (optional)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results (default: 20)",
                        "default": 20,
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_dataset_info",
            description="Get detailed information about a specific dataset",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_id": {
                        "type": "string",
                        "description": "The UUID of the dataset",
                    }
                },
                "required": ["dataset_id"],
            },
        ),
        Tool(
            name="list_tables",
            description="List all tables in a dataset",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_id": {
                        "type": "string",
                        "description": "The UUID of the dataset",
                    }
                },
                "required": ["dataset_id"],
            },
        ),
        Tool(
            name="get_table_info",
            description="Get detailed information about a specific table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "The UUID of the table",
                    }
                },
                "required": ["table_id"],
            },
        ),
        Tool(
            name="list_columns",
            description="List all columns in a table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "The UUID of the table",
                    }
                },
                "required": ["table_id"],
            },
        ),
        Tool(
            name="get_column_info",
            description="Get detailed information about a specific column",
            inputSchema={
                "type": "object",
                "properties": {
                    "column_id": {
                        "type": "string",
                        "description": "The UUID of the column",
                    }
                },
                "required": ["column_id"],
            },
        ),
        Tool(
            name="generate_sql_query",
            description="Generate a SQL query for querying a table in BigQuery",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "The UUID of the table",
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of column names to include (optional, includes all if not specified)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "LIMIT clause for the query (optional)",
                    },
                },
                "required": ["table_id"],
            },
        ),
    ]

# =============================================================================
# MCP Tool Handlers
# =============================================================================

@server.call_tool()
async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """
    Handle incoming tool calls from MCP clients.
    
    Args:
        name: Name of the tool to execute
        arguments: Arguments passed to the tool
        
    Returns:
        List of TextContent responses
    """
    
    if name == "search_datasets":
        query = arguments.get("query", "")
        theme = arguments.get("theme")
        organization = arguments.get("organization")
        limit = arguments.get("limit", 20)
        
        # GraphQL query to search datasets  
        if query:
            graphql_query = """
            query SearchDatasets($query: String, $first: Int) {
                allDataset(
                    name_Icontains: $query,
                    first: $first
                ) {
                edges {
                    node {
                        id
                        name
                        description
                        slug
                        organizations {
                            edges {
                                node {
                                    name
                                }
                            }
                        }
                        themes {
                            edges {
                                node {
                                    name
                                }
                            }
                        }
                        tags {
                            edges {
                                node {
                                    name
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        
        try:
            variables = {"first": limit}
            if query:
                variables["query"] = query
            # Note: theme and organization filtering may need different approach
            # For now, we'll filter by name only and do post-processing for theme/org
                
            result = await make_graphql_request(graphql_query, variables)
            
            datasets = []
            if result.get("data", {}).get("allDataset", {}).get("edges"):
                for edge in result["data"]["allDataset"]["edges"]:
                    node = edge["node"]
                    # Get organization names
                    org_names = [org["node"]["name"] for org in node.get("organizations", {}).get("edges", [])]
                    theme_names = [t["node"]["name"] for t in node.get("themes", {}).get("edges", [])]
                    tag_names = [t["node"]["name"] for t in node.get("tags", {}).get("edges", [])]
                    
                    # Client-side filtering for theme and organization
                    include_dataset = True
                    
                    if theme and theme.lower() not in [t.lower() for t in theme_names]:
                        include_dataset = False
                    
                    if organization and organization.lower() not in [org.lower() for org in org_names]:
                        include_dataset = False
                    
                    if include_dataset:
                        datasets.append({
                            "id": node["id"],
                            "name": node["name"],
                            "slug": node.get("slug", ""),
                            "description": node.get("description", ""),
                            "organizations": ", ".join(org_names),
                            "themes": theme_names,
                            "tags": tag_names,
                        })
            
            return [TextContent(
                type="text",
                text=f"Found {len(datasets)} datasets:\n\n" + 
                     "\n\n".join([
                         f"**{ds['name']}** (ID: {ds['id']}, Slug: {ds['slug']})\n"
                         f"Description: {ds['description']}\n"
                         f"Organizations: {ds['organizations']}\n"
                         f"Themes: {', '.join(ds['themes'])}\n"
                         f"Tags: {', '.join(ds['tags'])}"
                         for ds in datasets
                     ])
            )]
            
        except Exception as e:
            return [TextContent(type="text", text=f"Error searching datasets: {str(e)}")]
    
    elif name == "get_dataset_info":
        dataset_id = clean_graphql_id(arguments.get("dataset_id"))
        
        graphql_query = """
        query GetDataset($id: ID!) {
            allDataset(id: $id, first: 1) {
                edges {
                    node {
                        id
                        name
                        slug
                        description
                        organizations {
                            edges {
                                node {
                                    name
                                }
                            }
                        }
                        themes {
                            edges {
                                node {
                                    name
                                }
                            }
                        }
                        tags {
                            edges {
                                node {
                                    name
                                }
                            }
                        }
                        tables {
                            edges {
                                node {
                                    id
                                    name
                                    slug
                                    description
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        
        try:
            result = await make_graphql_request(graphql_query, {"id": dataset_id})
            
            if result.get("data", {}).get("allDataset", {}).get("edges"):
                edges = result["data"]["allDataset"]["edges"]
                if edges:
                    dataset = edges[0]["node"]
                    org_names = [org["node"]["name"] for org in dataset.get("organizations", {}).get("edges", [])]
                
                info = f"""**Dataset Information**
Name: {dataset['name']}
ID: {dataset['id']}
Slug: {dataset.get('slug', '')}
Description: {dataset.get('description', 'No description available')}
Organizations: {', '.join(org_names)}
Themes: {', '.join([t['node']['name'] for t in dataset.get('themes', {}).get('edges', [])])}
Tags: {', '.join([t['node']['name'] for t in dataset.get('tags', {}).get('edges', [])])}

**Tables in this dataset:**
"""
                    for edge in dataset.get("tables", {}).get("edges", []):
                        table = edge["node"]
                        info += f"- {table['name']} (ID: {table['id']}, Slug: {table.get('slug', '')}): {table.get('description', 'No description')}\n"
                    
                    return [TextContent(type="text", text=info)]
                else:
                    return [TextContent(type="text", text="Dataset not found")]
            else:
                return [TextContent(type="text", text="Dataset not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error getting dataset info: {str(e)}")]
    
    elif name == "list_tables":
        dataset_id = clean_graphql_id(arguments.get("dataset_id"))
        
        graphql_query = """
        query GetDatasetTables($id: ID!) {
            allDataset(id: $id, first: 1) {
                edges {
                    node {
                        id
                        name
                        tables {
                            edges {
                                node {
                                    id
                                    name
                                    slug
                                    description
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        
        try:
            result = await make_graphql_request(graphql_query, {"id": dataset_id})
            
            if result.get("data", {}).get("allDataset", {}).get("edges"):
                edges = result["data"]["allDataset"]["edges"]
                if edges:
                    dataset = edges[0]["node"]
                    tables = []
                    
                    for edge in dataset.get("tables", {}).get("edges", []):
                        table = edge["node"]
                        tables.append({
                            "id": table["id"],
                            "name": table["name"],
                            "slug": table.get("slug", ""),
                            "description": table.get("description", "No description available")
                        })
                    
                    return [TextContent(
                        type="text",
                        text=f"**Tables in dataset '{dataset['name']}':**\n\n" +
                             "\n".join([
                                 f"• **{table['name']}** (ID: {table['id']}, Slug: {table['slug']})\n"
                                 f"  {table['description']}"
                                 for table in tables
                             ])
                    )]
                else:
                    return [TextContent(type="text", text="Dataset not found")]
            else:
                return [TextContent(type="text", text="Dataset not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error listing tables: {str(e)}")]
    
    elif name == "get_table_info":
        table_id = clean_graphql_id(arguments.get("table_id"))
        
        graphql_query = """
        query GetTable($id: ID!) {
            allTable(id: $id, first: 1) {
                edges {
                    node {
                        id
                        name
                        slug
                        description
                        dataset {
                            id
                            name
                            slug
                        }
                        columns {
                            edges {
                                node {
                                    id
                                    name
                                    description
                                    bigqueryType {
                                        name
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        
        try:
            result = await make_graphql_request(graphql_query, {"id": table_id})
            
            if result.get("data", {}).get("allTable", {}).get("edges"):
                edges = result["data"]["allTable"]["edges"]
                if edges:
                    table = edges[0]["node"]
                    dataset = table["dataset"]
                    
                    info = f"""**Table Information**
Name: {table['name']}
ID: {table['id']}
Slug: {table.get('slug', '')}
Description: {table.get('description', 'No description available')}

**Dataset:**
{dataset['name']} (ID: {dataset['id']}, Slug: {dataset.get('slug', '')})

**Columns:**
"""
                    
                    for edge in table.get("columns", {}).get("edges", []):
                        column = edge["node"]
                        bigquery_type = column.get("bigqueryType", {}).get("name", "Unknown")
                        info += f"• {column['name']} ({bigquery_type})\n"
                        if column.get("description"):
                            info += f"  {column['description']}\n"
                    
                    return [TextContent(type="text", text=info)]
                else:
                    return [TextContent(type="text", text="Table not found")]
            else:
                return [TextContent(type="text", text="Table not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error getting table info: {str(e)}")]
    
    elif name == "list_columns":
        table_id = clean_graphql_id(arguments.get("table_id"))
        
        graphql_query = """
        query GetTableColumns($id: ID!) {
            allTable(id: $id, first: 1) {
                edges {
                    node {
                        id
                        name
                        columns {
                            edges {
                                node {
                                    id
                                    name
                                    description
                                    bigqueryType {
                                        name
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        
        try:
            result = await make_graphql_request(graphql_query, {"id": table_id})
            
            if result.get("data", {}).get("allTable", {}).get("edges"):
                edges = result["data"]["allTable"]["edges"]
                if edges:
                    table = edges[0]["node"]
                    columns = []
                    
                    for edge in table.get("columns", {}).get("edges", []):
                        column = edge["node"]
                        bigquery_type = column.get("bigqueryType", {}).get("name", "Unknown")
                        columns.append({
                            "id": column["id"],
                            "name": column["name"],
                            "description": column.get("description", "No description available"),
                            "type": bigquery_type
                        })
                    
                    return [TextContent(
                        type="text",
                        text=f"**Columns in table '{table['name']}':**\n\n" +
                             "\n".join([
                                 f"• **{col['name']}** ({col['type']}) - ID: {col['id']}\n"
                                 f"  {col['description']}"
                                 for col in columns
                             ])
                    )]
                else:
                    return [TextContent(type="text", text="Table not found")]
            else:
                return [TextContent(type="text", text="Table not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error listing columns: {str(e)}")]
    
    elif name == "get_column_info":
        column_id = clean_graphql_id(arguments.get("column_id"))
        
        graphql_query = """
        query GetColumn($id: ID!) {
            allColumn(id: $id, first: 1) {
                edges {
                    node {
                        id
                        name
                        description
                        bigqueryType {
                            name
                        }
                        table {
                            id
                            name
                            slug
                            dataset {
                                id
                                name
                                slug
                            }
                        }
                    }
                }
            }
        }
        """
        
        try:
            result = await make_graphql_request(graphql_query, {"id": column_id})
            
            if result.get("data", {}).get("allColumn", {}).get("edges"):
                edges = result["data"]["allColumn"]["edges"]
                if edges:
                    column = edges[0]["node"]
                    table = column["table"]
                    dataset = table["dataset"]
                    bigquery_type = column.get("bigqueryType", {}).get("name", "Unknown")
                    
                    info = f"""**Column Information**
Name: {column['name']}
ID: {column['id']}
Type: {bigquery_type}
Description: {column.get('description', 'No description available')}

**Table:**
{table['name']} (ID: {table['id']}, Slug: {table.get('slug', '')})

**Dataset:**
{dataset['name']} (ID: {dataset['id']}, Slug: {dataset.get('slug', '')})
"""
                    
                    return [TextContent(type="text", text=info)]
                else:
                    return [TextContent(type="text", text="Column not found")]
            else:
                return [TextContent(type="text", text="Column not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error getting column info: {str(e)}")]
    
    elif name == "generate_sql_query":
        table_id = clean_graphql_id(arguments.get("table_id"))
        columns = arguments.get("columns", [])
        limit = arguments.get("limit")
        
        # This would typically use the backend's OneBigTableQueryGenerator
        # For now, we'll create a basic SQL query structure
        try:
            # First get table information
            graphql_query = """
            query GetTable($id: ID!) {
                allTable(id: $id, first: 1) {
                    edges {
                        node {
                            id
                            name
                            slug
                            dataset {
                                slug
                            }
                            columns {
                                edges {
                                    node {
                                        name
                                        bigqueryType {
                                            name
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """
            
            result = await make_graphql_request(graphql_query, {"id": table_id})
            
            if result.get("data", {}).get("allTable", {}).get("edges"):
                edges = result["data"]["allTable"]["edges"]
                if edges:
                    table = edges[0]["node"]
                    dataset_slug = table["dataset"]["slug"]
                    table_slug = table["slug"]
                    
                    # Get column names if not specified
                    if not columns:
                        columns = [col["node"]["name"] for col in table.get("columns", {}).get("edges", [])]
                    
                    # Generate SQL query
                    columns_str = ", ".join(columns) if columns else "*"
                    sql_query = f"SELECT {columns_str}\nFROM `basedosdados.{dataset_slug}.{table_slug}`"
                    
                    if limit:
                        sql_query += f"\nLIMIT {limit}"
                    
                    return [TextContent(
                        type="text", 
                        text=f"**Generated SQL Query for {table['name']}:**\n\n```sql\n{sql_query}\n```\n\n"
                             f"**Usage:** You can run this query in BigQuery or use the Base dos Dados Python package."
                    )]
                else:
                    return [TextContent(type="text", text="Table not found")]
            else:
                return [TextContent(type="text", text="Table not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error generating SQL query: {str(e)}")]
    
    else:
        return [TextContent(type="text", text=f"Unknown tool: {name}")]

# =============================================================================
# Server Initialization and Main Entry Point
# =============================================================================

async def main():
    """
    Main entry point for the MCP server.
    
    Initializes the server with stdio communication and runs the event loop.
    """
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="basedosdados-mcp",
                server_version="0.1.0",
                capabilities=ServerCapabilities(
                    resources={},
                    tools={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())