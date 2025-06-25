#!/usr/bin/env python3
"""
Base dos Dados MCP Server

A Model Context Protocol server that provides access to Base dos Dados (Brazilian open data platform) functionality.
This server offers tools for querying datasets, tables, columns, and metadata from the Base dos Dados API.
"""

import asyncio
import httpx
import json
from typing import Any, Dict, List, Optional
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.types import ServerCapabilities
from mcp.server.stdio import stdio_server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
    ImageContent,
    EmbeddedResource,
    LoggingLevel
)
from pydantic import BaseModel, Field

# Base dos Dados API configuration
BASE_URL = "https://backend.basedosdados.org"
GRAPHQL_ENDPOINT = f"{BASE_URL}/graphql"

class DatasetInfo(BaseModel):
    """Dataset information model"""
    id: str
    name: str
    description: Optional[str] = None
    organization: Optional[str] = None
    themes: List[str] = []
    tags: List[str] = []
    
class TableInfo(BaseModel):
    """Table information model"""
    id: str
    name: str
    description: Optional[str] = None
    dataset_id: str
    columns: List[str] = []

class ColumnInfo(BaseModel):
    """Column information model"""
    id: str
    name: str
    description: Optional[str] = None
    bigquery_type: Optional[str] = None
    table_id: str

# Initialize the MCP server
server = Server("basedosdados-mcp")

# Sample metadata - in a real implementation, this would come from a metadata API or file
SAMPLE_DATASETS = [
    {
        "id": "br_ibge_populacao",
        "name": "População - IBGE",
        "slug": "br_ibge_populacao", 
        "description": "Dados populacionais do IBGE por município e estado",
        "organizations": ["IBGE"],
        "themes": ["Demografia", "População"],
        "tags": ["censo", "população", "ibge"],
        "tables": [
            {
                "id": "municipio",
                "name": "Município",
                "slug": "municipio",
                "description": "População por município",
                "columns": ["ano", "id_municipio", "populacao"]
            }
        ]
    },
    {
        "id": "br_cgu_beneficios_emergenciais",
        "name": "Benefícios Emergenciais - CGU",
        "slug": "br_cgu_beneficios_emergenciais",
        "description": "Dados sobre benefícios emergenciais durante a pandemia",
        "organizations": ["CGU"],
        "themes": ["Assistência Social", "COVID-19"],
        "tags": ["auxilio emergencial", "covid", "beneficios"],
        "tables": [
            {
                "id": "auxilio_emergencial",
                "name": "Auxílio Emergencial", 
                "slug": "auxilio_emergencial",
                "description": "Beneficiários do auxílio emergencial",
                "columns": ["mes_competencia", "uf", "municipio", "valor"]
            }
        ]
    },
    {
        "id": "br_me_cnpj",
        "name": "CNPJ - Ministério da Economia",
        "slug": "br_me_cnpj",
        "description": "Dados de empresas cadastradas no CNPJ",
        "organizations": ["Ministério da Economia"],
        "themes": ["Empresas", "Economia"],
        "tags": ["cnpj", "empresas", "receita federal"],
        "tables": [
            {
                "id": "empresas",
                "name": "Empresas",
                "slug": "empresas", 
                "description": "Dados das empresas cadastradas",
                "columns": ["cnpj", "razao_social", "uf", "municipio", "atividade_principal"]
            }
        ]
    }
]

def search_datasets_metadata(query: str = "", theme: Optional[str] = None, organization: Optional[str] = None, limit: int = 20):
    """Search datasets in metadata"""
    results = []
    query_lower = query.lower() if query else ""
    theme_lower = theme.lower() if theme else ""
    org_lower = organization.lower() if organization else ""
    
    for dataset in SAMPLE_DATASETS:
        # Check if dataset matches search criteria
        matches = True
        
        if query_lower:
            name_match = query_lower in dataset["name"].lower()
            desc_match = query_lower in dataset.get("description", "").lower()
            tag_match = any(query_lower in tag.lower() for tag in dataset.get("tags", []))
            matches = matches and (name_match or desc_match or tag_match)
        
        if theme_lower:
            theme_match = any(theme_lower in t.lower() for t in dataset.get("themes", []))
            matches = matches and theme_match
            
        if org_lower:
            org_match = any(org_lower in org.lower() for org in dataset.get("organizations", []))
            matches = matches and org_match
        
        if matches:
            results.append(dataset)
            
        if len(results) >= limit:
            break
    
    return results

def get_dataset_by_id(dataset_id: str):
    """Get dataset by ID"""
    for dataset in SAMPLE_DATASETS:
        if dataset["id"] == dataset_id or dataset["slug"] == dataset_id:
            return dataset
    return None

async def make_graphql_request(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Make a GraphQL request to the Base dos Dados API"""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                GRAPHQL_ENDPOINT,
                json={"query": query, "variables": variables or {}},
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 400:
                # Parse the error response to provide better feedback
                error_data = response.json()
                if "errors" in error_data:
                    error_messages = [err.get("message", "Unknown error") for err in error_data["errors"]]
                    raise Exception(f"GraphQL errors: {'; '.join(error_messages)}")
                else:
                    raise Exception(f"Bad Request (400): {error_data}")
            
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
        # Re-raise our custom exceptions
        if "GraphQL errors" in str(e) or "Request timeout" in str(e) or "Network error" in str(e):
            raise
        else:
            raise Exception(f"Unexpected error: {str(e)}")

@server.list_resources()
async def handle_list_resources() -> List[Resource]:
    """List available resources"""
    return [
        Resource(
            uri="basedosdados://datasets",
            name="Available Datasets",
            description="List of available datasets in Base dos Dados",
            mimeType="application/json",
        ),
        Resource(
            uri="basedosdados://help",
            name="Base dos Dados Help",
            description="Information about using Base dos Dados MCP server",
            mimeType="text/plain",
        ),
    ]

@server.read_resource()
async def handle_read_resource(uri: str) -> str:
    """Read a specific resource"""
    if uri == "basedosdados://help":
        return """Base dos Dados MCP Server Help

This server provides tools to interact with the Base dos Dados (Brazilian open data platform) API.

Available tools:
- search_datasets: Search for datasets by name or theme
- get_dataset_info: Get detailed information about a specific dataset
- list_tables: List tables in a dataset
- get_table_info: Get detailed information about a specific table
- list_columns: List columns in a table
- get_column_info: Get detailed information about a specific column
- generate_sql_query: Generate SQL query for a table

Resources:
- basedosdados://datasets: List available datasets
- basedosdados://help: This help information

For more information about Base dos Dados, visit: https://basedosdados.org
"""
    elif uri == "basedosdados://datasets":
        # This would typically fetch from the API, but for now return a placeholder
        return '{"message": "Use the search_datasets tool to find available datasets"}'
    else:
        raise ValueError(f"Unknown resource: {uri}")

@server.list_tools()
async def handle_list_tools() -> List[Tool]:
    """List available tools"""
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
        Tool(
            name="test_api_connection",
            description="Test the connection to Base dos Dados API with comprehensive endpoint testing",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="get_api_info",
            description="Get information about Base dos Dados API and alternative access methods",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool calls"""
    
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
            # Fallback to metadata search if API fails
            metadata_results = search_datasets_metadata(query, theme, organization, limit)
            
            if metadata_results:
                return [TextContent(
                    type="text",
                    text=f"API unavailable, showing sample metadata ({len(metadata_results)} datasets):\n\n" + 
                         "\n\n".join([
                             f"**{ds['name']}** (ID: {ds['id']}, Slug: {ds['slug']})\n"
                             f"Description: {ds['description']}\n"
                             f"Organizations: {', '.join(ds['organizations'])}\n"
                             f"Themes: {', '.join(ds['themes'])}\n"
                             f"Tags: {', '.join(ds['tags'])}"
                             for ds in metadata_results
                         ]) + f"\n\n*Note: API Error: {str(e)}*"
                )]
            else:
                return [TextContent(type="text", text=f"Error searching datasets: {str(e)}\nNo sample data matches your query.")]
    
    elif name == "get_dataset_info":
        dataset_id = arguments.get("dataset_id")
        
        graphql_query = """
        query GetDataset($id: ID!) {
            dataset(id: $id) {
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
        """
        
        try:
            result = await make_graphql_request(graphql_query, {"id": dataset_id})
            
            if result.get("data", {}).get("dataset"):
                dataset = result["data"]["dataset"]
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
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error getting dataset info: {str(e)}")]
    
    elif name == "generate_sql_query":
        table_id = arguments.get("table_id")
        columns = arguments.get("columns", [])
        limit = arguments.get("limit")
        
        # This would typically use the backend's OneBigTableQueryGenerator
        # For now, we'll create a basic SQL query structure
        try:
            # First get table information
            graphql_query = """
            query GetTable($id: ID!) {
                table(id: $id) {
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
            """
            
            result = await make_graphql_request(graphql_query, {"id": table_id})
            
            if result.get("data", {}).get("table"):
                table = result["data"]["table"]
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
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error generating SQL query: {str(e)}")]
    
    elif name == "test_api_connection":
        import time
        
        # Test known working base URL with comprehensive queries
        base_url = "https://backend.basedosdados.org"
        endpoints_to_test = [
            f"{base_url}/graphql",
            f"{base_url}/api/graphql", 
            f"{base_url}/api/v1/graphql",
        ]
        
        # Comprehensive test queries for full API validation
        test_queries = {
            "schema_introspection": """
                query SchemaIntrospection {
                    __schema {
                        queryType {
                            name
                            fields {
                                name
                                description
                                args {
                                    name
                                    type {
                                        name
                                        kind
                                        ofType {
                                            name
                                            kind
                                        }
                                    }
                                    defaultValue
                                    description
                                }
                            }
                        }
                        types {
                            name
                            kind
                            description
                        }
                    }
                }
            """,
            "dataset_filters_discovery": """
                query DatasetFilters {
                    __type(name: "Query") {
                        fields {
                            name
                            args {
                                name
                                type {
                                    name
                                    kind
                                    ofType {
                                        name
                                        kind
                                    }
                                }
                                defaultValue
                                description
                            }
                        }
                    }
                }
            """,
            "dataset_node_structure": """
                query DatasetNodeStructure {
                    __type(name: "DatasetNode") {
                        name
                        kind
                        description
                        fields {
                            name
                            type {
                                name
                                kind
                                ofType {
                                    name
                                    kind
                                }
                            }
                            description
                        }
                    }
                }
            """,
            "table_node_structure": """
                query TableNodeStructure {
                    __type(name: "TableNode") {
                        name
                        kind
                        description
                        fields {
                            name
                            type {
                                name
                                kind
                                ofType {
                                    name
                                    kind
                                }
                            }
                            description
                        }
                    }
                }
            """,
            "real_dataset_sample": """
                query RealDatasetSample {
                    allDataset(first: 3) {
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
                                            slug
                                        }
                                    }
                                }
                                themes {
                                    edges {
                                        node {
                                            name
                                            slug
                                        }
                                    }
                                }
                                tags {
                                    edges {
                                        node {
                                            name
                                            slug
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
            """,
            "search_functionality_test": """
                query SearchTest {
                    allDataset(name_Icontains: "ibge", first: 2) {
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
            """
        }
        
        results = []
        working_endpoint = None
        
        for endpoint in endpoints_to_test:
            endpoint_results = {"endpoint": endpoint, "tests": {}}
            
            for test_name, test_query in test_queries.items():
                start_time = time.time()
                try:
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.post(
                            endpoint,
                            json={"query": test_query},
                            headers={"Content-Type": "application/json"}
                        )
                        
                        elapsed = round(time.time() - start_time, 2)
                        
                        if response.status_code == 200:
                            result = response.json()
                            if "errors" in result:
                                # Show detailed GraphQL errors
                                errors = result.get('errors', [])
                                error_details = []
                                for error in errors[:2]:  # Show first 2 errors
                                    msg = error.get('message', 'Unknown error')
                                    path = error.get('path', [])
                                    error_details.append(f"{msg} (path: {'.'.join(map(str, path)) if path else 'root'})")
                                endpoint_results["tests"][test_name] = f"❌ GraphQL errors ({elapsed}s): {'; '.join(error_details)}"
                            elif "data" in result:
                                # Detailed success reporting with data insights
                                success_details = self._analyze_test_result(test_name, result, elapsed)
                                endpoint_results["tests"][test_name] = success_details
                                
                                if not working_endpoint:
                                    working_endpoint = endpoint
                            else:
                                endpoint_results["tests"][test_name] = f"❌ Unexpected response format ({elapsed}s)"
                        else:
                            # Get more details about HTTP errors
                            try:
                                error_body = response.text[:200] if response.text else "No response body"
                                endpoint_results["tests"][test_name] = f"❌ HTTP {response.status_code} ({error_body})"
                            except:
                                endpoint_results["tests"][test_name] = f"❌ HTTP {response.status_code}"
                            
                except Exception as e:
                    endpoint_results["tests"][test_name] = f"❌ {str(e)}"
            
            results.append(endpoint_results)
        
        # Format results
        output_lines = ["# Base dos Dados API Endpoint Testing Results\n"]
        
        if working_endpoint:
            output_lines.append(f"✅ **Working endpoint found:** {working_endpoint}\n")
        
        for result in results:
            output_lines.append(f"## {result['endpoint']}")
            for test_name, test_result in result['tests'].items():
                output_lines.append(f"- **{test_name}**: {test_result}")
            output_lines.append("")
        
        if not working_endpoint:
            output_lines.append("❌ No fully working endpoints found. API may require authentication.")
        else:
            output_lines.append("## ✅ API Status: READY")
            output_lines.append(f"**Endpoint:** `{working_endpoint}`")
            output_lines.append("**Tools available:** search_datasets, get_dataset_info, generate_sql_query")
            output_lines.append("**Filter syntax:** Use `name_Icontains` (underscore, not double underscore)")
        
        return [TextContent(
            type="text", 
            text="\n".join(output_lines)
        )]
    
    elif name == "get_api_info":
        info_text = """# Base dos Dados API Access Information

Based on the backend analysis, the Base dos Dados API appears to require authentication. Here are the recommended approaches:

## 1. Python Package (Recommended)
The official way to access Base dos Dados data is through their Python package:
```python
import basedosdados as bd

# List available datasets
datasets = bd.list_datasets()

# Download specific table
df = bd.read_table(
    dataset_id="br_ibge_populacao", 
    table_id="municipio"
)
```

## 2. BigQuery Direct Access
Base dos Dados data is available in Google BigQuery:
```sql
SELECT * FROM `basedosdados.br_ibge_populacao.municipio` LIMIT 100
```

## 3. Website API (Browser)
The main website (basedosdados.org) likely uses an authenticated API that requires:
- User authentication tokens
- Specific headers or parameters

## 4. Alternative Implementation
For this MCP server, we could:
1. Use the Python `basedosdados` package directly
2. Provide BigQuery SQL generation tools
3. Create dataset/table lookup tools based on static metadata

Would you like me to implement any of these approaches?
"""
        return [TextContent(type="text", text=info_text)]
    
    else:
        return [TextContent(type="text", text=f"Tool '{name}' is not yet implemented")]

async def main():
    """Main entry point for the server"""
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