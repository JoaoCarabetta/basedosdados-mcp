import asyncio
import json
import logging
import os
import sys

import httpx
from mcp.server.fastmcp import FastMCP

# =============================================================================
# Encoding Configuration
# =============================================================================

# Force UTF-8 encoding for stdout/stderr to prevent encoding issues in Claude Desktop
# Set environment variables for UTF-8 support early
os.environ['PYTHONIOENCODING'] = 'utf-8'
os.environ['LC_ALL'] = 'en_US.UTF-8'
os.environ['LANG'] = 'en_US.UTF-8'

# Try to reconfigure stdout/stderr encoding if available (Python 3.7+)
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')
except (AttributeError, OSError):
    # Fallback: Python doesn't support reconfigure or system doesn't support UTF-8
    pass

# =============================================================================
# UTF-8 JSON Helper
# =============================================================================

def safe_json_dumps(obj):
    """Safely serialize JSON with UTF-8 encoding preservation."""
    return json.dumps(obj, ensure_ascii=False, indent=2)

# Monkey-patch json.dumps to always use ensure_ascii=False for UTF-8 support
_original_json_dumps = json.dumps
def utf8_json_dumps(obj, **kwargs):
    """Override json.dumps to always preserve UTF-8 characters."""
    kwargs['ensure_ascii'] = False
    return _original_json_dumps(obj, **kwargs)

# Apply the monkey-patch
json.dumps = utf8_json_dumps

# =============================================================================
# UTF-8 Response Wrapper
# =============================================================================

def utf8_response_wrapper(response_text: str) -> str:
    """
    Wrapper to ensure all MCP tool responses preserve UTF-8 characters.
    
    This prevents the MCP library from escaping Portuguese characters as Unicode sequences.
    """
    # Ensure the response is properly encoded as UTF-8
    if isinstance(response_text, str):
        # Encode and decode to ensure proper UTF-8 handling
        response_bytes = response_text.encode('utf-8')
        response_text = response_bytes.decode('utf-8')

        # Double-check: replace any Unicode escape sequences that might have snuck in
        # This is a safety net in case the MCP library has already processed the text
        import re
        unicode_pattern = r'\\u([0-9a-fA-F]{4})'

        def replace_unicode_escape(match):
            unicode_code = int(match.group(1), 16)
            return chr(unicode_code)

        response_text = re.sub(unicode_pattern, replace_unicode_escape, response_text)

    return response_text


def utf8_tool(func):
    """
    Decorator to ensure all MCP tool responses preserve UTF-8 characters.
    
    This decorator automatically wraps the return value of tool functions
    to prevent the MCP library from escaping Portuguese characters.
    """
    import functools

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # Call the original function
        result = await func(*args, **kwargs)

        # Wrap the response to ensure UTF-8 preservation
        if isinstance(result, str):
            return utf8_response_wrapper(result)
        else:
            return result

    return wrapper
from basedosdados_mcp.bigquery_client import (
    BigQueryClient,
    execute_query,
    format_query_results,
    validate_query,
)
from basedosdados_mcp.graphql_client import (
    DATASET_OVERVIEW_QUERY,
    FAST_SEARCH_ENRICHMENT_QUERY,
    SEARCH_ENRICHMENT_QUERY,
    TABLE_DETAILS_QUERY,
    make_graphql_request,
)
from basedosdados_mcp.utils import (
    clean_graphql_id,
    format_bigquery_reference,
    format_sql_query_with_reference,
)

# =============================================================================
# Logging Setup
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# FastMCP Server Initialization
# =============================================================================

# Initialize the FastMCP server
mcp = FastMCP("Base dos Dados MCP")

# =============================================================================
# Backend Search API Integration
# =============================================================================

@mcp.tool()
@utf8_tool
async def search_datasets(
    query: str,
    limit: int = 15,
    fast_mode: bool = True
) -> str:
    """
    🔍 **Enhanced Search for Base dos Dados - LLM Optimized**
    
    This tool provides comprehensive access to Brazilian open data with rich context
    for LLM decision-making. It combines the reliable backend search with detailed
    GraphQL enrichment to deliver complete dataset structures in a single call.
    
    **Key Features:**
    - **Comprehensive Context**: Full dataset metadata, table structures, and column samples
    - **LLM-Optimized**: Rich formatting with clear hierarchies and decision-support information
    - **Ready-to-Use References**: Complete BigQuery paths and API identifiers
    - **Smart Enrichment**: Parallel data fetching for maximum efficiency
    - **Error Resilient**: Graceful degradation if enrichment fails
    
    **What You Get Per Dataset:**
    - Complete identifiers (GraphQL IDs, slugs, BigQuery references)
    - Rich metadata (descriptions, organizations, themes, tags)
    - Table structure previews (names, column counts, sample columns with types)
    - Ready-to-use BigQuery references for immediate data access
    - Clear workflow guidance for next steps
    
    **Search Examples:**
    - Organizations: "ibge", "anvisa", "ministerio da saude"
    - Themes: "saude", "educacao", "economia", "transporte"
    - Topics: "covid", "eleicoes", "clima", "energia"
    - Data types: "municipios", "estados", "empresas"
    
    **Response Structure:**
    - Dataset overview with complete metadata
    - Table previews with column samples and types
    - BigQuery references for immediate querying
    - Next-step guidance for deeper exploration
    - Pro tips for efficient workflow
    
    Args:
        query: Search term to find relevant datasets (e.g., "ibge", "saude", "educacao", "covid")
        limit: Maximum number of results to return (default: 10, max: 50)
        
    Returns:
        Comprehensive search results with complete dataset context, table structures, 
        and ready-to-use BigQuery references for LLM decision-making
    """

    logger.info(f"Starting search for query: '{query}' with limit: {limit}")

    # Use Backend API (same infrastructure as the site - most reliable)
    try:
        logger.info("Attempting backend API search...")
        response = await search_datasets_backend(query, limit, fast_mode)
        logger.info("Backend API search completed")
        return response
    except Exception as e:
        logger.error(f"Backend API search failed: {str(e)}")
        return f"Search failed for query '{query}'. Please try again later."

# =============================================================================
# Internal Search Functions (not exposed as tools)
# =============================================================================





@mcp.tool()
@utf8_tool
async def get_dataset_overview(dataset_id: str, fast_mode: bool = True) -> str:
    """
    📊 Get comprehensive dataset overview with all tables, columns, and BigQuery references.
    
    This tool provides detailed information about a specific dataset, including all its tables,
    column structures, and ready-to-use BigQuery table references for direct data access.
    
    **Features:**
    - Complete dataset metadata (description, organizations, themes, tags)
    - All tables with column counts and sample column names
    - BigQuery table references for each table
    - Ready-to-use SQL query examples
    - Data structure overview (total tables and columns)
    
    **Use Cases:**
    - Explore dataset structure before querying data
    - Get BigQuery table references for data analysis
    - Understand data coverage and organization
    - Plan SQL queries with proper table references
    
    **Returns:**
    - Dataset basic information and metadata
    - Complete list of tables with column counts
    - BigQuery table references for each table
    - Sample column names for each table
    - Ready-to-use SQL query examples
    
    Args:
        dataset_id: The GraphQL ID of the dataset (obtained from search results)
        
    Returns:
        Comprehensive dataset overview with all tables and BigQuery references
    """

    dataset_id = clean_graphql_id(dataset_id)

    try:
        # Add performance monitoring
        start_time = asyncio.get_event_loop().time()
        result = await make_graphql_request(DATASET_OVERVIEW_QUERY, {"id": dataset_id})
        duration = asyncio.get_event_loop().time() - start_time

        if result.get("data", {}).get("allDataset", {}).get("edges"):
            edges = result["data"]["allDataset"]["edges"]
            if edges:
                dataset = edges[0]["node"]
                org_names = [org["node"]["name"] for org in dataset.get("organizations", {}).get("edges", [])]
                theme_names = [t["node"]["name"] for t in dataset.get("themes", {}).get("edges", [])]
                tag_names = [t["node"]["name"] for t in dataset.get("tags", {}).get("edges", [])]

                # Process tables with their columns
                tables_info = []
                total_columns = 0
                bigquery_paths = []

                # Extract organization slug for BigQuery reference
                organization_slug = None
                if dataset.get("organizations", {}).get("edges"):
                    organization_slug = dataset["organizations"]["edges"][0]["node"].get("slug", "")

                for table_edge in dataset.get("tables", {}).get("edges", []):
                    table = table_edge["node"]
                    columns = table.get("columns", {}).get("edges", [])
                    column_count = len(columns)
                    total_columns += column_count

                    # Generate full BigQuery table reference with validation
                    dataset_slug = dataset.get("slug", "")
                    table_slug = table.get("slug", "")
                    bigquery_ref = None
                    if dataset_slug and table_slug:
                        bigquery_ref = format_bigquery_reference(dataset_slug, table_slug, organization_slug)
                        bigquery_paths.append(bigquery_ref)

                    tables_info.append({
                        "id": table["id"],
                        "name": table["name"],
                        "slug": table_slug,
                        "description": table.get("description", ""),
                        "column_count": column_count,
                        "bigquery_reference": bigquery_ref
                    })

                # Build compact, LLM-optimized response
                response = f"📊 {dataset['name']} [{dataset.get('slug', '')}] ({len(tables_info)} tables, {total_columns} cols, {duration:.1f}s)\n"
                response += f"ID: {dataset['id']}\n"
                
                # Add metadata if available
                metadata_parts = []
                if org_names:
                    metadata_parts.append(f"Org: {', '.join(org_names)}")
                if theme_names:
                    metadata_parts.append(f"Theme: {', '.join(theme_names)}")
                if tag_names:
                    metadata_parts.append(f"Tags: {', '.join(tag_names)}")
                
                if metadata_parts:
                    response += f"{' | '.join(metadata_parts)}\n"

                # Add dataset description
                if dataset.get('description'):
                    desc = dataset['description']
                    if len(desc) > 200:
                        desc = desc[:200] + "..."
                    response += f"Desc: {desc}\n"

                response += "\nTables & BigQuery Paths:\n"
                
                # List tables with BigQuery paths
                for i, table in enumerate(tables_info, 1):
                    table_desc = table['description'][:60] + "..." if len(table['description']) > 60 else table['description']
                    
                    if table['bigquery_reference']:
                        response += f"{i}. {table['name']} ({table['column_count']} cols) → {table['bigquery_reference']}\n"
                    else:
                        response += f"{i}. {table['name']} ({table['column_count']} cols) → Use get_table_details for BigQuery path\n"
                    
                    if table_desc:
                        response += f"   {table_desc}\n"

                # Add workflow guidance
                if tables_info:
                    first_table_id = tables_info[0]['id']
                    response += f"\nNext: get_table_details('{first_table_id}') for column details"

                # Log performance
                logger.info(f"Dataset overview completed in {duration:.2f}s for {len(tables_info)} tables")

                return response
            else:
                return "Dataset not found"
        else:
            return "Dataset not found"

    except Exception as e:
        return f"Error getting dataset overview: {str(e)}"


@mcp.tool()
@utf8_tool
async def get_table_details(table_id: str, fast_mode: bool = True) -> str:
    """
    📋 Get detailed table information with all columns, types, and BigQuery access instructions.
    
    This tool provides comprehensive information about a specific table, including all columns,
    their data types, descriptions, and multiple ways to access the data in BigQuery.
    
    **Features:**
    - Complete table metadata and description
    - All columns with data types and descriptions
    - BigQuery table reference for direct access
    - Sample SQL queries for different use cases
    - Multiple access methods (BigQuery Console, Python package, direct SQL)
    - Column information queries for schema exploration
    
    **Use Cases:**
    - Understand table structure and column types
    - Get BigQuery table reference for data analysis
    - Generate SQL queries with proper column selection
    - Explore data schema and relationships
    - Plan data analysis workflows
    
    **Returns:**
    - Table metadata and dataset context
    - Complete column list with types and descriptions
    - BigQuery table reference
    - Sample SQL queries (basic select, full schema, column info)
    - Access instructions for different platforms
    
    Args:
        table_id: The GraphQL ID of the table (obtained from dataset overview or search results)
        
    Returns:
        Comprehensive table details with all columns and BigQuery access instructions
    """

    table_id = clean_graphql_id(table_id)

    try:
        # Add performance monitoring
        start_time = asyncio.get_event_loop().time()
        result = await make_graphql_request(TABLE_DETAILS_QUERY, {"id": table_id})
        duration = asyncio.get_event_loop().time() - start_time

        if result.get("data", {}).get("allTable", {}).get("edges"):
            edges = result["data"]["allTable"]["edges"]
            if edges:
                table = edges[0]["node"]
                dataset = table["dataset"]
                columns = table.get("columns", {}).get("edges", [])

                # Generate BigQuery table reference with validation
                dataset_slug = dataset.get("slug", "")
                table_slug = table.get("slug", "")
                
                # Extract organization slug for BigQuery reference
                organization_slug = None
                if dataset.get("organizations", {}).get("edges"):
                    organization_slug = dataset["organizations"]["edges"][0]["node"].get("slug", "")
                
                bigquery_ref = None
                if dataset_slug and table_slug:
                    bigquery_ref = format_bigquery_reference(dataset_slug, table_slug, organization_slug)

                # Build compact, LLM-optimized response
                if bigquery_ref:
                    response = f"📋 {bigquery_ref} ({len(columns)} cols, {duration:.1f}s)\n"
                else:
                    response = f"📋 {table['name']} (BigQuery path unavailable, {len(columns)} cols, {duration:.1f}s)\n"
                
                response += f"Table: {table['name']} | Dataset: {dataset['name']} [{dataset.get('slug', '')}]\n"

                # Add table description if available
                if table.get('description'):
                    desc = table['description']
                    if len(desc) > 150:
                        desc = desc[:150] + "..."
                    response += f"Desc: {desc}\n"

                response += "\nColumns:\n"
                
                # List columns in compact format
                column_limit = 15 if fast_mode else len(columns)
                for col_edge in columns[:column_limit]:
                    column = col_edge["node"]
                    col_type = column.get("bigqueryType", {}).get("name", "Unknown")
                    col_desc = column.get("description", "")
                    
                    # Compact column description
                    if col_desc and len(col_desc) > 80:
                        col_desc = col_desc[:80] + "..."
                    
                    if col_desc:
                        response += f"{column['name']} ({col_type}) - {col_desc}\n"
                    else:
                        response += f"{column['name']} ({col_type})\n"
                
                if len(columns) > column_limit:
                    response += f"... and {len(columns) - column_limit} more columns\n"

                # Add ready-to-use SQL templates
                if bigquery_ref:
                    response += f"\nSQL Templates:\n"
                    response += f"SELECT * FROM `{bigquery_ref}` LIMIT 100\n"
                    
                    # Add a few sample columns for select query
                    if columns:
                        sample_cols = [col["node"]["name"] for col in columns[:3]]
                        response += f"SELECT {', '.join(sample_cols)} FROM `{bigquery_ref}` WHERE [condition]\n"

                # Add workflow guidance
                response += "\nNext: execute_bigquery_sql | Copy BigQuery path above"

                # Log performance
                logger.info(f"Table details completed in {duration:.2f}s for {len(columns)} columns")

                return response
            else:
                return "Table not found"
        else:
            return "Table not found"

    except Exception as e:
        return f"Error getting table details: {str(e)}"


@mcp.tool()
@utf8_tool
async def execute_bigquery_sql(
    query: str,
    max_results: int = 1000,
    timeout_seconds: int = 300
) -> str:
    """
    🚀 Execute SQL queries directly on Base dos Dados data in BigQuery.
    
    This tool allows you to run SQL queries on the Base dos Dados dataset in BigQuery,
    providing direct access to Brazilian open data for analysis and exploration.
    
    **Features:**
    - Execute SELECT queries on any basedosdados.* table
    - Automatic query validation and security checks
    - Configurable result limits and timeout settings
    - Formatted results with column information
    - Support for complex SQL operations (JOINs, aggregations, etc.)
    
    **Security:**
    - Only SELECT queries are allowed for data safety
    - Queries are restricted to basedosdados.* tables
    - Automatic validation prevents unauthorized operations
    
    **Use Cases:**
    - Data exploration and analysis
    - Statistical calculations and aggregations
    - Data quality assessment
    - Cross-dataset analysis with JOINs
    - Time series analysis and trends
    
    **Examples:**
    - Basic data exploration: `SELECT * FROM basedosdados.br_ibge_pnad_covid.microdados LIMIT 100`
    - Aggregations: `SELECT estado, COUNT(*) FROM basedosdados.br_ibge_pnad_covid.microdados GROUP BY estado`
    - Time analysis: `SELECT DATE(data), COUNT(*) FROM basedosdados.br_ibge_pnad_covid.microdados GROUP BY DATE(data)`
    
    Args:
        query: SQL SELECT query to execute on basedosdados.* tables
        max_results: Maximum number of rows to return (default: 1000, max: 10000)
        timeout_seconds: Query timeout in seconds (default: 300, max: 600)
        
    Returns:
        Formatted query results with data and column information
    """
    is_valid, error = validate_query(query)
    if not is_valid:
        return f"❌ Query rejected: {error}"

    results = await execute_query(query, max_results=max_results, timeout_seconds=timeout_seconds)
    return format_query_results(results)


@mcp.tool()
@utf8_tool
async def check_bigquery_status() -> str:
    """
    🔧 Check BigQuery authentication status and configuration.
    
    This tool verifies the BigQuery connection and authentication setup,
    providing detailed information about the current configuration and any issues.
    
    **Features:**
    - Authentication status verification
    - Project ID and configuration source information
    - Detailed error messages and troubleshooting instructions
    - Setup guidance for different authentication methods
    
    **Use Cases:**
    - Verify BigQuery access before running queries
    - Troubleshoot authentication issues
    - Check project configuration
    - Validate setup for data analysis workflows
    
    **Returns:**
    - Authentication status (✅ Authenticated / ❌ Not authenticated)
    - Project ID and configuration source
    - Error details if authentication fails
    - Step-by-step setup instructions if needed
    
    Returns:
        Detailed BigQuery status information with authentication details and setup instructions
    """
    client = BigQueryClient()
    auth_status = client.get_auth_status()

    response = "**Status do BigQuery**\n\n"

    if auth_status["authenticated"]:
        response += "✅ **Autenticado:** Sim\n"
        response += f"📊 **Project ID:** {auth_status['project_id']}\n"
        response += f"🔧 **Fonte:** {auth_status.get('config_source', 'unknown')}\n"
        response += "\n💡 **Pronto para executar queries!**\n"
    else:
        response += "❌ **Autenticado:** Não\n"
        response += f"⚠️  **Erro:** {auth_status['error']}\n\n"
        response += "**📋 Instruções:**\n"
        for instruction in auth_status['instructions']:
            response += f"- {instruction}\n"

    return response

async def enrich_datasets_with_fast_data(dataset_ids: list[str], timeout_seconds: float = 2.0) -> dict:
    """
    Fast dataset enrichment optimized for sub-1-second search performance.
    
    This function provides essential metadata without the performance bottleneck
    of fetching detailed column information. Perfect for search result enrichment.
    
    Args:
        dataset_ids: List of dataset IDs to enrich
        timeout_seconds: Maximum time to spend on GraphQL enrichment
        
    Returns:
        Dictionary mapping dataset IDs to essential enriched dataset data
    """
    if not dataset_ids:
        return {}
    
    try:
        # Use the fast query that excludes column details
        clean_ids = [clean_graphql_id(id) for id in dataset_ids]
        
        # Set a timeout for the GraphQL request
        start_time = asyncio.get_event_loop().time()
        result = await make_graphql_request(FAST_SEARCH_ENRICHMENT_QUERY, {"ids": clean_ids})
        duration = asyncio.get_event_loop().time() - start_time
        
        logger.info(f"Fast enrichment completed in {duration:.2f}s for {len(clean_ids)} datasets")
        
        enriched_datasets = {}
        
        if result.get("data", {}).get("allDataset", {}).get("edges"):
            for edge in result["data"]["allDataset"]["edges"]:
                dataset = edge["node"]
                dataset_id = dataset["id"]
                
                # Process organizations (names only for speed)
                org_names = [org["node"]["name"] for org in dataset.get("organizations", {}).get("edges", [])]
                
                # Process themes (names only for speed)
                theme_names = [theme["node"]["name"] for theme in dataset.get("themes", {}).get("edges", [])]
                
                # Process tags (names only for speed)
                tag_names = [tag["node"]["name"] for tag in dataset.get("tags", {}).get("edges", [])]
                
                # Extract organization slug for BigQuery references
                organization_slug = None
                if org_names:  # Using the already processed org data
                    # Get the slug from the first organization
                    for org_edge in dataset.get("organizations", {}).get("edges", []):
                        organization_slug = org_edge["node"].get("slug", "")
                        break

                # Process tables (basic info only - no columns)
                table_info = []
                for table_edge in dataset.get("tables", {}).get("edges", []):
                    table = table_edge["node"]
                    table_slug = table.get("slug", "")
                    
                    # Generate BigQuery reference if we have both dataset and table slugs
                    dataset_slug = dataset.get("slug", "")
                    bigquery_ref = None
                    if dataset_slug and table_slug:
                        bigquery_ref = format_bigquery_reference(dataset_slug, table_slug, organization_slug)
                    
                    table_info.append({
                        "name": table["name"],
                        "slug": table_slug,
                        "bigquery_reference": bigquery_ref
                    })
                
                # Store essential enriched data
                enriched_datasets[dataset_id] = {
                    "id": dataset_id,
                    "name": dataset["name"],
                    "slug": dataset.get("slug", ""),
                    "description": dataset.get("description", ""),
                    "organizations": org_names,
                    "themes": theme_names,
                    "tags": tag_names,
                    "tables": table_info,
                    "total_tables": len(table_info)
                }
        
        return enriched_datasets
        
    except Exception as e:
        logger.warning(f"Fast enrichment failed: {str(e)}")
        return {}


async def enrich_datasets_with_comprehensive_data(dataset_ids: list[str]) -> dict:
    """
    Enrich datasets with comprehensive details using GraphQL.
    
    This function takes a list of dataset IDs from the backend search and enriches them
    with detailed metadata, table information, and column details for LLM consumption.
    
    Args:
        dataset_ids: List of dataset IDs to enrich
        
    Returns:
        Dictionary mapping dataset IDs to enriched dataset data
    """
    if not dataset_ids:
        return {}

    try:
        # Clean the GraphQL IDs
        clean_ids = [clean_graphql_id(id) for id in dataset_ids]

        # Make GraphQL request for enrichment
        result = await make_graphql_request(SEARCH_ENRICHMENT_QUERY, {"ids": clean_ids})

        # Process the enriched data
        enriched_datasets = {}

        if result.get("data", {}).get("allDataset", {}).get("edges"):
            for edge in result["data"]["allDataset"]["edges"]:
                dataset = edge["node"]
                dataset_id = dataset["id"]

                # Process organizations
                org_info = []
                for org_edge in dataset.get("organizations", {}).get("edges", []):
                    org = org_edge["node"]
                    org_info.append({
                        "id": org["id"],
                        "name": org["name"],
                        "slug": org.get("slug", "")
                    })

                # Process themes
                theme_info = []
                for theme_edge in dataset.get("themes", {}).get("edges", []):
                    theme = theme_edge["node"]
                    theme_info.append({
                        "id": theme["id"],
                        "name": theme["name"],
                        "slug": theme.get("slug", "")
                    })

                # Process tags
                tag_info = []
                for tag_edge in dataset.get("tags", {}).get("edges", []):
                    tag = tag_edge["node"]
                    tag_info.append({
                        "id": tag["id"],
                        "name": tag["name"],
                        "slug": tag.get("slug", "")
                    })

                # Process tables with detailed column information
                table_info = []
                total_columns = 0

                for table_edge in dataset.get("tables", {}).get("edges", []):
                    table = table_edge["node"]
                    columns = table.get("columns", {}).get("edges", [])
                    column_count = len(columns)
                    total_columns += column_count

                    # Get sample columns with types (first 5)
                    sample_columns = []
                    for col_edge in columns[:5]:
                        col = col_edge["node"]
                        col_type = col.get("bigqueryType", {}).get("name", "Unknown")
                        sample_columns.append({
                            "id": col["id"],
                            "name": col["name"],
                            "type": col_type,
                            "description": col.get("description", "")
                        })

                    # Generate BigQuery reference with organization
                    organization_slug = None
                    if org_info:  # Get organization slug from already processed data
                        organization_slug = org_info[0].get("slug", "")
                    
                    bigquery_ref = format_bigquery_reference(dataset.get("slug", ""), table.get("slug", ""), organization_slug)

                    table_info.append({
                        "id": table["id"],
                        "name": table["name"],
                        "slug": table.get("slug", ""),
                        "description": table.get("description", ""),
                        "bigquery_reference": bigquery_ref,
                        "column_count": column_count,
                        "sample_columns": sample_columns,
                        "has_more_columns": column_count > 5
                    })

                # Store enriched dataset data
                enriched_datasets[dataset_id] = {
                    "id": dataset_id,
                    "name": dataset["name"],
                    "slug": dataset.get("slug", ""),
                    "description": dataset.get("description", ""),
                    "organizations": org_info,
                    "themes": theme_info,
                    "tags": tag_info,
                    "tables": table_info,
                    "total_tables": len(table_info),
                    "total_columns": total_columns
                }

        return enriched_datasets

    except Exception as e:
        logger.error(f"Error enriching datasets: {str(e)}")
        return {}


async def search_backend_api(query: str, limit: int = 10) -> dict:
    """
    Internal function: Use the Base dos Dados backend search API directly.
    
    This function calls the same API that the website uses internally:
    https://backend.basedosdados.org/search/
    
    Args:
        query: Search term to find datasets
        limit: Maximum number of results to return
        
    Returns:
        Raw API response with search results
    """
    try:
        # Use the backend search API directly
        search_url = "https://backend.basedosdados.org/search/"

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                search_url,
                params={"q": query, "page_size": limit}
            )
            response.raise_for_status()
            result = response.json()

            logger.info(f"Backend API response: found {result.get('count', 0)} total results, showing {len(result.get('results', []))} results")

            return result

    except httpx.TimeoutException:
        raise Exception("Search request timeout - the backend API is taking too long to respond")
    except httpx.RequestError as e:
        raise Exception(f"Network error accessing backend search: {str(e)}")
    except Exception as e:
        raise Exception(f"Error accessing backend search API: {str(e)}")

async def search_datasets_backend(
    query: str,
    limit: int = 10,
    fast_mode: bool = True
) -> str:
    """
    Internal function: Search for datasets using the Base dos Dados backend search API
    with comprehensive GraphQL enrichment for LLM consumption.
    
    This function uses the same API that the website uses internally,
    then enriches the results with detailed metadata and structure information
    to provide LLMs with comprehensive context for decision making.
    
    Args:
        query: Search term to find datasets
        limit: Maximum number of results to return
        
    Returns:
        Formatted search results with comprehensive dataset information
    """

    try:
        # Track total operation time
        search_start = asyncio.get_event_loop().time()
        
        # Use the backend's search API
        backend_start = asyncio.get_event_loop().time()
        result = await search_backend_api(query, limit)
        backend_duration = asyncio.get_event_loop().time() - backend_start

        # Extract search results
        datasets = result.get("results", [])
        total_count = result.get("count", 0)

        logger.info(f"Backend search returned {len(datasets)} results out of {total_count} total matches")

        if not datasets:
            return f"Search results for '{query}':\n\nNo datasets found matching your search criteria.\n\nTry different keywords or check spelling."

        # Optional GraphQL enrichment based on fast_mode setting
        dataset_ids = [d.get('id', '') for d in datasets if d.get('id')]
        enrichment_start = asyncio.get_event_loop().time()
        enriched_data = {}
        enrichment_duration = 0
        
        if not fast_mode and len(datasets) <= 10:  # Only enrich small result sets in slow mode
            try:
                # Set aggressive timeout for GraphQL enrichment
                enriched_data = await asyncio.wait_for(
                    enrich_datasets_with_fast_data(dataset_ids), 
                    timeout=0.8  # Even more aggressive timeout
                )
                enrichment_duration = asyncio.get_event_loop().time() - enrichment_start
                logger.info(f"GraphQL enrichment successful in {enrichment_duration:.2f}s")
            except asyncio.TimeoutError:
                enrichment_duration = asyncio.get_event_loop().time() - enrichment_start
                logger.info(f"GraphQL enrichment timed out after {enrichment_duration:.2f}s, using backend-only mode")
            except Exception as e:
                enrichment_duration = asyncio.get_event_loop().time() - enrichment_start
                logger.info(f"GraphQL enrichment failed: {str(e)}, using backend-only mode")
        else:
            logger.info(f"Fast mode enabled or large result set ({len(datasets)} datasets), skipping GraphQL enrichment")
        
        # Calculate total duration
        total_duration = asyncio.get_event_loop().time() - search_start

        # Build high-density, compact response for fast LLM consumption
        response = f"🔍 **{query}** ({len(datasets)} results, {total_duration:.1f}s)\n\n"
        
        # Log performance metrics
        logger.info(f"Performance: total={total_duration:.2f}s, backend={backend_duration:.2f}s, enrichment={enrichment_duration:.2f}s")

        for i, dataset in enumerate(datasets, 1):
            dataset_id = dataset.get('id', '')
            dataset_name = dataset.get('name', 'Unnamed Dataset')
            dataset_slug = dataset.get('slug', '')
            dataset_description = dataset.get('description', 'No description available')
            n_tables = dataset.get('n_tables', 0)

            # Get enriched data if available (try both raw ID and GraphQL node ID)
            enriched = enriched_data.get(dataset_id, {}) or enriched_data.get(f"DatasetNode:{dataset_id}", {})

            response += f"## {i}. {dataset_name} [{dataset_slug}]\n"
            response += f"ID: {dataset_id}\n"
            response += f"Desc: {dataset_description}\n"

            if enriched:
                # Compact metadata display
                if enriched.get('organizations'):
                    response += f"Org: {', '.join(enriched['organizations'])}\n"
                if enriched.get('themes'):
                    response += f"Theme: {', '.join(enriched['themes'])}\n"
                if enriched.get('tags'):
                    response += f"Tags: {', '.join(enriched['tags'])}\n"

                # Compact table information with accurate BigQuery paths
                tables = enriched.get('tables', [])
                response += f"Tables({len(tables)}): "

                table_summaries = []
                bigquery_paths = []

                for table in tables:
                    table_summaries.append(table['name'])
                    if table.get('bigquery_reference'):
                        bigquery_paths.append(table['bigquery_reference'])

                response += f"{', '.join(table_summaries)}\n"

                # Show BigQuery paths if we have accurate ones, otherwise guide to other API
                if bigquery_paths:
                    response += f"BQ: {', '.join(bigquery_paths)}\n"
                else:
                    response += f"BQ: Use get_dataset_overview('{dataset_id}') for exact paths\n"

            else:
                # Fallback if enrichment failed
                response += f"Tables: {n_tables} (use get_dataset_overview for details)\n"
                response += f"BQ: Use get_dataset_overview('{dataset_id}') for exact paths\n"

            response += "\n"

        response += "Next: get_dataset_overview(ID) → get_table_details(tableID) → execute_bigquery_sql"

        return response

    except Exception as e:
        logger.error(f"Error in comprehensive search: {str(e)}")
        return f"Error searching datasets: {str(e)}\n\nTry again with different keywords or check your connection."

# =============================================================================
# Server Entry Point
# =============================================================================

if __name__ == "__main__":
    mcp.run()
