from typing import Optional
import httpx
import logging
import sys
import os
from mcp.server.fastmcp import FastMCP
from basedosdados_mcp.graphql_client import make_graphql_request, DATASET_OVERVIEW_QUERY, TABLE_DETAILS_QUERY, ENHANCED_SEARCH_QUERY, COMPREHENSIVE_SEARCH_QUERY
from basedosdados_mcp.utils import (
    clean_graphql_id, format_bigquery_reference, format_bigquery_reference_with_highlighting, format_sql_query_with_reference
)
from basedosdados_mcp.bigquery_client import (
    execute_query, execute_simple_query, get_table_schema, get_table_info,
    validate_query, format_query_results, BigQueryClient
)

# =============================================================================
# Logging Setup
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# UTF-8 Encoding Configuration
# =============================================================================

# Ensure proper UTF-8 encoding for all output
# Set environment variables for UTF-8 encoding
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')
os.environ.setdefault('LC_ALL', 'en_US.UTF-8')
os.environ.setdefault('LANG', 'en_US.UTF-8')

# Force UTF-8 encoding for stdout and stderr
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)

# =============================================================================
# FastMCP Server Initialization
# =============================================================================

# Initialize the FastMCP server with UTF-8 encoding
mcp = FastMCP("Base dos Dados MCP")

# =============================================================================
# Response Encoding Helper
# =============================================================================

def ensure_utf8_response(response: str) -> str:
    """
    Ensure the response is properly UTF-8 encoded and convert Unicode escape sequences.
    
    Args:
        response: The response string to encode
        
    Returns:
        Properly encoded UTF-8 string with Unicode characters
    """
    if isinstance(response, bytes):
        response = response.decode('utf-8')
    elif not isinstance(response, str):
        response = str(response)
    
    # Handle Unicode escape sequences (e.g., \u00e7 -> ç)
    try:
        # First, try to decode any Unicode escape sequences
        import codecs
        response = codecs.decode(response, 'unicode_escape')
    except (UnicodeDecodeError, ValueError):
        # If that fails, try a more robust approach
        import re
        
        def replace_unicode_escapes(match):
            try:
                return chr(int(match.group(1), 16))
            except (ValueError, OverflowError):
                return match.group(0)
        
        # Replace \uXXXX patterns with actual Unicode characters
        response = re.sub(r'\\u([0-9a-fA-F]{4})', replace_unicode_escapes, response)
        
        # Also handle \xXX patterns
        def replace_hex_escapes(match):
            try:
                return chr(int(match.group(1), 16))
            except (ValueError, OverflowError):
                return match.group(0)
        
        response = re.sub(r'\\x([0-9a-fA-F]{2})', replace_hex_escapes, response)
    
    # Ensure final encoding is UTF-8
    try:
        return response.encode('utf-8').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        # Fallback: try to encode as UTF-8, ignoring errors
        return response.encode('utf-8', errors='ignore').decode('utf-8')

def clean_api_data(data: dict) -> dict:
    """
    Clean API response data to handle Unicode escape sequences in nested structures.
    
    Args:
        data: Dictionary containing API response data
        
    Returns:
        Cleaned dictionary with proper Unicode characters
    """
    if isinstance(data, dict):
        cleaned = {}
        for key, value in data.items():
            cleaned[key] = clean_api_data(value)
        return cleaned
    elif isinstance(data, list):
        return [clean_api_data(item) for item in data]
    elif isinstance(data, str):
        return ensure_utf8_response(data)
    else:
        return data

# =============================================================================
# Backend Search API Integration
# =============================================================================

@mcp.tool()
async def search_datasets(
    query: str,
    limit: int = 10
) -> str:
    """
    🔍 Search for datasets in Base dos Dados using the same infrastructure as the website.
    
    This tool provides access to the comprehensive Brazilian open data repository,
    searching across all datasets, organizations, themes, and descriptions.
    
    **Features:**
    - Uses the same search API as basedosdados.org for consistent results
    - Returns detailed dataset information with BigQuery table references
    - Includes organizations, themes, tags, and data coverage information
    - Provides ready-to-use BigQuery table references for direct data access
    
    **Examples:**
    - Search by organization: "ibge", "anvisa", "ministerio da saude"
    - Search by theme: "saude", "educacao", "economia", "transporte"
    - Search by topic: "covid", "eleicoes", "clima", "energia"
    - Search by data type: "municipios", "estados", "empresas"
    
    **Returns:**
    - Dataset names, descriptions, and metadata
    - Organization and theme information
    - Table counts and BigQuery references
    - Temporal and spatial coverage details
    - Ready-to-use SQL query examples
    
    Args:
        query: Search term to find relevant datasets (e.g., "ibge", "saude", "educacao", "covid")
        limit: Maximum number of results to return (default: 10, max: 50)
        
    Returns:
        Comprehensive search results with dataset information and BigQuery table references
    """
    
    logger.info(f"Starting search for query: '{query}' with limit: {limit}")
    
    # Use Backend API (same infrastructure as the site - most reliable)
    try:
        logger.info("Attempting backend API search...")
        response = await search_datasets_backend(query, limit)
        logger.info("Backend API search completed")
        return ensure_utf8_response(response)
    except Exception as e:
        logger.error(f"Backend API search failed: {str(e)}")
        return ensure_utf8_response(f"Search failed for query '{query}'. Please try again later.")

# =============================================================================
# Internal Search Functions (not exposed as tools)
# =============================================================================

@mcp.tool()
async def get_dataset_overview(dataset_id: str) -> str:
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
        result = await make_graphql_request(DATASET_OVERVIEW_QUERY, {"id": dataset_id})
        
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
                
                for table_edge in dataset.get("tables", {}).get("edges", []):
                    table = table_edge["node"]
                    columns = table.get("columns", {}).get("edges", [])
                    column_count = len(columns)
                    total_columns += column_count
                    
                    # Get sample column names (first 5)
                    sample_columns = [col["node"]["name"] for col in columns[:5]]
                    if len(columns) > 5:
                        sample_columns.append(f"... and {len(columns) - 5} more")
                    
                    # Generate full BigQuery table reference
                    dataset_slug = dataset.get("slug", "")
                    table_slug = table.get("slug", "")
                    bigquery_ref = format_bigquery_reference(dataset_slug, table_slug)
                    
                    tables_info.append({
                        "id": table["id"],
                        "name": table["name"],
                        "slug": table_slug,
                        "description": table.get("description", "No description available"),
                        "column_count": column_count,
                        "sample_columns": sample_columns,
                        "bigquery_reference": bigquery_ref
                    })
                
                # Build comprehensive response
                response = f"**📊 Dataset Overview: {dataset['name']}**\n\n"
                response += f"**💡 BigQuery Format:** `basedosdados.dataset_slug.table_slug` (e.g., `basedosdados.br_abrinq_oca.municipio_primeira_infancia`)\n\n"
                response += f"**Basic Information:**\n"
                response += f"- **ID:** {dataset['id']}\n"
                response += f"- **Slug:** {dataset.get('slug', '')}\n"
                response += f"- **Description:** {dataset.get('description', 'No description available')}\n"
                response += f"- **Organizations:** {', '.join(org_names)}\n"
                response += f"- **Themes:** {', '.join(theme_names)}\n"
                response += f"- **Tags:** {', '.join(tag_names)}\n\n"
                response += f"**Data Structure:**\n"
                response += f"- **Total Tables:** {len(tables_info)}\n"
                response += f"- **Total Columns:** {total_columns}\n\n"
                response += f"**📋 Tables with BigQuery Access:**\n"
                
                for table in tables_info:
                    response += f"\n**{table['name']}** ({table['column_count']} columns)\n"
                    response += f"- **BigQuery Reference:** `{table['bigquery_reference']}`\n"
                    response += f"- **Table ID:** {table['id']}\n"
                    response += f"- **Description:** {table['description']}\n"
                    response += f"- **Sample Columns:** {', '.join(table['sample_columns'])}\n"
                    response += f"- **💡 Quick Query:** `SELECT * FROM `{table['bigquery_reference']}` LIMIT 10`\n"
                
                sample_ref = tables_info[0]['bigquery_reference'] if tables_info else 'basedosdados.dataset.table'
                response += f"\n\n**🔍 Next Steps:**\n"
                response += f"- Use `get_table_details` with a table ID to see all columns and types with sample SQL queries\n"
                response += f"- **Ready-to-use BigQuery references above** - copy any of the table references for direct access\n"
                response += f"- Example: `SELECT * FROM {sample_ref} LIMIT 100`"
                
                return ensure_utf8_response(response)
            else:
                return ensure_utf8_response("Dataset not found")
        else:
            return ensure_utf8_response("Dataset not found")
            
    except Exception as e:
        return ensure_utf8_response(f"Error getting dataset overview: {str(e)}")

@mcp.tool()
async def get_table_details(table_id: str) -> str:
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
        result = await make_graphql_request(TABLE_DETAILS_QUERY, {"id": table_id})
        
        if result.get("data", {}).get("allTable", {}).get("edges"):
            edges = result["data"]["allTable"]["edges"]
            if edges:
                table = edges[0]["node"]
                dataset = table["dataset"]
                columns = table.get("columns", {}).get("edges", [])
                
                # Generate BigQuery table reference
                dataset_slug = dataset.get("slug", "")
                table_slug = table.get("slug", "")
                bigquery_ref = format_bigquery_reference(dataset_slug, table_slug)
                
                response = f"**📋 Table Details: {table['name']}**\n\n"
                response += f"**🚀 BigQuery Access:** `{bigquery_ref}`\n\n"
                response += f"**💡 Example Format:** `basedosdados.br_abrinq_oca.municipio_primeira_infancia`\n\n"
                response += f"**Basic Information:**\n"
                response += f"- **Table ID:** {table['id']}\n"
                response += f"- **Table Slug:** {table_slug}\n"
                response += f"- **Description:** {table.get('description', 'No description available')}\n"
                response += f"**Dataset Context:**\n"
                response += f"- **Dataset:** {dataset['name']}\n"
                response += f"- **Dataset ID:** {dataset['id']}\n"
                response += f"- **Dataset Slug:** {dataset.get('slug', '')}\n\n"
                response += f"**📊 Columns ({len(columns)} total):**\n"
                
                for col_edge in columns:
                    column = col_edge["node"]
                    col_type = column.get("bigqueryType", {}).get("name", "Unknown")
                    col_desc = column.get("description", "No description")
                    response += f"\n**{column['name']}** ({col_type})\n"
                    response += f"- ID: {column['id']}\n"
                    response += f"- Description: {col_desc}\n"
                
                # Generate sample SQL queries
                column_names = [col["node"]["name"] for col in columns]
                sample_columns = ", ".join(column_names[:5])
                if len(column_names) > 5:
                    sample_columns += f", ... -- and {len(column_names) - 5} more"
                
                response += f"\n\n**🔍 Sample SQL Queries:**\n\n"
                response += f"**Basic Select:**\n"
                response += f"```sql\n"
                response += f"{format_sql_query_with_reference(bigquery_ref, sample_columns, 100)}\n"
                response += f"```\n\n"
                response += f"**Full Table Schema:**\n"
                response += f"```sql\n"
                response += f"{format_sql_query_with_reference(bigquery_ref, '*', 10)}\n"
                response += f"```\n\n"
                response += f"**Column Info:**\n"
                response += f"```sql\n"
                response += f"SELECT column_name, data_type, description\n"
                response += f"FROM `{dataset_slug}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS\n"
                response += f"WHERE table_name = '{table_slug}'\n"
                response += f"```\n\n"
                response += f"**🚀 Access Instructions:**\n"
                response += f"1. **BigQuery Console:** Use `{bigquery_ref}` in your queries\n"
                response += f"2. **Python Package:** `bd.read_table('{dataset_slug}', '{table_slug}')`\n"
                response += f"3. **Direct SQL:** Copy any query above and replace the table reference\n"
                
                return ensure_utf8_response(response)
            else:
                return ensure_utf8_response("Table not found")
        else:
            return ensure_utf8_response("Table not found")
            
    except Exception as e:
        return ensure_utf8_response(f"Error getting table details: {str(e)}")

@mcp.tool()
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
        return ensure_utf8_response(f"❌ Query rejected: {error}")

    results = await execute_query(query, max_results=max_results, timeout_seconds=timeout_seconds)
    return ensure_utf8_response(format_query_results(results))

@mcp.tool()
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
        response += f"✅ **Autenticado:** Sim\n"
        response += f"📊 **Project ID:** {auth_status['project_id']}\n"
        response += f"🔧 **Fonte:** {auth_status.get('config_source', 'unknown')}\n"
        response += f"\n💡 **Pronto para executar queries!**\n"
    else:
        response += f"❌ **Autenticado:** Não\n"
        response += f"⚠️  **Erro:** {auth_status['error']}\n\n"
        response += f"**📋 Instruções:**\n"
        for instruction in auth_status['instructions']:
            response += f"- {instruction}\n"
    
    return ensure_utf8_response(response)

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
    limit: int = 10
) -> str:
    """
    Internal function: Search for datasets using the Base dos Dados backend search API.
    
    This function uses the same API that the website uses internally,
    providing the most accurate and comprehensive results.
    
    Args:
        query: Search term to find datasets
        limit: Maximum number of results to return
        
    Returns:
        Formatted search results with dataset information
    """
    
    try:
        # Use the backend's search API
        result = await search_backend_api(query, limit)
        
        # Clean the API response data to handle Unicode escape sequences
        result = clean_api_data(result)
        
        # Extract search results
        datasets = result.get("results", [])
        total_count = result.get("count", 0)
        
        # Build response
        response = f"**🔍 Search Results for: '{query}'**\n\n"
        response += f"**💡 Using Base dos Dados Backend API (Same as Website)**\n\n"
        
        if datasets:
            response += f"Found {len(datasets)} datasets (showing {len(datasets)} of {total_count} total):\n\n"
            response += f"**💡 BigQuery Format:** `basedosdados.dataset_slug.table_slug` (e.g., `basedosdados.br_abrinq_oca.municipio_primeira_infancia`)\n\n"
            
            for i, dataset in enumerate(datasets, 1):
                response += f"**{i}. {dataset.get('name', 'Unnamed Dataset')}**\n"
                
                if dataset.get('description'):
                    response += f"📝 **Description:** {dataset['description']}\n"
                
                if dataset.get('slug'):
                    response += f"🔗 **Slug:** {dataset['slug']}\n"
                
                # Organizations
                if dataset.get('organizations'):
                    org_names = [org.get('name', '') for org in dataset['organizations']]
                    response += f"🏢 **Organizations:** {', '.join(org_names)}\n"
                
                # Themes
                if dataset.get('themes'):
                    theme_names = [theme.get('name', '') for theme in dataset['themes']]
                    response += f"🎨 **Themes:** {', '.join(theme_names)}\n"
                
                # Tags
                if dataset.get('tags'):
                    tag_names = [tag.get('name', '') for tag in dataset['tags']]
                    response += f"🏷️ **Tags:** {', '.join(tag_names)}\n"
                
                # Tables info
                n_tables = dataset.get('n_tables', 0)
                if n_tables > 0:
                    response += f"📊 **Tables:** {n_tables} tables\n"
                    
                    # Generate BigQuery reference if we have table info
                    if dataset.get('slug') and dataset.get('first_table_id'):
                        # For now, we'll use a generic format since we don't have table slug
                        bigquery_ref = f"basedosdados.{dataset['slug']}.table_name"
                        response += f"🔗 **BigQuery:** `{bigquery_ref}`\n"
                        response += f"   💡 **Quick Query:** `SELECT * FROM `{bigquery_ref}` LIMIT 10`\n"
                
                # Coverage info
                if dataset.get('temporal_coverage'):
                    response += f"📅 **Temporal Coverage:** {', '.join(dataset['temporal_coverage'])}\n"
                
                if dataset.get('spatial_coverage'):
                    spatial_names = [spatial.get('name', '') for spatial in dataset['spatial_coverage']]
                    response += f"🌍 **Spatial Coverage:** {', '.join(spatial_names)}\n"
                
                # Data availability
                if dataset.get('contains_open_data'):
                    response += f"✅ **Open Data:** Available\n"
                
                if dataset.get('contains_tables'):
                    response += f"📋 **Tables:** Available\n"
                
                response += "\n"
        
        else:
            response += "No datasets found matching your search criteria."
        
        response += f"\n**💡 Next Steps:**\n"
        response += f"- Use `get_dataset_overview` with a dataset ID for detailed table information\n"
        response += f"- Use `get_table_details` with a table ID for complete column information\n"
        response += f"- Use `execute_bigquery_sql` to run queries on the data\n"
        
        return ensure_utf8_response(response)
        
    except Exception as e:
        return ensure_utf8_response(f"Error searching datasets via backend API: {str(e)}")

# =============================================================================
# Server Entry Point
# =============================================================================

if __name__ == "__main__":
    mcp.run()