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
    🔍 **High-Performance Brazilian Data Discovery - LLM Optimized**
    
    Single-call comprehensive search delivering complete dataset context with sub-2s response times.
    Optimized for LLM decision-making with structured results and ready-to-use BigQuery references.
    
    **⚡ Performance**: 0.8-2.0s response | Fast mode: backend-only | Enrichment: +GraphQL metadata
    
    **🎯 Use When**:
    - Starting Brazilian data exploration workflow
    - Need BigQuery table references for analysis
    - Searching by organization, theme, or topic keywords
    - Require comprehensive dataset context in single call
    
    **📊 Returns Per Dataset**:
    - Complete metadata (description, organization, themes, tags)
    - Table structure previews with BigQuery references
    - Ready-to-use SQL table paths: `basedosdados.br_org_dataset.table`
    - Column samples with data types for planning queries
    - Next-step workflow guidance for deeper exploration
    
    **🔍 Search Strategy Examples**:
    - **Organizations**: "ibge", "anvisa", "ministerio da saude", "inep", "tse"
    - **Themes**: "saude", "educacao", "economia", "meio ambiente", "justica"
    - **Topics**: "covid", "eleicoes", "clima", "populacao", "empresas"
    - **Acronyms**: "rais", "pnad", "sus", "enem" (auto-prioritized)
    
    **⚡ Workflow Integration**:
    ```
    search_datasets("covid") → get_dataset_overview(ID) → get_table_details(tableID) → execute_bigquery_sql(query)
    ```
    
    **🚀 Performance Modes**:
    - **Fast Mode (default)**: Backend search only, sub-2s responses, 15+ results
    - **Enriched Mode**: +GraphQL metadata for small result sets (<10 datasets)
    
    Args:
        query: Brazilian data search term (Portuguese accent-aware: "populacao" = "população")
        limit: Max results (default: 15, optimal for LLM processing)
        fast_mode: True for sub-2s backend-only mode, False for GraphQL enrichment
        
    Returns:
        Structured search results with complete BigQuery context, metadata, and workflow guidance
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
    📊 **Complete Dataset Structure Explorer - Table & BigQuery Reference Hub**
    
    Deep-dive into a specific dataset to get all tables with BigQuery references and column structures.
    Essential second step after search_datasets() for comprehensive data exploration planning.
    
    **⚡ Performance**: 1-3s response | Fetches complete dataset structure in single call
    
    **🎯 Use When**:
    - After finding datasets with search_datasets()  
    - Need complete list of all tables in a dataset
    - Want BigQuery references for multiple tables
    - Planning multi-table analysis or JOINs
    
    **📊 Returns Structure**:
    - **Dataset Metadata**: Name, description, organizations, themes, tags
    - **All Tables**: Names, descriptions, column counts per table  
    - **BigQuery Paths**: Ready-to-use `basedosdados.br_org_dataset.table` references
    - **Column Previews**: Sample column names and types for each table
    - **SQL Templates**: Basic SELECT queries for immediate use
    
    **🔄 Workflow Position**:
    ```
    search_datasets("ibge") → get_dataset_overview(datasetID) → get_table_details(specificTableID) → execute_bigquery_sql()
    ```
    
    **💡 Use Cases**:
    - **Structure Exploration**: See all available tables before drilling down
    - **BigQuery Planning**: Get complete table references for complex queries
    - **Data Coverage**: Understand dataset scope and organization
    - **Multi-table Analysis**: Plan JOINs across related tables
    
    **⚠️ Decision Guide**:
    - Use this for **exploring dataset structure** (all tables)
    - Use get_table_details() for **specific table analysis** (all columns)
    
    Args:
        dataset_id: GraphQL ID from search_datasets() results (e.g., "d30222ad-7a5c-4778...")
        fast_mode: Performance optimization (always recommended)
        
    Returns:
        Structured dataset overview with all tables, BigQuery references, and workflow guidance
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

                # Note: organization_slug no longer needed since we use cloudTables data directly

                for table_edge in dataset.get("tables", {}).get("edges", []):
                    table = table_edge["node"]
                    columns = table.get("columns", {}).get("edges", [])
                    column_count = len(columns)
                    total_columns += column_count
                    table_slug = table.get("slug", "")

                    # Generate BigQuery table reference from cloudTables data only
                    gcp_project_id = None
                    gcp_dataset_id = None
                    gcp_table_id = None
                    cloud_tables = table.get("cloudTables", {}).get("edges", [])
                    if cloud_tables:
                        cloud_table = cloud_tables[0]["node"]
                        gcp_project_id = cloud_table.get("gcpProjectId")
                        gcp_dataset_id = cloud_table.get("gcpDatasetId")
                        gcp_table_id = cloud_table.get("gcpTableId")
                    
                    bigquery_ref = format_bigquery_reference(
                        gcp_project_id=gcp_project_id,
                        gcp_dataset_id=gcp_dataset_id,
                        gcp_table_id=gcp_table_id
                    )
                    if bigquery_ref:
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
    📋 **Complete Table Schema Explorer - Column Details & SQL Generator**
    
    Final step in data exploration: get all columns, types, and ready-to-use SQL queries for a specific table.
    Essential for understanding data structure before writing BigQuery analyses.
    
    **⚡ Performance**: 1-2s response | Full column schema + SQL templates in single call
    
    **🎯 Use When**:
    - After identifying target table via get_dataset_overview()
    - Need complete column schema with data types
    - Want SQL query templates for immediate data access
    - Planning specific analyses requiring column selection
    
    **📊 Returns Structure**:
    - **Table Metadata**: Name, description, dataset context
    - **All Columns**: Names, data types, descriptions for every column
    - **BigQuery Reference**: Exact `basedosdados.br_org_dataset.table` path
    - **SQL Templates**: Ready-to-execute SELECT queries with column examples
    - **Access Methods**: BigQuery Console, Python package, direct SQL instructions
    
    **🔄 Workflow Position**:
    ```
    search_datasets("ibge") → get_dataset_overview(datasetID) → get_table_details(tableID) → execute_bigquery_sql(query)
    ```
    
    **💡 SQL Examples Provided**:
    - **Basic Exploration**: `SELECT * FROM table LIMIT 100`
    - **Column Selection**: `SELECT col1, col2, col3 FROM table WHERE condition`
    - **Schema Inspection**: Column names, types, and descriptions
    - **Analysis Templates**: Common query patterns for data exploration
    
    **🔍 Column Information**:
    - **Data Types**: STRING, INTEGER, FLOAT, DATE, TIMESTAMP, etc.
    - **Descriptions**: Business context and meaning for each column
    - **Type Guidance**: How to use each column in WHERE clauses and aggregations
    
    **⚠️ Decision Guide**:
    - Use this when you need **complete column details** for a specific table
    - Use execute_bigquery_sql() next to **run actual queries** on the data
    
    Args:
        table_id: GraphQL table ID from get_dataset_overview() results (e.g., "t89abc123...")
        fast_mode: Performance optimization, limits to first 15 columns for speed
        
    Returns:
        Complete table schema with all columns, types, BigQuery reference, and SQL templates
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

                # Generate BigQuery table reference from cloudTables data only
                gcp_project_id = None
                gcp_dataset_id = None
                gcp_table_id = None
                cloud_tables = table.get("cloudTables", {}).get("edges", [])
                if cloud_tables:
                    cloud_table = cloud_tables[0]["node"]
                    gcp_project_id = cloud_table.get("gcpProjectId")
                    gcp_dataset_id = cloud_table.get("gcpDatasetId")
                    gcp_table_id = cloud_table.get("gcpTableId")
                
                bigquery_ref = format_bigquery_reference(
                    gcp_project_id=gcp_project_id,
                    gcp_dataset_id=gcp_dataset_id,
                    gcp_table_id=gcp_table_id
                )

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
    🚀 **BigQuery SQL Executor - Direct Brazilian Data Analysis**
    
    Execute SQL queries directly on Base dos Dados in BigQuery for real-time data analysis.
    Final workflow step after exploring datasets and tables - now analyze the actual data.
    
    **⚡ Performance**: Depends on query complexity | 10GB billing limit | 5min timeout
    
    **🔒 Security (Auto-Enforced)**:
    - **READ-ONLY**: Only SELECT queries allowed (no INSERT/UPDATE/DELETE)
    - **Restricted Scope**: Must query `basedosdados.*` tables only
    - **Billing Protection**: 10GB maximum data processing limit per query
    - **Validation**: Automatic security checks before execution
    
    **🎯 Use When**:
    - After getting BigQuery references from get_table_details()
    - Ready to analyze actual data (not just explore schema)
    - Need statistical calculations, aggregations, or filtering
    - Want to JOIN multiple Base dos Dados tables
    
    **📊 Query Examples**:
    ```sql
    -- Basic Exploration
    SELECT * FROM `basedosdados.br_ibge_populacao.municipio` LIMIT 100
    
    -- State Aggregation  
    SELECT sigla_uf, SUM(populacao) as total_pop 
    FROM `basedosdados.br_ibge_populacao.municipio` 
    GROUP BY sigla_uf ORDER BY total_pop DESC
    
    -- Time Series Analysis
    SELECT ano, AVG(pib_per_capita) as pib_medio
    FROM `basedosdados.br_ibge_pib.municipio`
    WHERE ano >= 2010 GROUP BY ano ORDER BY ano
    
    -- Multi-table JOIN
    SELECT p.nome, pop.populacao, pib.pib_per_capita
    FROM `basedosdados.br_bd_diretorios_brasil.municipio` p
    JOIN `basedosdados.br_ibge_populacao.municipio` pop ON p.id_municipio = pop.id_municipio
    JOIN `basedosdados.br_ibge_pib.municipio` pib ON p.id_municipio = pib.id_municipio
    WHERE pop.ano = 2020 AND pib.ano = 2020
    ```
    
    **🔄 Complete Workflow**:
    ```
    search_datasets("ibge") → get_dataset_overview(ID) → get_table_details(tableID) → execute_bigquery_sql("SELECT...")
    ```
    
    **💡 Pro Tips**:
    - Use LIMIT for initial exploration to avoid large bills
    - Add WHERE clauses to filter by year, state, or other dimensions
    - Use aggregate functions (COUNT, SUM, AVG) for statistical analysis
    - JOIN tables using id_municipio, sigla_uf, or other common keys
    
    Args:
        query: SQL SELECT query (must reference basedosdados.* tables)
        max_results: Max rows returned (default: 1000, helps control output size)
        timeout_seconds: Query timeout (default: 300s, max: 600s for complex queries)
        
    Returns:
        Formatted results with data rows, column info, performance metrics, and data volumes processed
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
    🔧 **BigQuery Authentication Diagnostics - Setup & Troubleshooting**
    
    Diagnostic tool to verify BigQuery authentication and troubleshoot connection issues.
    Use this FIRST if execute_bigquery_sql() fails with authentication errors.
    
    **⚡ Performance**: Instant response | No BigQuery API calls | Local auth check only
    
    **🎯 Use When**:
    - Before running first BigQuery queries to verify setup
    - execute_bigquery_sql() returns authentication errors
    - Setting up Base dos Dados access for the first time
    - Troubleshooting connection issues
    
    **✅ Success Response**:
    - **Authenticated**: ✅ Confirmed working
    - **Project ID**: Your Google Cloud project identifier
    - **Config Source**: Environment variables vs gcloud default
    - **Ready Status**: Confirmed ready for execute_bigquery_sql()
    
    **❌ Error Response + Solutions**:
    - **No Credentials Found**: Setup instructions for service account or gcloud auth
    - **Invalid Project**: Guidance for setting BIGQUERY_PROJECT_ID correctly
    - **Permission Issues**: Steps to grant BigQuery access to your account
    - **Configuration Problems**: Environment variable setup guidance
    
    **🔧 Setup Methods Supported**:
    1. **Service Account**: GOOGLE_APPLICATION_CREDENTIALS + BIGQUERY_PROJECT_ID
    2. **gcloud CLI**: `gcloud auth application-default login`
    3. **Environment Auto-Detection**: Automatic project discovery
    
    **🔄 Troubleshooting Workflow**:
    ```
    check_bigquery_status() → Fix auth issues → execute_bigquery_sql() → Analyze data
    ```
    
    **💡 Common Solutions**:
    - **Missing Credentials**: Set GOOGLE_APPLICATION_CREDENTIALS to service account JSON path
    - **No Project**: Set BIGQUERY_PROJECT_ID environment variable
    - **Permission Denied**: Add BigQuery Data Viewer role to your account
    - **gcloud Alternative**: Run `gcloud auth application-default login`
    
    **⚠️ When NOT to Use**:
    - Don't use this repeatedly - it's a diagnostic tool
    - If already authenticated, proceed directly to execute_bigquery_sql()
    
    Returns:
        Authentication status with detailed setup instructions if configuration is missing
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
                
                # Note: organization_slug no longer needed since we use cloudTables data directly

                # Process tables (basic info only - no columns)
                table_info = []
                for table_edge in dataset.get("tables", {}).get("edges", []):
                    table = table_edge["node"]
                    table_slug = table.get("slug", "")
                    
                    # Generate BigQuery reference from cloudTables data only
                    gcp_project_id = None
                    gcp_dataset_id = None
                    gcp_table_id = None
                    cloud_tables = table.get("cloudTables", {}).get("edges", [])
                    if cloud_tables:
                        cloud_table = cloud_tables[0]["node"]
                        gcp_project_id = cloud_table.get("gcpProjectId")
                        gcp_dataset_id = cloud_table.get("gcpDatasetId")
                        gcp_table_id = cloud_table.get("gcpTableId")
                    
                    bigquery_ref = format_bigquery_reference(
                        gcp_project_id=gcp_project_id,
                        gcp_dataset_id=gcp_dataset_id,
                        gcp_table_id=gcp_table_id
                    )
                    
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

                    # Generate BigQuery reference from cloudTables data only
                    gcp_project_id = None
                    gcp_dataset_id = None
                    gcp_table_id = None
                    cloud_tables = table.get("cloudTables", {}).get("edges", [])
                    if cloud_tables:
                        cloud_table = cloud_tables[0]["node"]
                        gcp_project_id = cloud_table.get("gcpProjectId")
                        gcp_dataset_id = cloud_table.get("gcpDatasetId")
                        gcp_table_id = cloud_table.get("gcpTableId")
                    
                    bigquery_ref = format_bigquery_reference(
                        gcp_project_id=gcp_project_id,
                        gcp_dataset_id=gcp_dataset_id,
                        gcp_table_id=gcp_table_id
                    )

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
