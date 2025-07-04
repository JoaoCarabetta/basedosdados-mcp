from typing import Optional
from mcp.server.fastmcp import FastMCP
from basedosdados_mcp.graphql_client import make_graphql_request, DATASET_OVERVIEW_QUERY, TABLE_DETAILS_QUERY, ENHANCED_SEARCH_QUERY
from basedosdados_mcp.utils import (
    clean_graphql_id, format_bigquery_reference, format_bigquery_reference_with_highlighting, format_sql_query_with_reference
)
from basedosdados_mcp.bigquery_client import (
    execute_query, execute_simple_query, get_table_schema, get_table_info,
    validate_query, format_query_results, BigQueryClient
)

# =============================================================================
# FastMCP Server Initialization
# =============================================================================

# Initialize the FastMCP server
mcp = FastMCP("Base dos Dados MCP")

# =============================================================================
# MCP Tools using FastMCP Decorators
# =============================================================================

@mcp.tool()
async def search_datasets(
    query: str,
    theme: Optional[str] = None,
    organization: Optional[str] = None,
    limit: int = 10
) -> str:
    """Search for datasets with comprehensive information including table and column counts"""
    
    try:
        # Simple direct search
        variables = {"first": limit, "query": query}
        result = await make_graphql_request(ENHANCED_SEARCH_QUERY, variables)
        
        datasets = []
        if result.get("data", {}).get("allDataset", {}).get("edges"):
            for edge in result["data"]["allDataset"]["edges"]:
                node = edge["node"]
                
                # Extract basic info
                org_names = [org["node"]["name"] for org in node.get("organizations", {}).get("edges", [])]
                theme_names = [t["node"]["name"] for t in node.get("themes", {}).get("edges", [])]
                tag_names = [t["node"]["name"] for t in node.get("tags", {}).get("edges", [])]
                
                # Simple filtering
                if theme and theme.lower() not in [t.lower() for t in theme_names]:
                    continue
                if organization and organization.lower() not in [org.lower() for org in org_names]:
                    continue
                
                # Calculate table info
                tables = node.get("tables", {}).get("edges", [])
                table_count = len(tables)
                
                # Skip datasets with no tables
                if table_count == 0:
                    continue
                    
                total_columns = sum(len(table["node"].get("columns", {}).get("edges", [])) for table in tables)
                
                # Get sample table names
                sample_tables = [table["node"]["name"] for table in tables[:3]]
                if len(tables) > 3:
                    sample_tables.append(f"... and {len(tables) - 3} more")
                
                # Generate BigQuery reference
                sample_bigquery_ref = ""
                if tables:
                    dataset_slug = node.get("slug", "")
                    first_table_slug = tables[0]["node"].get("slug", "")
                    sample_bigquery_ref = format_bigquery_reference(dataset_slug, first_table_slug)
                
                datasets.append({
                    "id": node["id"],
                    "name": node["name"],
                    "slug": node.get("slug", ""),
                    "description": node.get("description", ""),
                    "organizations": ", ".join(org_names),
                    "themes": theme_names,
                    "tags": tag_names,
                    "table_count": table_count,
                    "total_columns": total_columns,
                    "sample_tables": sample_tables,
                    "sample_bigquery_ref": sample_bigquery_ref
                })
        
        # Build response
        response = ""
        if datasets:
            response += f"Found {len(datasets)} datasets with tables:\n\n"
            response += f"**💡 BigQuery Format:** `basedosdados.dataset_slug.table_slug` (e.g., `basedosdados.br_abrinq_oca.municipio_primeira_infancia`)\n\n"
            
            for ds in datasets:
                response += f"**{ds['name']}** (ID: {ds['id']}, Slug: {ds['slug']})\n"
                
                response += f"📊 **Data:** {ds['table_count']} tables, {ds['total_columns']} total columns\n"
                if ds['sample_bigquery_ref']:
                    response += f"🔗 **BigQuery Access:** `{ds['sample_bigquery_ref']}`\n"
                    response += f"   💡 **Copy & Use:** `SELECT * FROM `{ds['sample_bigquery_ref']}` LIMIT 100`\n"
                
                if ds['sample_tables']:
                    response += f"📋 **Tables:** {', '.join(ds['sample_tables'])}\n"
                
                response += f"**Description:** {ds['description']}\n"
                if ds['organizations']:
                    response += f"**Organizations:** {ds['organizations']}\n"
                if ds['themes']:
                    response += f"**Themes:** {', '.join(ds['themes'])}\n"
                if ds['tags']:
                    response += f"**Tags:** {', '.join(ds['tags'])}\n"
                response += "\n"
            
            sample_ref = datasets[0]['sample_bigquery_ref'] if datasets[0]['sample_bigquery_ref'] else 'basedosdados.dataset.table'
            response += f"\n💡 **Next Steps:**\n"
            response += f"- Use `get_dataset_overview` with a dataset ID to see all tables and columns\n"
            response += f"- Use `get_table_details` with a table ID for complete column information and sample SQL\n"
            response += f"- **Ready-to-use BigQuery reference:** `{sample_ref}`"
        else:
            response += "No datasets found."
        
        return response
        
    except Exception as e:
        return f"Error searching datasets: {str(e)}"


@mcp.tool()
async def get_dataset_overview(dataset_id: str) -> str:
    """Get complete dataset overview including all tables with columns, descriptions, and ready-to-use BigQuery table references"""
    
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
                
                return response
            else:
                return "Dataset not found"
        else:
            return "Dataset not found"
            
    except Exception as e:
        return f"Error getting dataset overview: {str(e)}"


@mcp.tool()
async def get_table_details(table_id: str) -> str:
    """Get comprehensive table information with all columns, types, descriptions, and BigQuery access instructions"""
    
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
                
                return response
            else:
                return "Table not found"
        else:
            return "Table not found"
            
    except Exception as e:
        return f"Error getting table details: {str(e)}"


@mcp.tool()
async def execute_bigquery_sql(
    query: str,
    max_results: int = 1000,
    timeout_seconds: int = 300
) -> str:
    """
    Execute a SQL query directly on Base dos Dados in BigQuery.
    Only SELECT queries on basedosdados.* tables are allowed.
    """
    is_valid, error = validate_query(query)
    if not is_valid:
        return f"❌ Query rejected: {error}"

    results = await execute_query(query, max_results=max_results, timeout_seconds=timeout_seconds)
    return format_query_results(results)


@mcp.tool()
async def check_bigquery_status() -> str:
    """
    Check BigQuery authentication status and configuration.
    Returns detailed information about the current setup.
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
    
    return response


# =============================================================================
# Server Entry Point
# =============================================================================

if __name__ == "__main__":
    mcp.run()