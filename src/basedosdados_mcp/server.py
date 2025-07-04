from typing import Optional
from mcp.server.fastmcp import FastMCP
from basedosdados_mcp.graphql_client import make_graphql_request, DATASET_OVERVIEW_QUERY, TABLE_DETAILS_QUERY, ENHANCED_SEARCH_QUERY
from basedosdados_mcp.utils import clean_graphql_id, preprocess_search_query, rank_search_results

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
    
    # Enhanced search with preprocessing and fallback strategies
    processed_query, fallback_keywords = preprocess_search_query(query)
    
    all_datasets = []
    seen_ids = set()
    search_attempts = []
    
    # Strategy 1: Enhanced search with comprehensive information
    try:
        variables = {"first": limit, "query": processed_query}
        result = await make_graphql_request(ENHANCED_SEARCH_QUERY, variables)
        
        if result.get("data", {}).get("allDataset", {}).get("edges"):
            for edge in result["data"]["allDataset"]["edges"]:
                node = edge["node"]
                if node["id"] not in seen_ids:
                    seen_ids.add(node["id"])
                    all_datasets.append(edge)
            search_attempts.append(f"Enhanced search: {len(all_datasets)} results")
    except Exception as e:
        search_attempts.append(f"Enhanced search failed: {str(e)}")
    
    # Strategy 2: Slug search for exact matches (highest priority for acronyms)
    if len(all_datasets) < 3 and processed_query and len(processed_query.strip()) <= 10:
        try:
            slug_query = """
            query SearchBySlug($slug: String, $first: Int) {
                allDataset(slug: $slug, first: $first) {
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
                                        columns {
                                            edges {
                                                node {
                                                    id
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """
            variables = {"slug": processed_query.lower(), "first": 1}
            slug_result = await make_graphql_request(slug_query, variables)
            
            if slug_result.get("data", {}).get("allDataset", {}).get("edges"):
                initial_count = len(all_datasets)
                for edge in slug_result["data"]["allDataset"]["edges"]:
                    node = edge["node"]
                    if node["id"] not in seen_ids:
                        seen_ids.add(node["id"])
                        all_datasets.insert(0, edge)
                if len(all_datasets) > initial_count:
                    search_attempts.append(f"Slug search: +{len(all_datasets) - initial_count} (prioritized)")
        except Exception as e:
            search_attempts.append(f"Slug search failed: {str(e)}")
    
    # Strategy 3: Fallback keyword searches
    if len(all_datasets) < max(5, limit // 4) and fallback_keywords:
        for keyword in fallback_keywords[:2]:
            if len(all_datasets) >= limit:
                break
            
            try:
                graphql_query_desc = """
                query SearchDatasetsByDescription($query: String, $first: Int) {
                    allDataset(
                        description_Icontains: $query,
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
                variables = {"first": min(10, limit - len(all_datasets)), "query": keyword}
                keyword_result = await make_graphql_request(graphql_query_desc, variables)
                
                if keyword_result.get("data", {}).get("allDataset", {}).get("edges"):
                    initial_count = len(all_datasets)
                    for edge in keyword_result["data"]["allDataset"]["edges"]:
                        node = edge["node"]
                        if node["id"] not in seen_ids:
                            seen_ids.add(node["id"])
                            all_datasets.append(edge)
                            if len(all_datasets) >= limit:
                                break
                    if len(all_datasets) > initial_count:
                        search_attempts.append(f"Keyword '{keyword}': +{len(all_datasets) - initial_count}")
            except Exception as e:
                search_attempts.append(f"Keyword '{keyword}' failed: {str(e)}")
    
    # Process all collected datasets
    datasets = []
    if all_datasets:
        for edge in all_datasets:
            node = edge["node"]
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
                # Calculate table and column counts
                tables = node.get("tables", {}).get("edges", [])
                table_count = len(tables)
                total_columns = sum(len(table["node"].get("columns", {}).get("edges", [])) for table in tables)
                
                # Get sample table names
                sample_tables = [table["node"]["name"] for table in tables[:3]]
                if len(tables) > 3:
                    sample_tables.append(f"... and {len(tables) - 3} more")
                
                # Generate a sample BigQuery reference if we have tables
                sample_bigquery_ref = ""
                if tables:
                    dataset_slug = node.get("slug", "")
                    first_table_slug = tables[0]["node"].get("slug", "")
                    sample_bigquery_ref = f"basedosdados.{dataset_slug}.{first_table_slug}"
                
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
    
    # Apply intelligent ranking to improve result relevance
    if datasets:
        datasets = rank_search_results(query, datasets)
    
    # Build response
    response = ""
    debug_info = ""
    if search_attempts:
        debug_info += f"\n\n**Search Debug:** {'; '.join(search_attempts)}"
    if processed_query != query:
        debug_info += f"\n**Query Processing:** \"{query}\" → \"{processed_query}\""
    if fallback_keywords:
        debug_info += f"\n**Fallback Keywords:** {', '.join(fallback_keywords)}"
    
    if debug_info:
        response += debug_info + "\n\n"

    if datasets:
        response += f"Found {len(datasets)} datasets:\n\n"
        for ds in datasets:
            response += f"**{ds['name']}** (ID: {ds['id']}, Slug: {ds['slug']})\n"
            
            if ds['table_count'] > 0:
                response += f"📊 **Data:** {ds['table_count']} tables, {ds['total_columns']} total columns\n"
                if ds['sample_bigquery_ref']:
                    response += f"🔗 **Sample Access:** `{ds['sample_bigquery_ref']}`\n"
            else:
                response += "📊 **Data:** No tables available\n"
            
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
        response += f"- Access data using BigQuery references like: `{sample_ref}`"
    else:
        response += "No datasets found."
    
    return response


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
                    bigquery_ref = f"basedosdados.{dataset_slug}.{table_slug}"
                    
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
                
                sample_ref = tables_info[0]['bigquery_reference'] if tables_info else 'basedosdados.dataset.table'
                response += f"\n\n**🔍 Next Steps:**\n"
                response += f"- Use `get_table_details` with a table ID to see all columns and types with sample SQL queries\n"
                response += f"- Access data in BigQuery using the table references above (e.g., `SELECT * FROM {sample_ref} LIMIT 100`)\n"
                
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
                bigquery_ref = f"basedosdados.{dataset_slug}.{table_slug}"
                
                response = f"**📋 Table Details: {table['name']}**\n\n"
                response += f"**Basic Information:**\n"
                response += f"- **Table ID:** {table['id']}\n"
                response += f"- **Table Slug:** {table_slug}\n"
                response += f"- **Description:** {table.get('description', 'No description available')}\n"
                response += f"- **BigQuery Reference:** `{bigquery_ref}`\n\n"
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
                response += f"SELECT {sample_columns}\n"
                response += f"FROM `{bigquery_ref}`\n"
                response += f"LIMIT 100\n"
                response += f"```\n\n"
                response += f"**Full Table Schema:**\n"
                response += f"```sql\n"
                response += f"SELECT *\n"
                response += f"FROM `{bigquery_ref}`\n"
                response += f"LIMIT 10\n"
                response += f"```\n\n"
                response += f"**Column Info:**\n"
                response += f"```sql\n"
                response += f"SELECT column_name, data_type, description\n"
                response += f"FROM `{dataset_slug}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS\n"
                response += f"WHERE table_name = '{table_slug}'\n"
                response += f"```\n\n"
                response += f"**🚀 Access Instructions:**\n"
                response += f"1. Use the BigQuery reference: `{bigquery_ref}`\n"
                response += f"2. Run queries in Google BigQuery console\n"
                response += f"3. Or use the Base dos Dados Python package: `bd.read_table('{dataset_slug}', '{table_slug}')`\n"
                
                return response
            else:
                return "Table not found"
        else:
            return "Table not found"
            
    except Exception as e:
        return f"Error getting table details: {str(e)}"



# =============================================================================
# Server Entry Point
# =============================================================================

if __name__ == "__main__":
    mcp.run()