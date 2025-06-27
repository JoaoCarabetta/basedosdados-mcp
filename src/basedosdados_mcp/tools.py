from typing import Any, Dict, List, Optional
from mcp.types import Tool, TextContent, CallToolResult, ReadResourceResult
from .server import server
from .graphql_client import make_graphql_request, DATASET_OVERVIEW_QUERY, TABLE_DETAILS_QUERY, ENHANCED_SEARCH_QUERY
from .utils import clean_graphql_id, preprocess_search_query, rank_search_results

# =============================================================================
# MCP Tool Definitions
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
            description="Search for datasets with comprehensive information including table and column counts",
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
                        "description": "Maximum number of results (default: 10)",
                        "default": 10,
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_dataset_overview",
            description="Get complete dataset overview including all tables with columns, descriptions, and ready-to-use BigQuery table references (e.g., basedosdados.br_bd_vizinhanca.municipio)",
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
            name="get_table_details",
            description="Get comprehensive table information with all columns, types, descriptions, and BigQuery access instructions",
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
            name="explore_data",
            description="Multi-level data exploration: get dataset overview with top tables and key columns, or table details with sample queries",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_id": {
                        "type": "string",
                        "description": "The UUID of the dataset to explore (optional if table_id provided)",
                    },
                    "table_id": {
                        "type": "string",
                        "description": "The UUID of the table to explore (optional if dataset_id provided)",
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["overview", "detailed", "related"],
                        "description": "Exploration mode: 'overview' for quick summary, 'detailed' for complete info, 'related' for finding similar data",
                        "default": "overview"
                    }
                },
                "required": [],
            },
        ),
    ]

# =============================================================================
# MCP Tool Handlers
# =============================================================================

@server.call_tool()
async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> CallToolResult:
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
        limit = arguments.get("limit", 50)  # Balanced default for performance
        
        # Enhanced GraphQL query to search datasets across multiple fields
        if query:
            # First try description search (most comprehensive)
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
            
            # Secondary search by name if description search yields few results
            graphql_query_name = """
            query SearchDatasetsByName($query: String, $first: Int) {
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
            # Enhanced search with preprocessing and fallback strategies
            processed_query, fallback_keywords = preprocess_search_query(query)
            
            all_datasets = []
            seen_ids = set()
            search_attempts = []
            
            # Strategy 1: Enhanced search with comprehensive information
            try:
                variables = {"first": limit, "query": processed_query}
                result = await make_graphql_request(ENHANCED_SEARCH_QUERY, variables)
                print(f"DEBUG: GraphQL Search Result: {result}") # Added debug print
                
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
            if len(all_datasets) < 3 and processed_query and len(processed_query.strip()) <= 10:  # Likely acronym
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
                                all_datasets.insert(0, edge)  # Insert at beginning for highest priority
                        if len(all_datasets) > initial_count:
                            search_attempts.append(f"Slug search: +{len(all_datasets) - initial_count} (prioritized)")
                except Exception as e:
                    search_attempts.append(f"Slug search failed: {str(e)}")
            
            # Strategy 4: Fallback keyword searches (if main search failed or returned few results)
            if len(all_datasets) < max(5, limit // 4) and fallback_keywords:
                for keyword in fallback_keywords[:2]:  # Try top 2 keywords only to avoid timeout
                    if len(all_datasets) >= limit:
                        break
                    
                    try:
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
            
            # Strategy 5: If original query was different from processed, try original as fallback
            if len(all_datasets) < max(3, limit // 5) and query != processed_query and query.strip():
                try:
                    variables = {"first": min(10, limit - len(all_datasets)), "query": query.strip()}
                    original_result = await make_graphql_request(graphql_query_desc, variables)
                    
                    if original_result.get("data", {}).get("allDataset", {}).get("edges"):
                        initial_count = len(all_datasets)
                        for edge in original_result["data"]["allDataset"]["edges"]:
                            node = edge["node"]
                            if node["id"] not in seen_ids:
                                seen_ids.add(node["id"])
                                all_datasets.append(edge)
                                if len(all_datasets) >= limit:
                                    break
                        if len(all_datasets) > initial_count:
                            search_attempts.append(f"Original query fallback: +{len(all_datasets) - initial_count}")
                except Exception as e:
                    search_attempts.append(f"Original query fallback failed: {str(e)}")
            
            # Process all collected datasets
            datasets = []
            if all_datasets:
                for edge in all_datasets:
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
            
            # Build response content as a list of TextContent objects
            response_content = []
            debug_info_text = ""
            if search_attempts:
                debug_info_text += f"\n\n**Search Debug:** {'; '.join(search_attempts)}"
            if processed_query != query:
                debug_info_text += f"\n**Query Processing:** \"{query}\" \u2192 \"{processed_query}\""
            if fallback_keywords:
                debug_info_text += f"\n**Fallback Keywords:** {', '.join(fallback_keywords)}"
            
            if debug_info_text:
                response_content.append(TextContent(type="text", text=debug_info_text))

            if datasets:
                response_content.append(TextContent(type="text", text=f"Found {len(datasets)} datasets:"))
                for ds in datasets:
                    basic_info = f"**{ds['name']}** (ID: {ds['id']}, Slug: {ds['slug']})"
                    response_content.append(TextContent(type="text", text=basic_info))
                    
                    if ds['table_count'] > 0:
                        structure_info = f"📊 **Data:** {ds['table_count']} tables, {ds['total_columns']} total columns"
                        if ds['sample_bigquery_ref']:
                            structure_info += f"\n🔗 **Sample Access:** `{ds['sample_bigquery_ref']}`"
                        response_content.append(TextContent(type="text", text=structure_info))
                    else:
                        response_content.append(TextContent(type="text", text="📊 **Data:** No tables available"))
                    
                    if ds['sample_tables']:
                        tables_info = f"📋 **Tables:** {', '.join(ds['sample_tables'])}"
                        response_content.append(TextContent(type="text", text=tables_info))
                    
                    metadata = f"**Description:** {ds['description']}"
                    if ds['organizations']:
                        metadata += f"\n**Organizations:** {ds['organizations']}"
                    if ds['themes']:
                        metadata += f"\n**Themes:** {', '.join(ds['themes'])}"
                    if ds['tags']:
                        metadata += f"\n**Tags:** {', '.join(ds['tags'])}"
                    response_content.append(TextContent(type="text", text=metadata))
                
                usage_tips = f"""\n\n💡 **Next Steps:**\n- Use `get_dataset_overview` with a dataset ID to see all tables and columns\n- Use `get_table_details` with a table ID for complete column information and sample SQL\n- Access data using BigQuery references like: `{datasets[0]['sample_bigquery_ref'] if datasets[0]['sample_bigquery_ref'] else 'basedosdados.dataset.table'}`"""
                response_content.append(TextContent(type="text", text=usage_tips))
            else:
                response_content.append(TextContent(type="text", text="No datasets found."))
            
            return CallToolResult(content=response_content)
            
        except Exception as e:
            return CallToolResult(content=[TextContent(type="text", text=f"Error searching datasets: {str(e)}")], isError=True)
    
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
                    
                    info = f"""**Dataset Information**\nName: {dataset['name']}\nID: {dataset['id']}\nSlug: {dataset.get('slug', '')}\nDescription: {dataset.get('description', 'No description available')}\nOrganizations: {', '.join(org_names)}\nThemes: {', '.join([t['node']['name'] for t in dataset.get('themes', {}).get('edges', [])])}\nTags: {', '.join([t['node']['name'] for t in dataset.get('tags', {}).get('edges', [])])}\n\n**Tables in this dataset:**\n"""
                    for edge in dataset.get("tables", {}).get("edges", []):
                        table = edge["node"]
                        info += f"- {table['name']} (ID: {table['id']}, Slug: {table.get('slug', '')}): {table.get('description', 'No description')}\n"
                    
                    return CallToolResult(content=[TextContent(type="text", text=info)])
                else:
                    return CallToolResult(content=[TextContent(type="text", text="Dataset not found")])
            else:
                return CallToolResult(content=[TextContent(type="text", text="Dataset not found")])
                
        except Exception as e:
            return CallToolResult(content=[TextContent(type="text", text=f"Error getting dataset info: {str(e)}")], isError=True)
    
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
                    
                    return CallToolResult(content=[
                        TextContent(
                            type="text",
                            text=f"**Tables in dataset '{dataset['name']}':**\n\n" +
                                 "\n".join([
                                     f"• **{table['name']}** (ID: {table['id']}, Slug: {table['slug']})\n"
                                     f"  {table['description']}"
                                     for table in tables
                                 ])
                        )
                    ])
                else:
                    return CallToolResult(content=[TextContent(type="text", text="Dataset not found")])
            else:
                return CallToolResult(content=[TextContent(type="text", text="Dataset not found")])
                
        except Exception as e:
            return CallToolResult(content=[TextContent(type="text", text=f"Error listing tables: {str(e)}")], isError=True)
    
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
                    
                    info = f"""**Table Information**\nName: {table['name']}\nID: {table['id']}\nSlug: {table.get('slug', '')}\nDescription: {table.get('description', 'No description available')}\n\n**Dataset:**\n{dataset['name']} (ID: {dataset['id']}, Slug: {dataset.get('slug', '')})\n\n**Columns:**\n"""
                    
                    for edge in table.get("columns", {}).get("edges", []):
                        column = edge["node"]
                        bigquery_type = column.get("bigqueryType", {}).get("name", "Unknown")
                        info += f"• {column['name']} ({bigquery_type})\n"
                        if column.get("description"):
                            info += f"  {column['description']}\n"
                    
                    return CallToolResult(content=[TextContent(type="text", text=info)])
                else:
                    return CallToolResult(content=[TextContent(type="text", text="Table not found")])
            else:
                return CallToolResult(content=[TextContent(type="text", text="Table not found")])
                
        except Exception as e:
            return CallToolResult(content=[TextContent(type="text", text=f"Error getting table info: {str(e)}")], isError=True)
    
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
                    
                    return CallToolResult(content=[
                        TextContent(
                            type="text",
                            text=f"**Columns in table '{table['name']}':**\n\n" +
                                 "\n".join([
                                     f"• **{col['name']}** ({col['type']}) - ID: {col['id']}\n"
                                     f"  {col['description']}"
                                     for col in columns
                                 ])
                        )
                    ])
                else:
                    return CallToolResult(content=[TextContent(type="text", text="Table not found")])
            else:
                return CallToolResult(content=[TextContent(type="text", text="Table not found")])
                
        except Exception as e:
            return CallToolResult(content=[TextContent(type="text", text=f"Error listing columns: {str(e)}")], isError=True)
    
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
                    
                    info = f"""**Column Information**\nName: {column['name']}\nID: {column['id']}\nType: {bigquery_type}\nDescription: {column.get('description', 'No description available')}\n\n**Table:**\n{table['name']} (ID: {table['id']}, Slug: {table.get('slug', '')})\n\n**Dataset:**\n{dataset['name']} (ID: {dataset['id']}, Slug: {dataset.get('slug', '')})\n"""
                    
                    return CallToolResult(content=[TextContent(type="text", text=info)])
                else:
                    return CallToolResult(content=[TextContent(type="text", text="Column not found")])
            else:
                return CallToolResult(content=[TextContent(type="text", text="Column not found")])
                
        except Exception as e:
            return CallToolResult(content=[TextContent(type="text", text=f"Error getting column info: {str(e)}")], isError=True)
    
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
                    
                    return CallToolResult(content=[
                        TextContent(
                            type="text", 
                            text=f"**Generated SQL Query for {table['name']}:**\n\n```sql\n{sql_query}\n```\n\n"
                                 f"**Usage:** You can run this query in BigQuery or use the Base dos Dados Python package."
                        )
                    ])
                else:
                    return CallToolResult(content=[TextContent(type="text", text="Table not found")])
            else:
                return CallToolResult(content=[TextContent(type="text", text="Table not found")])
                
        except Exception as e:
            return CallToolResult(content=[TextContent(type="text", text=f"Error generating SQL query: {str(e)}")], isError=True)
    
    elif name == "get_dataset_overview":
        dataset_id = clean_graphql_id(arguments.get("dataset_id"))
        
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
                    response = f"""**📊 Dataset Overview: {dataset['name']}**\n\n**Basic Information:**\n- **ID:** {dataset['id']}\n- **Slug:** {dataset.get('slug', '')}\n- **Description:** {dataset.get('description', 'No description available')}\n- **Organizations:** {', '.join(org_names)}\n- **Themes:** {', '.join(theme_names)}\n- **Tags:** {', '.join(tag_names)}\n\n**Data Structure:**\n- **Total Tables:** {len(tables_info)}\n- **Total Columns:** {total_columns}\n\n**📋 Tables with BigQuery Access:**\n"""
                    
                    for table in tables_info:
                        response += f"""\n**{table['name']}** ({table['column_count']} columns)\n- **BigQuery Reference:** `{table['bigquery_reference']}`\n- **Table ID:** {table['id']}\n- **Description:** {table['description']}\n- **Sample Columns:** {', '.join(table['sample_columns'])}\n"""
                    
                    response += f"""\n\n**🔍 Next Steps:**\n- Use `get_table_details` with a table ID to see all columns and types with sample SQL queries\n- Access data in BigQuery using the table references above (e.g., `SELECT * FROM {tables_info[0]['bigquery_reference'] if tables_info else 'basedosdados.dataset.table'} LIMIT 100`)\n"""
                    
                    return CallToolResult(content=[TextContent(type="text", text=response)])
                else:
                    return CallToolResult(content=[TextContent(type="text", text="Dataset not found")])
            else:
                return CallToolResult(content=[TextContent(type="text", text="Dataset not found")])
                
        except Exception as e:
            return CallToolResult(content=[TextContent(type="text", text=f"Error getting dataset overview: {str(e)}")], isError=True)
    
    elif name == "get_table_details":
        table_id = clean_graphql_id(arguments.get("table_id"))
        
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
                    
                    response = f"""**📋 Table Details: {table['name']}**\n\n**Basic Information:**\n- **Table ID:** {table['id']}\n- **Table Slug:** {table_slug}\n- **Description:** {table.get('description', 'No description available')}\n- **BigQuery Reference:** `{bigquery_ref}`\n\n**Dataset Context:**\n- **Dataset:** {dataset['name']} \n- **Dataset ID:** {dataset['id']}\n- **Dataset Slug:** {dataset.get('slug', '')}\n\n**📊 Columns ({len(columns)} total):**\n"""
                    
                    for col_edge in columns:
                        column = col_edge["node"]
                        col_type = column.get("bigqueryType", {}).get("name", "Unknown")
                        col_desc = column.get("description", "No description")
                        response += f"""\n**{column['name']}** ({col_type})\n- ID: {column['id']}\n- Description: {col_desc}\n"""
                    
                    # Generate sample SQL queries
                    column_names = [col["node"]["name"] for col in columns]
                    sample_columns = ", ".join(column_names[:5])
                    if len(column_names) > 5:
                        sample_columns += f", ... -- and {len(column_names) - 5} more"
                    
                    response += f"""\n\n**🔍 Sample SQL Queries:**\n\n**Basic Select:**\n```sql\nSELECT {sample_columns}\nFROM `{bigquery_ref}`\nLIMIT 100\n```\n\n**Full Table Schema:**\n```sql\nSELECT *\nFROM `{bigquery_ref}`\nLIMIT 10\n```\n
**Column Info:**\n```sql\nSELECT column_name, data_type, description\nFROM `{dataset_slug}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS\nWHERE table_name = '{table_slug}'\n```\n\n**🚀 Access Instructions:**\n1. Use the BigQuery reference: `{bigquery_ref}`\n2. Run queries in Google BigQuery console\n3. Or use the Base dos Dados Python package: `bd.read_table('{dataset_slug}', '{table_slug}')`\n"""
                    
                    return CallToolResult(content=[TextContent(type="text", text=response)])
                else:
                    return CallToolResult(content=[TextContent(type="text", text="Table not found")])
            else:
                return CallToolResult(content=[TextContent(type="text", text="Table not found")])
                
        except Exception as e:
            return CallToolResult(content=[TextContent(type="text", text=f"Error getting table details: {str(e)}")], isError=True)
    
    elif name == "explore_data":
        dataset_id = arguments.get("dataset_id")
        table_id = arguments.get("table_id")
        mode = arguments.get("mode", "overview")
        
        if not dataset_id and not table_id:
            return CallToolResult(content=[TextContent(type="text", text="Please provide either dataset_id or table_id")])
        
        try:
            if dataset_id:
                # Explore dataset
                dataset_id = clean_graphql_id(dataset_id)
                result = await make_graphql_request(DATASET_OVERVIEW_QUERY, {"id": dataset_id})
                
                if result.get("data", {}).get("allDataset", {}).get("edges"):
                    dataset = result["data"]["allDataset"]["edges"][0]["node"]
                    tables = dataset.get("tables", {}).get("edges", [])
                    
                    if mode == "overview":
                        # Quick overview with top 3 tables
                        response = f"""**🔍 Dataset Exploration: {dataset['name']}**\n\n**Quick Overview:**\n- **Total Tables:** {len(tables)}\n- **Organizations:** {', '.join([org["node"]["name"] for org in dataset.get("organizations", {}).get("edges", [])])}\n- **Themes:** {', '.join([t["node"]["name"] for t in dataset.get("themes", {}).get("edges", [])])}\n\n**🏆 Top Tables:**\n"""
                        for table_edge in tables[:3]:
                            table = table_edge["node"]
                            col_count = len(table.get("columns", {}).get("edges", []))
                            bigquery_ref = f"basedosdados.{dataset.get('slug', '')}.{table.get('slug', '')}"
                            response += f"- **{table['name']}** ({col_count} cols) - `{bigquery_ref}`\n"
                        
                        if len(tables) > 3:
                            response += f"\n... and {len(tables) - 3} more tables. Use mode='detailed' to see all."
                        
                    elif mode == "detailed":
                        # Use the full dataset overview
                        return await handle_call_tool("get_dataset_overview", {"dataset_id": arguments.get("dataset_id")})
                    
                    return CallToolResult(content=[TextContent(type="text", text=response)])
                else:
                    return CallToolResult(content=[TextContent(type="text", text="Dataset not found")])
            
            elif table_id:
                # Explore table - delegate to get_table_details
                return await handle_call_tool("get_table_details", {"table_id": table_id})
                
        except Exception as e:
            return CallToolResult(content=[TextContent(type="text", text=f"Error exploring data: {str(e)}")], isError=True)
    
    else:
        return CallToolResult(content=[TextContent(type="text", text=f"Unknown tool: {name}")], isError=True)
