from typing import Optional

# =============================================================================
# Utility Functions
# =============================================================================

def clean_graphql_id(graphql_id: Optional[str]) -> str:
    """
    Clean GraphQL node IDs to extract pure UUIDs.
    
    The API returns IDs like 'DatasetNode:uuid', 'TableNode:uuid', 'ColumnNode:uuid'
    but expects pure UUIDs for queries.
    
    Args:
        graphql_id: GraphQL node ID (e.g., 'DatasetNode:d30222ad-7a5c-4778-a1ec-f0785371d1ca')
        
    Returns:
        Pure UUID string (e.g., 'd30222ad-7a5c-4778-a1ec-f0785371d1ca')
        
    Raises:
        ValueError: If graphql_id is None or empty
    """
    if not graphql_id:
        raise ValueError("GraphQL ID cannot be None or empty")
    
    if ':' in graphql_id:
        return graphql_id.split(':', 1)[1]
    return graphql_id


def format_bigquery_reference(dataset_slug: str, table_slug: str, organization_slug: str = None) -> str:
    """
    Format a BigQuery table reference with consistent formatting.
    
    Base dos Dados uses the pattern: basedosdados.{country}_{organization}_{dataset}.{table}
    
    Args:
        dataset_slug: Dataset slug (e.g., 'populacao')
        table_slug: Table slug (e.g., 'brasil')
        organization_slug: Organization slug (e.g., 'ibge')
        
    Returns:
        Formatted BigQuery reference (e.g., 'basedosdados.br_ibge_populacao.brasil')
    """
    if organization_slug:
        # Use the correct pattern: br_{organization}_{dataset}
        bigquery_dataset_id = f"br_{organization_slug}_{dataset_slug}"
        return f"basedosdados.{bigquery_dataset_id}.{table_slug}"
    else:
        # Fallback to old pattern if organization not available
        return f"basedosdados.{dataset_slug}.{table_slug}"


def format_bigquery_reference_with_highlighting(dataset_slug: str, table_slug: str, organization_slug: str = None) -> str:
    """
    Format a BigQuery table reference with enhanced highlighting for display.
    
    Args:
        dataset_slug: Dataset slug (e.g., 'populacao')
        table_slug: Table slug (e.g., 'brasil')
        organization_slug: Organization slug (e.g., 'ibge')
        
    Returns:
        Formatted BigQuery reference with highlighting (e.g., '`basedosdados.br_ibge_populacao.brasil`')
    """
    ref = format_bigquery_reference(dataset_slug, table_slug, organization_slug)
    return f"`{ref}`"


def format_sql_query_with_reference(table_reference: str, columns: str = "*", limit: int = 100) -> str:
    """
    Format a complete SQL query using a BigQuery table reference.
    
    Args:
        table_reference: BigQuery table reference (e.g., 'basedosdados.br_abrinq_oca.municipio_primeira_infancia')
        columns: Columns to select (default: "*")
        limit: Row limit (default: 100)
        
    Returns:
        Formatted SQL query
    """
    return f"SELECT {columns}\nFROM `{table_reference}`\nLIMIT {limit}"
