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


def format_bigquery_reference(
    gcp_project_id: Optional[str] = None,
    gcp_dataset_id: Optional[str] = None,
    gcp_table_id: Optional[str] = None
) -> Optional[str]:
    """
    Format a BigQuery table reference using actual GCP IDs from cloudTables.
    
    Only uses authoritative GCP data - no fallback construction to avoid incorrect references.
    
    Args:
        gcp_project_id: Actual GCP project ID from cloudTables (e.g., 'basedosdados')
        gcp_dataset_id: Actual GCP dataset ID from cloudTables (e.g., 'br_ms_populacao')
        gcp_table_id: Actual GCP table ID from cloudTables (e.g., 'municipio')
        
    Returns:
        Formatted BigQuery reference (e.g., 'basedosdados.br_ms_populacao.municipio') or None if data unavailable
    """
    # Only use actual GCP IDs from cloudTables - no fallback to avoid wrong references
    if gcp_project_id and gcp_dataset_id and gcp_table_id:
        return f"{gcp_project_id}.{gcp_dataset_id}.{gcp_table_id}"
    
    # Return None if we don't have complete GCP data
    return None


def format_bigquery_reference_with_highlighting(
    gcp_project_id: Optional[str] = None,
    gcp_dataset_id: Optional[str] = None,
    gcp_table_id: Optional[str] = None
) -> Optional[str]:
    """
    Format a BigQuery table reference with enhanced highlighting for display.
    
    Args:
        gcp_project_id: Actual GCP project ID from cloudTables (e.g., 'basedosdados')
        gcp_dataset_id: Actual GCP dataset ID from cloudTables (e.g., 'br_ms_populacao')
        gcp_table_id: Actual GCP table ID from cloudTables (e.g., 'municipio')
        
    Returns:
        Formatted BigQuery reference with highlighting (e.g., '`basedosdados.br_ms_populacao.municipio`') or None if data unavailable
    """
    ref = format_bigquery_reference(
        gcp_project_id=gcp_project_id,
        gcp_dataset_id=gcp_dataset_id,
        gcp_table_id=gcp_table_id
    )
    return f"`{ref}`" if ref else None


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
