"""
BigQuery Client for Base dos Dados MCP Server.

This module provides BigQuery integration for executing SQL queries
against Base dos Dados datasets in Google BigQuery.
"""

import os
import logging
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from google.cloud import bigquery
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError

logger = logging.getLogger(__name__)

# =============================================================================
# UTF-8 Encoding Configuration
# =============================================================================

# Ensure proper UTF-8 encoding for all output
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')
os.environ.setdefault('LC_ALL', 'en_US.UTF-8')
os.environ.setdefault('LANG', 'en_US.UTF-8')

def ensure_utf8_response(response: str) -> str:
    """
    Ensure the response is properly UTF-8 encoded.
    
    Args:
        response: The response string to encode
        
    Returns:
        Properly encoded UTF-8 string
    """
    if isinstance(response, bytes):
        return response.decode('utf-8')
    elif isinstance(response, str):
        # Ensure proper encoding by encoding and decoding
        return response.encode('utf-8').decode('utf-8')
    else:
        return str(response)

# =============================================================================
# BigQuery Client Configuration
# =============================================================================

def get_bigquery_config() -> Dict[str, Any]:
    """Get BigQuery configuration from environment variables."""
    return {
        "project_id": os.getenv("BIGQUERY_PROJECT_ID"),
        "location": os.getenv("BIGQUERY_LOCATION", "US"),
        "credentials_file": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        "enabled": bool(os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.getenv("BIGQUERY_PROJECT_ID"))
    }

class BigQueryClient:
    """Client for executing BigQuery queries against Base dos Dados datasets."""
    
    def __init__(self):
        """Initialize BigQuery client with authentication."""
        self.client = None
        self.project_id = None
        self._initialize_client()
    
    def _initialize_client(self) -> None:
        """Initialize BigQuery client with proper authentication."""
        config = get_bigquery_config()
        
        if config["enabled"]:
            # Use explicit configuration from environment
            try:
                self.project_id = config["project_id"]
                
                # Initialize BigQuery client
                self.client = bigquery.Client(project=self.project_id)
                
                logger.info(f"BigQuery client initialized with config: {self.project_id}")
                return
            except Exception as e:
                logger.error(f"Error initializing BigQuery with config: {e}")
        
        # Fallback to default credentials
        try:
            credentials, project = default()
            self.project_id = project
            
            self.client = bigquery.Client(
                credentials=credentials,
                project=project
            )
            
            logger.info(f"BigQuery client initialized with default credentials: {project}")
            
        except DefaultCredentialsError:
            logger.warning("No BigQuery credentials found. BigQuery functionality will be limited.")
            self.client = None
            self.project_id = None
        except Exception as e:
            logger.error(f"Error initializing BigQuery client: {e}")
            self.client = None
            self.project_id = None
    
    def is_available(self) -> bool:
        """Check if BigQuery client is available and authenticated."""
        return self.client is not None
    
    def get_auth_status(self) -> Dict[str, Any]:
        """Get authentication status and project information."""
        config = get_bigquery_config()
        
        if not self.is_available():
            if config["enabled"]:
                return {
                    "authenticated": False,
                    "error": "BigQuery config found but authentication failed",
                    "config": config,
                    "instructions": [
                        "1. Verifique se o arquivo de credenciais existe e é válido",
                        "2. Verifique se o project_id está correto",
                        "3. Execute: gcloud auth application-default login (alternativa)"
                    ]
                }
            else:
                return {
                    "authenticated": False,
                    "error": "No BigQuery configuration found",
                    "config": config,
                    "instructions": [
                        "1. Configure as variáveis de ambiente:",
                        "   - GOOGLE_APPLICATION_CREDENTIALS",
                        "   - BIGQUERY_PROJECT_ID",
                        "2. Ou execute: gcloud auth application-default login"
                    ]
                }
        
        return {
            "authenticated": True,
            "project_id": self.project_id,
            "client_available": True,
            "config": config
        }

# =============================================================================
# Query Execution Functions
# =============================================================================

async def execute_query(
    query: str,
    max_results: int = 1000,
    timeout_seconds: int = 300
) -> Dict[str, Any]:
    """
    Execute a BigQuery SQL query and return results.
    
    Args:
        query: SQL query to execute
        max_results: Maximum number of rows to return
        timeout_seconds: Query timeout in seconds
        
    Returns:
        Dict containing query results, metadata, and execution info
    """
    client = BigQueryClient()
    
    if not client.is_available():
        auth_status = client.get_auth_status()
        return {
            "success": False,
            "error": "BigQuery not available",
            "auth_status": auth_status,
            "query": query
        }
    
    try:
        # Configure query job
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=10 * 1024 * 1024 * 1024,  # 10GB limit
            use_query_cache=True
        )
        
        # Execute query
        query_job = client.client.query(
            query,
            job_config=job_config
        )
        
        # Wait for completion with timeout
        query_job.result(timeout=timeout_seconds)
        
        # Get results as native BigQuery rows
        results = []
        columns = []
        
        # Fetch results and get column names from first row
        try:
            # Get results iterator
            results_iterator = query_job.result(max_results=max_results)
            
            # Process each row
            for row in results_iterator:
                if not columns:  # Get column names from first row
                    columns = list(row.keys())
                results.append(dict(row.items()))
                
        except Exception as e:
            logger.warning(f"Error fetching results: {e}")
            # If no results, try to get columns from the job's destination table schema
            try:
                if hasattr(query_job, 'destination') and query_job.destination:
                    table = client.client.get_table(query_job.destination)
                    columns = [field.name for field in table.schema]
            except Exception as schema_error:
                logger.warning(f"Error getting schema: {schema_error}")
                columns = []
        
        # Get query metadata (handle cases where attributes might not exist)
        try:
            total_rows = getattr(query_job, 'total_rows', 0)
            total_bytes_processed = getattr(query_job, 'total_bytes_processed', 0)
            slot_millis = getattr(query_job, 'slot_millis', 0)
        except Exception as e:
            logger.warning(f"Error getting query metadata: {e}")
            total_rows = len(results)
            total_bytes_processed = 0
            slot_millis = 0
        
        return {
            "success": True,
            "results": results,
            "total_rows": total_rows,
            "returned_rows": len(results),
            "total_bytes_processed": total_bytes_processed,
            "slot_millis": slot_millis,
            "query": query,
            "columns": columns
        }
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        return {
            "success": False,
            "error": str(e),
            "query": query
        }

async def execute_simple_query(
    table_reference: str,
    columns: Optional[List[str]] = None,
    limit: int = 100,
    where_clause: Optional[str] = None
) -> Dict[str, Any]:
    """
    Execute a simple SELECT query on a Base dos Dados table.
    
    Args:
        table_reference: Full table reference (e.g., 'basedosdados.br_ibge_populacao.municipio')
        columns: List of columns to select (None for all columns)
        limit: Maximum number of rows to return
        where_clause: Optional WHERE clause (without 'WHERE' keyword)
        
    Returns:
        Dict containing query results and metadata
    """
    # Build SELECT clause
    if columns:
        select_clause = ", ".join(columns)
    else:
        select_clause = "*"
    
    # Build WHERE clause
    where_sql = ""
    if where_clause:
        where_sql = f" WHERE {where_clause}"
    
    # Build complete query
    query = f"""
    SELECT {select_clause}
    FROM `{table_reference}`
    {where_sql}
    LIMIT {limit}
    """
    
    return await execute_query(query, max_results=limit)

async def get_table_schema(table_reference: str) -> Dict[str, Any]:
    """
    Get schema information for a Base dos Dados table.
    
    Args:
        table_reference: Full table reference
        
    Returns:
        Dict containing table schema information
    """
    query = f"""
    SELECT 
        column_name,
        data_type,
        is_nullable,
        description
    FROM `{table_reference.split('.')[0]}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    WHERE table_name = '{table_reference.split('.')[-1]}'
    ORDER BY ordinal_position
    """
    
    return await execute_query(query, max_results=1000)

async def get_table_info(table_reference: str) -> Dict[str, Any]:
    """
    Get basic information about a table including row count and size.
    
    Args:
        table_reference: Full table reference
        
    Returns:
        Dict containing table information
    """
    query = f"""
    SELECT 
        COUNT(*) as row_count,
        SUM(CAST(TO_JSON_STRING(_FILE_NAME) AS STRING)) as file_count
    FROM `{table_reference}`
    """
    
    return await execute_query(query, max_results=10)

# =============================================================================
# Query Validation and Safety
# =============================================================================

def validate_query(query: str) -> Tuple[bool, Optional[str]]:
    """
    Validate a SQL query for safety and compatibility.
    
    Args:
        query: SQL query to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Convert to uppercase for easier checking
    query_upper = query.upper()
    
    # Check for dangerous operations
    dangerous_keywords = [
        'DROP', 'DELETE', 'UPDATE', 'INSERT', 'CREATE', 'ALTER', 'TRUNCATE',
        'GRANT', 'REVOKE', 'EXECUTE', 'EXEC'
    ]
    
    for keyword in dangerous_keywords:
        if keyword in query_upper:
            return False, f"Query contains forbidden keyword: {keyword}"
    
    # Check for Base dos Dados table references
    if 'BASEDOSDADOS.' not in query_upper:
        return False, "Query must reference Base dos Dados tables (basedosdados.*)"
    
    # Basic syntax check
    if not query_upper.strip().startswith('SELECT'):
        return False, "Only SELECT queries are allowed"
    
    return True, None

def format_query_results(results: Dict[str, Any]) -> str:
    """
    Format query results into a readable string.
    
    Args:
        results: Query results dictionary
        
    Returns:
        Formatted string representation of results
    """
    if not results.get("success", False):
        error_msg = results.get("error", "Unknown error")
        return ensure_utf8_response(f"❌ **Query Failed:** {error_msg}")
    
    # Extract data
    data = results.get("results", [])
    columns = results.get("columns", [])
    total_rows = results.get("total_rows", 0)
    returned_rows = results.get("returned_rows", 0)
    total_bytes = results.get("total_bytes_processed", 0)
    
    # Format response
    response = f"✅ **Query Executed Successfully**\n\n"
    response += f"** Results:** {returned_rows} rows returned (of {total_rows} total)\n"
    response += f"**💾 Data Processed:** {total_bytes / (1024*1024):.2f} MB\n"
    response += f"**📋 Columns:** {', '.join(columns)}\n\n"
    
    if data:
        # Show first few rows as example
        response += "** Sample Data:**\n"
        for i, row in enumerate(data[:5]):
            response += f"Row {i+1}: {row}\n"
        
        if len(data) > 5:
            response += f"... and {len(data) - 5} more rows\n"
    else:
        response += "** No data returned**\n"
    
    return ensure_utf8_response(response)