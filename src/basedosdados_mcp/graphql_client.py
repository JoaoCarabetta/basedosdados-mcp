from typing import Any, Dict, Optional
import httpx
from .config import GRAPHQL_ENDPOINT

# =============================================================================
# GraphQL API Client
# =============================================================================

async def make_graphql_request(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Make a GraphQL request to the Base dos Dados API.
    
    This function handles communication with the Base dos Dados GraphQL endpoint,
    including error handling for common issues like network timeouts and GraphQL errors.
    
    Args:
        query: GraphQL query string
        variables: Optional variables for the GraphQL query
        
    Returns:
        Dict containing the GraphQL response data
        
    Raises:
        Exception: For various error conditions including:
            - GraphQL validation errors (400 status)
            - Network timeouts (30 second limit)
            - Connection errors
            - Unexpected API responses
            
    Note:
        The API uses Django GraphQL auto-generation, so filter arguments use
        single underscores (e.g., name_Icontains) not double underscores.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                GRAPHQL_ENDPOINT,
                json={"query": query, "variables": variables or {}},
                headers={"Content-Type": "application/json"}
            )
            
            # Handle GraphQL validation errors (common with wrong filter syntax)
            if response.status_code == 400:
                error_data = response.json()
                if "errors" in error_data:
                    error_messages = [err.get("message", "Unknown error") for err in error_data["errors"]]
                    raise Exception(f"GraphQL errors: {'; '.join(error_messages)}")
                else:
                    raise Exception(f"Bad Request (400): {error_data}")
            
            # Raise for other HTTP errors
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
        # Re-raise our custom exceptions without modification
        if "GraphQL errors" in str(e) or "Request timeout" in str(e) or "Network error" in str(e):
            raise
        else:
            raise Exception(f"Unexpected error: {str(e)}")

# =============================================================================
# Enhanced GraphQL Queries for Consolidated Data Fetching
# =============================================================================

# Comprehensive dataset overview query with tables and sample columns
DATASET_OVERVIEW_QUERY = """
query GetDatasetOverview($id: ID!) {
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
        }
    }
}
"""

# Comprehensive table details query with all columns and metadata
TABLE_DETAILS_QUERY = """
query GetTableDetails($id: ID!) {
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

# Enhanced search query with table and column counts
ENHANCED_SEARCH_QUERY = """
query EnhancedSearchDatasets($query: String, $first: Int) {
    allDataset(
        description_Icontains: $query,
        first: $first
    ) {
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
