from typing import Any, Dict, Optional
import httpx
import os
import sys
from .config import GRAPHQL_ENDPOINT

# =============================================================================
# UTF-8 Encoding Configuration
# =============================================================================

# Ensure proper UTF-8 encoding for all output
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')
os.environ.setdefault('LC_ALL', 'en_US.UTF-8')
os.environ.setdefault('LANG', 'en_US.UTF-8')

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


def clean_graphql_data(data: dict) -> dict:
    """
    Clean GraphQL response data to handle Unicode escape sequences in nested structures.
    
    Args:
        data: Dictionary containing GraphQL response data
        
    Returns:
        Cleaned dictionary with proper Unicode characters
    """
    if isinstance(data, dict):
        cleaned = {}
        for key, value in data.items():
            cleaned[key] = clean_graphql_data(value)
        return cleaned
    elif isinstance(data, list):
        return [clean_graphql_data(item) for item in data]
    elif isinstance(data, str):
        return ensure_utf8_response(data)
    else:
        return data

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
            
            # Clean the response data to handle Unicode escape sequences
            result = clean_graphql_data(result)
            
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

# More comprehensive search query that searches in multiple fields
COMPREHENSIVE_SEARCH_QUERY = """
query ComprehensiveSearchDatasets($query: String, $first: Int) {
    allDataset(
        name_Icontains: $query,
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
