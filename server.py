#!/usr/bin/env python3
"""
Base dos Dados MCP Server

A Model Context Protocol (MCP) server that provides access to Base dos Dados, Brazil's open data platform.

This server connects to the Base dos Dados GraphQL API to provide metadata about Brazilian public datasets,
including information about datasets, tables, columns, and their relationships. It enables users to:

- Search for datasets by name, theme, or organization
- Get detailed information about specific datasets and tables  
- Generate BigQuery SQL queries for data access
- Browse the complete metadata catalog

GraphQL API Endpoint: https://backend.basedosdados.org/graphql
Base dos Dados Website: https://basedosdados.org

Usage Example:
    # Search for population datasets
    search_datasets(query="população", theme="Demografia")
    
    # Get dataset details
    get_dataset_info(dataset_id="br_ibge_populacao")
    
    # Generate SQL for a table
    generate_sql_query(table_id="municipio", limit=100)

Note: This server provides metadata access only. To query actual data, use the generated
BigQuery SQL statements with appropriate credentials.
"""

# Standard library imports
import asyncio
from typing import Any, Dict, List, Optional

# Third-party imports
import httpx

# MCP server imports
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import ServerCapabilities, Resource, Tool, TextContent

# Pydantic for data models
from pydantic import BaseModel

# =============================================================================
# API Configuration
# =============================================================================

# Base dos Dados GraphQL API endpoint
# This is the backend API that provides metadata about all datasets, tables, and columns
BASE_URL = "https://backend.basedosdados.org"
GRAPHQL_ENDPOINT = f"{BASE_URL}/graphql"

# =============================================================================
# Data Models
# =============================================================================

class DatasetInfo(BaseModel):
    """
    Dataset information model for Base dos Dados.
    
    Represents a collection of related tables containing Brazilian public data,
    typically organized by source organization and theme.
    """
    id: str  # UUID identifier
    name: str  # Human-readable name
    description: Optional[str] = None  # Detailed description
    organization: Optional[str] = None  # Source organization
    themes: List[str] = []  # Thematic categories
    tags: List[str] = []  # Keywords for discovery
    
class TableInfo(BaseModel):
    """
    Table information model for Base dos Dados.
    
    Represents a specific data table within a dataset, containing columns
    and accessible via BigQuery.
    """
    id: str  # UUID identifier
    name: str  # Human-readable name
    description: Optional[str] = None  # Table description
    dataset_id: str  # Parent dataset UUID
    columns: List[str] = []  # Column names

class ColumnInfo(BaseModel):
    """
    Column information model for Base dos Dados.
    
    Represents a data field within a table, with type information
    for BigQuery compatibility.
    """
    id: str  # UUID identifier
    name: str  # Column name
    description: Optional[str] = None  # Column description
    bigquery_type: Optional[str] = None  # BigQuery data type
    table_id: str  # Parent table UUID

# =============================================================================
# MCP Server Initialization
# =============================================================================

# Initialize the MCP server with a descriptive name
server = Server("basedosdados-mcp")


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

def normalize_portuguese_accents(text: str) -> str:
    """
    Normalize Portuguese text by adding common accents that users often omit.
    
    This function handles the most common Portuguese accent patterns where users
    type without accents but the database content contains accented characters.
    
    Args:
        text: Input text that may be missing Portuguese accents
        
    Returns:
        Text with common Portuguese accents added where appropriate
        
    Examples:
        "populacao" -> "população"
        "educacao" -> "educação" 
        "saude" -> "saúde"
        "inflacao" -> "inflação"
        "violencia" -> "violência"
    """
    if not text:
        return text
    
    # Common Portuguese accent patterns (most frequent cases)
    accent_patterns = {
        # ação/são pattern endings
        'cao': 'ção',
        'sao': 'são',
        'nao': 'não',
        
        # Common word-specific patterns
        'populacao': 'população',
        'educacao': 'educação', 
        'saude': 'saúde',
        'inflacao': 'inflação',
        'violencia': 'violência',
        'ciencia': 'ciência',
        'experiencia': 'experiência',
        'situacao': 'situação',
        'informacao': 'informação',
        'comunicacao': 'comunicação',
        'administracao': 'administração',
        'organizacao': 'organização',
        'producao': 'produção',
        'construcao': 'construção',
        'operacao': 'operação',
        'participacao': 'participação',
        'avaliacao': 'avaliação',
        'aplicacao': 'aplicação',
        'investigacao': 'investigação',
        'observacao': 'observação',
        'conservacao': 'conservação',
        'preservacao': 'preservação',
        'transformacao': 'transformação',
        'democratica': 'democrática',
        'economica': 'econômica',
        'publica': 'pública',
        'politica': 'política',
        'historica': 'histórica',
        'geografica': 'geográfica',
        'demografica': 'demográfica',
        'academica': 'acadêmica',
        'medica': 'médica',
        'tecnica': 'técnica',
        'biologica': 'biológica',
        'matematica': 'matemática',
        
        # Other common words
        'familia': 'família',
        'historia': 'história',
        'memoria': 'memória',
        'secretaria': 'secretária',
        'area': 'área',
        'energia': 'energia',
        'materia': 'matéria',
        'territorio': 'território',
        'relatorio': 'relatório',
        'laboratorio': 'laboratório',
        'diretorio': 'diretório',
        'repositorio': 'repositório',
        'brasilia': 'brasília',
        'agua': 'água',
        'orgao': 'órgão',
        'orgaos': 'órgãos',
        'opcao': 'opção',
        'opcoes': 'opções',
        'acao': 'ação',
        'acoes': 'ações',
        'regiao': 'região',
        'regioes': 'regiões',
        'estado': 'estado',  # This one typically doesn't need accents
        'municipio': 'município',
        'municipios': 'municípios',
    }
    
    # Create normalized version by applying pattern substitutions
    normalized = text.lower()
    
    # Apply word-level substitutions (exact matches)
    for unaccented, accented in accent_patterns.items():
        if normalized == unaccented:
            return accented
    
    # Apply pattern-based substitutions for partial matches
    for unaccented, accented in accent_patterns.items():
        if unaccented in normalized:
            normalized = normalized.replace(unaccented, accented)
    
    return normalized

def preprocess_search_query(query: str) -> tuple[str, list[str]]:
    """
    Preprocess search queries to handle complex cases and improve search success.
    
    This function cleans and normalizes search queries, handling:
    - Special characters that may cause GraphQL issues
    - Multi-word phrase processing
    - Portuguese accent normalization
    - Query sanitization
    
    Args:
        query: Raw search query from user
        
    Returns:
        tuple of (processed_main_query, fallback_keywords)
        - processed_main_query: Cleaned query for primary search
        - fallback_keywords: Individual keywords for fallback searches
    """
    if not query or not query.strip():
        return "", []
    
    # Clean and normalize the query
    clean_query = query.strip()
    
    # Remove problematic characters for GraphQL
    # Keep Portuguese characters, letters, numbers, spaces, and common punctuation
    import re
    clean_query = re.sub(r'[^\w\sáàâãéêíóôõúçÁÀÂÃÉÊÍÓÔÕÚÇ\-]', ' ', clean_query)
    
    # Normalize multiple spaces to single space
    clean_query = re.sub(r'\s+', ' ', clean_query).strip()
    
    # Apply Portuguese accent normalization
    normalized_query = normalize_portuguese_accents(clean_query)
    
    # Create fallback keywords by splitting the query
    # Remove common stop words that don't add search value
    stop_words = {'de', 'da', 'do', 'das', 'dos', 'e', 'em', 'na', 'no', 'nas', 'nos', 
                  'para', 'por', 'com', 'sem', 'sobre', 'entre', 'the', 'and', 'or', 'of', 'in', 'to'}
    
    # Split into individual words and filter
    words = [word.strip() for word in normalized_query.split() if len(word.strip()) > 2]
    fallback_keywords = [word for word in words if word.lower() not in stop_words]
    
    # Limit fallback keywords to most relevant (first 3-4 words)
    fallback_keywords = fallback_keywords[:4]
    
    return normalized_query, fallback_keywords

def calculate_search_relevance(query: str, dataset: dict) -> float:
    """
    Calculate relevance score for search results to improve ranking.
    
    This implements a scoring system that prioritizes:
    1. Exact matches (especially for acronyms like RAIS, IBGE)
    2. Name matches over description matches
    3. Complete word matches over partial matches
    4. Important/popular datasets
    
    Args:
        query: Original search query
        dataset: Dataset dictionary with name, description, etc.
        
    Returns:
        Float relevance score (higher = more relevant)
    """
    if not query or not dataset:
        return 0.0
    
    score = 0.0
    query_lower = query.lower().strip()
    name = dataset.get('name', '').lower()
    description = dataset.get('description', '').lower()
    slug = dataset.get('slug', '').lower()
    
    # 1. Exact match bonuses (highest priority)
    if query_lower == name.lower():
        score += 100.0  # Perfect name match
    elif query_lower in name.split():
        score += 80.0   # Exact word in name
    elif query_lower == slug:
        score += 200.0  # Perfect slug match - highest priority!
    
    # Special case: if query matches slug exactly, this should be the top result
    if query_lower == slug.replace('_', ' ') or query_lower == slug:
        score += 250.0  # Maximum priority for slug matches
    
    # 2. Acronym and important dataset bonuses
    important_acronyms = {
        'rais': 'relação anual de informações sociais',
        'ibge': 'instituto brasileiro de geografia e estatística',
        'ipea': 'instituto de pesquisa econômica aplicada',
        'inep': 'instituto nacional de estudos e pesquisas educacionais',
        'tse': 'tribunal superior eleitoral',
        'sus': 'sistema único de saúde',
        'pnad': 'pesquisa nacional por amostra de domicílios',
        'pof': 'pesquisa de orçamentos familiares',
        'censo': 'censo demográfico',
        'caged': 'cadastro geral de empregados e desempregados',
        'sinasc': 'sistema de informações sobre nascidos vivos',
        'sim': 'sistema de informações sobre mortalidade'
    }
    
    if query_lower in important_acronyms:
        expected_name = important_acronyms[query_lower]
        # Check if this dataset matches the expected full name
        if any(word in name for word in expected_name.split()):
            score += 150.0  # Major bonus for matching important acronym
        elif any(word in description for word in expected_name.split()):
            score += 100.0  # Good bonus for description match
        
        # Extra bonus if the acronym appears in parentheses in the name (like "RAIS" in the name)
        import re
        acronym_pattern = r'\b' + re.escape(query_lower.upper()) + r'\b'
        if re.search(acronym_pattern, dataset.get('name', '')):
            score += 180.0  # Very high bonus for acronym in name
    
    # 3. Name vs description positioning bonus
    if query_lower in name:
        score += 50.0  # Name contains query
        # Extra bonus for position in name (earlier = better)
        name_words = name.split()
        for i, word in enumerate(name_words):
            if query_lower in word:
                score += max(20.0 - i * 2, 5.0)  # Earlier position = higher bonus
                break
    
    if query_lower in description:
        score += 20.0  # Description contains query
    
    # 4. Word boundary matches (complete words better than substrings)
    import re
    word_pattern = r'\b' + re.escape(query_lower) + r'\b'
    if re.search(word_pattern, name):
        score += 30.0  # Complete word in name
    if re.search(word_pattern, description):
        score += 15.0  # Complete word in description
    
    # 5. Length and specificity bonuses
    if len(query_lower) >= 4:  # Longer queries get bonus for specificity
        score += min(len(query_lower) * 2, 20.0)
    
    # 6. Popular/official source bonuses
    organizations = dataset.get('organizations', '').lower()
    official_sources = ['ibge', 'ipea', 'inep', 'ministério', 'secretaria', 'agência nacional']
    for source in official_sources:
        if source in organizations:
            score += 10.0
            break
    
    # 7. Penalty for very long names (likely less relevant)
    if len(name) > 100:
        score -= 5.0
    
    return score

def rank_search_results(query: str, datasets: list) -> list:
    """
    Rank search results by relevance score.
    
    Args:
        query: Original search query
        datasets: List of dataset dictionaries
        
    Returns:
        List of datasets sorted by relevance (highest first)
    """
    if not datasets:
        return datasets
    
    # Calculate relevance scores
    scored_datasets = []
    for dataset in datasets:
        score = calculate_search_relevance(query, dataset)
        scored_datasets.append((score, dataset))
    
    # Sort by score (descending) and return datasets
    scored_datasets.sort(key=lambda x: x[0], reverse=True)
    return [dataset for score, dataset in scored_datasets]

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

# =============================================================================
# MCP Tool Definitions
# =============================================================================

@server.list_resources()
async def handle_list_resources() -> List[Resource]:
    """
    List available MCP resources.
    
    Resources provide static information and documentation about the server.
    """
    return [
        Resource(
            uri="basedosdados://datasets",
            name="Available Datasets",
            description="Information about accessing Base dos Dados datasets",
            mimeType="application/json",
        ),
        Resource(
            uri="basedosdados://help",
            name="Base dos Dados Help",
            description="Comprehensive help and usage information",
            mimeType="text/plain",
        ),
    ]

@server.read_resource()
async def handle_read_resource(uri: str) -> str:
    """
    Read the content of a specific resource.
    
    Args:
        uri: Resource URI to read
        
    Returns:
        String content of the requested resource
    """
    if uri == "basedosdados://help":
        return """Base dos Dados MCP Server Help

This server provides comprehensive metadata access to Base dos Dados, Brazil's open data platform.

🔧 **Enhanced Tools for AI:**
- **search_datasets**: Search with rich info including table/column counts and BigQuery references
- **get_dataset_overview**: Complete dataset view with all tables, columns, and ready-to-use SQL
- **get_table_details**: Comprehensive table info with column types, descriptions, and sample queries
- **explore_data**: Multi-level exploration for quick discovery or detailed analysis
- **generate_queries**: Context-aware SQL generation with optimization tips

📊 **What is Base dos Dados?**
Base dos Dados is Brazil's public data platform that standardizes and provides
access to Brazilian public datasets through Google BigQuery with references like:
`basedosdados.br_ibge_censo_demografico.municipio`

🚀 **AI-Optimized Workflow:**
1. **Discover**: Use `search_datasets` to find relevant data with structure preview
2. **Explore**: Use `get_dataset_overview` to see complete dataset structure in one call
3. **Analyze**: Use `get_table_details` for full column information and sample queries
4. **Query**: Use `generate_queries` for context-aware SQL with BigQuery references

📝 **Key Features:**
- **Single-call efficiency**: Get comprehensive info without multiple API calls
- **Ready-to-use BigQuery references**: Direct table access paths included
- **AI-friendly responses**: Structured for LLM consumption and decision making
- **Smart search ranking**: Acronyms like "RAIS", "IBGE" prioritized correctly

🌐 **More Information:**
- Website: https://basedosdados.org
- Documentation: https://docs.basedosdados.org  
- Python Package: pip install basedosdados
"""
    elif uri == "basedosdados://datasets":
        return '{"message": "Use the search_datasets tool to discover available datasets", "endpoint": "https://backend.basedosdados.org/graphql"}'
    else:
        raise ValueError(f"Unknown resource: {uri}")

# =============================================================================
# MCP Resource Handlers
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
        Tool(
            name="generate_queries",
            description="Generate context-aware SQL queries with BigQuery table references and optimization suggestions",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "The UUID of the table",
                    },
                    "query_type": {
                        "type": "string",
                        "enum": ["select", "aggregate", "filter", "join", "sample"],
                        "description": "Type of SQL query to generate",
                        "default": "select"
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Specific columns to include (optional)",
                    },
                    "filters": {
                        "type": "object",
                        "description": "Filter conditions as key-value pairs (optional)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Row limit for the query (optional)",
                    },
                },
                "required": ["table_id"],
            },
        ),
    ]

# =============================================================================
# MCP Tool Handlers
# =============================================================================

@server.call_tool()
async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
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
            
            # Build response with debug information for troubleshooting
            debug_info = f"\n\n**Search Debug:** {'; '.join(search_attempts)}" if search_attempts else ""
            if processed_query != query:
                debug_info += f"\n**Query Processing:** '{query}' → '{processed_query}'"
            if fallback_keywords:
                debug_info += f"\n**Fallback Keywords:** {', '.join(fallback_keywords)}"
            
            # Build enhanced response with data structure information
            response_parts = []
            for ds in datasets:
                basic_info = f"**{ds['name']}** (ID: {ds['id']}, Slug: {ds['slug']})"
                
                # Data structure summary
                if ds['table_count'] > 0:
                    structure_info = f"📊 **Data:** {ds['table_count']} tables, {ds['total_columns']} total columns"
                    if ds['sample_bigquery_ref']:
                        structure_info += f"\n🔗 **Sample Access:** `{ds['sample_bigquery_ref']}`"
                else:
                    structure_info = f"📊 **Data:** No tables available"
                
                # Tables preview
                if ds['sample_tables']:
                    tables_info = f"📋 **Tables:** {', '.join(ds['sample_tables'])}"
                else:
                    tables_info = ""
                
                # Metadata
                metadata = f"**Description:** {ds['description']}"
                if ds['organizations']:
                    metadata += f"\n**Organizations:** {ds['organizations']}"
                if ds['themes']:
                    metadata += f"\n**Themes:** {', '.join(ds['themes'])}"
                if ds['tags']:
                    metadata += f"\n**Tags:** {', '.join(ds['tags'])}"
                
                # Combine all parts
                dataset_text = f"{basic_info}\n{structure_info}"
                if tables_info:
                    dataset_text += f"\n{tables_info}"
                dataset_text += f"\n{metadata}"
                
                response_parts.append(dataset_text)
            
            final_response = f"Found {len(datasets)} datasets:{debug_info}\n\n" + "\n\n".join(response_parts)
            
            # Add usage tips for first-time users
            if datasets:
                final_response += f"""\n\n💡 **Next Steps:**
- Use `get_dataset_overview` with a dataset ID to see all tables and columns
- Use `get_table_details` with a table ID for complete column information  
- Use `generate_queries` to create SQL for BigQuery access
- Access data using BigQuery references like: `{datasets[0]['sample_bigquery_ref'] if datasets[0]['sample_bigquery_ref'] else 'basedosdados.dataset.table'}`"""
            
            return [TextContent(type="text", text=final_response)]
            
        except Exception as e:
            return [TextContent(type="text", text=f"Error searching datasets: {str(e)}")]
    
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
                    
                    info = f"""**Dataset Information**
Name: {dataset['name']}
ID: {dataset['id']}
Slug: {dataset.get('slug', '')}
Description: {dataset.get('description', 'No description available')}
Organizations: {', '.join(org_names)}
Themes: {', '.join([t['node']['name'] for t in dataset.get('themes', {}).get('edges', [])])}
Tags: {', '.join([t['node']['name'] for t in dataset.get('tags', {}).get('edges', [])])}

**Tables in this dataset:**
"""
                    for edge in dataset.get("tables", {}).get("edges", []):
                        table = edge["node"]
                        info += f"- {table['name']} (ID: {table['id']}, Slug: {table.get('slug', '')}): {table.get('description', 'No description')}\n"
                    
                    return [TextContent(type="text", text=info)]
                else:
                    return [TextContent(type="text", text="Dataset not found")]
            else:
                return [TextContent(type="text", text="Dataset not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error getting dataset info: {str(e)}")]
    
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
                    
                    return [TextContent(
                        type="text",
                        text=f"**Tables in dataset '{dataset['name']}':**\n\n" +
                             "\n".join([
                                 f"• **{table['name']}** (ID: {table['id']}, Slug: {table['slug']})\n"
                                 f"  {table['description']}"
                                 for table in tables
                             ])
                    )]
                else:
                    return [TextContent(type="text", text="Dataset not found")]
            else:
                return [TextContent(type="text", text="Dataset not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error listing tables: {str(e)}")]
    
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
                    
                    info = f"""**Table Information**
Name: {table['name']}
ID: {table['id']}
Slug: {table.get('slug', '')}
Description: {table.get('description', 'No description available')}

**Dataset:**
{dataset['name']} (ID: {dataset['id']}, Slug: {dataset.get('slug', '')})

**Columns:**
"""
                    
                    for edge in table.get("columns", {}).get("edges", []):
                        column = edge["node"]
                        bigquery_type = column.get("bigqueryType", {}).get("name", "Unknown")
                        info += f"• {column['name']} ({bigquery_type})\n"
                        if column.get("description"):
                            info += f"  {column['description']}\n"
                    
                    return [TextContent(type="text", text=info)]
                else:
                    return [TextContent(type="text", text="Table not found")]
            else:
                return [TextContent(type="text", text="Table not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error getting table info: {str(e)}")]
    
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
                    
                    return [TextContent(
                        type="text",
                        text=f"**Columns in table '{table['name']}':**\n\n" +
                             "\n".join([
                                 f"• **{col['name']}** ({col['type']}) - ID: {col['id']}\n"
                                 f"  {col['description']}"
                                 for col in columns
                             ])
                    )]
                else:
                    return [TextContent(type="text", text="Table not found")]
            else:
                return [TextContent(type="text", text="Table not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error listing columns: {str(e)}")]
    
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
                    
                    info = f"""**Column Information**
Name: {column['name']}
ID: {column['id']}
Type: {bigquery_type}
Description: {column.get('description', 'No description available')}

**Table:**
{table['name']} (ID: {table['id']}, Slug: {table.get('slug', '')})

**Dataset:**
{dataset['name']} (ID: {dataset['id']}, Slug: {dataset.get('slug', '')})
"""
                    
                    return [TextContent(type="text", text=info)]
                else:
                    return [TextContent(type="text", text="Column not found")]
            else:
                return [TextContent(type="text", text="Column not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error getting column info: {str(e)}")]
    
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
                    
                    return [TextContent(
                        type="text", 
                        text=f"**Generated SQL Query for {table['name']}:**\n\n```sql\n{sql_query}\n```\n\n"
                             f"**Usage:** You can run this query in BigQuery or use the Base dos Dados Python package."
                    )]
                else:
                    return [TextContent(type="text", text="Table not found")]
            else:
                return [TextContent(type="text", text="Table not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error generating SQL query: {str(e)}")]
    
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
                    response = f"""**📊 Dataset Overview: {dataset['name']}**

**Basic Information:**
- **ID:** {dataset['id']}
- **Slug:** {dataset.get('slug', '')}
- **Description:** {dataset.get('description', 'No description available')}
- **Organizations:** {', '.join(org_names)}
- **Themes:** {', '.join(theme_names)}
- **Tags:** {', '.join(tag_names)}

**Data Structure:**
- **Total Tables:** {len(tables_info)}
- **Total Columns:** {total_columns}

**📋 Tables with BigQuery Access:**
"""
                    
                    for table in tables_info:
                        response += f"""
**{table['name']}** ({table['column_count']} columns)
- **BigQuery Reference:** `{table['bigquery_reference']}`
- **Table ID:** {table['id']}
- **Description:** {table['description']}
- **Sample Columns:** {', '.join(table['sample_columns'])}
"""
                    
                    response += f"""

**🔍 Next Steps:**
- Use `get_table_details` with a table ID to see all columns and types
- Use `generate_queries` to create SQL for specific tables
- Access data in BigQuery using the table references above (e.g., `SELECT * FROM {tables_info[0]['bigquery_reference'] if tables_info else 'basedosdados.dataset.table'} LIMIT 100`)
"""
                    
                    return [TextContent(type="text", text=response)]
                else:
                    return [TextContent(type="text", text="Dataset not found")]
            else:
                return [TextContent(type="text", text="Dataset not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error getting dataset overview: {str(e)}")]
    
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
                    
                    response = f"""**📋 Table Details: {table['name']}**

**Basic Information:**
- **Table ID:** {table['id']}
- **Table Slug:** {table_slug}
- **Description:** {table.get('description', 'No description available')}
- **BigQuery Reference:** `{bigquery_ref}`

**Dataset Context:**
- **Dataset:** {dataset['name']} 
- **Dataset ID:** {dataset['id']}
- **Dataset Slug:** {dataset.get('slug', '')}

**📊 Columns ({len(columns)} total):**
"""
                    
                    for col_edge in columns:
                        column = col_edge["node"]
                        col_type = column.get("bigqueryType", {}).get("name", "Unknown")
                        col_desc = column.get("description", "No description")
                        response += f"""
**{column['name']}** ({col_type})
- ID: {column['id']}
- Description: {col_desc}
"""
                    
                    # Generate sample SQL queries
                    column_names = [col["node"]["name"] for col in columns]
                    sample_columns = ", ".join(column_names[:5])
                    if len(column_names) > 5:
                        sample_columns += f", ... -- and {len(column_names) - 5} more"
                    
                    response += f"""

**🔍 Sample SQL Queries:**

**Basic Select:**
```sql
SELECT {sample_columns}
FROM `{bigquery_ref}`
LIMIT 100
```

**Full Table Schema:**
```sql
SELECT *
FROM `{bigquery_ref}`
LIMIT 10
```

**Column Info:**
```sql
SELECT column_name, data_type, description
FROM `{dataset_slug}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
WHERE table_name = '{table_slug}'
```

**🚀 Access Instructions:**
1. Use the BigQuery reference: `{bigquery_ref}`
2. Run queries in Google BigQuery console
3. Or use the Base dos Dados Python package: `bd.read_table('{dataset_slug}', '{table_slug}')`
"""
                    
                    return [TextContent(type="text", text=response)]
                else:
                    return [TextContent(type="text", text="Table not found")]
            else:
                return [TextContent(type="text", text="Table not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error getting table details: {str(e)}")]
    
    elif name == "explore_data":
        dataset_id = arguments.get("dataset_id")
        table_id = arguments.get("table_id")
        mode = arguments.get("mode", "overview")
        
        if not dataset_id and not table_id:
            return [TextContent(type="text", text="Please provide either dataset_id or table_id")]
        
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
                        response = f"""**🔍 Dataset Exploration: {dataset['name']}**

**Quick Overview:**
- **Total Tables:** {len(tables)}
- **Organizations:** {', '.join([org["node"]["name"] for org in dataset.get("organizations", {}).get("edges", [])])}
- **Themes:** {', '.join([t["node"]["name"] for t in dataset.get("themes", {}).get("edges", [])])}

**🏆 Top Tables:**
"""
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
                    
                    return [TextContent(type="text", text=response)]
                else:
                    return [TextContent(type="text", text="Dataset not found")]
            
            elif table_id:
                # Explore table - delegate to get_table_details
                return await handle_call_tool("get_table_details", {"table_id": table_id})
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error exploring data: {str(e)}")]
    
    elif name == "generate_queries":
        table_id = clean_graphql_id(arguments.get("table_id"))
        query_type = arguments.get("query_type", "select")
        columns = arguments.get("columns", [])
        filters = arguments.get("filters", {})
        limit = arguments.get("limit")
        
        try:
            # Get table information for context
            result = await make_graphql_request(TABLE_DETAILS_QUERY, {"id": table_id})
            
            if result.get("data", {}).get("allTable", {}).get("edges"):
                table = result["data"]["allTable"]["edges"][0]["node"]
                dataset = table["dataset"]
                table_columns = table.get("columns", {}).get("edges", [])
                
                # Generate BigQuery reference
                dataset_slug = dataset.get("slug", "")
                table_slug = table.get("slug", "")
                bigquery_ref = f"basedosdados.{dataset_slug}.{table_slug}"
                
                # Get available columns if not specified
                if not columns:
                    columns = [col["node"]["name"] for col in table_columns]
                
                # Generate different types of queries
                queries = []
                
                if query_type == "select" or query_type == "sample":
                    column_list = ", ".join(columns[:10])  # Limit for readability
                    if len(columns) > 10:
                        column_list += f" -- and {len(columns) - 10} more columns"
                    
                    query = f"SELECT {column_list}\nFROM `{bigquery_ref}`"
                    if limit:
                        query += f"\nLIMIT {limit}"
                    else:
                        query += "\nLIMIT 100"
                    queries.append(("Basic Select", query))
                
                if query_type == "aggregate":
                    numeric_cols = [col["node"]["name"] for col in table_columns 
                                   if col["node"].get("bigqueryType", {}).get("name", "") in ["INTEGER", "FLOAT", "NUMERIC"]]
                    if numeric_cols:
                        query = f"""SELECT COUNT(*) as total_rows,
       AVG({numeric_cols[0]}) as avg_{numeric_cols[0]},
       MIN({numeric_cols[0]}) as min_{numeric_cols[0]},
       MAX({numeric_cols[0]}) as max_{numeric_cols[0]}
FROM `{bigquery_ref}`"""
                        queries.append(("Aggregation Example", query))
                
                if filters:
                    filter_conditions = []
                    for col, value in filters.items():
                        if isinstance(value, str):
                            filter_conditions.append(f"{col} = '{value}'")
                        else:
                            filter_conditions.append(f"{col} = {value}")
                    
                    if filter_conditions:
                        query = f"""SELECT *
FROM `{bigquery_ref}`
WHERE {' AND '.join(filter_conditions)}"""
                        if limit:
                            query += f"\nLIMIT {limit}"
                        queries.append(("Filtered Query", query))
                
                # Build response
                response = f"""**🔍 Generated SQL Queries for {table['name']}**

**Table Reference:** `{bigquery_ref}`
**Available Columns:** {len(table_columns)}

"""
                for query_name, query_sql in queries:
                    response += f"""**{query_name}:**
```sql
{query_sql}
```

"""
                
                response += f"""**💡 Optimization Tips:**
- Use `LIMIT` to test queries on large tables
- Consider partitioning columns for better performance
- Use `SELECT *` sparingly on tables with many columns
- Check data freshness and update frequency

**🚀 Access Methods:**
1. **BigQuery Console:** Copy the SQL above
2. **Python:** `bd.read_sql(query, billing_project_id='your-project')`
3. **R:** Use `bigrquery` package with the table reference
"""
                
                return [TextContent(type="text", text=response)]
            else:
                return [TextContent(type="text", text="Table not found")]
                
        except Exception as e:
            return [TextContent(type="text", text=f"Error generating queries: {str(e)}")]
    
    else:
        return [TextContent(type="text", text=f"Unknown tool: {name}")]

# =============================================================================
# Server Initialization and Main Entry Point
# =============================================================================

async def main():
    """
    Main entry point for the MCP server.
    
    Initializes the server with stdio communication and runs the event loop.
    """
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="basedosdados-mcp",
                server_version="0.1.0",
                capabilities=ServerCapabilities(
                    resources={},
                    tools={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())