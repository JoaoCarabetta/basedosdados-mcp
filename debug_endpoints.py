#!/usr/bin/env python3
"""
Debug and Testing Script for Base dos Dados MCP Server

This script provides comprehensive testing functionality for the Base dos Dados GraphQL API endpoints.
It includes configurable endpoint URLs, detailed error reporting, and performance monitoring.

Usage:
    python debug_endpoints.py                           # Test default endpoint
    python debug_endpoints.py --endpoint <custom_url>   # Test custom endpoint
    python debug_endpoints.py --quick                   # Run quick connectivity test only
"""

import asyncio
import argparse
import json
import time
from typing import Any, Dict, Optional, Tuple
from datetime import datetime

# Third-party imports
import httpx

# =============================================================================
# Configuration
# =============================================================================

# Default Base dos Dados GraphQL API endpoint - can be overridden
DEFAULT_ENDPOINT = "https://backend.basedosdados.org/graphql"
ENDPOINT_URL = DEFAULT_ENDPOINT  # This will be configurable

# Test data for comprehensive endpoint testing
SAMPLE_SEARCH_TERMS = ["população", "populacao", "IBGE", "covid", "educação", "saúde"]

# Extended search terms for comprehensive testing
COMPREHENSIVE_SEARCH_TERMS = [
    # Population and demographics
    "população", "populacao", "demográfico", "demografico", "censo", "habitantes", 
    "mortalidade", "natalidade", "nascimentos", "óbitos",
    
    # Organizations and institutions
    "IBGE", "IPEA", "TSE", "INEP", "Ministério", "Secretaria",
    
    # Health and pandemic
    "covid", "saúde", "sus", "epidemiologia", "vacina", "hospital",
    
    # Education
    "educação", "educacao", "escola", "universidade", "ENEM", "ensino",
    
    # Economics and finance
    "PIB", "economia", "inflação", "salário", "renda", "emprego",
    
    # Environment and geography
    "meio ambiente", "clima", "desmatamento", "biodiversidade", "água",
    
    # Social and urban
    "violência", "segurança", "transporte", "habitação", "urbanização"
]
SAMPLE_DATASET_IDS = ['d30222ad-7a5c-4778-a1ec-f0785371d1ca']  # Will be populated from search results
SAMPLE_TABLE_IDS = ['2440d076-8934-471f-8cbe-51faae387c66']    # Will be populated from dataset queries
SAMPLE_COLUMN_IDS = []   # Will be populated from table queries

# =============================================================================
# GraphQL Test Queries
# =============================================================================

# Basic connectivity test query
CONNECTIVITY_QUERY = """
query TestConnectivity {
    __schema {
        queryType {
            name
        }
    }
}
"""

# Enhanced search query - now searches descriptions (more comprehensive)
SEARCH_DATASETS_QUERY = """
query SearchDatasets($query: String, $first: Int) {
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
            }
        }
    }
}
"""

# Get dataset info query
GET_DATASET_QUERY = """
query GetDataset($id: ID!) {
    allDataset(id: $id, first: 1) {
        edges {
            node {
                id
                name
                slug
                description
                tables {
                    edges {
                        node {
                            id
                            name
                            slug
                        }
                    }
                }
            }
        }
    }
}
"""

# Get table info query
GET_TABLE_QUERY = """
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
                }
                columns {
                    edges {
                        node {
                            id
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

# Get column info query
GET_COLUMN_QUERY = """
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
                }
            }
        }
    }
}
"""

# =============================================================================
# Utility Functions
# =============================================================================

def clean_graphql_id(graphql_id: str) -> str:
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
    (Same function as in server.py for consistency)
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
    (Same function as in server.py for consistency)
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

# =============================================================================
# HTTP Client and GraphQL Request Function
# =============================================================================

async def make_debug_graphql_request(
    query: str, 
    variables: Optional[Dict[str, Any]] = None,
    endpoint: Optional[str] = None
) -> Tuple[Dict[str, Any], float, Optional[str]]:
    """
    Make a GraphQL request with detailed debugging information.
    
    Args:
        query: GraphQL query string
        variables: Optional variables for the GraphQL query
        endpoint: Optional custom endpoint URL
        
    Returns:
        Tuple of (response_data, response_time_seconds, error_message)
    """
    if endpoint is None:
        endpoint = ENDPOINT_URL
        
    start_time = time.time()
    error_message = None
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                endpoint,
                json={"query": query, "variables": variables or {}},
                headers={"Content-Type": "application/json"}
            )
            
            response_time = time.time() - start_time
            
            # Handle different response scenarios
            if response.status_code == 400:
                error_data = response.json()
                if "errors" in error_data:
                    error_messages = [err.get("message", "Unknown error") for err in error_data["errors"]]
                    error_message = f"GraphQL errors: {'; '.join(error_messages)}"
                else:
                    error_message = f"Bad Request (400): {error_data}"
                return {}, response_time, error_message
            
            response.raise_for_status()
            result = response.json()
            
            # Check for GraphQL errors in successful responses
            if "errors" in result:
                error_messages = [err.get("message", "Unknown error") for err in result["errors"]]
                error_message = f"GraphQL errors: {'; '.join(error_messages)}"
                
            return result, response_time, error_message
            
    except httpx.TimeoutException:
        response_time = time.time() - start_time
        error_message = "Request timeout - the API is taking too long to respond"
        return {}, response_time, error_message
    except httpx.RequestError as e:
        response_time = time.time() - start_time
        error_message = f"Network error: {str(e)}"
        return {}, response_time, error_message
    except Exception as e:
        response_time = time.time() - start_time
        error_message = f"Unexpected error: {str(e)}"
        return {}, response_time, error_message

# =============================================================================
# Test Functions
# =============================================================================

async def test_endpoint_connectivity(endpoint: Optional[str] = None) -> Dict[str, Any]:
    """Test basic connectivity to the GraphQL endpoint."""
    print("🔌 Testing endpoint connectivity...")
    
    result, response_time, error = await make_debug_graphql_request(
        CONNECTIVITY_QUERY, 
        endpoint=endpoint
    )
    
    test_result = {
        "test_name": "connectivity",
        "success": error is None,
        "response_time": response_time,
        "error": error,
        "endpoint": endpoint or ENDPOINT_URL
    }
    
    if test_result["success"]:
        print(f"✅ Connectivity test passed ({response_time:.3f}s)")
    else:
        print(f"❌ Connectivity test failed: {error}")
    
    return test_result

async def test_search_datasets(endpoint: Optional[str] = None) -> Dict[str, Any]:
    """Test dataset search functionality."""
    print("🔍 Testing dataset search...")
    
    results = []
    global SAMPLE_DATASET_IDS
    
    for search_term in SAMPLE_SEARCH_TERMS[:3]:  # Test first 3 terms
        print(f"  Searching for: '{search_term}'")
        
        # Apply same preprocessing as MCP server
        enhanced_search_term, fallback_keywords = preprocess_search_query(search_term)
        
        result, response_time, error = await make_debug_graphql_request(
            SEARCH_DATASETS_QUERY,
            {"query": enhanced_search_term, "first": 5},
            endpoint=endpoint
        )
        
        test_result = {
            "search_term": search_term,
            "success": error is None,
            "response_time": response_time,
            "error": error,
            "results_count": 0
        }
        
        if error is None and result.get("data", {}).get("allDataset", {}).get("edges"):
            edges = result["data"]["allDataset"]["edges"]
            test_result["results_count"] = len(edges)
            
            # Collect sample dataset IDs for further testing
            for edge in edges[:2]:  # Take first 2 results
                dataset_id = edge["node"]["id"]
                if dataset_id not in SAMPLE_DATASET_IDS:
                    SAMPLE_DATASET_IDS.append(dataset_id)
            
            print(f"    ✅ Found {len(edges)} datasets ({response_time:.3f}s)")
        else:
            print(f"    ❌ Search failed: {error}")
        
        results.append(test_result)
    
    return {
        "test_name": "search_datasets",
        "overall_success": all(r["success"] for r in results),
        "individual_results": results,
        "sample_dataset_ids_found": len(SAMPLE_DATASET_IDS)
    }

async def test_comprehensive_search(endpoint: Optional[str] = None) -> Dict[str, Any]:
    """Test comprehensive search across many different terms and categories."""
    print("🔍 Testing comprehensive search across multiple categories...")
    
    categories = {
        "Population & Demographics": ["população", "populacao", "demográfico", "censo", "habitantes"],
        "Organizations": ["IBGE", "IPEA", "TSE", "INEP"],
        "Health & Pandemic": ["covid", "saúde", "sus", "epidemiologia"],
        "Education": ["educação", "educacao", "escola", "ENEM"],
        "Economics": ["PIB", "economia", "inflação", "renda"],
        "Environment": ["meio ambiente", "clima", "desmatamento"],
        "Social & Urban": ["violência", "transporte", "habitação"]
    }
    
    all_results = {}
    total_successful_searches = 0
    total_datasets_found = 0
    
    for category, terms in categories.items():
        print(f"\n  📂 {category}:")
        category_results = []
        
        for term in terms[:3]:  # Test first 3 terms per category
            # Apply same preprocessing as MCP server
            enhanced_search_term, fallback_keywords = preprocess_search_query(term)
            
            try:
                result, response_time, error = await make_debug_graphql_request(
                    SEARCH_DATASETS_QUERY,
                    {"query": enhanced_search_term, "first": 10},
                    endpoint=endpoint
                )
                
                if error is None and result.get("data", {}).get("allDataset", {}).get("edges"):
                    edges = result["data"]["allDataset"]["edges"]
                    count = len(edges)
                    total_datasets_found += count
                    total_successful_searches += 1
                    print(f"    ✅ '{term}': {count} datasets ({response_time:.2f}s)")
                    
                    # Sample a dataset name for verification
                    if edges:
                        sample_name = edges[0]["node"]["name"][:50] + "..." if len(edges[0]["node"]["name"]) > 50 else edges[0]["node"]["name"]
                        print(f"       Example: {sample_name}")
                    
                    category_results.append({
                        "term": term,
                        "success": True,
                        "count": count,
                        "response_time": response_time
                    })
                else:
                    print(f"    ❌ '{term}': No results or error")
                    category_results.append({
                        "term": term,
                        "success": False,
                        "count": 0,
                        "error": error
                    })
                    
            except Exception as e:
                print(f"    ❌ '{term}': Error - {str(e)}")
                category_results.append({
                    "term": term,
                    "success": False,
                    "count": 0,
                    "error": str(e)
                })
        
        all_results[category] = category_results
    
    return {
        "test_name": "comprehensive_search",
        "overall_success": total_successful_searches > 0,
        "total_successful_searches": total_successful_searches,
        "total_datasets_found": total_datasets_found,
        "categories": all_results,
        "summary": {
            "categories_tested": len(categories),
            "terms_tested": sum(len(terms[:3]) for terms in categories.values()),
            "success_rate": total_successful_searches / sum(len(terms[:3]) for terms in categories.values()) if categories else 0
        }
    }

async def test_accent_variations(endpoint: Optional[str] = None) -> Dict[str, Any]:
    """Test accent handling and variations."""
    print("🔠 Testing accent variations and character handling...")
    
    accent_tests = [
        {"without_accent": "populacao", "with_accent": "população", "description": "population"},
        {"without_accent": "educacao", "with_accent": "educação", "description": "education"},
        {"without_accent": "saude", "with_accent": "saúde", "description": "health"},
        {"without_accent": "inflacao", "with_accent": "inflação", "description": "inflation"},
        {"without_accent": "violencia", "with_accent": "violência", "description": "violence"}
    ]
    
    results = []
    
    for test_case in accent_tests:
        print(f"\n  Testing {test_case['description']}:")
        
        # Test without accent
        result_without, time_without, error_without = await make_debug_graphql_request(
            SEARCH_DATASETS_QUERY,
            {"query": test_case["without_accent"], "first": 5},
            endpoint=endpoint
        )
        
        # Test with accent
        result_with, time_with, error_with = await make_debug_graphql_request(
            SEARCH_DATASETS_QUERY,
            {"query": test_case["with_accent"], "first": 5},
            endpoint=endpoint
        )
        
        count_without = 0
        count_with = 0
        
        if error_without is None and result_without.get("data", {}).get("allDataset", {}).get("edges"):
            count_without = len(result_without["data"]["allDataset"]["edges"])
        
        if error_with is None and result_with.get("data", {}).get("allDataset", {}).get("edges"):
            count_with = len(result_with["data"]["allDataset"]["edges"])
        
        print(f"    '{test_case['without_accent']}' (no accent): {count_without} results")
        print(f"    '{test_case['with_accent']}' (with accent): {count_with} results")
        
        # Check if accent handling is working (should get similar results)
        accent_handling_works = abs(count_without - count_with) <= 2  # Allow small variation
        
        if accent_handling_works and count_with > 0:
            print(f"    ✅ Accent handling working properly")
        elif count_with > count_without:
            print(f"    ⚠️ Accented version performs better (+{count_with - count_without} results)")
        else:
            print(f"    ❌ Potential accent handling issue")
        
        results.append({
            "term": test_case["description"],
            "without_accent": {"term": test_case["without_accent"], "count": count_without},
            "with_accent": {"term": test_case["with_accent"], "count": count_with},
            "accent_handling_works": accent_handling_works
        })
    
    return {
        "test_name": "accent_variations",
        "overall_success": any(r["accent_handling_works"] for r in results),
        "results": results,
        "summary": {
            "total_tests": len(accent_tests),
            "working_accent_handling": sum(1 for r in results if r["accent_handling_works"]),
            "accent_success_rate": sum(1 for r in results if r["accent_handling_works"]) / len(results) if results else 0
        }
    }

async def test_complex_queries(endpoint: Optional[str] = None) -> Dict[str, Any]:
    """Test complex multi-word queries that previously failed."""
    print("🔍 Testing complex multi-word queries...")
    
    complex_queries = [
        "covid-19 pandemic coronavirus",
        "IBGE censo demográfico população", 
        "base dos dados educação pública",
        "ministério da saúde SUS",
        "violência urbana segurança pública",
        "população rural agricultura familiar",
        "educação infantil primeira infância"
    ]
    
    results = []
    
    for query in complex_queries:
        print(f"\n  Testing: '{query}'")
        
        # Use preprocessing to improve success rate
        processed_query, fallback_keywords = preprocess_search_query(query)
        print(f"    Processed: '{processed_query}'")
        if fallback_keywords:
            print(f"    Fallbacks: {fallback_keywords}")
        
        try:
            result, response_time, error = await make_debug_graphql_request(
                SEARCH_DATASETS_QUERY,
                {"query": processed_query, "first": 10},
                endpoint=endpoint
            )
            
            if error is None and result.get("data", {}).get("allDataset", {}).get("edges"):
                count = len(result["data"]["allDataset"]["edges"])
                print(f"    ✅ Success: {count} results in {response_time:.2f}s")
                
                results.append({
                    "query": query,
                    "processed_query": processed_query,
                    "fallback_keywords": fallback_keywords,
                    "success": True,
                    "count": count,
                    "response_time": response_time
                })
            else:
                print(f"    ❌ Failed: {error or 'No results'}")
                results.append({
                    "query": query,
                    "processed_query": processed_query,
                    "fallback_keywords": fallback_keywords,
                    "success": False,
                    "count": 0,
                    "error": error
                })
                
        except Exception as e:
            print(f"    ❌ Exception: {str(e)}")
            results.append({
                "query": query,
                "processed_query": processed_query,
                "fallback_keywords": fallback_keywords,
                "success": False,
                "count": 0,
                "error": str(e)
            })
    
    successful_queries = [r for r in results if r["success"]]
    
    return {
        "test_name": "complex_queries",
        "overall_success": len(successful_queries) > 0,
        "results": results,
        "summary": {
            "total_queries": len(complex_queries),
            "successful_queries": len(successful_queries),
            "success_rate": len(successful_queries) / len(complex_queries) if complex_queries else 0,
            "average_response_time": sum(r["response_time"] for r in successful_queries) / len(successful_queries) if successful_queries else 0
        }
    }

async def test_search_performance(endpoint: Optional[str] = None) -> Dict[str, Any]:
    """Test search performance with different query sizes and limits."""
    print("⚡ Testing search performance across different scenarios...")
    
    performance_tests = [
        {"query": "população", "limit": 5, "description": "Small query, small limit"},
        {"query": "população", "limit": 20, "description": "Small query, medium limit"},
        {"query": "população", "limit": 50, "description": "Small query, large limit"},
        {"query": "covid-19 pandemic coronavirus", "limit": 10, "description": "Long query, medium limit"},
        {"query": "a", "limit": 10, "description": "Single character query"},
        {"query": "IBGE censo demográfico população", "limit": 15, "description": "Multi-word query"}
    ]
    
    results = []
    
    for test in performance_tests:
        print(f"\n  {test['description']}:")
        
        start_time = time.time()
        result, response_time, error = await make_debug_graphql_request(
            SEARCH_DATASETS_QUERY,
            {"query": test["query"], "first": test["limit"]},
            endpoint=endpoint
        )
        end_time = time.time()
        
        if error is None and result.get("data", {}).get("allDataset", {}).get("edges"):
            count = len(result["data"]["allDataset"]["edges"])
            print(f"    ✅ Query: '{test['query'][:30]}...' → {count} results in {response_time:.2f}s")
            
            # Performance assessment
            if response_time < 1.0:
                perf_status = "🚀 Excellent"
            elif response_time < 3.0:
                perf_status = "✅ Good"
            elif response_time < 5.0:
                perf_status = "⚠️ Acceptable"
            else:
                perf_status = "❌ Slow"
            
            print(f"       Performance: {perf_status}")
            
            results.append({
                "description": test["description"],
                "query": test["query"],
                "limit": test["limit"],
                "results_count": count,
                "response_time": response_time,
                "performance_status": perf_status.split(" ")[1],
                "success": True
            })
        else:
            print(f"    ❌ Query failed: {error}")
            results.append({
                "description": test["description"],
                "query": test["query"],
                "limit": test["limit"],
                "results_count": 0,
                "response_time": response_time,
                "error": error,
                "success": False
            })
    
    return {
        "test_name": "search_performance",
        "overall_success": any(r["success"] for r in results),
        "results": results,
        "summary": {
            "total_tests": len(performance_tests),
            "successful_tests": sum(1 for r in results if r["success"]),
            "average_response_time": sum(r["response_time"] for r in results if r["success"]) / sum(1 for r in results if r["success"]) if any(r["success"] for r in results) else 0,
            "fastest_query": min((r for r in results if r["success"]), key=lambda x: x["response_time"], default=None),
            "slowest_query": max((r for r in results if r["success"]), key=lambda x: x["response_time"], default=None)
        }
    }

async def test_dataset_operations(endpoint: Optional[str] = None) -> Dict[str, Any]:
    """Test dataset information retrieval."""
    print("📊 Testing dataset operations...")
    
    if not SAMPLE_DATASET_IDS:
        print("  ⚠️ No dataset IDs available, skipping dataset operations test")
        return {
            "test_name": "dataset_operations",
            "overall_success": False,
            "error": "No sample dataset IDs available"
        }
    
    results = []
    global SAMPLE_TABLE_IDS
    
    for dataset_id in SAMPLE_DATASET_IDS[:2]:  # Test first 2 datasets
        clean_id = clean_graphql_id(dataset_id)
        print(f"  Testing dataset: {dataset_id} (clean: {clean_id})")
        
        result, response_time, error = await make_debug_graphql_request(
            GET_DATASET_QUERY,
            {"id": clean_id},
            endpoint=endpoint
        )
        
        test_result = {
            "dataset_id": dataset_id,
            "success": error is None,
            "response_time": response_time,
            "error": error,
            "tables_count": 0
        }
        
        if error is None and result.get("data", {}).get("allDataset", {}).get("edges"):
            edges = result["data"]["allDataset"]["edges"]
            if edges:
                dataset = edges[0]["node"]
                tables_edges = dataset.get("tables", {}).get("edges", [])
                test_result["tables_count"] = len(tables_edges)
                
                # Collect sample table IDs for further testing
                for edge in tables_edges[:2]:  # Take first 2 tables
                    table_id = edge["node"]["id"]
                    if table_id not in SAMPLE_TABLE_IDS:
                        SAMPLE_TABLE_IDS.append(table_id)
                
                print(f"    ✅ Dataset info retrieved, {len(tables_edges)} tables ({response_time:.3f}s)")
        else:
            print(f"    ❌ Dataset operation failed: {error}")
        
        results.append(test_result)
    
    return {
        "test_name": "dataset_operations",
        "overall_success": all(r["success"] for r in results),
        "individual_results": results,
        "sample_table_ids_found": len(SAMPLE_TABLE_IDS)
    }

async def test_table_operations(endpoint: Optional[str] = None) -> Dict[str, Any]:
    """Test table information retrieval."""
    print("📋 Testing table operations...")
    
    if not SAMPLE_TABLE_IDS:
        print("  ⚠️ No table IDs available, skipping table operations test")
        return {
            "test_name": "table_operations",
            "overall_success": False,
            "error": "No sample table IDs available"
        }
    
    results = []
    global SAMPLE_COLUMN_IDS
    
    for table_id in SAMPLE_TABLE_IDS[:2]:  # Test first 2 tables
        clean_id = clean_graphql_id(table_id)
        print(f"  Testing table: {table_id} (clean: {clean_id})")
        
        result, response_time, error = await make_debug_graphql_request(
            GET_TABLE_QUERY,
            {"id": clean_id},
            endpoint=endpoint
        )
        
        test_result = {
            "table_id": table_id,
            "success": error is None,
            "response_time": response_time,
            "error": error,
            "columns_count": 0
        }
        
        if error is None and result.get("data", {}).get("allTable", {}).get("edges"):
            edges = result["data"]["allTable"]["edges"]
            if edges:
                table = edges[0]["node"]
                columns_edges = table.get("columns", {}).get("edges", [])
                test_result["columns_count"] = len(columns_edges)
                
                # Collect sample column IDs for further testing
                for edge in columns_edges[:2]:  # Take first 2 columns
                    column_id = edge["node"]["id"]
                    if column_id not in SAMPLE_COLUMN_IDS:
                        SAMPLE_COLUMN_IDS.append(column_id)
                
                print(f"    ✅ Table info retrieved, {len(columns_edges)} columns ({response_time:.3f}s)")
        else:
            print(f"    ❌ Table operation failed: {error}")
        
        results.append(test_result)
    
    return {
        "test_name": "table_operations",
        "overall_success": all(r["success"] for r in results),
        "individual_results": results,
        "sample_column_ids_found": len(SAMPLE_COLUMN_IDS)
    }

async def test_column_operations(endpoint: Optional[str] = None) -> Dict[str, Any]:
    """Test column information retrieval."""
    print("📝 Testing column operations...")
    
    if not SAMPLE_COLUMN_IDS:
        print("  ⚠️ No column IDs available, skipping column operations test")
        return {
            "test_name": "column_operations",
            "overall_success": False,
            "error": "No sample column IDs available"
        }
    
    results = []
    
    for column_id in SAMPLE_COLUMN_IDS[:2]:  # Test first 2 columns
        clean_id = clean_graphql_id(column_id)
        print(f"  Testing column: {column_id} (clean: {clean_id})")
        
        result, response_time, error = await make_debug_graphql_request(
            GET_COLUMN_QUERY,
            {"id": clean_id},
            endpoint=endpoint
        )
        
        test_result = {
            "column_id": column_id,
            "success": error is None,
            "response_time": response_time,
            "error": error
        }
        
        if error is None and result.get("data", {}).get("allColumn", {}).get("edges"):
            edges = result["data"]["allColumn"]["edges"]
            if edges:
                column = edges[0]["node"]
                print(f"    ✅ Column info retrieved: {column['name']} ({response_time:.3f}s)")
        else:
            print(f"    ❌ Column operation failed: {error}")
        
        results.append(test_result)
    
    return {
        "test_name": "column_operations",
        "overall_success": all(r["success"] for r in results),
        "individual_results": results
    }

# =============================================================================
# Main Test Runner
# =============================================================================

async def run_all_endpoint_tests(endpoint: Optional[str] = None, quick_mode: bool = False) -> Dict[str, Any]:
    """
    Run comprehensive endpoint tests.
    
    Args:
        endpoint: Custom endpoint URL to test
        quick_mode: If True, only run connectivity test
        
    Returns:
        Dict containing all test results
    """
    global ENDPOINT_URL
    if endpoint:
        ENDPOINT_URL = endpoint
    
    print(f"🚀 Starting Base dos Dados API endpoint tests")
    print(f"📍 Testing endpoint: {ENDPOINT_URL}")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    test_results = {
        "endpoint": ENDPOINT_URL,
        "timestamp": datetime.now().isoformat(),
        "quick_mode": quick_mode,
        "tests": {}
    }
    
    # Always run connectivity test
    test_results["tests"]["connectivity"] = await test_endpoint_connectivity(ENDPOINT_URL)
    
    if quick_mode:
        print("⚡ Quick mode enabled - skipping comprehensive tests")
    else:
        # Run comprehensive tests if connectivity passes
        if test_results["tests"]["connectivity"]["success"]:
            test_results["tests"]["search_datasets"] = await test_search_datasets(ENDPOINT_URL)
            test_results["tests"]["comprehensive_search"] = await test_comprehensive_search(ENDPOINT_URL)
            test_results["tests"]["accent_variations"] = await test_accent_variations(ENDPOINT_URL)
            test_results["tests"]["complex_queries"] = await test_complex_queries(ENDPOINT_URL)
            test_results["tests"]["search_performance"] = await test_search_performance(ENDPOINT_URL)
            test_results["tests"]["dataset_operations"] = await test_dataset_operations(ENDPOINT_URL)
            test_results["tests"]["table_operations"] = await test_table_operations(ENDPOINT_URL)
            test_results["tests"]["column_operations"] = await test_column_operations(ENDPOINT_URL)
        else:
            print("❌ Connectivity test failed - skipping comprehensive tests")
    
    # Calculate summary statistics
    successful_tests = sum(1 for test in test_results["tests"].values() 
                          if test.get("success") or test.get("overall_success"))
    total_tests = len(test_results["tests"])
    
    print("=" * 60)
    print(f"📊 Test Summary: {successful_tests}/{total_tests} tests passed")
    
    if successful_tests == total_tests:
        print("🎉 All tests passed!")
    else:
        print("⚠️ Some tests failed - check the detailed results above")
    
    test_results["summary"] = {
        "total_tests": total_tests,
        "successful_tests": successful_tests,
        "success_rate": successful_tests / total_tests if total_tests > 0 else 0
    }
    
    return test_results

# =============================================================================
# Command Line Interface
# =============================================================================

def main():
    """Main entry point for the debug script."""
    parser = argparse.ArgumentParser(
        description="Debug and test Base dos Dados GraphQL API endpoints"
    )
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help=f"GraphQL endpoint URL to test (default: {DEFAULT_ENDPOINT})"
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run only connectivity test (quick mode)"
    )
    parser.add_argument(
        "--output",
        help="Save detailed test results to JSON file"
    )
    
    args = parser.parse_args()
    
    # Run the async tests
    results = asyncio.run(run_all_endpoint_tests(args.endpoint, args.quick))
    
    # Save results to file if requested
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"📄 Detailed results saved to: {args.output}")

if __name__ == "__main__":
    main()