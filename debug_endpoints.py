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
        
        result, response_time, error = await make_debug_graphql_request(
            SEARCH_DATASETS_QUERY,
            {"query": search_term, "first": 5},
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