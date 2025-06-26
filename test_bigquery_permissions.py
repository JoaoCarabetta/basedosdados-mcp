#!/usr/bin/env python3
"""
Test BigQuery permissions for Base dos Dados access using service account
"""

import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Dict, List, Optional


def test_service_account_permissions(service_account_path: str) -> Dict[str, any]:
    """
    Test BigQuery permissions for Base dos Dados access.
    
    Args:
        service_account_path: Path to the service account JSON file
        
    Returns:
        Dictionary with test results
    """
    results = {
        "service_account_valid": False,
        "bigquery_connection": False,
        "basedosdados_access": False,
        "project_id": None,
        "accessible_datasets": [],
        "test_queries": {},
        "errors": []
    }
    
    try:
        # 1. Load and validate service account
        print("🔑 Testing service account...")
        
        if not os.path.exists(service_account_path):
            results["errors"].append(f"Service account file not found: {service_account_path}")
            return results
        
        with open(service_account_path, 'r') as f:
            sa_info = json.load(f)
        
        results["project_id"] = sa_info.get("project_id")
        print(f"✅ Service account loaded. Project: {results['project_id']}")
        results["service_account_valid"] = True
        
        # 2. Create BigQuery client
        print("🔗 Testing BigQuery connection...")
        
        credentials = service_account.Credentials.from_service_account_file(
            service_account_path,
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        
        client = bigquery.Client(credentials=credentials, project=results["project_id"])
        
        # Test basic connection
        datasets = list(client.list_datasets(max_results=1))
        print(f"✅ BigQuery connection successful")
        results["bigquery_connection"] = True
        
        # 3. Test Base dos Dados access
        print("📊 Testing Base dos Dados access...")
        
        # Test query on a small Base dos Dados table
        test_queries = [
            {
                "name": "IBGE Municipal Population",
                "query": """
                    SELECT ano, sigla_uf, COUNT(*) as municipios
                    FROM `basedosdados.br_ibge_populacao.municipio` 
                    WHERE ano = 2020
                    GROUP BY ano, sigla_uf
                    ORDER BY sigla_uf
                    LIMIT 5
                """,
                "description": "Test access to IBGE population data"
            },
            {
                "name": "Dataset List",
                "query": """
                    SELECT table_catalog, table_schema, table_name, table_type
                    FROM `basedosdados.INFORMATION_SCHEMA.TABLES`
                    WHERE table_schema LIKE 'br_%'
                    LIMIT 10
                """,
                "description": "List Base dos Dados tables"
            },
            {
                "name": "Basic Count",
                "query": """
                    SELECT COUNT(*) as total_rows
                    FROM `basedosdados.br_ibge_populacao.municipio`
                    WHERE ano = 2020
                """,
                "description": "Simple count query"
            }
        ]
        
        for test in test_queries:
            try:
                print(f"  Testing: {test['name']}")
                
                job_config = bigquery.QueryJobConfig(
                    dry_run=False,
                    maximum_bytes_billed=10**8  # 100MB limit
                )
                
                query_job = client.query(test["query"], job_config=job_config)
                rows = list(query_job.result(max_results=10))
                
                results["test_queries"][test["name"]] = {
                    "status": "success",
                    "rows_returned": len(rows),
                    "sample_data": [dict(row) for row in rows[:3]],
                    "bytes_processed": query_job.total_bytes_processed,
                    "bytes_billed": query_job.total_bytes_billed
                }
                
                print(f"    ✅ Success: {len(rows)} rows, {query_job.total_bytes_processed:,} bytes processed")
                results["basedosdados_access"] = True
                
            except Exception as e:
                error_msg = str(e)
                results["test_queries"][test["name"]] = {
                    "status": "error",
                    "error": error_msg
                }
                print(f"    ❌ Failed: {error_msg}")
                results["errors"].append(f"{test['name']}: {error_msg}")
        
        # 4. List accessible datasets
        print("📋 Listing accessible datasets...")
        try:
            datasets_query = """
                SELECT DISTINCT table_schema as dataset
                FROM `basedosdados.INFORMATION_SCHEMA.TABLES`
                WHERE table_schema LIKE 'br_%'
                ORDER BY table_schema
                LIMIT 20
            """
            
            job_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**7)  # 10MB limit
            query_job = client.query(datasets_query, job_config=job_config)
            
            for row in query_job.result():
                results["accessible_datasets"].append(row.dataset)
            
            print(f"✅ Found {len(results['accessible_datasets'])} accessible datasets")
            
        except Exception as e:
            results["errors"].append(f"Dataset listing failed: {str(e)}")
            print(f"❌ Dataset listing failed: {str(e)}")
        
    except Exception as e:
        error_msg = f"BigQuery client creation failed: {str(e)}"
        results["errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    return results


def print_results_summary(results: Dict[str, any]):
    """Print a formatted summary of test results."""
    
    print("\n" + "="*60)
    print("🧪 BIGQUERY PERMISSIONS TEST SUMMARY")
    print("="*60)
    
    # Overall status
    print(f"Service Account Valid: {'✅' if results['service_account_valid'] else '❌'}")
    print(f"BigQuery Connection: {'✅' if results['bigquery_connection'] else '❌'}")
    print(f"Base dos Dados Access: {'✅' if results['basedosdados_access'] else '❌'}")
    print(f"Project ID: {results['project_id']}")
    
    # Test queries
    if results["test_queries"]:
        print(f"\n📊 Query Test Results:")
        for query_name, result in results["test_queries"].items():
            status = "✅" if result["status"] == "success" else "❌"
            print(f"  {status} {query_name}")
            
            if result["status"] == "success":
                print(f"    - Rows: {result['rows_returned']}")
                print(f"    - Bytes processed: {result.get('bytes_processed', 0):,}")
                if result.get('sample_data'):
                    print(f"    - Sample: {result['sample_data'][0] if result['sample_data'] else 'No data'}")
            else:
                print(f"    - Error: {result.get('error', 'Unknown error')}")
    
    # Accessible datasets
    if results["accessible_datasets"]:
        print(f"\n📋 Accessible Datasets ({len(results['accessible_datasets'])}):")
        for dataset in results["accessible_datasets"][:10]:
            print(f"  - {dataset}")
        if len(results["accessible_datasets"]) > 10:
            print(f"  ... and {len(results['accessible_datasets']) - 10} more")
    
    # Errors
    if results["errors"]:
        print(f"\n❌ Errors ({len(results['errors'])}):")
        for error in results["errors"]:
            print(f"  - {error}")
    
    # Recommendations
    print(f"\n💡 Recommendations:")
    if not results["basedosdados_access"]:
        print("  - Ensure the service account has BigQuery Data Viewer role")
        print("  - Check if Base dos Dados project allows external access")
        print("  - Verify the service account is enabled and not expired")
    else:
        print("  ✅ Service account has sufficient permissions for Base dos Dados")
        print("  ✅ You can use this service account for querying Base dos Dados")
    
    print("="*60)


def main():
    """Main test function."""
    service_account_path = "/Users/joaoc/Documents/service_accounts/rj-escritorio-dev-claude.json"
    
    print("🔧 BigQuery Service Account Permissions Tester")
    print("=" * 55)
    print(f"Testing: {service_account_path}")
    print()
    
    # Check if google-cloud-bigquery is available
    try:
        import google.cloud.bigquery
        print("✅ google-cloud-bigquery is available")
    except ImportError:
        print("❌ google-cloud-bigquery not found. Install with:")
        print("  pip install google-cloud-bigquery")
        return
    
    # Run the test
    results = test_service_account_permissions(service_account_path)
    
    # Print summary
    print_results_summary(results)
    
    # Return exit code based on success
    if results["basedosdados_access"]:
        print("\n🎉 SUCCESS: Service account can access Base dos Dados!")
        return 0
    else:
        print("\n⚠️  WARNING: Service account has limited or no Base dos Dados access")
        return 1


if __name__ == "__main__":
    exit(main())