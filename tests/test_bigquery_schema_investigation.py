#!/usr/bin/env python3
"""
BigQuery Schema Investigation for Base dos Dados GraphQL API.

This script explores the GraphQL schema to find the correct fields
that contain BigQuery dataset identifiers like 'br_ibge_populacao'.
"""

import asyncio
import json
import sys
import os

# Add the source directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from basedosdados_mcp.graphql_client import make_graphql_request


class BigQuerySchemaInvestigator:
    """Investigates GraphQL schema for BigQuery-related fields."""
    
    def __init__(self):
        self.tested_fields = []
        self.successful_fields = []
        self.failed_fields = []
    
    async def test_dataset_fields(self, dataset_id: str):
        """Test various potential BigQuery field names on a dataset."""
        print(f"🔍 Testing BigQuery fields for dataset: {dataset_id}")
        
        # List of potential field names to test
        potential_fields = [
            "gcpDatasetId",
            "cloudDatasetId", 
            "bigqueryDatasetId",
            "datasetId",
            "bigqueryId",
            "gcp_dataset_id",
            "cloud_dataset_id",
            "bigquery_dataset_id",
            "dataset_id",
            "bigquery_id",
            "bqDatasetId",
            "bq_dataset_id",
            "gcpProjectId",
            "gcpTableId",
            "cloudProjectId",
        ]
        
        for field in potential_fields:
            print(f"  Testing field: {field}")
            
            query = f'''
            {{
                allDataset(id: "{dataset_id}", first: 1) {{
                    edges {{
                        node {{
                            id
                            name
                            slug
                            {field}
                        }}
                    }}
                }}
            }}
            '''
            
            try:
                result = await make_graphql_request(query)
                self.successful_fields.append(field)
                print(f"    ✅ Field '{field}' exists!")
                
                # Extract the value
                if result.get("data", {}).get("allDataset", {}).get("edges"):
                    node = result["data"]["allDataset"]["edges"][0]["node"]
                    value = node.get(field)
                    print(f"    Value: {value}")
                
            except Exception as e:
                self.failed_fields.append((field, str(e)))
                if "Cannot query field" in str(e):
                    print(f"    ❌ Field '{field}' does not exist")
                else:
                    print(f"    ⚠️  Field '{field}' error: {str(e)}")
    
    async def test_table_fields(self, table_id: str):
        """Test various potential BigQuery field names on a table."""
        print(f"\n🔍 Testing BigQuery fields for table: {table_id}")
        
        # List of potential field names to test for tables
        potential_fields = [
            "gcpDatasetId",
            "gcpTableId",
            "cloudDatasetId", 
            "cloudTableId",
            "bigqueryDatasetId",
            "bigqueryTableId",
            "bqDatasetId",
            "bqTableId",
            "datasetId",
            "tableId",
        ]
        
        for field in potential_fields:
            print(f"  Testing field: {field}")
            
            query = f'''
            {{
                allTable(id: "{table_id}", first: 1) {{
                    edges {{
                        node {{
                            id
                            name
                            slug
                            {field}
                        }}
                    }}
                }}
            }}
            '''
            
            try:
                result = await make_graphql_request(query)
                self.successful_fields.append(f"table.{field}")
                print(f"    ✅ Table field '{field}' exists!")
                
                # Extract the value
                if result.get("data", {}).get("allTable", {}).get("edges"):
                    node = result["data"]["allTable"]["edges"][0]["node"]
                    value = node.get(field)
                    print(f"    Value: {value}")
                
            except Exception as e:
                self.failed_fields.append((f"table.{field}", str(e)))
                if "Cannot query field" in str(e):
                    print(f"    ❌ Table field '{field}' does not exist")
                else:
                    print(f"    ⚠️  Table field '{field}' error: {str(e)}")
    
    async def test_nested_bigquery_fields(self, dataset_id: str):
        """Test for nested BigQuery field structures."""
        print(f"\n🔍 Testing nested BigQuery fields for dataset: {dataset_id}")
        
        # Test potential nested structures
        nested_tests = [
            "bigquery { datasetId }",
            "gcp { datasetId }",
            "cloud { datasetId }",
            "bigQueryRef { datasetId }",
            "cloudRef { datasetId }",
            "gcpRef { datasetId }",
        ]
        
        for nested_field in nested_tests:
            print(f"  Testing nested: {nested_field}")
            
            query = f'''
            {{
                allDataset(id: "{dataset_id}", first: 1) {{
                    edges {{
                        node {{
                            id
                            name
                            slug
                            {nested_field}
                        }}
                    }}
                }}
            }}
            '''
            
            try:
                result = await make_graphql_request(query)
                self.successful_fields.append(f"nested.{nested_field}")
                print(f"    ✅ Nested field '{nested_field}' exists!")
                print(f"    Result: {json.dumps(result, indent=2)}")
                
            except Exception as e:
                if "Cannot query field" in str(e):
                    print(f"    ❌ Nested field '{nested_field}' does not exist")
                else:
                    print(f"    ⚠️  Nested field '{nested_field}' error: {str(e)}")
    
    async def explore_dataset_full_schema(self, dataset_id: str):
        """Try to get a comprehensive view of available fields."""
        print(f"\n📋 Full schema exploration for dataset: {dataset_id}")
        
        # A comprehensive query with many possible fields
        query = f'''
        {{
            allDataset(id: "{dataset_id}", first: 1) {{
                edges {{
                    node {{
                        id
                        name
                        slug
                        description
                        createdAt
                        updatedAt
                        organization {{
                            id
                            name
                            slug
                        }}
                        tables {{
                            edges {{
                                node {{
                                    id
                                    name
                                    slug
                                    description
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
        '''
        
        try:
            result = await make_graphql_request(query)
            print("✅ Full schema query successful!")
            print(json.dumps(result, indent=2))
            
        except Exception as e:
            print(f"❌ Full schema query failed: {str(e)}")
    
    def print_summary(self):
        """Print a summary of the investigation."""
        print("\n" + "="*60)
        print("📊 BIGQUERY SCHEMA INVESTIGATION SUMMARY")
        print("="*60)
        
        print(f"\n✅ Successful fields found: {len(self.successful_fields)}")
        for field in self.successful_fields:
            print(f"  - {field}")
        
        print(f"\n❌ Fields that don't exist: {len([f for f, e in self.failed_fields if 'Cannot query field' in e])}")
        non_existent = [f for f, e in self.failed_fields if 'Cannot query field' in e]
        for field in non_existent[:10]:  # Show first 10
            print(f"  - {field}")
        if len(non_existent) > 10:
            print(f"  ... and {len(non_existent) - 10} more")
        
        print(f"\n⚠️  Other errors: {len([f for f, e in self.failed_fields if 'Cannot query field' not in e])}")
        other_errors = [f for f, e in self.failed_fields if 'Cannot query field' not in e]
        for field in other_errors:
            print(f"  - {field}")


async def main():
    """Run the BigQuery schema investigation."""
    print("🧪 Base dos Dados BigQuery Schema Investigation")
    print("=" * 60)
    print("Goal: Find correct GraphQL fields for BigQuery dataset identifiers")
    
    investigator = BigQuerySchemaInvestigator()
    
    # Test with IBGE population dataset
    dataset_id = "d30222ad-7a5c-4778-a1ec-f0785371d1ca"
    table_id = "2440d076-8934-471f-8cbe-51faae387c66"  # brasil table
    
    # Run all tests
    await investigator.test_dataset_fields(dataset_id)
    await investigator.test_table_fields(table_id)
    await investigator.test_nested_bigquery_fields(dataset_id)
    await investigator.explore_dataset_full_schema(dataset_id)
    
    # Print summary
    investigator.print_summary()
    
    print("\n🎯 Next Steps:")
    print("1. If successful fields were found, update GraphQL queries")
    print("2. If no fields found, investigate alternative approaches")
    print("3. Consider using backend API table details for BigQuery refs")


if __name__ == "__main__":
    asyncio.run(main())