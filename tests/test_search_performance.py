#!/usr/bin/env python3
"""
Performance test suite for search_datasets API optimization.

This test suite benchmarks response times and monitors performance
to ensure sub-1-second response times for high-volume results.
"""

import asyncio
import time
import sys
import os
from typing import Dict, List, Tuple
import statistics

# Add the source directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from basedosdados_mcp.server import search_datasets, search_backend_api, enrich_datasets_with_comprehensive_data


class PerformanceMonitor:
    """Monitor and track performance metrics for search operations."""
    
    def __init__(self):
        self.results: List[Dict] = []
    
    def record_timing(self, operation: str, duration: float, result_count: int = 0, 
                     success: bool = True, error: str = None):
        """Record a performance measurement."""
        self.results.append({
            'operation': operation,
            'duration': duration,
            'result_count': result_count,
            'success': success,
            'error': error,
            'timestamp': time.time()
        })
    
    def get_stats(self, operation: str = None) -> Dict:
        """Get performance statistics for operations."""
        if operation:
            data = [r for r in self.results if r['operation'] == operation]
        else:
            data = self.results
        
        if not data:
            return {}
        
        durations = [r['duration'] for r in data if r['success']]
        if not durations:
            return {'error': 'No successful operations recorded'}
        
        return {
            'count': len(durations),
            'mean': statistics.mean(durations),
            'median': statistics.median(durations),
            'min': min(durations),
            'max': max(durations),
            'p95': statistics.quantiles(durations, n=20)[18] if len(durations) > 5 else max(durations),
            'success_rate': len(durations) / len(data),
            'total_results': sum(r['result_count'] for r in data if r['success'])
        }


async def time_operation(func, *args, **kwargs) -> Tuple[float, any, str]:
    """Time an async operation and return duration, result, and any error."""
    start_time = time.time()
    error = None
    result = None
    
    try:
        result = await func(*args, **kwargs)
    except Exception as e:
        error = str(e)
    
    duration = time.time() - start_time
    return duration, result, error


async def test_backend_api_performance(monitor: PerformanceMonitor):
    """Test backend API performance with various queries and limits."""
    print("🔍 Testing Backend API Performance...")
    
    test_cases = [
        ("ibge", 5),
        ("ibge", 10),
        ("ibge", 15),
        ("saude", 10),
        ("educacao", 10),
        ("covid", 5),
        ("economia", 15),
    ]
    
    for query, limit in test_cases:
        duration, result, error = await time_operation(search_backend_api, query, limit)
        
        result_count = len(result.get('results', [])) if result else 0
        success = error is None
        
        monitor.record_timing(
            f"backend_api_{limit}",
            duration,
            result_count,
            success,
            error
        )
        
        status = "✅" if success else "❌"
        print(f"  {status} {query} (limit={limit}): {duration:.2f}s, {result_count} results")
        
        if error:
            print(f"    Error: {error}")


async def test_enrichment_performance(monitor: PerformanceMonitor):
    """Test GraphQL enrichment performance with real dataset IDs."""
    print("\n📊 Testing GraphQL Enrichment Performance...")
    
    # First get some dataset IDs from backend search
    backend_result = await search_backend_api("ibge", 5)
    dataset_ids = [d.get('id', '') for d in backend_result.get('results', []) if d.get('id')]
    
    if not dataset_ids:
        print("❌ No dataset IDs found for enrichment testing")
        return
    
    # Test enrichment with different batch sizes
    test_batches = [
        dataset_ids[:1],   # 1 dataset
        dataset_ids[:3],   # 3 datasets  
        dataset_ids[:5],   # 5 datasets
    ]
    
    for i, batch in enumerate(test_batches, 1):
        duration, result, error = await time_operation(
            enrich_datasets_with_comprehensive_data, 
            batch
        )
        
        result_count = len(result) if result else 0
        success = error is None
        
        monitor.record_timing(
            f"enrichment_{len(batch)}",
            duration,
            result_count,
            success,
            error
        )
        
        status = "✅" if success else "❌"
        print(f"  {status} Batch {i} ({len(batch)} datasets): {duration:.2f}s, {result_count} enriched")
        
        if error:
            print(f"    Error: {error}")


async def test_full_search_performance(monitor: PerformanceMonitor):
    """Test complete search_datasets performance with current implementation."""
    print("\n🚀 Testing Full search_datasets Performance...")
    
    test_cases = [
        ("ibge", 5),
        ("ibge", 10),
        ("ibge", 15),
        ("saude", 10),
        ("educacao", 5),
        ("covid", 3),
    ]
    
    for query, limit in test_cases:
        duration, result, error = await time_operation(search_datasets, query, limit)
        
        # Count results by looking for dataset entries
        result_count = result.count("## ") if result and isinstance(result, str) else 0
        success = error is None and duration < 10  # Consider >10s as timeout
        
        monitor.record_timing(
            f"full_search_{limit}",
            duration,
            result_count,
            success,
            error
        )
        
        status = "✅" if success else "❌"
        timeout_warning = " ⚠️  TIMEOUT" if duration > 5 else ""
        print(f"  {status} {query} (limit={limit}): {duration:.2f}s, {result_count} datasets{timeout_warning}")
        
        if error:
            print(f"    Error: {error}")
        
        # Stop testing if we hit a major timeout
        if duration > 15:
            print("    ⚠️  Stopping due to excessive timeout")
            break


async def test_concurrent_performance(monitor: PerformanceMonitor):
    """Test concurrent request handling."""
    print("\n⚡ Testing Concurrent Performance...")
    
    async def single_request():
        return await time_operation(search_datasets, "ibge", 5)
    
    # Test 3 concurrent requests
    start_time = time.time()
    results = await asyncio.gather(*[single_request() for _ in range(3)])
    total_duration = time.time() - start_time
    
    successful_results = [r for r in results if r[2] is None]  # No error
    avg_individual = statistics.mean([r[0] for r in successful_results]) if successful_results else 0
    
    monitor.record_timing(
        "concurrent_3",
        total_duration,
        len(successful_results),
        len(successful_results) > 0,
        f"Failed: {3 - len(successful_results)}" if len(successful_results) < 3 else None
    )
    
    print(f"  Concurrent requests (3): {total_duration:.2f}s total, {avg_individual:.2f}s avg individual")
    print(f"  Success rate: {len(successful_results)}/3")


def print_performance_summary(monitor: PerformanceMonitor):
    """Print a comprehensive performance summary."""
    print("\n" + "="*60)
    print("📈 PERFORMANCE SUMMARY")
    print("="*60)
    
    # Backend API stats
    backend_stats = monitor.get_stats("backend_api_10")
    if backend_stats:
        print(f"\n🔍 Backend API (10 results):")
        print(f"  Mean: {backend_stats['mean']:.2f}s")
        print(f"  P95:  {backend_stats.get('p95', 0):.2f}s")
        print(f"  Success: {backend_stats['success_rate']:.1%}")
    
    # Enrichment stats
    enrichment_stats = monitor.get_stats("enrichment_5")
    if enrichment_stats:
        print(f"\n📊 GraphQL Enrichment (5 datasets):")
        print(f"  Mean: {enrichment_stats['mean']:.2f}s")
        print(f"  P95:  {enrichment_stats.get('p95', 0):.2f}s")
        print(f"  Success: {enrichment_stats['success_rate']:.1%}")
    
    # Full search stats
    full_stats = monitor.get_stats("full_search_10")
    if full_stats:
        print(f"\n🚀 Full Search (10 results):")
        print(f"  Mean: {full_stats['mean']:.2f}s")
        print(f"  P95:  {full_stats.get('p95', 0):.2f}s")
        print(f"  Success: {full_stats['success_rate']:.1%}")
        
        # Performance assessment
        target_met = full_stats['mean'] < 1.0
        status = "✅ TARGET MET" if target_met else "❌ NEEDS OPTIMIZATION"
        print(f"  Target (<1s): {status}")
    
    # Overall assessment
    print(f"\n🎯 OPTIMIZATION NEEDS:")
    all_full_search = [r for r in monitor.results if r['operation'].startswith('full_search')]
    if all_full_search:
        slow_searches = [r for r in all_full_search if r['duration'] > 1.0]
        print(f"  Searches >1s: {len(slow_searches)}/{len(all_full_search)}")
        timeout_searches = [r for r in all_full_search if r['duration'] > 5.0]
        print(f"  Timeouts >5s: {len(timeout_searches)}/{len(all_full_search)}")
        
        if len(slow_searches) > len(all_full_search) * 0.2:  # >20% slow
            print("  🚨 CRITICAL: Significant performance issues detected")
        elif len(slow_searches) > 0:
            print("  ⚠️  WARNING: Some slow responses detected")
        else:
            print("  ✅ GOOD: Performance targets met")


async def main():
    """Run comprehensive performance testing."""
    print("🧪 Base dos Dados MCP - Search Performance Test Suite")
    print("=" * 60)
    print("Target: Sub-1-second response times for search_datasets")
    print("Testing current implementation before optimization...")
    
    monitor = PerformanceMonitor()
    
    # Run all performance tests
    await test_backend_api_performance(monitor)
    await test_enrichment_performance(monitor)
    await test_full_search_performance(monitor)
    await test_concurrent_performance(monitor)
    
    # Print comprehensive summary
    print_performance_summary(monitor)
    
    print(f"\n📊 Total operations tested: {len(monitor.results)}")
    print("Performance testing complete!")


if __name__ == "__main__":
    asyncio.run(main())