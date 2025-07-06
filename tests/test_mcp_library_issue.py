#!/usr/bin/env python3
"""
Test to reproduce the exact MCP library JSON serialization issue.
"""

import sys
import os
import asyncio

# Add the source directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import before our server to avoid monkey-patch
import json as original_json

# Now import our server (which applies the monkey-patch)
from basedosdados_mcp.server import search_datasets


async def test_mcp_library_json_issue():
    """Test the exact JSON serialization issue from MCP library."""
    
    print("🧪 Testing MCP Library JSON Serialization Issue")
    print("=" * 60)
    
    # Get a real response from our search function
    print("1. Getting response from search_datasets...")
    result = await search_datasets("desmatamento", 1)
    print(f"Response type: {type(result)}")
    print(f"Response length: {len(result)} characters")
    print()
    
    # Simulate the problematic line from MCP library
    # Line 468 in mcp/server/lowlevel/server.py:
    # text=json.dumps(results, indent=2)
    
    print("2. Testing different JSON serialization methods...")
    print()
    
    # Method 1: Our monkey-patched json.dumps (should preserve UTF-8)
    print("Method 1: Monkey-patched json.dumps")
    import json as patched_json
    try:
        patched_result = patched_json.dumps({"text": result}, indent=2)
        print("✅ Monkey-patched version worked")
        # Check for UTF-8 characters
        has_utf8 = any(char in patched_result for char in ["á", "é", "í", "ó", "ú", "ã", "ç", "ô", "ê", "à", "õ"])
        has_unicode_escapes = "\\u00" in patched_result
        print(f"   Contains UTF-8 characters: {has_utf8}")
        print(f"   Contains Unicode escapes: {has_unicode_escapes}")
        if has_unicode_escapes:
            print("   ❌ Still has Unicode escapes!")
            # Show a sample
            sample = patched_result[:200] + "..." if len(patched_result) > 200 else patched_result
            print(f"   Sample: {sample}")
    except Exception as e:
        print(f"❌ Monkey-patched version failed: {e}")
    print()
    
    # Method 2: Original json.dumps (should cause Unicode escapes)
    print("Method 2: Original json.dumps (simulating MCP library)")
    try:
        original_result = original_json.dumps({"text": result}, indent=2)
        print("✅ Original version worked")
        # Check for UTF-8 characters
        has_utf8 = any(char in original_result for char in ["á", "é", "í", "ó", "ú", "ã", "ç", "ô", "ê", "à", "õ"])
        has_unicode_escapes = "\\u00" in original_result
        print(f"   Contains UTF-8 characters: {has_utf8}")
        print(f"   Contains Unicode escapes: {has_unicode_escapes}")
        if has_unicode_escapes:
            print("   ✅ Found the issue! Unicode escapes present")
            # Show a sample of the problem
            sample = original_result[:200] + "..." if len(original_result) > 200 else original_result
            print(f"   Sample: {sample}")
    except Exception as e:
        print(f"❌ Original version failed: {e}")
    print()
    
    # Method 3: Explicit ensure_ascii=False (the fix)
    print("Method 3: json.dumps with ensure_ascii=False (the fix)")
    try:
        fixed_result = original_json.dumps({"text": result}, indent=2, ensure_ascii=False)
        print("✅ Fixed version worked")
        # Check for UTF-8 characters
        has_utf8 = any(char in fixed_result for char in ["á", "é", "í", "ó", "ú", "ã", "ç", "ô", "ê", "à", "õ"])
        has_unicode_escapes = "\\u00" in fixed_result
        print(f"   Contains UTF-8 characters: {has_utf8}")
        print(f"   Contains Unicode escapes: {has_unicode_escapes}")
        if has_utf8 and not has_unicode_escapes:
            print("   ✅ Fix confirmed! UTF-8 preserved, no Unicode escapes")
    except Exception as e:
        print(f"❌ Fixed version failed: {e}")
    print()
    
    print("🔍 Analysis:")
    print("- If Method 2 shows Unicode escapes, that's the MCP library issue")
    print("- If Method 1 still shows Unicode escapes, our monkey-patch isn't working")
    print("- Method 3 should always work as the proper fix")


if __name__ == "__main__":
    asyncio.run(test_mcp_library_json_issue())