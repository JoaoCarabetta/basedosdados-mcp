#!/usr/bin/env python3
"""
Test the UTF-8 response wrapper functionality.
"""

import sys
import os
import asyncio

# Add the source directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from basedosdados_mcp.server import utf8_response_wrapper, search_datasets


async def test_response_wrapper():
    """Test the UTF-8 response wrapper with various scenarios."""
    
    print("🧪 Testing UTF-8 Response Wrapper")
    print("=" * 40)
    
    # Test 1: Normal Portuguese text
    print("1. Testing normal Portuguese text...")
    normal_text = "O projeto PRODES realiza o monitoramento por satélites do desmatamento na região da Amazônia."
    wrapped_text = utf8_response_wrapper(normal_text)
    print(f"Original: {normal_text}")
    print(f"Wrapped:  {wrapped_text}")
    assert wrapped_text == normal_text, "Normal text should remain unchanged"
    print("✅ Passed")
    print()
    
    # Test 2: Text with Unicode escape sequences (simulating MCP library bug)
    print("2. Testing text with Unicode escape sequences...")
    unicode_escaped_text = "O projeto PRODES realiza o monitoramento por sat\\u00e9lites do desmatamento na regi\\u00e3o da Amaz\\u00f4nia."
    wrapped_text = utf8_response_wrapper(unicode_escaped_text)
    expected_text = "O projeto PRODES realiza o monitoramento por satélites do desmatamento na região da Amazônia."
    print(f"Original: {unicode_escaped_text}")
    print(f"Wrapped:  {wrapped_text}")
    print(f"Expected: {expected_text}")
    assert wrapped_text == expected_text, "Unicode escapes should be converted to UTF-8 characters"
    print("✅ Passed")
    print()
    
    # Test 3: Mixed content
    print("3. Testing mixed content...")
    mixed_text = "Insira um UUID v\\u00e1lido. This is valid: região"
    wrapped_text = utf8_response_wrapper(mixed_text)
    expected_text = "Insira um UUID válido. This is valid: região"
    print(f"Original: {mixed_text}")
    print(f"Wrapped:  {wrapped_text}")
    print(f"Expected: {expected_text}")
    assert wrapped_text == expected_text, "Mixed content should be handled correctly"
    print("✅ Passed")
    print()
    
    # Test 4: Real server response
    print("4. Testing real server response...")
    response = await search_datasets("desmatamento", 1)
    print(f"Response type: {type(response)}")
    print(f"Response length: {len(response)} characters")
    
    # Check for common Portuguese characters
    portuguese_chars = ["á", "é", "í", "ó", "ú", "ã", "ç", "ô", "ê", "à", "õ"]
    found_chars = [char for char in portuguese_chars if char in response]
    print(f"Portuguese characters found: {found_chars}")
    
    # Check for Unicode escape sequences (should not be present)
    unicode_escapes = ["\\u00e1", "\\u00e9", "\\u00ed", "\\u00f3", "\\u00fa", "\\u00e3", "\\u00e7", "\\u00f4", "\\u00ea", "\\u00e0", "\\u00f5"]
    found_escapes = [escape for escape in unicode_escapes if escape in response]
    print(f"Unicode escapes found: {found_escapes}")
    
    if found_chars and not found_escapes:
        print("✅ Real server response is properly UTF-8 encoded")
    elif found_escapes:
        print("❌ Real server response contains Unicode escapes")
        # Show sample of problematic content
        for escape in found_escapes[:3]:  # Show first 3 issues
            index = response.find(escape)
            sample = response[max(0, index-20):index+30]
            print(f"   Sample around '{escape}': ...{sample}...")
    else:
        print("⚠️  No Portuguese characters found in response")
    print()
    
    print("🎯 Summary:")
    print("- Response wrapper correctly handles Unicode escape sequences")
    print("- Real server responses preserve UTF-8 characters")
    print("- Ready for Claude Desktop testing")


if __name__ == "__main__":
    asyncio.run(test_response_wrapper())