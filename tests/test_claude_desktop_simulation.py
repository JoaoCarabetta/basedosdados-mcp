#!/usr/bin/env python3
"""
Claude Desktop simulation test for UTF-8 encoding issues.

This test simulates the exact MCP protocol communication that Claude Desktop uses,
including the JSON serialization that causes Portuguese characters to be escaped.
"""

import asyncio
import json
import sys
import os
from unittest.mock import AsyncMock, patch
import pytest

# Add the source directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from basedosdados_mcp.server import search_datasets, mcp
from mcp.server.lowlevel.server import Server
from mcp import types


class MCPProtocolSimulator:
    """Simulates the MCP protocol communication like Claude Desktop."""
    
    def __init__(self):
        self.server = None
    
    async def simulate_tool_call(self, tool_name: str, arguments: dict) -> str:
        """
        Simulate a tool call through the MCP protocol, including the JSON serialization
        that causes UTF-8 encoding issues.
        """
        # Create a mock call request with proper method field
        call_request = types.CallToolRequest(
            method="tools/call",
            params=types.CallToolRequestParams(
                name=tool_name,
                arguments=arguments
            )
        )
        
        # Get the tool function directly
        if tool_name == "search_datasets":
            # Call our search_datasets function directly
            result = await search_datasets(**arguments)
        else:
            raise ValueError(f"Unknown tool: {tool_name}")
        
        # Simulate the MCP library's JSON serialization process
        # This is where the encoding issue occurs - the MCP library uses json.dumps
        # without ensure_ascii=False
        
        # Step 1: Simulate how MCP library processes the result
        if isinstance(result, dict):
            # This simulates line 468 in mcp/server/lowlevel/server.py
            # unstructured_content = [types.TextContent(type="text", text=json.dumps(results, indent=2))]
            
            # Import the original json module to bypass our monkey-patch
            import importlib
            original_json = importlib.import_module('json')
            
            # Reset the original dumps function to simulate MCP library behavior
            original_dumps = getattr(original_json, '_original_dumps', None)
            if original_dumps is None:
                # If no monkey-patch backup exists, use the standard library directly
                import builtins
                original_json_module = __import__('json')
                mcp_serialized = original_json_module.dumps(result, indent=2)  # This should cause the issue!
            else:
                mcp_serialized = original_dumps(result, indent=2)  # This should cause the issue!
        else:
            # String result gets wrapped in TextContent
            mcp_serialized = result
        
        # Step 2: Simulate the full MCP response structure
        text_content = types.TextContent(type="text", text=mcp_serialized)
        call_result = types.CallToolResult(content=[text_content])
        server_result = types.ServerResult(call_result)
        
        # Step 3: Simulate final JSON serialization for transport
        # This is what gets sent to Claude Desktop
        transport_json = json.dumps(server_result.model_dump(), indent=2)
        
        return transport_json
    
    async def simulate_claude_desktop_interaction(self, query: str) -> str:
        """
        Simulate a complete Claude Desktop interaction with Portuguese characters.
        """
        # Simulate Claude Desktop calling search_datasets
        arguments = {"query": query, "limit": 5}
        
        response = await self.simulate_tool_call("search_datasets", arguments)
        
        # Parse the response like Claude Desktop would
        response_data = json.loads(response)
        
        # Extract the text content
        if "result" in response_data and "content" in response_data["result"]:
            content_blocks = response_data["result"]["content"]
            if content_blocks and len(content_blocks) > 0:
                return content_blocks[0].get("text", "")
        
        return response


@pytest.mark.asyncio
class TestClaudeDesktopSimulation:
    """Test Claude Desktop simulation for UTF-8 encoding issues."""
    
    async def test_portuguese_characters_encoding_issue(self):
        """Test that reproduces the UTF-8 encoding issue with Portuguese characters."""
        simulator = MCPProtocolSimulator()
        
        # Test with a Portuguese query that should contain accented characters
        query = "desmatamento"
        
        # Simulate the interaction
        response = await simulator.simulate_claude_desktop_interaction(query)
        
        print("🔍 Claude Desktop Simulation Response:")
        print(response)
        print()
        
        # Check for Unicode escape sequences (the problem)
        unicode_issues = []
        problematic_patterns = [
            "\\u00e1",  # á
            "\\u00e9",  # é
            "\\u00ed",  # í
            "\\u00f3",  # ó
            "\\u00fa",  # ú
            "\\u00e3",  # ã
            "\\u00e7",  # ç
            "\\u00f4",  # ô
            "\\u00ea",  # ê
            "\\u00e0",  # à
            "\\u00f5",  # õ
        ]
        
        for pattern in problematic_patterns:
            if pattern in response:
                # Find the actual character that should be there
                char_map = {
                    "\\u00e1": "á", "\\u00e9": "é", "\\u00ed": "í", 
                    "\\u00f3": "ó", "\\u00fa": "ú", "\\u00e3": "ã",
                    "\\u00e7": "ç", "\\u00f4": "ô", "\\u00ea": "ê",
                    "\\u00e0": "à", "\\u00f5": "õ"
                }
                expected_char = char_map.get(pattern, "?")
                unicode_issues.append(f"Found {pattern} instead of '{expected_char}'")
        
        if unicode_issues:
            print("❌ UTF-8 Encoding Issues Found:")
            for issue in unicode_issues:
                print(f"  {issue}")
            print()
            
            # This test should initially fail to demonstrate the problem
            pytest.fail(f"UTF-8 encoding issues detected: {unicode_issues}")
        else:
            print("✅ No UTF-8 encoding issues found!")
    
    async def test_direct_tool_call_utf8_preservation(self):
        """Test that our tool function preserves UTF-8 when called directly."""
        # Call the tool function directly (without MCP protocol)
        result = await search_datasets("desmatamento", 5)
        
        print("🔧 Direct Tool Call Result:")
        print(result)
        print()
        
        # Check that Portuguese characters are preserved in direct calls
        portuguese_chars = ["ã", "é", "í", "ó", "ú", "á", "ç", "ô", "ê", "à", "õ"]
        found_chars = [char for char in portuguese_chars if char in result]
        
        print(f"✅ Portuguese characters found in direct call: {found_chars}")
        
        # This should pass - direct calls preserve UTF-8
        assert len(found_chars) > 0, "No Portuguese characters found in direct tool call"
    
    async def test_json_serialization_comparison(self):
        """Compare different JSON serialization approaches."""
        test_data = {
            "message": "O projeto PRODES realiza o monitoramento por satélites do desmatamento na região da Amazônia."
        }
        
        # Method 1: Default json.dumps (causes the issue)
        default_json = json.dumps(test_data, indent=2)
        
        # Method 2: json.dumps with ensure_ascii=False (the fix)
        utf8_json = json.dumps(test_data, indent=2, ensure_ascii=False)
        
        print("📊 JSON Serialization Comparison:")
        print("Default json.dumps (problematic):")
        print(default_json)
        print()
        print("json.dumps with ensure_ascii=False (fixed):")
        print(utf8_json)
        print()
        
        # Verify the difference
        has_unicode_escapes = "\\u00" in default_json
        preserves_utf8 = "satélites" in utf8_json and "região" in utf8_json
        
        print(f"Default method has Unicode escapes: {has_unicode_escapes}")
        print(f"UTF-8 method preserves characters: {preserves_utf8}")
        
        assert has_unicode_escapes, "Default json.dumps should create Unicode escapes"
        assert preserves_utf8, "UTF-8 method should preserve Portuguese characters"


async def main():
    """Run the simulation manually for debugging."""
    simulator = MCPProtocolSimulator()
    
    print("🧪 Claude Desktop MCP Protocol Simulation")
    print("=" * 50)
    
    # Test with Portuguese query
    query = "desmatamento"
    print(f"Query: {query}")
    print()
    
    try:
        response = await simulator.simulate_claude_desktop_interaction(query)
        print("Response received:")
        print(response[:500] + "..." if len(response) > 500 else response)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    # Run the simulation directly
    asyncio.run(main())