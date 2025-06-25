#!/usr/bin/env python3
"""
MCP Server Connection Test and Debug Tool

This script tests the Base dos Dados MCP server connection and diagnoses issues.
It simulates how Claude Desktop would communicate with the MCP server.
"""

import asyncio
import json
import subprocess
import sys
import time
from typing import Dict, Any, Optional
import tempfile
import os


class MCPTester:
    """Test MCP server connection and functionality."""
    
    def __init__(self, server_command: list, cwd: str):
        self.server_command = server_command
        self.cwd = cwd
        self.process = None
        
    async def start_server(self) -> bool:
        """Start the MCP server process."""
        try:
            print("🚀 Starting MCP server...")
            print(f"Command: {' '.join(self.server_command)}")
            print(f"Working directory: {self.cwd}")
            
            self.process = await asyncio.create_subprocess_exec(
                *self.server_command,
                cwd=self.cwd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Give server time to start
            await asyncio.sleep(1)
            
            if self.process.returncode is not None:
                stderr = await self.process.stderr.read()
                print(f"❌ Server failed to start: {stderr.decode()}")
                return False
                
            print("✅ Server started successfully")
            return True
            
        except Exception as e:
            print(f"❌ Failed to start server: {e}")
            return False
    
    async def send_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Send a JSON-RPC message to the MCP server."""
        if not self.process or not self.process.stdin:
            print("❌ Server not running")
            return None
            
        try:
            # Convert message to JSON-RPC format
            json_message = json.dumps(message) + '\n'
            print(f"📤 Sending: {json_message.strip()}")
            
            # Send message
            self.process.stdin.write(json_message.encode())
            await self.process.stdin.drain()
            
            # Read response with timeout
            try:
                response_line = await asyncio.wait_for(
                    self.process.stdout.readline(), 
                    timeout=10.0
                )
                
                if response_line:
                    response = json.loads(response_line.decode().strip())
                    print(f"📥 Received: {json.dumps(response, indent=2)}")
                    return response
                else:
                    print("❌ No response received")
                    return None
                    
            except asyncio.TimeoutError:
                print("⏰ Response timeout")
                return None
                
        except Exception as e:
            print(f"❌ Error sending message: {e}")
            return None
    
    async def test_initialization(self) -> bool:
        """Test MCP server initialization."""
        print("\n🔧 Testing MCP initialization...")
        
        init_message = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "roots": {
                        "listChanged": True
                    },
                    "sampling": {}
                },
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0.0"
                }
            }
        }
        
        response = await self.send_message(init_message)
        if response and "result" in response:
            print("✅ Initialization successful")
            return True
        else:
            print("❌ Initialization failed")
            return False
    
    async def test_list_tools(self) -> bool:
        """Test listing available tools."""
        print("\n🛠️ Testing tool listing...")
        
        tools_message = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list"
        }
        
        response = await self.send_message(tools_message)
        if response and "result" in response:
            tools = response["result"].get("tools", [])
            print(f"✅ Found {len(tools)} tools:")
            for tool in tools:
                print(f"  - {tool.get('name', 'Unknown')}: {tool.get('description', 'No description')}")
            return True
        else:
            print("❌ Failed to list tools")
            return False
    
    async def test_search_datasets(self) -> bool:
        """Test the search_datasets tool."""
        print("\n🔍 Testing search_datasets tool...")
        
        search_message = {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "search_datasets",
                "arguments": {
                    "query": "população",
                    "limit": 5
                }
            }
        }
        
        response = await self.send_message(search_message)
        if response and "result" in response:
            print("✅ Search datasets test successful")
            return True
        else:
            print("❌ Search datasets test failed")
            return False
    
    async def test_list_resources(self) -> bool:
        """Test listing available resources."""
        print("\n📚 Testing resource listing...")
        
        resources_message = {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "resources/list"
        }
        
        response = await self.send_message(resources_message)
        if response and "result" in response:
            resources = response["result"].get("resources", [])
            print(f"✅ Found {len(resources)} resources:")
            for resource in resources:
                print(f"  - {resource.get('name', 'Unknown')}: {resource.get('uri', 'No URI')}")
            return True
        else:
            print("❌ Failed to list resources")
            return False
    
    async def stop_server(self):
        """Stop the MCP server."""
        if self.process:
            print("\n🛑 Stopping server...")
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5.0)
                print("✅ Server stopped gracefully")
            except asyncio.TimeoutError:
                print("⚠️ Server didn't stop gracefully, killing...")
                self.process.kill()
                await self.process.wait()
    
    async def run_full_test(self) -> Dict[str, bool]:
        """Run all tests."""
        results = {}
        
        print("🧪 Starting MCP Server Connection Test")
        print("=" * 50)
        
        # Start server
        if not await self.start_server():
            return {"server_start": False}
        
        results["server_start"] = True
        
        try:
            # Run tests
            results["initialization"] = await self.test_initialization()
            results["list_tools"] = await self.test_list_tools()
            results["list_resources"] = await self.test_list_resources()
            results["search_datasets"] = await self.test_search_datasets()
            
        finally:
            await self.stop_server()
        
        return results


def check_dependencies():
    """Check if required dependencies are available."""
    print("🔍 Checking dependencies...")
    
    # Check if uv is available
    try:
        result = subprocess.run(["uv", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ uv is available: {result.stdout.strip()}")
        else:
            print("❌ uv is not available")
            return False
    except FileNotFoundError:
        print("❌ uv command not found")
        return False
    
    # Check if server.py exists
    server_path = "/Users/joaoc/Documents/projects/basedosdados_mcp/server.py"
    if os.path.exists(server_path):
        print(f"✅ server.py found at {server_path}")
    else:
        print(f"❌ server.py not found at {server_path}")
        return False
    
    return True


def check_claude_config():
    """Check Claude Desktop configuration."""
    print("\n📋 Checking Claude Desktop configuration...")
    
    config_path = "/Users/joaoc/Library/Application Support/Claude/claude_desktop_config.json"
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        if "mcpServers" in config and "basedosdados" in config["mcpServers"]:
            server_config = config["mcpServers"]["basedosdados"]
            print("✅ Base dos Dados MCP server found in Claude config:")
            print(f"  Command: {server_config.get('command')}")
            print(f"  Args: {server_config.get('args')}")
            print(f"  CWD: {server_config.get('cwd')}")
            return True
        else:
            print("❌ Base dos Dados MCP server not found in Claude config")
            return False
            
    except FileNotFoundError:
        print(f"❌ Claude config file not found at {config_path}")
        return False
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in Claude config: {e}")
        return False


async def main():
    """Main test function."""
    print("🔧 Base dos Dados MCP Server Debug Tool")
    print("=" * 60)
    
    # Check dependencies
    if not check_dependencies():
        print("\n❌ Dependency check failed")
        sys.exit(1)
    
    # Check Claude config
    claude_config_ok = check_claude_config()
    
    # Test MCP server
    server_command = ["uv", "run", "server.py"]
    cwd = "/Users/joaoc/Documents/projects/basedosdados_mcp"
    
    tester = MCPTester(server_command, cwd)
    results = await tester.run_full_test()
    
    # Print summary
    print("\n📊 Test Results Summary")
    print("=" * 30)
    
    all_passed = True
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name}: {status}")
        if not passed:
            all_passed = False
    
    print(f"\nClaude Desktop Config: {'✅ OK' if claude_config_ok else '❌ ISSUE'}")
    
    if all_passed and claude_config_ok:
        print("\n🎉 All tests passed! Your MCP server should work with Claude Desktop.")
        print("\nNext steps:")
        print("1. Restart Claude Desktop")
        print("2. Try asking: 'Search for Brazilian population datasets'")
    else:
        print("\n⚠️ Some tests failed. Check the output above for details.")
        print("\nCommon fixes:")
        print("- Ensure all dependencies are installed: uv sync")
        print("- Check that the server.py file is correct")
        print("- Verify Claude Desktop config file")
        print("- Restart Claude Desktop after making changes")


if __name__ == "__main__":
    asyncio.run(main())