#!/usr/bin/env python3
"""
Fixed MCP Server Connection Test with proper protocol handling
"""

import asyncio
import json
import subprocess
import sys
import time
from typing import Dict, Any, Optional
import os


class MCPTesterFixed:
    """Test MCP server connection with proper protocol handling."""
    
    def __init__(self, server_command: list, cwd: str):
        self.server_command = server_command
        self.cwd = cwd
        self.process = None
        self.initialized = False
        
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
            await asyncio.sleep(2)
            
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
                    timeout=15.0
                )
                
                if response_line:
                    response = json.loads(response_line.decode().strip())
                    print(f"📥 Received: {json.dumps(response, indent=2)}")
                    return response
                else:
                    print("❌ No response received")
                    return None
                    
            except asyncio.TimeoutError:
                print("⏰ Response timeout (15s)")
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
                    }
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
            self.initialized = True
            
            # Send initialized notification
            initialized_message = {
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            }
            await self.send_message(initialized_message)
            return True
        else:
            print("❌ Initialization failed")
            return False
    
    async def test_list_tools(self) -> bool:
        """Test listing available tools."""
        if not self.initialized:
            print("❌ Server not initialized")
            return False
            
        print("\n🛠️ Testing tool listing...")
        
        tools_message = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
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
            if response and "error" in response:
                print(f"   Error: {response['error']}")
            return False
    
    async def test_list_resources(self) -> bool:
        """Test listing available resources."""
        if not self.initialized:
            print("❌ Server not initialized")
            return False
            
        print("\n📚 Testing resource listing...")
        
        resources_message = {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "resources/list",
            "params": {}
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
            if response and "error" in response:
                print(f"   Error: {response['error']}")
            return False
    
    async def test_search_datasets(self) -> bool:
        """Test the search_datasets tool."""
        if not self.initialized:
            print("❌ Server not initialized")
            return False
            
        print("\n🔍 Testing search_datasets tool...")
        
        search_message = {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "search_datasets",
                "arguments": {
                    "query": "população",
                    "limit": 3
                }
            }
        }
        
        response = await self.send_message(search_message)
        if response and "result" in response:
            print("✅ Search datasets test successful")
            content = response["result"].get("content", [])
            if content:
                print(f"   Found {len(content)} content items")
                for item in content[:2]:  # Show first 2 items
                    print(f"   - {item.get('type', 'unknown')}: {item.get('text', 'no text')[:100]}...")
            return True
        else:
            print("❌ Search datasets test failed")
            if response and "error" in response:
                print(f"   Error: {response['error']}")
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
        
        print("🧪 Starting Fixed MCP Server Connection Test")
        print("=" * 55)
        
        # Start server
        if not await self.start_server():
            return {"server_start": False}
        
        results["server_start"] = True
        
        try:
            # Run tests in sequence
            results["initialization"] = await self.test_initialization()
            
            if results["initialization"]:
                results["list_tools"] = await self.test_list_tools()
                results["list_resources"] = await self.test_list_resources()
                results["search_datasets"] = await self.test_search_datasets()
            else:
                results["list_tools"] = False
                results["list_resources"] = False  
                results["search_datasets"] = False
            
        finally:
            await self.stop_server()
        
        return results


def check_mcp_version():
    """Check MCP library version compatibility."""
    print("🔍 Checking MCP library version...")
    
    try:
        result = subprocess.run(
            ["uv", "run", "python", "-c", "import mcp.server; print(f'MCP Server module: {mcp.server.__file__}')"],
            cwd="/Users/joaoc/Documents/projects/basedosdados_mcp",
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(f"✅ MCP server module accessible: {result.stdout.strip()}")
        else:
            print(f"❌ MCP server import failed: {result.stderr}")
            return False
            
        # Check specific imports
        result2 = subprocess.run(
            ["uv", "run", "python", "-c", "from mcp.server.stdio import stdio_server; print('stdio_server imported successfully')"],
            cwd="/Users/joaoc/Documents/projects/basedosdados_mcp",
            capture_output=True,
            text=True
        )
        
        if result2.returncode == 0:
            print(f"✅ stdio_server import: {result2.stdout.strip()}")
            return True
        else:
            print(f"❌ stdio_server import failed: {result2.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error checking MCP version: {e}")
        return False


async def main():
    """Main test function."""
    print("🔧 Base dos Dados MCP Server Fixed Debug Tool")
    print("=" * 65)
    
    # Check MCP version compatibility
    if not check_mcp_version():
        print("\n❌ MCP compatibility check failed")
        sys.exit(1)
    
    # Test MCP server
    server_command = ["uv", "run", "server.py"]
    cwd = "/Users/joaoc/Documents/projects/basedosdados_mcp"
    
    tester = MCPTesterFixed(server_command, cwd)
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
    
    if all_passed:
        print("\n🎉 All tests passed! Your MCP server should work with Claude Desktop.")
        print("\nNext steps:")
        print("1. Restart Claude Desktop completely (quit and reopen)")
        print("2. Try asking: 'Search for Brazilian population datasets using Base dos Dados'")
        print("3. Look for the basedosdados server in Claude's available tools")
    else:
        print("\n⚠️ Some tests failed. The server may have protocol issues.")
        print("\nTroubleshooting:")
        print("- Check if dependencies match Claude Desktop's expected MCP version")
        print("- Verify the server.py implementation follows current MCP specs")
        print("- Try updating the MCP library: uv add mcp --upgrade")


if __name__ == "__main__":
    asyncio.run(main())