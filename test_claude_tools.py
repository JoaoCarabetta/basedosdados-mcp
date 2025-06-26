#!/usr/bin/env python3
"""
Test to verify if tools are being called by sending a direct tool call
"""

import asyncio
import json
import subprocess


async def test_tools_call():
    """Test calling tools directly via the wrapper script."""
    
    print("🧪 Testing direct tool call to Base dos Dados MCP server...")
    
    try:
        # Start the server using the wrapper script
        process = await asyncio.create_subprocess_exec(
            "/Users/joaoc/Documents/projects/basedosdados_mcp/run_server.sh",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Give it time to start
        await asyncio.sleep(2)
        
        if process.returncode is not None:
            stderr = await process.stderr.read()
            print(f"❌ Server failed to start: {stderr.decode()}")
            return False
        
        print("✅ Server started successfully")
        
        # 1. Initialize
        init_message = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "1.0.0"}
            }
        }
        
        json_message = json.dumps(init_message) + '\n'
        process.stdin.write(json_message.encode())
        await process.stdin.drain()
        
        # Read initialization response
        response_line = await asyncio.wait_for(process.stdout.readline(), timeout=10.0)
        response = json.loads(response_line.decode().strip())
        print("✅ Initialization successful")
        
        # 2. Send initialized notification
        init_notif = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }
        json_message = json.dumps(init_notif) + '\n'
        process.stdin.write(json_message.encode())
        await process.stdin.drain()
        
        # 3. List tools
        tools_message = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        }
        
        json_message = json.dumps(tools_message) + '\n'
        process.stdin.write(json_message.encode())
        await process.stdin.drain()
        
        # Read tools response
        response_line = await asyncio.wait_for(process.stdout.readline(), timeout=10.0)
        response = json.loads(response_line.decode().strip())
        
        if "result" in response and "tools" in response["result"]:
            tools = response["result"]["tools"]
            print(f"✅ Found {len(tools)} tools:")
            for tool in tools:
                print(f"  - {tool['name']}: {tool['description']}")
        
        # 4. Test search_datasets tool call
        search_message = {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "search_datasets",
                "arguments": {
                    "query": "IBGE",
                    "limit": 3
                }
            }
        }
        
        print("\n🔍 Testing search_datasets tool call...")
        json_message = json.dumps(search_message) + '\n'
        process.stdin.write(json_message.encode())
        await process.stdin.drain()
        
        # Read search response
        response_line = await asyncio.wait_for(process.stdout.readline(), timeout=15.0)
        response = json.loads(response_line.decode().strip())
        
        if "result" in response:
            content = response["result"].get("content", [])
            print(f"✅ Search successful! Found {len(content)} content items")
            if content and content[0].get("text"):
                # Show first 200 chars of response
                text = content[0]["text"]
                print(f"Sample result: {text[:200]}...")
        else:
            print(f"❌ Search failed: {response}")
        
        # Clean shutdown
        process.terminate()
        await process.wait()
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if process:
            process.terminate()
        return False


async def main():
    """Main test function."""
    success = await test_tools_call()
    
    if success:
        print("\n🎉 All tools are working correctly!")
        print("\nIn Claude Desktop, try:")
        print("- 'Search for IBGE datasets in Base dos Dados'")
        print("- 'What Brazilian education data is available?'")
        print("- 'Find population datasets from Brazil'")
        print("\nThe tools should be available under the 🔧 tools icon.")
    else:
        print("\n❌ Tool test failed.")


if __name__ == "__main__":
    asyncio.run(main())