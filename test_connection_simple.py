#!/usr/bin/env python3
"""
Simple MCP connection test using the wrapper script
"""

import asyncio
import json
import subprocess
import sys


async def test_wrapper_script():
    """Test the wrapper script with MCP protocol."""
    
    print("🧪 Testing wrapper script with MCP protocol...")
    
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
        
        # Send initialization message
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
        
        # Read response
        try:
            response_line = await asyncio.wait_for(process.stdout.readline(), timeout=10.0)
            if response_line:
                response = json.loads(response_line.decode().strip())
                print("✅ Initialization successful!")
                print(f"Server info: {response.get('result', {}).get('serverInfo', {})}")
                
                # Clean shutdown
                process.terminate()
                await process.wait()
                return True
            else:
                print("❌ No response received")
                return False
        except asyncio.TimeoutError:
            print("❌ Response timeout")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


async def main():
    """Main test function."""
    success = await test_wrapper_script()
    
    if success:
        print("\n🎉 Wrapper script test passed!")
        print("Configuration should work with Claude Desktop.")
        print("\nNext steps:")
        print("1. Restart Claude Desktop completely")
        print("2. Check the logs: tail -f ~/Library/Logs/Claude/mcp-server-basedosdados.log")
        print("3. Try asking Claude Desktop to search for datasets")
    else:
        print("\n❌ Wrapper script test failed.")
        print("Check the error messages above for troubleshooting.")


if __name__ == "__main__":
    asyncio.run(main())