#!/usr/bin/env python3
"""
Base dos Dados MCP Server

A Model Context Protocol (MCP) server that provides access to Base dos Dados, Brazil's open data platform.

This server connects to the Base dos Dados GraphQL API to provide metadata about Brazilian public datasets,
including information about datasets, tables, columns, and their relationships. It enables users to:

- Search for datasets by name, theme, or organization
- Get detailed information about specific datasets and tables  
- Generate BigQuery SQL queries for data access
- Browse the complete metadata catalog

GraphQL API Endpoint: https://backend.basedosdados.org/graphql
Base dos Dados Website: https://basedosdados.org

Usage Example:
    # Search for population datasets
    search_datasets(query="população", theme="Demografia")
    
    # Get dataset details
    get_dataset_info(dataset_id="br_ibge_populacao")
    
    # Generate SQL for a table
    generate_sql_query(table_id="municipio", limit=100)

Note: This server provides metadata access only. To query actual data, use the generated
BigQuery SQL statements with appropriate credentials.
"""

import asyncio
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import ServerCapabilities
from mcp.server.lowlevel import NotificationOptions

from .server import server
from .resources import handle_list_resources, handle_read_resource
from .tools import handle_list_tools, handle_call_tool

# =============================================================================
# Server Initialization and Main Entry Point
# =============================================================================

async def main():
    """
    Main entry point for the MCP server.
    
    Initializes the server with stdio communication and runs the event loop.
    """
    # Register capabilities
    server.list_resources()(handle_list_resources)
    server.read_resource()(handle_read_resource)
    server.list_tools()(handle_list_tools)
    server.call_tool()(handle_call_tool)

    # Get the registered capabilities
    capabilities = server.get_capabilities(NotificationOptions(), {})

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="basedosdados-mcp",
                server_version="0.1.0",
                capabilities=capabilities,
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())
