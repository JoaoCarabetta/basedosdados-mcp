#!/usr/bin/env python3
"""
Production entry point for Base dos Dados MCP Server.

This module provides the main entry point for running the Base dos Dados
Model Context Protocol server in production environments.
"""

import logging
import os
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP
# Import the configured server with tools already registered
from .server import mcp as configured_server


# =============================================================================
# Configuration and Context
# =============================================================================

@dataclass
class AppContext:
    """Application context for managing server state and resources."""
    environment: str
    log_level: str
    api_endpoint: str


@asynccontextmanager
async def app_lifespan(mcp_server: FastMCP):  # noqa: ARG001
    """
    Manage the application lifecycle with proper startup and shutdown.
    
    This context manager handles:
    - Environment configuration
    - Logging setup
    - Resource initialization
    - Graceful shutdown
    """
    # Environment configuration
    environment = os.getenv("ENVIRONMENT", "production")
    log_level = os.getenv("LOG_LEVEL", "INFO")
    api_endpoint = os.getenv("BD_API_ENDPOINT", "https://backend.basedosdados.org/graphql")
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger("basedosdados_mcp")
    logger.info(f"Starting Base dos Dados MCP Server (env: {environment})")
    logger.info(f"API endpoint: {api_endpoint}")
    
    try:
        # Yield application context
        yield AppContext(
            environment=environment,
            log_level=log_level,
            api_endpoint=api_endpoint
        )
    except Exception as e:
        logger.error(f"Error during server execution: {e}")
        raise
    finally:
        logger.info("Shutting down Base dos Dados MCP Server")


# =============================================================================
# Production Server Setup
# =============================================================================

def create_production_server() -> FastMCP:
    """
    Create a production-ready FastMCP server with proper configuration.
    
    Returns:
        FastMCP: Configured server instance ready for production deployment
    """
    # Use the server instance from server.py where tools are already registered
    # Add production configuration
    configured_server._lifespan = app_lifespan
    
    return configured_server


# =============================================================================
# Entry Points
# =============================================================================

def main() -> None:
    """
    Main entry point for the production server.
    
    This function is called when the package is executed as a script
    or when installed via pip and run as `basedosdados-mcp`.
    """
    try:
        # Use the configured server directly instead of creating a new one
        configured_server.run()
    except KeyboardInterrupt:
        logging.getLogger("basedosdados_mcp").info("Server interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.getLogger("basedosdados_mcp").error(f"Server failed to start: {e}")
        sys.exit(1)


def dev_main() -> None:
    """
    Development entry point with additional debugging features.
    
    This can be used for development and testing scenarios.
    """
    # Set development defaults
    os.environ.setdefault("ENVIRONMENT", "development")
    os.environ.setdefault("LOG_LEVEL", "DEBUG")
    
    # Use the configured server directly
    configured_server.run()


if __name__ == "__main__":
    main()