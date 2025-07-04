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
# Import the server module to ensure tools are registered
from . import server  # noqa: F401


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
    # Create server with production configuration
    server = FastMCP(
        name="Base dos Dados MCP",
        dependencies=[
            "httpx>=0.25.0",
            "pydantic>=2.0.0,<2.11",
            "google-cloud-bigquery>=3.0.0",
        ],
        lifespan=app_lifespan
    )
    
    # Register tools from server module
    # Note: Tools are already registered via decorators in server.py
    # This ensures they're available when the server module is imported
    
    return server


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
        # Create and run the production server
        server = create_production_server()
        server.run()
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
    
    main()


if __name__ == "__main__":
    main()