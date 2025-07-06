"""
Base dos Dados MCP Server

A Model Context Protocol server that provides AI-optimized access to 
Base dos Dados, Brazil's largest open data platform.
"""

__version__ = "0.1.0"
__author__ = "João Carabetta"
__email__ = "joao.carabetta@gmail.com"

# Make key components available at package level
from .server import mcp

__all__ = ["mcp", "__version__"]