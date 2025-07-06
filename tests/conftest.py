#!/usr/bin/env python3
"""
Pytest configuration and shared fixtures for Base dos Dados MCP encoding tests.
"""

import pytest
import sys
import os

# Add src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture(scope="session")
def encoding_issue_patterns():
    """Common encoding corruption patterns to test for."""
    return [
        "Ã©",  # é corrupted
        "Ã¡",  # á corrupted  
        "Ã­",  # í corrupted
        "Ã³",  # ó corrupted
        "Ãº",  # ú corrupted
        "Ã£",  # ã corrupted
        "Ã§",  # ç corrupted
        "Ã´",  # ô corrupted
        "Ãª",  # ê corrupted
        "Ã ",  # à corrupted
        "Ãµ",  # õ corrupted
    ]


@pytest.fixture(scope="session")
def portuguese_test_words():
    """Common Portuguese words used in Brazilian datasets."""
    return [
        "população",
        "educação", 
        "saúde",
        "região",
        "satélites",
        "desmatamento",
        "Amazônia",
        "monitoramento",
        "políticas",
        "públicas",
        "informação",
        "situação",
        "operação",
        "posição",
        "órgão",
        "ministério",
        "relatório",
        "província"
    ]


@pytest.fixture
def sample_dataset_response():
    """Sample dataset response with Portuguese content."""
    return {
        "name": "Desmatamento PRODES",
        "description": ("O projeto PRODES realiza o monitoramento por satélites "
                       "do desmatamento por corte raso na Amazônia Legal e produz, "
                       "desde 1988, as taxas anuais de desmatamento na região, que "
                       "são usadas pelo governo brasileiro para o estabelecimento "
                       "de políticas públicas."),
        "organizations": ["Instituto Nacional de Pesquisas Espaciais (INPE)"],
        "themes": ["Meio Ambiente"],
        "tags": ["conservacao", "desmatamento", "mudancas_climaticas", "uso_da_terra"],
        "temporal_coverage": ["2000 - 2022"],
        "spatial_coverage": ["Brasil"]
    }


@pytest.fixture
def mcp_server_available():
    """Check if MCP server modules are available for testing."""
    try:
        import basedosdados_mcp.server
        return True
    except ImportError:
        return False


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", 
        "live: mark test as requiring live MCP server (may be slow)"
    )
    config.addinivalue_line(
        "markers",
        "encoding: mark test as encoding-related"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers automatically."""
    for item in items:
        # Mark tests that use asyncio
        if "async" in item.nodeid:
            item.add_marker(pytest.mark.asyncio)
        
        # Mark tests that test live endpoints
        if "live" in item.nodeid or "mcp" in item.nodeid:
            item.add_marker(pytest.mark.live)
        
        # Mark all tests as encoding-related
        item.add_marker(pytest.mark.encoding)


@pytest.fixture
def check_encoding_corruption():
    """Helper function to check for encoding corruption in text."""
    def _check(text, patterns):
        """Check if text contains any corruption patterns."""
        found_patterns = [pattern for pattern in patterns if pattern in text]
        return found_patterns
    return _check