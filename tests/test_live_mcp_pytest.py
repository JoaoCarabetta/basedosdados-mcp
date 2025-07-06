#!/usr/bin/env python3
"""
Pytest-based live MCP server tests for encoding issues.

Tests actual MCP server endpoints with Portuguese data to verify encoding integrity.
"""

import asyncio
import sys
import os
import pytest

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


# Common encoding corruption patterns
ENCODING_ISSUE_PATTERNS = [
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


@pytest.mark.asyncio
class TestLiveMCPEndpoints:
    """Test live MCP server endpoints for encoding issues."""
    
    async def test_search_datasets_prodes(self):
        """Test the PRODES search that was causing encoding issues."""
        try:
            from basedosdados_mcp.server import search_datasets
            
            result = await search_datasets("PRODES", 5)
            
            assert isinstance(result, str), "search_datasets should return a string"
            assert len(result) > 0, "search_datasets should return non-empty result"
            
            # Check for encoding corruption
            for pattern in ENCODING_ISSUE_PATTERNS:
                assert pattern not in result, f"Found encoding corruption '{pattern}' in PRODES search"
            
            # Verify Portuguese words are correctly encoded
            expected_words = ["satélites", "região", "políticas", "públicas"]
            found_words = [word for word in expected_words if word in result]
            
            assert len(found_words) > 0, f"Expected Portuguese words not found in result. Found: {found_words}"
            
        except ImportError as e:
            pytest.skip(f"Cannot import MCP server module: {e}")
    
    @pytest.mark.parametrize("query", [
        "população",
        "educação", 
        "saúde",
        "satélites",
        "Amazônia",
        "políticas públicas"
    ])
    async def test_search_datasets_portuguese_queries(self, query):
        """Test search_datasets with various Portuguese queries."""
        try:
            from basedosdados_mcp.server import search_datasets
            
            result = await search_datasets(query, 5)
            
            assert isinstance(result, str), f"search_datasets should return string for query: {query}"
            
            # Check for encoding corruption
            for pattern in ENCODING_ISSUE_PATTERNS:
                assert pattern not in result, f"Found encoding corruption '{pattern}' in search for: {query}"
                
        except ImportError:
            pytest.skip("Cannot import MCP server module")
    
    async def test_backend_api_encoding(self):
        """Test backend API response encoding."""
        try:
            from basedosdados_mcp.server import search_backend_api
            
            result = await search_backend_api("PRODES", 5)
            
            assert isinstance(result, dict), "Backend API should return dict"
            
            datasets = result.get("results", [])
            assert len(datasets) > 0, "Backend API should return datasets"
            
            # Check first dataset for encoding issues
            first_dataset = datasets[0]
            name = first_dataset.get("name", "")
            description = first_dataset.get("description", "")
            
            combined_text = name + " " + description
            
            # Check for encoding corruption
            for pattern in ENCODING_ISSUE_PATTERNS:
                assert pattern not in combined_text, f"Found encoding corruption '{pattern}' in backend API"
            
            # Verify some Portuguese characters are present and correct
            portuguese_chars = ["é", "ã", "ç", "á", "ó", "í", "ú", "ê", "ô", "à", "õ"]
            found_chars = [char for char in portuguese_chars if char in combined_text]
            
            # Should find at least some Portuguese characters in Brazilian data descriptions
            assert len(found_chars) > 0, f"No Portuguese characters found in: {combined_text[:100]}..."
            
        except ImportError:
            pytest.skip("Cannot import MCP server module")
    
    async def test_get_dataset_overview_encoding(self):
        """Test get_dataset_overview endpoint encoding."""
        try:
            from basedosdados_mcp.server import get_dataset_overview
            
            # Use a known dataset ID (this might need to be updated based on actual available datasets)
            # For now, we'll test with a placeholder and expect it to handle encoding properly even if not found
            result = await get_dataset_overview("test_dataset_id")
            
            assert isinstance(result, str), "get_dataset_overview should return string"
            
            # Check for encoding corruption
            for pattern in ENCODING_ISSUE_PATTERNS:
                assert pattern not in result, f"Found encoding corruption '{pattern}' in dataset overview"
                
        except ImportError:
            pytest.skip("Cannot import MCP server module")
    
    async def test_get_table_details_encoding(self):
        """Test get_table_details endpoint encoding."""
        try:
            from basedosdados_mcp.server import get_table_details
            
            # Test with placeholder table ID
            result = await get_table_details("test_table_id")
            
            assert isinstance(result, str), "get_table_details should return string"
            
            # Check for encoding corruption
            for pattern in ENCODING_ISSUE_PATTERNS:
                assert pattern not in result, f"Found encoding corruption '{pattern}' in table details"
                
        except ImportError:
            pytest.skip("Cannot import MCP server module")


@pytest.mark.asyncio 
class TestMCPProtocolEncoding:
    """Test MCP protocol-specific encoding scenarios."""
    
    async def test_json_response_encoding(self):
        """Test that MCP responses maintain encoding through JSON serialization."""
        try:
            from basedosdados_mcp.server import search_datasets
            import json
            
            result = await search_datasets("educação", 3)
            
            # Simulate MCP protocol JSON serialization
            json_str = json.dumps(result, ensure_ascii=False)
            deserialized = json.loads(json_str)
            
            assert deserialized == result, "JSON round-trip should preserve content"
            
            # Check for encoding corruption after JSON processing
            for pattern in ENCODING_ISSUE_PATTERNS:
                assert pattern not in deserialized, f"Found encoding corruption '{pattern}' after JSON processing"
                
        except ImportError:
            pytest.skip("Cannot import MCP server module")
    
    async def test_large_response_encoding(self):
        """Test encoding with large responses that might stress the encoding system."""
        try:
            from basedosdados_mcp.server import search_datasets
            
            # Search for a common term that should return many results
            result = await search_datasets("brasil", 20)
            
            assert len(result) > 1000, "Large search should return substantial content"
            
            # Check for encoding corruption in large response
            for pattern in ENCODING_ISSUE_PATTERNS:
                assert pattern not in result, f"Found encoding corruption '{pattern}' in large response"
                
        except ImportError:
            pytest.skip("Cannot import MCP server module")


class TestEncodingHelpers:
    """Test encoding helper functions and utilities."""
    
    def test_encoding_detection_patterns(self):
        """Test that our encoding issue detection patterns are correct."""
        # Test strings that should trigger detection
        corrupted_examples = [
            "monitoramento por satÃ©lites",  # é corrupted
            "desmatamento na regiÃ£o",       # ã corrupted  
            "polÃ­ticas pÃºblicas",          # í and ú corrupted
        ]
        
        for example in corrupted_examples:
            found_patterns = [pattern for pattern in ENCODING_ISSUE_PATTERNS if pattern in example]
            assert len(found_patterns) > 0, f"Should detect corruption in: {example}"
    
    def test_correct_encoding_examples(self):
        """Test that correctly encoded strings don't trigger false positives."""
        correct_examples = [
            "monitoramento por satélites",
            "desmatamento na região", 
            "políticas públicas",
            "educação e saúde",
            "informação da população"
        ]
        
        for example in correct_examples:
            found_patterns = [pattern for pattern in ENCODING_ISSUE_PATTERNS if pattern in example]
            assert len(found_patterns) == 0, f"Should not detect corruption in correct text: {example}"


# Test fixtures
@pytest.fixture
def sample_prodes_description():
    """Sample PRODES description with Portuguese characters."""
    return ("O projeto PRODES realiza o monitoramento por satélites do desmatamento "
            "por corte raso na Amazônia Legal e produz, desde 1988, as taxas anuais "
            "de desmatamento na região, que são usadas pelo governo brasileiro para "
            "o estabelecimento de políticas públicas.")


@pytest.fixture
def sample_corrupted_text():
    """Sample text with encoding corruption for testing detection."""
    return ("O projeto PRODES realiza o monitoramento por satÃ©lites do desmatamento "
            "por corte raso na AmazÃ´nia Legal e produz, desde 1988, as taxas anuais "
            "de desmatamento na regiÃ£o, que sÃ£o usadas pelo governo brasileiro.")


def test_correct_encoding_fixture(sample_prodes_description):
    """Test that correct encoding is preserved in fixtures."""
    for pattern in ENCODING_ISSUE_PATTERNS:
        assert pattern not in sample_prodes_description, f"Fixture should not contain corruption: {pattern}"
    
    # Should contain correct Portuguese characters
    assert "satélites" in sample_prodes_description
    assert "região" in sample_prodes_description
    assert "políticas" in sample_prodes_description


def test_corruption_detection_fixture(sample_corrupted_text):
    """Test that corruption is detected in test fixtures."""
    found_patterns = [pattern for pattern in ENCODING_ISSUE_PATTERNS if pattern in sample_corrupted_text]
    assert len(found_patterns) > 0, "Should detect corruption patterns in corrupted fixture"


if __name__ == "__main__":
    # Run tests if script is executed directly
    pytest.main([__file__, "-v", "--tb=short"])