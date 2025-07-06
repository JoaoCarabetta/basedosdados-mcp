#!/usr/bin/env python3
"""
Pytest-based encoding tests for Portuguese characters in Base dos Dados MCP server.

This test suite verifies that UTF-8 encoding problems don't occur with Portuguese characters
(e.g., prevents "satÃ©lites" instead of "satélites").
"""

import asyncio
import json
import pytest


# Test data with Portuguese characters that commonly cause encoding issues
PORTUGUESE_TEST_CASES = [
    # Common Portuguese words that appear in Brazilian datasets
    "população",
    "educação", 
    "saúde",
    "região",
    "satélites",  # From the PRODES case
    "desmatamento",  # From the PRODES case
    "Amazônia",
    "monitoramento",
    "políticas",
    "públicas",
    
    # Specific case that's failing
    "PRODES",
    "prodes desmatamento",
    
    # Words with various Portuguese accents
    "informação",
    "situação",
    "operação",
    "posição",
    "órgão",
    "ministério",
    "relatório",
    "província",
    "José",
    "João",
    "São Paulo",
    "Ceará",
    "Paraná",
    "Goiás",
    "Piauí",
    "Maranhão",
    "Pará",
    "Rondônia",
    "Amapá"
]

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


class TestPortugueseEncoding:
    """Test class for Portuguese character encoding."""
    
    def test_utf8_encoding_decoding(self):
        """Test basic UTF-8 encoding/decoding for Portuguese strings."""
        for test_string in PORTUGUESE_TEST_CASES:
            encoded = test_string.encode('utf-8')
            decoded = encoded.decode('utf-8')
            
            assert decoded == test_string, f"UTF-8 round-trip failed for: {test_string}"
    
    def test_json_serialization(self):
        """Test JSON serialization preserves Portuguese characters."""
        for test_string in PORTUGUESE_TEST_CASES:
            json_str = json.dumps(test_string, ensure_ascii=False)
            json_loaded = json.loads(json_str)
            
            assert json_loaded == test_string, f"JSON serialization failed for: {test_string}"
    
    def test_no_encoding_corruption_patterns(self):
        """Test that strings don't contain encoding corruption patterns."""
        for test_string in PORTUGUESE_TEST_CASES:
            for pattern in ENCODING_ISSUE_PATTERNS:
                assert pattern not in test_string, f"Found corruption pattern '{pattern}' in: {test_string}"
    
    @pytest.mark.parametrize("test_string", PORTUGUESE_TEST_CASES)
    def test_individual_portuguese_strings(self, test_string):
        """Test individual Portuguese strings for encoding integrity."""
        # Test UTF-8 encoding
        encoded = test_string.encode('utf-8')
        decoded = encoded.decode('utf-8')
        assert decoded == test_string
        
        # Test JSON serialization
        json_str = json.dumps(test_string, ensure_ascii=False)
        json_loaded = json.loads(json_str)
        assert json_loaded == test_string
        
        # Test no corruption patterns
        for pattern in ENCODING_ISSUE_PATTERNS:
            assert pattern not in test_string


class TestMCPToolArguments:
    """Test MCP tool arguments for encoding issues."""
    
    @pytest.mark.parametrize("query", [
        "população", "educação", "saúde", "PRODES", 
        "prodes desmatamento", "satélites", "Amazônia"
    ])
    def test_search_datasets_arguments(self, query):
        """Test search_datasets tool arguments maintain encoding."""
        args = {"query": query, "limit": 5}
        
        # Test JSON serialization of arguments (simulates MCP protocol)
        json_str = json.dumps(args, ensure_ascii=False)
        json_loaded = json.loads(json_str)
        
        assert json_loaded == args
        assert json_loaded["query"] == query
        
        # Check for corruption patterns in the query
        for pattern in ENCODING_ISSUE_PATTERNS:
            assert pattern not in query


@pytest.mark.asyncio
class TestLiveMCPServer:
    """Test live MCP server responses for encoding issues."""
    
    async def test_search_datasets_prodes(self):
        """Test the specific PRODES search that was causing encoding issues."""
        try:
            # Import the actual MCP server function
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
            
            from basedosdados_mcp.server import search_datasets
            
            result = await search_datasets("PRODES", 5)
            
            # Check that result doesn't contain encoding issues
            for pattern in ENCODING_ISSUE_PATTERNS:
                assert pattern not in result, f"Found encoding issue '{pattern}' in PRODES search result"
            
            # Check that specific Portuguese words are present and correctly encoded
            expected_words = ["satélites", "região", "políticas", "públicas"]
            found_words = [word for word in expected_words if word in result]
            
            # At least some Portuguese words should be present
            assert len(found_words) > 0, "No Portuguese words found in PRODES result"
            
        except ImportError:
            pytest.skip("MCP server module not available for live testing")
    
    async def test_backend_api_encoding(self):
        """Test backend API response encoding."""
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
            
            from basedosdados_mcp.server import search_backend_api
            
            result = await search_backend_api("PRODES", 5)
            
            assert isinstance(result, dict), "Backend API should return a dict"
            
            datasets = result.get("results", [])
            assert len(datasets) > 0, "Backend API should return some datasets"
            
            # Check first dataset for encoding issues
            first_dataset = datasets[0]
            name = first_dataset.get("name", "")
            description = first_dataset.get("description", "")
            
            combined_text = name + " " + description
            
            for pattern in ENCODING_ISSUE_PATTERNS:
                assert pattern not in combined_text, f"Found encoding issue '{pattern}' in backend API response"
                
        except ImportError:
            pytest.skip("MCP server module not available for live testing")


class TestClaudeDesktopIntegration:
    """Test Claude Desktop integration scenarios."""
    
    def test_mcp_protocol_json_encoding(self):
        """Test JSON encoding as used in MCP protocol."""
        # Simulate a typical MCP response with Portuguese content
        mcp_response = {
            "content": [
                {
                    "type": "text",
                    "text": "O projeto PRODES realiza o monitoramento por satélites do desmatamento na região da Amazônia."
                }
            ]
        }
        
        # Test JSON serialization (what MCP protocol does)
        json_str = json.dumps(mcp_response, ensure_ascii=False)
        deserialized = json.loads(json_str)
        
        assert deserialized == mcp_response
        
        text_content = deserialized["content"][0]["text"]
        
        # Check for encoding corruption
        for pattern in ENCODING_ISSUE_PATTERNS:
            assert pattern not in text_content, f"Found encoding issue '{pattern}' in MCP response"
        
        # Verify Portuguese characters are preserved
        assert "satélites" in text_content
        assert "região" in text_content
    
    def test_large_response_encoding(self):
        """Test encoding with large responses (simulating real dataset descriptions)."""
        large_text = " ".join(PORTUGUESE_TEST_CASES * 50)  # Repeat to create large text
        
        # Test JSON serialization
        json_str = json.dumps(large_text, ensure_ascii=False)
        deserialized = json.loads(json_str)
        
        assert deserialized == large_text
        
        # Check for encoding corruption
        for pattern in ENCODING_ISSUE_PATTERNS:
            assert pattern not in deserialized, f"Found encoding issue '{pattern}' in large response"


# Fixtures for test data
@pytest.fixture
def sample_portuguese_text():
    """Fixture providing sample Portuguese text with various accents."""
    return "O projeto PRODES realiza o monitoramento por satélites do desmatamento por corte raso na Amazônia Legal e produz, desde 1988, as taxas anuais de desmatamento na região, que são usadas pelo governo brasileiro para o estabelecimento de políticas públicas."


@pytest.fixture
def sample_dataset_response():
    """Fixture providing a sample dataset response with Portuguese content."""
    return {
        "name": "Desmatamento PRODES",
        "description": "O projeto PRODES realiza o monitoramento por satélites do desmatamento por corte raso na Amazônia Legal e produz, desde 1988, as taxas anuais de desmatamento na região, que são usadas pelo governo brasileiro para o estabelecimento de políticas públicas.",
        "organizations": ["Instituto Nacional de Pesquisas Espaciais (INPE)"],
        "themes": ["Meio Ambiente"],
        "tags": ["conservacao", "desmatamento", "mudancas_climaticas"]
    }


def test_sample_text_encoding(sample_portuguese_text):
    """Test encoding with sample Portuguese text fixture."""
    # Test UTF-8 encoding
    encoded = sample_portuguese_text.encode('utf-8')
    decoded = encoded.decode('utf-8')
    assert decoded == sample_portuguese_text
    
    # Check for corruption patterns
    for pattern in ENCODING_ISSUE_PATTERNS:
        assert pattern not in sample_portuguese_text


def test_sample_dataset_encoding(sample_dataset_response):
    """Test encoding with sample dataset response fixture."""
    # Test JSON serialization
    json_str = json.dumps(sample_dataset_response, ensure_ascii=False)
    deserialized = json.loads(json_str)
    
    assert deserialized == sample_dataset_response
    
    # Check description for encoding issues
    description = deserialized["description"]
    for pattern in ENCODING_ISSUE_PATTERNS:
        assert pattern not in description


if __name__ == "__main__":
    # Run tests if script is executed directly
    pytest.main([__file__, "-v"])