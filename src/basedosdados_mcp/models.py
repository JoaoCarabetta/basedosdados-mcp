from typing import List, Optional
from pydantic import BaseModel

# =============================================================================
# Data Models
# =============================================================================

class DatasetInfo(BaseModel):
    """
    Dataset information model for Base dos Dados.
    
    Represents a collection of related tables containing Brazilian public data,
    typically organized by source organization and theme.
    """
    id: str  # UUID identifier
    name: str  # Human-readable name
    description: Optional[str] = None  # Detailed description
    organization: Optional[str] = None  # Source organization
    themes: List[str] = []  # Thematic categories
    tags: List[str] = []  # Keywords for discovery
    
class TableInfo(BaseModel):
    """
    Table information model for Base dos Dados.
    
    Represents a specific data table within a dataset, containing columns
    and accessible via BigQuery.
    """
    id: str  # UUID identifier
    name: str  # Human-readable name
    description: Optional[str] = None  # Table description
    dataset_id: str  # Parent dataset UUID
    columns: List[str] = []  # Column names

class ColumnInfo(BaseModel):
    """
    Column information model for Base dos Dados.
    
    Represents a data field within a table, with type information
    for BigQuery compatibility.
    """
    id: str  # UUID identifier
    name: str  # Column name
    description: Optional[str] = None  # Column description
    bigquery_type: Optional[str] = None  # BigQuery data type
    table_id: str  # Parent table UUID
