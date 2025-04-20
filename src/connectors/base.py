"""
Base connector module defining the abstract interface for database connections.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union

class DatabaseConnector(ABC):
    """Abstract base class for database connections"""
    
    @abstractmethod
    def connect(self) -> Any:
        """Establish connection to the database"""
        pass
    
    @abstractmethod
    def get_schemas(self) -> List[str]:
        """Get list of schemas in the database"""
        pass
    
    @abstractmethod
    def get_tables(self, schema: str) -> List[str]:
        """Get list of tables in a schema"""
        pass
    
    @abstractmethod
    def get_columns(self, schema: str, table: str) -> List[Dict[str, str]]:
        """Get column information for a table"""
        pass
    
    @abstractmethod
    def get_sample_data(self, schema: str, table: str, column: str, sample_size: int = 100) -> List[Any]:
        """Get sample data from a column"""
        pass
    
    @abstractmethod
    def apply_tag(self, schema: str, table: str, column: str, tag: str, tag_value: str) -> bool:
        """Apply a tag to a column"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the database connection"""
        pass