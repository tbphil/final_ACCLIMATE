"""
Base class for infrastructure data sources
Defines the interface that all infrastructure providers must implement
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from pydantic import BaseModel


class BoundingBox(BaseModel):
    """Geographic bounding box for spatial queries"""
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float


class InfrastructureDataSource(ABC):
    """
    Abstract base class for infrastructure data sources
    
    All infrastructure providers (MongoDB, custom uploads, external APIs)
    must inherit from this class and implement these methods.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the data source with optional configuration
        
        Args:
            config: Provider-specific configuration (connection strings, credentials, etc.)
        """
        self.config = config or {}
    
    @abstractmethod
    async def fetch(
        self,
        bbox: BoundingBox,
        sector: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch infrastructure assets within bounding box
        
        Args:
            bbox: Geographic bounding box to query
            sector: Infrastructure sector (e.g., "Energy Grid")
            filters: Additional filters (component_type, owner, critical, etc.)
        
        Returns:
            List of asset dictionaries matching the query
        """
        pass
    
    @abstractmethod
    async def get_stats(self, sector: str) -> Dict[str, Any]:
        """
        Get statistics about available data
        
        Args:
            sector: Infrastructure sector
        
        Returns:
            Dictionary with stats (total_count, component_types, coverage, etc.)
        """
        pass
    
    @abstractmethod
    async def validate_connection(self) -> bool:
        """
        Validate that the data source is accessible
        
        Returns:
            True if connection is valid, False otherwise
        """
        pass
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """
        Human-readable name of this data source
        
        Returns:
            Name like "MongoDB AHA Core" or "Custom Upload"
        """
        pass
    
    @property
    @abstractmethod
    def supports_realtime_updates(self) -> bool:
        """
        Whether this source supports real-time data updates
        
        Returns:
            True if data can be updated without restart
        """
        pass