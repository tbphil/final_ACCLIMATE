"""
Abstract interface for HBOM data sources
Any HBOM provider must implement this interface

Location: backend/hbom/data_sources/base.py
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class HBOMDataSource(ABC):
    """
    Abstract base class for HBOM data providers
    
    All HBOM sources (MongoDB, JSON files, external APIs) must inherit
    from this class and implement these methods.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize data source with optional configuration
        
        Args:
            config: Provider-specific configuration
        """
        self.config = config or {}
    
    @abstractmethod
    async def fetch_components_by_sector(
        self,
        sector: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch all component nodes for a sector.
        
        Args:
            sector: Infrastructure sector (e.g., "Energy Grid")
        
        Returns:
            List of flat component node dictionaries
        """
        pass
    
    @abstractmethod
    async def fetch_component_by_uuid(
        self,
        uuid: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch single component by UUID.
        
        Args:
            uuid: Component UUID
        
        Returns:
            Component node or None if not found
        """
        pass
    
    @abstractmethod
    async def fetch_descendants(
        self,
        parent_uuid: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch all descendants of a component (recursive children).
        
        Args:
            parent_uuid: UUID of parent component
        
        Returns:
            List of all descendant nodes
        """
        pass
    
    @abstractmethod
    async def fetch_fragilities_by_hazard(
        self,
        hazard: str,
        component_uuids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch fragility curves for a hazard.
        
        Args:
            hazard: Hazard type (e.g., "Wind", "Heat Stress")
            component_uuids: Optional filter to specific components
        
        Returns:
            List of fragility curve documents
        """
        pass
    
    @abstractmethod
    async def fetch_fragilities_for_components(
        self,
        component_uuids: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Fetch all fragility curves for specific components.
        
        Args:
            component_uuids: List of component UUIDs
        
        Returns:
            List of fragility curve documents (all hazards)
        """
        pass
    
    @abstractmethod
    async def get_stats(
        self,
        sector: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get statistics about available data.
        
        Args:
            sector: Optional sector filter
        
        Returns:
            Statistics dictionary
        """
        pass
    
    @abstractmethod
    async def validate_connection(self) -> bool:
        """
        Validate that data source is accessible.
        
        Returns:
            True if connection valid, False otherwise
        """
        pass
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """
        Human-readable name of this data source.
        
        Returns:
            Name like "MongoDB HBOM Baseline" or "JSON File Source"
        """
        pass