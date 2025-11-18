"""
HBOM Data Source Registry
Factory pattern for obtaining HBOM data sources

Location: backend/hbom/data_sources/registry.py
"""

import logging
from typing import Dict, Any, Optional, Type

from .base import HBOMDataSource
from .mongodb_baseline import MongoDBBaselineSource

logger = logging.getLogger(__name__)

# Registry mapping source names to classes
_SOURCES: Dict[str, Type[HBOMDataSource]] = {
    'mongodb_baseline': MongoDBBaselineSource,
}

# Global configuration
_SOURCE_CONFIGS: Dict[str, Dict[str, Any]] = {}


def configure_source(name: str, config: Dict[str, Any]) -> None:
    """
    Set configuration for a data source.
    Call at app startup.
    
    Args:
        name: Source identifier
        config: Configuration dictionary
    """
    if name not in _SOURCES:
        raise ValueError(f"Unknown source: {name}")
    
    _SOURCE_CONFIGS[name] = config
    logger.info(f"Configured HBOM source: {name}")


def get_source(name: str = 'mongodb_baseline', config: Optional[Dict[str, Any]] = None) -> HBOMDataSource:
    """
    Get instance of HBOM data source.
    
    Args:
        name: Source identifier (defaults to 'mongodb_baseline')
        config: Optional config override
    
    Returns:
        Initialized data source instance
    """
    if name not in _SOURCES:
        raise ValueError(f"Unknown HBOM source: {name}")
    
    instance_config = config or _SOURCE_CONFIGS.get(name, {})
    source_class = _SOURCES[name]
    instance = source_class(config=instance_config)
    
    logger.info(f"Created {name} source instance")
    
    return instance


def list_sources() -> list[str]:
    """List all registered HBOM data sources"""
    return list(_SOURCES.keys())