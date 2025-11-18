"""
Infrastructure Data Source Registry
Factory pattern for obtaining the correct infrastructure data source
"""

import logging
from typing import Dict, Any, Optional, Type, List  # ← Add List here

from .base import InfrastructureDataSource
from .mongodb_aha import MongoDBahaSource
from .custom_upload import CustomUploadSource

logger = logging.getLogger(__name__)


# Registry mapping source names to classes
_SOURCES: Dict[str, Type[InfrastructureDataSource]] = {
    'mongodb_aha': MongoDBahaSource,
    'custom_upload': CustomUploadSource,
}


# Global configuration for sources (set at app startup)
_SOURCE_CONFIGS: Dict[str, Dict[str, Any]] = {}


def register_source(
    name: str,
    source_class: Type[InfrastructureDataSource]
) -> None:
    """
    Register a new infrastructure data source
    
    Allows plugins to add custom data sources at runtime
    
    Args:
        name: Unique identifier for this source
        source_class: Class implementing InfrastructureDataSource
    
    Example:
        register_source('external_api', ExternalAPISource)
    """
    if not issubclass(source_class, InfrastructureDataSource):
        raise TypeError(
            f"{source_class.__name__} must inherit from InfrastructureDataSource"
        )
    
    _SOURCES[name] = source_class
    logger.info(f"Registered infrastructure source: {name}")


def configure_source(name: str, config: Dict[str, Any]) -> None:
    """
    Set configuration for a data source
    
    Call this at app startup to configure connections, credentials, etc.
    
    Args:
        name: Source identifier (must be registered)
        config: Configuration dictionary for this source
    
    Example:
        configure_source('mongodb_aha', {
            'mongo_uri': 'mongodb://localhost:27017',
            'database': 'acclimate_db',
            'collection': 'energy_grid'
        })
    """
    if name not in _SOURCES:
        raise ValueError(f"Unknown source: {name}. Register it first.")
    
    _SOURCE_CONFIGS[name] = config
    logger.info(f"Configured source: {name}")


def get_source(name: str, config: Optional[Dict[str, Any]] = None) -> InfrastructureDataSource:
    """
    Get an instance of the specified infrastructure data source
    
    Args:
        name: Source identifier ('mongodb_aha', 'custom_upload', etc.)
        config: Optional config override (uses global config if not provided)
    
    Returns:
        Initialized data source instance
    
    Raises:
        ValueError: If source name is not registered
    
    Example:
        # Use global config (set at startup)
        source = get_source('mongodb_aha')
        
        # Override config for this instance
        source = get_source('custom_upload', {'persist_to_mongo': True})
    """
    if name not in _SOURCES:
        available = ', '.join(_SOURCES.keys())
        raise ValueError(
            f"Unknown infrastructure source: '{name}'. "
            f"Available sources: {available}"
        )
    
    # Use provided config or fall back to global config
    instance_config = config or _SOURCE_CONFIGS.get(name, {})
    
    # Instantiate the source class
    source_class = _SOURCES[name]
    instance = source_class(config=instance_config)
    
    logger.info(f"Created {name} source instance")
    
    return instance


def list_sources() -> List[str]:  # ← List is now imported
    """
    List all registered infrastructure data sources
    
    Returns:
        List of source identifiers
    """
    return list(_SOURCES.keys())


def get_source_info() -> Dict[str, Dict[str, Any]]:
    """
    Get information about all registered sources
    
    Returns:
        Dictionary mapping source names to their metadata
    
    Example output:
        {
            'mongodb_aha': {
                'class': 'MongoDBahaSource',
                'supports_realtime': True,
                'configured': True
            },
            'custom_upload': {
                'class': 'CustomUploadSource',
                'supports_realtime': False,
                'configured': False
            }
        }
    """
    info = {}
    
    for name, source_class in _SOURCES.items():
        # Create temporary instance to get properties
        temp_instance = source_class(config=_SOURCE_CONFIGS.get(name, {}))
        
        info[name] = {
            'class': source_class.__name__,
            'name': temp_instance.source_name,
            'supports_realtime': temp_instance.supports_realtime_updates,
            'configured': name in _SOURCE_CONFIGS
        }
    
    return info


# Initialize default sources on module import
def _initialize_defaults():
    """Register built-in sources"""
    # Already registered in _SOURCES dict above
    logger.info(f"Infrastructure registry initialized with {len(_SOURCES)} sources")


_initialize_defaults()