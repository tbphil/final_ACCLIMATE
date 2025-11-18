"""
Infrastructure Module
Modular infrastructure data retrieval with pluggable sources

Public API:
    - InfrastructureFetcher: Main orchestrator
    - BoundingBox: Geographic bounds model
    - Data sources: mongodb_aha, custom_upload
    - Router: FastAPI endpoints
"""

from .infrastructure_fetcher import InfrastructureFetcher
from .infrastructure_preparers import (
    prepare_for_frontend,
    prepare_upload_response,
    prepare_stats_response
)
from .infrastructure_router import router
from .data_sources.base import BoundingBox, InfrastructureDataSource
from .data_sources.registry import (
    get_source,
    register_source,
    configure_source,
    list_sources
)

__all__ = [
    # Main orchestrator
    'InfrastructureFetcher',
    
    # Data models
    'BoundingBox',
    'InfrastructureDataSource',
    
    # Registry functions
    'get_source',
    'register_source',
    'configure_source',
    'list_sources',
    
    # Preparers
    'prepare_for_frontend',
    'prepare_upload_response',
    'prepare_stats_response',
    
    # FastAPI router
    'router',
]

# Version info
__version__ = '0.1.0'