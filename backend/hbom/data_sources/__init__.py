"""
HBOM Data Sources
Pluggable data source implementations for HBOM components and fragilities

Location: backend/hbom/data_sources/__init__.py
"""

from .base import HBOMDataSource
from .mongodb_baseline import MongoDBBaselineSource
from .registry import configure_source, get_source, list_sources

__all__ = [
    'HBOMDataSource',
    'MongoDBBaselineSource',
    'configure_source',
    'get_source',
    'list_sources',
]