"""
Fragility Data Sources
Climate data access for fragility computation

Location: backend/fragility/data_sources/__init__.py
"""

from .base import ClimateDataSource
from .cache_source import CacheClimateSource

__all__ = [
    'ClimateDataSource',
    'CacheClimateSource',
]