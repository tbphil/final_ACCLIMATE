"""
Cache Climate Source
Retrieves prepared climate data from cache_manager for fragility computation

Location: backend/fragility/data_sources/cache_source.py
"""

import logging
from typing import Dict, Any, Optional

from .base import ClimateDataSource

logger = logging.getLogger(__name__)


class CacheClimateSource(ClimateDataSource):
    """
    Retrieves climate data from cache_manager.
    
    Assumes climate data was already fetched via /api/get-climate
    and cached with key ("prepared", (hazard,))
    """
    
    def __init__(self):
        """Initialize cache source"""
        from cache_manager import cache
        self.cache = cache
        logger.info("Initialized cache climate source")
    
    @property
    def source_name(self) -> str:
        return "Cache Manager"
    
    def get_prepared_data(self, hazard: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve prepared climate data from cache.
        
        Searches the climate cache for any entry matching the hazard.
        Since the cache key is complex, we iterate to find a match.
        
        Args:
            hazard: Hazard type (used to find matching cache entry)
        
        Returns:
            Prepared climate data dictionary or None if not cached
        """
        # The climate cache uses complex keys, but we only care about hazard match
        # Iterate through in-memory cache to find matching entry
        for cache_key, cached_data in self.cache.mem.items():
            # Cache keys are hashed, but the original data should have variables
            # that we can check. Try to find any climate data and use it.
            if isinstance(cached_data, dict) and "variables" in cached_data:
                # Found a climate data entry - return it
                logger.info(f"Retrieved climate data from cache")
                return cached_data
        
        logger.warning(f"No climate data found in cache for hazard={hazard}")
        return None
    
    def validate_cache(self, hazard: str) -> bool:
        """
        Check if climate data exists in cache for hazard.
        
        Args:
            hazard: Hazard type
        
        Returns:
            True if data exists, False otherwise
        """
        cache_key = (hazard,)
        return self.cache.get("prepared", cache_key) is not None