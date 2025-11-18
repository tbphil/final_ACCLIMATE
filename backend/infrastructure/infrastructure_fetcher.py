"""
Infrastructure Fetcher
Main orchestrator for retrieving infrastructure data from any registered source
"""

import logging
from typing import List, Dict, Any, Optional

from .data_sources.registry import get_source
from .data_sources.base import BoundingBox

logger = logging.getLogger(__name__)


class InfrastructureFetcher:
    """
    Orchestrates infrastructure data retrieval from multiple sources
    
    Handles source selection, data retrieval, and result aggregation
    """
    
    def __init__(self, default_source: str = 'mongodb_aha'):
        """
        Initialize fetcher with default data source
        
        Args:
            default_source: Default source to use ('mongodb_aha', 'custom_upload', etc.)
        """
        self.default_source = default_source
        self._active_source = None
        logger.info(f"InfrastructureFetcher initialized (default: {default_source})")
    
    async def fetch_infrastructure(
        self,
        bbox: BoundingBox,
        sector: str,
        source_name: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Fetch infrastructure assets from specified or default source
        
        Args:
            bbox: Geographic bounding box to query
            sector: Infrastructure sector (e.g., "Energy Grid")
            source_name: Which source to use (defaults to default_source)
            filters: Additional query filters
        
        Returns:
            Dictionary containing:
                - infrastructure: List of asset dictionaries
                - source: Name of data source used
                - bbox: Bounding box queried
                - count: Number of assets returned
                - stats: Additional metadata
        """
        # Determine which source to use
        source_name = source_name or self.default_source
        
        try:
            # Get source instance
            source = get_source(source_name)
            self._active_source = source
            
            # Validate connection
            if not await source.validate_connection():
                raise ConnectionError(f"Cannot connect to {source.source_name}")
            
            logger.info(f"Fetching from {source.source_name} for sector={sector}")
            
            # Fetch data
            assets = await source.fetch(bbox, sector, filters)
            
            # Get source stats
            stats = await source.get_stats(sector)
            
            result = {
                'infrastructure': assets,
                'source': source.source_name,
                'source_type': source_name,
                'bbox': {
                    'min_lat': bbox.min_lat,
                    'max_lat': bbox.max_lat,
                    'min_lon': bbox.min_lon,
                    'max_lon': bbox.max_lon
                },
                'count': len(assets),
                'sector': sector,
                'filters_applied': filters or {},
                'stats': stats
            }
            
            logger.info(f"Successfully fetched {len(assets)} assets from {source.source_name}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching infrastructure from {source_name}: {e}")
            raise
    
    async def fetch_by_uuid(
        self,
        uuid: str,
        source_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch single asset by UUID
        
        Args:
            uuid: Asset UUID
            source_name: Which source to query (defaults to default_source)
        
        Returns:
            Asset dictionary or None if not found
        """
        source_name = source_name or self.default_source
        
        try:
            source = get_source(source_name)
            
            # Check if source supports UUID lookup
            if hasattr(source, 'get_by_uuid'):
                asset = await source.get_by_uuid(uuid)
                logger.info(f"Fetched asset {uuid} from {source.source_name}")
                return asset
            else:
                logger.warning(f"{source.source_name} does not support UUID lookup")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching asset {uuid}: {e}")
            raise
    
    async def search_by_name(
        self,
        name: str,
        source_name: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search assets by name
        
        Args:
            name: Search term
            source_name: Which source to query
            limit: Maximum results
        
        Returns:
            List of matching assets
        """
        source_name = source_name or self.default_source
        
        try:
            source = get_source(source_name)
            
            # Check if source supports search
            if hasattr(source, 'search_by_name'):
                results = await source.search_by_name(name, limit)
                logger.info(f"Search '{name}' returned {len(results)} results")
                return results
            else:
                logger.warning(f"{source.source_name} does not support name search")
                return []
                
        except Exception as e:
            logger.error(f"Error searching by name '{name}': {e}")
            raise
    
    async def get_available_sources(self) -> Dict[str, Any]:
        """
        Get information about all available data sources
        
        Returns:
            Dictionary with source names and their capabilities
        """
        from .data_sources.registry import get_source_info
        return get_source_info()
    
    async def switch_source(self, source_name: str) -> bool:
        """
        Switch to a different data source
        
        Args:
            source_name: Name of source to switch to
        
        Returns:
            True if switch successful
        """
        try:
            source = get_source(source_name)
            
            if await source.validate_connection():
                self.default_source = source_name
                self._active_source = source
                logger.info(f"Switched to source: {source_name}")
                return True
            else:
                logger.error(f"Cannot switch to {source_name}: connection invalid")
                return False
                
        except Exception as e:
            logger.error(f"Error switching to {source_name}: {e}")
            return False
    
    async def upload_custom_file(
        self,
        file_content: bytes,
        filename: str,
        persist_to_mongo: bool = False,
        persist_to_cache: bool = True,
        required_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Handle custom file upload
        
        Args:
            file_content: Raw file bytes
            filename: Original filename
            persist_to_mongo: Whether to save to MongoDB
            persist_to_cache: Whether to save to cache
            required_columns: Required column names
        
        Returns:
            Upload result with upload_id, stats, and any errors
        """
        try:
            # Get or create custom upload source
            config = {
                'persist_to_mongo': persist_to_mongo,
                'cache_manager': self._get_cache_manager() if persist_to_cache else None
            }
            
            source = get_source('custom_upload', config=config)
            
            # Load the file
            result = await source.load_file(
                file_content,
                filename,
                required_columns=required_columns,
                persist=persist_to_cache or persist_to_mongo
            )
            
            if result['success']:
                # Switch to this custom upload as active source
                self._active_source = source
                self.default_source = 'custom_upload'
                
                logger.info(f"Custom upload successful: {result['upload_id']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error uploading custom file: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _get_cache_manager(self):
        """Get cache manager instance (imported from app context)"""
        try:
            from cache_manager import cache
            return cache
        except ImportError:
            logger.warning("Cache manager not available")
            return None
    
    async def close(self):
        """Close active source connection"""
        if self._active_source and hasattr(self._active_source, 'close'):
            await self._active_source.close()
            logger.info("Closed active source connection")