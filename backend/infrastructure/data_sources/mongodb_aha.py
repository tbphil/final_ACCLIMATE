"""
MongoDB AHA Core Data Source
Fetches infrastructure from MongoDB collections loaded from AHA Core workbooks
"""

import logging
from typing import List, Dict, Any, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from database import mongo_uri
from .base import InfrastructureDataSource, BoundingBox

logger = logging.getLogger(__name__)


class MongoDBahaSource(InfrastructureDataSource):
    """
    MongoDB implementation for AHA Core infrastructure data
    
    Uses geospatial indexes for fast bounding box queries
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize MongoDB connection
        
        Args:
            config: Optional config with 'mongo_uri', 'database', 'collection'
        """
        super().__init__(config)
        
        # Connection settings
        self.mongo_uri = self.config.get('mongo_uri', mongo_uri)
        self.database_name = self.config.get('database', 'acclimate_db')
        self.collection_name = self.config.get('collection', 'energy_grid')
        
        # Initialize client (lazy connection)
        self.client = AsyncIOMotorClient(self.mongo_uri)
        self.db = self.client[self.database_name]
        self.collection = self.db[self.collection_name]
        
        logger.info(f"Initialized MongoDB source: {self.database_name}.{self.collection_name}")
    
    @property
    def source_name(self) -> str:
        return "MongoDB AHA Core"
    
    @property
    def supports_realtime_updates(self) -> bool:
        return True  # MongoDB supports real-time updates
    
    async def validate_connection(self) -> bool:
        """Test MongoDB connection"""
        try:
            await self.client.admin.command('ping')
            logger.info("MongoDB connection validated")
            return True
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            return False
    
    async def fetch(
        self,
        bbox: BoundingBox,
        sector: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch infrastructure assets within bounding box
        
        Uses MongoDB geospatial query with $geoWithin for fast spatial filtering
        
        Args:
            bbox: Geographic bounding box
            sector: Infrastructure sector (not used if collection is sector-specific)
            filters: Additional MongoDB query filters
        
        Returns:
            List of matching asset documents
        """
        try:
            # Build geospatial query
            query = {
                "location": {
                    "$geoWithin": {
                        "$box": [
                            [bbox.min_lon, bbox.min_lat],  # Southwest corner
                            [bbox.max_lon, bbox.max_lat]   # Northeast corner
                        ]
                    }
                }
            }
            
            # Add additional filters
            if filters:
                query.update(filters)
            
            logger.info(f"Querying MongoDB: {query}")
            
            # Execute query
            cursor = self.collection.find(query)
            assets = await cursor.to_list(length=None)
            
            logger.info(f"Found {len(assets)} assets in bbox")
            
            # Clean MongoDB _id fields for JSON serialization
            for asset in assets:
                if '_id' in asset:
                    asset['_id'] = str(asset['_id'])
            
            return assets
            
        except Exception as e:
            logger.error(f"MongoDB fetch error: {e}")
            raise
    
    async def get_stats(self, sector: str) -> Dict[str, Any]:
        """
        Get collection statistics
        
        Args:
            sector: Infrastructure sector (ignored - collection is sector-specific)
        
        Returns:
            Statistics including total count, component types, geographic bounds
        """
        try:
            # Total count
            total_count = await self.collection.count_documents({})
            
            # Count by component type
            pipeline = [
                {
                    '$group': {
                        '_id': '$component_type',
                        'count': {'$sum': 1}
                    }
                },
                {'$sort': {'count': -1}}
            ]
            
            component_counts = await self.collection.aggregate(pipeline).to_list(length=None)
            
            # Geographic bounds (if we want to show coverage area)
            bounds_pipeline = [
                {
                    '$group': {
                        '_id': None,
                        'min_lat': {'$min': '$latitude'},
                        'max_lat': {'$max': '$latitude'},
                        'min_lon': {'$min': '$longitude'},
                        'max_lon': {'$max': '$longitude'}
                    }
                }
            ]
            
            bounds_result = await self.collection.aggregate(bounds_pipeline).to_list(length=1)
            bounds = bounds_result[0] if bounds_result else {}
            
            # Count by state
            state_pipeline = [
                {
                    '$group': {
                        '_id': '$state',
                        'count': {'$sum': 1}
                    }
                },
                {'$sort': {'count': -1}},
                {'$limit': 10}
            ]
            
            state_counts = await self.collection.aggregate(state_pipeline).to_list(length=10)
            
            return {
                'source': self.source_name,
                'collection': self.collection_name,
                'total_assets': total_count,
                'component_types': {
                    item['_id']: item['count'] 
                    for item in component_counts
                },
                'geographic_bounds': {
                    'min_lat': bounds.get('min_lat'),
                    'max_lat': bounds.get('max_lat'),
                    'min_lon': bounds.get('min_lon'),
                    'max_lon': bounds.get('max_lon')
                },
                'top_states': {
                    item['_id']: item['count']
                    for item in state_counts
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            raise
    
    async def get_by_uuid(self, uuid: str) -> Optional[Dict[str, Any]]:
        """
        Get single asset by UUID
        
        Args:
            uuid: Asset UUID
        
        Returns:
            Asset document or None if not found
        """
        try:
            asset = await self.collection.find_one({'uuid': uuid})
            
            if asset and '_id' in asset:
                asset['_id'] = str(asset['_id'])
            
            return asset
            
        except Exception as e:
            logger.error(f"Error fetching asset {uuid}: {e}")
            raise
    
    async def search_by_name(
        self,
        name: str,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search assets by name (case-insensitive partial match)
        
        Args:
            name: Search term
            limit: Maximum results to return
        
        Returns:
            List of matching assets
        """
        try:
            query = {
                'name': {
                    '$regex': name,
                    '$options': 'i'  # Case-insensitive
                }
            }
            
            cursor = self.collection.find(query).limit(limit)
            assets = await cursor.to_list(length=limit)
            
            for asset in assets:
                if '_id' in asset:
                    asset['_id'] = str(asset['_id'])
            
            return assets
            
        except Exception as e:
            logger.error(f"Error searching by name '{name}': {e}")
            raise
    
    async def close(self):
        """Close MongoDB connection"""
        self.client.close()
        logger.info("MongoDB connection closed")