"""
MongoDB Baseline Data Source
Queries hbom_baseline and fragility_db collections

Location: backend/hbom/data_sources/mongodb_baseline.py
"""

import logging
from typing import List, Dict, Any, Optional
from motor.motor_asyncio import AsyncIOMotorClient
import os
from database import mongo_uri
from .base import HBOMDataSource

logger = logging.getLogger(__name__)


class MongoDBBaselineSource(HBOMDataSource):
    """
    MongoDB implementation for HBOM baseline + fragility database
    
    Queries two collections:
    - hbom_baseline: Flat component nodes
    - fragility_db: Fragility curves
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize MongoDB connection
        
        Args:
            config: Optional config with 'mongo_uri', 'database'
        """
        super().__init__(config)
        
        # Connection settings
        self.mongo_uri = self.config.get('mongo_uri', os.getenv('MONGO_URI', mongo_uri))
        self.database_name = self.config.get('database', 'acclimate_db')
        
        # Initialize client
        self.client = AsyncIOMotorClient(self.mongo_uri)
        self.db = self.client[self.database_name]
        self.hbom_baseline = self.db['hbom_baseline']
        self.fragility_db = self.db['fragility_db']
        
        logger.info(f"Initialized MongoDB baseline source: {self.database_name}")
    
    @property
    def source_name(self) -> str:
        return "MongoDB HBOM Baseline"
    
    async def validate_connection(self) -> bool:
        """Test MongoDB connection"""
        try:
            await self.client.admin.command('ping')
            logger.info("MongoDB connection validated")
            return True
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            return False
    
    async def fetch_components_by_sector(
        self,
        sector: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch all canonical component nodes (those linked to canonical registry).
        
        Args:
            sector: Infrastructure sector (e.g., "Energy Grid")
        
        Returns:
            List of flat component nodes
        """
        # Query for all nodes that have been canonically linked
        query = {"canonical_component_type": {"$exists": True, "$ne": None}}
        
        cursor = self.hbom_baseline.find(query)
        nodes = await cursor.to_list(None)
        
        # Clean MongoDB _id field
        for node in nodes:
            if '_id' in node:
                node['_id'] = str(node['_id'])
        
        logger.info(f"Fetched {len(nodes)} canonical components")
        
        return nodes
    
    async def fetch_component_by_uuid(
        self,
        uuid: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch single component by UUID.
        
        Args:
            uuid: Component UUID
        
        Returns:
            Component node or None
        """
        node = await self.hbom_baseline.find_one({"uuid": uuid})
        
        if node and '_id' in node:
            node['_id'] = str(node['_id'])
        
        return node
    
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
        # We need to recursively find all children
        # Start with direct children
        all_descendants = []
        to_process = [parent_uuid]
        
        while to_process:
            current_uuid = to_process.pop(0)
            
            # Find children of current node
            cursor = self.hbom_baseline.find({"parent_uuid": current_uuid})
            children = await cursor.to_list(None)
            
            for child in children:
                if '_id' in child:
                    child['_id'] = str(child['_id'])
                all_descendants.append(child)
                to_process.append(child["uuid"])
        
        logger.info(f"Found {len(all_descendants)} descendants for {parent_uuid}")
        
        return all_descendants
    
    async def fetch_fragilities_by_hazard(
        self,
        hazard: str,
        component_uuids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch fragility curves for a hazard, optionally filtered to specific components.
        
        Args:
            hazard: Hazard type (e.g., "Wind", "Heat Stress")
            component_uuids: Optional list of component UUIDs to filter
        
        Returns:
            List of fragility curve documents
        """
        query = {"hazard": hazard}
        
        # Optionally filter to specific components
        if component_uuids:
            query["component_uuid"] = {"$in": component_uuids}
        
        cursor = self.fragility_db.find(query)
        curves = await cursor.to_list(None)
        
        # Clean MongoDB _id
        for curve in curves:
            if '_id' in curve:
                curve['_id'] = str(curve['_id'])
        
        logger.info(f"Fetched {len(curves)} fragility curves for hazard {hazard}")
        
        return curves
    
    async def fetch_fragilities_for_components(
        self,
        component_uuids: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Fetch all fragility curves for specific components (all hazards).
        
        Args:
            component_uuids: List of component UUIDs
        
        Returns:
            List of fragility curve documents
        """
        query = {"component_uuid": {"$in": component_uuids}}
        
        cursor = self.fragility_db.find(query)
        curves = await cursor.to_list(None)
        
        # Clean MongoDB _id
        for curve in curves:
            if '_id' in curve:
                curve['_id'] = str(curve['_id'])
        
        logger.info(f"Fetched {len(curves)} fragility curves for {len(component_uuids)} components")
        
        return curves
    
    async def get_stats(self, sector: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics about HBOM baseline.
        
        Args:
            sector: Optional sector filter
        
        Returns:
            Statistics dictionary
        """
        # Total component count
        if sector:
            asset_types = self._sector_to_asset_types(sector)
            query = {"asset_type": {"$in": asset_types}}
        else:
            query = {}
        
        total_components = await self.hbom_baseline.count_documents(query)
        
        # Count by asset type
        pipeline = [
            {"$match": query},
            {"$group": {"_id": "$asset_type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        asset_type_counts = await self.hbom_baseline.aggregate(pipeline).to_list(None)
        
        # Count roots
        root_query = {**query, "parent_uuid": None}
        root_count = await self.hbom_baseline.count_documents(root_query)
        
        # Fragility stats
        total_curves = await self.fragility_db.count_documents({})
        matched_curves = await self.fragility_db.count_documents({"component_uuid": {"$ne": None}})
        
        return {
            'source': self.source_name,
            'total_components': total_components,
            'root_components': root_count,
            'asset_types': {
                item['_id']: item['count']
                for item in asset_type_counts
            },
            'fragility_curves': {
                'total': total_curves,
                'matched': matched_curves,
                'unmatched': total_curves - matched_curves
            }
        }

    
    async def close(self):
        """Close MongoDB connection"""
        self.client.close()
        logger.info("MongoDB connection closed")