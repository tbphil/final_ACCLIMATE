"""
HBOM Fetcher
Main orchestrator for retrieving and assembling HBOM data

Location: backend/hbom/hbom_fetcher.py
"""

import logging
from typing import Dict, Any, List, Optional

from .data_sources.mongodb_baseline import MongoDBBaselineSource
from .hbom_preparers import (
    reconstruct_tree,
    prepare_for_frontend,
    get_roots_for_sector
)

logger = logging.getLogger(__name__)


class HBOMFetcher:
    """
    Orchestrates HBOM data retrieval from MongoDB baseline
    
    Responsibilities:
    - Query flat component nodes by sector
    - Query fragility curves by hazard
    - Coordinate tree reconstruction
    - Return frontend-ready payload
    """
    
    def __init__(self, data_source: Optional[MongoDBBaselineSource] = None):
        """
        Initialize fetcher with data source
        
        Args:
            data_source: MongoDB source (defaults to MongoDBBaselineSource)
        """
        self.data_source = data_source or MongoDBBaselineSource()
        logger.info("HBOMFetcher initialized")
    
    async def fetch_hbom_tree(
        self,
        sector: str,
        hazard: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch complete HBOM tree for a sector with optional hazard filtering.
        
        This is the main entry point for getting HBOM data.
        """
        logger.info(f"Fetching HBOM tree for sector={sector}, hazard={hazard}")
        
        # 1. Validate connection
        if not await self.data_source.validate_connection():
            raise ConnectionError("Cannot connect to MongoDB baseline")
        
        # 2. Fetch flat component nodes for sector
        flat_nodes = await self.data_source.fetch_components_by_sector(sector)
        
        if not flat_nodes:
            logger.warning(f"No components found for sector: {sector}")
            return {"sector": sector, "components": []}
        
        logger.info(f"Fetched {len(flat_nodes)} flat nodes for sector")
        
        # 3. Fetch fragility curves (optional)
        fragility_curves = None
        if hazard:
            fragility_curves = await self.data_source.fetch_fragilities_by_hazard(
                hazard=hazard,
                component_uuids=[node["uuid"] for node in flat_nodes]
            )
            logger.info(f"Fetched {len(fragility_curves) if fragility_curves else 0} fragility curves")
        
        # 4. Reconstruct nested tree structure
        roots = reconstruct_tree(flat_nodes, fragility_curves)
        
        # 5. Load canonical registry for aliases
        comp_lib = self.data_source.client[self.data_source.database_name]['component_library']
        canonical_cursor = comp_lib.find({"canonical_uuid": {"$exists": True}})
        canonical_list = await canonical_cursor.to_list(length=None)
        canonical_map = {c["component_type"]: c for c in canonical_list}
        
        # 6. Format for frontend
        response = await prepare_for_frontend(roots, sector, hazard, canonical_map)
        
        logger.info(f"Prepared {len(roots)} root components for frontend")
        
        return response
    
    async def fetch_component_by_uuid(
        self,
        uuid: str,
        include_fragilities: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single component and its subtree by UUID.
        
        Useful for asset-specific lookups.
        
        Args:
            uuid: Component UUID
            include_fragilities: Whether to include fragility data
        
        Returns:
            Component with nested subcomponents, or None if not found
        """
        logger.info(f"Fetching component by UUID: {uuid}")
        
        # 1. Fetch the target node
        target_node = await self.data_source.fetch_component_by_uuid(uuid)
        
        if not target_node:
            logger.warning(f"Component not found: {uuid}")
            return None
        
        # 2. Fetch all descendants
        descendants = await self.data_source.fetch_descendants(uuid)
        
        # 3. Combine target + descendants
        all_nodes = [target_node] + descendants
        
        # 4. Fetch fragilities if requested
        fragility_curves = None
        if include_fragilities:
            fragility_curves = await self.data_source.fetch_fragilities_for_components(
                [node["uuid"] for node in all_nodes]
            )
        
        # 5. Reconstruct subtree
        trees = reconstruct_tree(all_nodes, fragility_curves)
        
        # Return the root (should be just one)
        return trees[0] if trees else None
    
    async def fetch_roots_for_sector(
        self,
        sector: str
    ) -> List[Dict[str, Any]]:
        """
        Get just the root-level components for a sector (no full tree).
        
        Useful for listing available component types without loading full hierarchy.
        
        Args:
            sector: Infrastructure sector
        
        Returns:
            List of root component metadata (no subcomponents loaded)
        """
        logger.info(f"Fetching roots for sector: {sector}")
        
        flat_nodes = await self.data_source.fetch_components_by_sector(sector)
        roots = get_roots_for_sector(flat_nodes, sector)
        
        return roots
    
    async def get_stats(self, sector: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics about available HBOM data.
        
        Args:
            sector: Optional sector filter
        
        Returns:
            Statistics dictionary with counts, asset types, etc.
        """
        stats = await self.data_source.get_stats(sector)
        
        logger.info(f"Retrieved stats: {stats}")
        
        return stats
    
    async def close(self):
        """Close data source connection"""
        if hasattr(self.data_source, 'close'):
            await self.data_source.close()
            logger.info("Data source connection closed")