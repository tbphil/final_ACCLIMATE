"""
HBOM Router
FastAPI endpoints for HBOM tree retrieval and fragility analysis

Location: backend/hbom/hbom_router.py
"""

import logging
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
import json
import math

from .hbom_fetcher import HBOMFetcher
from models import HBOMDefinition

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/hbom", tags=["hbom"])

# Initialize fetcher
fetcher = HBOMFetcher()


def _json_safe(obj):
    """Replace NaN/Inf with None for JSON serialization"""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_json_safe(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj


@router.get("/tree/{sector}/{hazard}")
async def get_hbom_tree(sector: str, hazard: str):
    """
    Get HBOM tree for a sector with fragilities for a specific hazard.
    
    Returns nested component tree with fragility curves embedded.
    Replaces old /api/hbom/tree and /api/get-hbom endpoints.
    
    Args:
        sector: Infrastructure sector (e.g., "Energy Grid")
        hazard: Hazard type (e.g., "Heat Stress", "Wind")
    
    Returns:
        HBOMDefinition with nested components and embedded hazards
    """
    try:
        logger.info(f"HBOM tree request: sector={sector}, hazard={hazard}")
        
        # Fetch and reconstruct tree
        result = await fetcher.fetch_hbom_tree(sector=sector, hazard=hazard)
        
        if not result.get("components"):
            logger.warning(f"No components found for sector={sector}")
            return JSONResponse(content={"sector": sector, "components": []})
        
        # Sanitize before returning
        result = _json_safe(result)
        
        logger.info(f"Returning {len(result['components'])} root components")
        
        return JSONResponse(content=result)
        
    except Exception as e:
        logger.exception(f"Error fetching HBOM tree: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/component/{uuid}")
async def get_component_by_uuid(
    uuid: str,
    include_fragilities: bool = True
):
    """
    Get a single component and its subtree by UUID.
    
    Useful for drilling into specific components.
    
    Args:
        uuid: Component UUID
        include_fragilities: Whether to include fragility data
    
    Returns:
        Component with nested subcomponents
    """
    try:
        logger.info(f"Component lookup: uuid={uuid}")
        
        component = await fetcher.fetch_component_by_uuid(
            uuid=uuid,
            include_fragilities=include_fragilities
        )
        
        if not component:
            raise HTTPException(status_code=404, detail=f"Component {uuid} not found")
        
        return JSONResponse(content=component)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching component {uuid}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/roots/{sector}")
async def get_sector_roots(sector: str):
    """
    Get root-level components for a sector (no full tree).
    
    Faster than full tree - useful for listing available component types.
    
    Args:
        sector: Infrastructure sector
    
    Returns:
        List of root component metadata
    """
    try:
        logger.info(f"Roots request: sector={sector}")
        
        roots = await fetcher.fetch_roots_for_sector(sector)
        
        return JSONResponse(content={
            "sector": sector,
            "roots": roots,
            "count": len(roots)
        })
        
    except Exception as e:
        logger.exception(f"Error fetching roots: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_hbom_stats(sector: str = None):
    """
    Get statistics about HBOM baseline.
    
    Args:
        sector: Optional sector filter
    
    Returns:
        Statistics about components, asset types, fragility coverage
    """
    try:
        stats = await fetcher.get_stats(sector)
        
        return JSONResponse(content=stats)
        
    except Exception as e:
        logger.exception(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Legacy endpoint compatibility (redirects to new structure)
@router.get("/get-hbom/{sector}/{hazard}")
async def legacy_get_hbom(sector: str, hazard: str):
    """
    Legacy endpoint - redirects to /tree for backward compatibility
    """
    logger.info(f"Legacy endpoint called, redirecting to /tree")
    return await get_hbom_tree(sector, hazard)