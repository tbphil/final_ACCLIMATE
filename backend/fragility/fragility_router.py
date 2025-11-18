"""
Fragility Router
FastAPI endpoints for fragility curve computation and analysis

Location: backend/fragility/fragility_router.py
"""

import logging
import copy
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from .fragility_computer import FragilityComputer
from hbom import HBOMFetcher
from cache_manager import cache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fragility", tags=["fragility"])

# Initialize components
computer = FragilityComputer()
hbom_fetcher = HBOMFetcher()


def _json_safe(obj):
    """Sanitize NaN/Inf for JSON"""
    import math
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_json_safe(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj


@router.get("/compute/{sector}/{hazard}")
async def compute_fragility(sector: str, hazard: str):
    """
    Compute fragility curves for HBOM tree.
    
    Returns HBOM tree with fragility_curves computed for each component.
    
    Args:
        sector: Infrastructure sector
        hazard: Hazard type
    
    Returns:
        HBOM tree with fragility curves embedded in hazards[hazard]
    """
    try:
        logger.info(f"Fragility computation request: sector={sector}, hazard={hazard}")
        
        # 1. Get climate data from cache using the SAME complex key structure
        # We need to find ANY climate cache entry since we don't know the full key
        prepared_data = None
        
        # Search cache for climate data
        for hash_key in list(cache.mem.keys()):
            obj = cache.mem[hash_key]
            if isinstance(obj, dict) and "variables" in obj and "data" in obj:
                prepared_data = obj
                break
        
        # If not in RAM, check disk
        if not prepared_data:
            for cache_file in cache.dir.glob("*.pkl.gz"):
                try:
                    obj = cache._load(cache_file)
                    if isinstance(obj, dict) and "variables" in obj and "data" in obj:
                        prepared_data = obj
                        break
                except:
                    continue
        
        if not prepared_data:
            raise HTTPException(
                status_code=400,
                detail="Climate data not loaded. Call /api/get-climate first."
            )
        
        # 2. Get HBOM tree from hbom module
        hbom_tree = await hbom_fetcher.fetch_hbom_tree(sector, hazard)
        
        if not hbom_tree.get("components"):
            raise HTTPException(
                status_code=404,
                detail=f"No HBOM components found for sector: {sector}"
            )
        
        # 3. Compute fragility curves (mutates tree in-place)
        result = computer.compute_for_tree(
            copy.deepcopy(hbom_tree),  # Don't mutate original
            hazard,
            prepared_data
        )
        
        # 4. Sanitize and return
        result = _json_safe(result)
        
        logger.info(f"Fragility computation complete for {len(result['components'])} roots")
        
        return JSONResponse(content=result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error computing fragility: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/timeseries/{sector}/{hazard}")
async def fragility_timeseries(sector: str, hazard: str):
    """
    Compute PoF time series for all components.
    
    Returns flat map of {uuid: {var: [pof_over_time]}}
    
    Args:
        sector: Infrastructure sector
        hazard: Hazard type
    
    Returns:
        Dictionary mapping component UUIDs to time-series PoF data
    """
    try:
        logger.info(f"Fragility timeseries request: sector={sector}, hazard={hazard}")
        
        # 1. Get climate data from cache (simple key contract)
        prepared_data = cache.get("climate_latest", (hazard,))
        
        if not prepared_data:
            raise HTTPException(
                status_code=400,
                detail="Climate data not loaded. Call /api/get-climate first."
            )
        
        # 2. Get HBOM tree
        hbom_tree = await hbom_fetcher.fetch_hbom_tree(sector, hazard)
        
        if not hbom_tree.get("components"):
            raise HTTPException(
                status_code=404,
                detail=f"No HBOM components found for sector: {sector}"
            )
        
        # 3. Compute time series
        frag_ts = computer.compute_timeseries(
            copy.deepcopy(hbom_tree),
            hazard,
            prepared_data
        )
        
        # 4. Sanitize and return
        frag_ts = _json_safe(frag_ts)
        
        logger.info(f"Computed time series for {len(frag_ts)} components")
        
        return JSONResponse(content=frag_ts)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error computing timeseries: {e}")
        raise HTTPException(status_code=500, detail=str(e))