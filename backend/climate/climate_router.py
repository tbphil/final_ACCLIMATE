"""
FastAPI router for climate endpoints.
FIXED: Cache key now correctly handles both point and bbox modes
"""
import logging
from fastapi import APIRouter, HTTPException, Query
from typing import List

from models import DataRequest, ClimateData, ScenarioEnum
from cache_manager import cache
from .data_sources.na_cordex import NACordexDataSource
from .climate_fetcher import ClimateFetcher
from .climate_hazards import get_hazard, list_hazards

logger = logging.getLogger(__name__)

# Initialize pipeline components
data_source = NACordexDataSource()
fetcher = ClimateFetcher(data_source)

# Create router
router = APIRouter(prefix="/api", tags=["climate"])


@router.get("/hazards", response_model=List[str])
async def list_available_hazards():
    """List all available hazards"""
    return list_hazards()

@router.get("/climate-models/", response_model=List[str])
async def list_climate_models(
    scenario: ScenarioEnum = Query(..., description="Climate scenario"),
    domain: str = Query("NAM-22i", description="Grid/domain identifier"),
    hazard: str = Query(..., description="Hazard name")
):
    """
    List ensemble member IDs available for ALL base variables of the hazard.
    """
    try:
        hazard_def = get_hazard(hazard)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    
    try:
        models = data_source.list_available_models(
            variables=hazard_def.base_variables,
            scenario=scenario.value,
            domain=domain
        )
        return models
    except Exception as e:
        logger.error(f"Error listing models: {e}")
        raise HTTPException(status_code=500, detail="Failed to list climate models")


@router.post("/get-climate", response_model=ClimateData)
async def get_climate(request: DataRequest):
    """
    Fetch climate data for a hazard.
    
    This is the main entry point for climate data requests.
    """
    try:
        # Build cache key
        hazard_def = get_hazard(request.hazard.value)
        
        # Determine spatial mode and build appropriate key
        using_bbox = all(v is not None for v in [
            request.min_lat, request.max_lat, request.min_lon, request.max_lon
        ])
        
        if using_bbox:
            # Bbox mode: use bbox coordinates (rounded to avoid float precision issues)
            spatial_key = (
                "bbox",
                round(request.min_lat, 6),
                round(request.max_lat, 6),
                round(request.min_lon, 6),
                round(request.max_lon, 6)
            )
        else:
            # Point mode: use center point + num_cells
            spatial_key = (
                "point",
                round(request.lat, 6),
                round(request.lon, 6),
                request.num_cells
            )
        
        cache_key = (
            spatial_key,  
            tuple(hazard_def.all_variables()),
            request.scenario.value,
            request.domain,
            request.prior_years,
            request.future_years,
            request.aggregation_method.value,
            request.aggregation_q,
            request.aggregate_over_member_id,
            request.climate_model,
            data_source.source_name,
        )
        
        # Check cache
        cached = cache.get("climate", cache_key)
        if cached:
            logger.info("Returning cached climate data")
            return ClimateData(**cached)
        
        # Fetch fresh data
        logger.info(f"Fetching fresh climate data for {request.hazard.value}")
        response = fetcher.fetch_for_request(request)
        
        # Cache response with full key
        cache.set("climate", cache_key, response)

        # Also cache with simple hazard-only key for downstream modules
        cache.set("climate_latest", (request.hazard.value,), response)

        logger.info("Climate data cached successfully")

        return ClimateData(**response)
        
    except ValueError as e:
        # Bad hazard name or invalid request
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception(f"Error fetching climate data: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch climate data")