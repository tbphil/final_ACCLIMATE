"""
Infrastructure Router
FastAPI endpoints for infrastructure data retrieval and management
"""

import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse

from .infrastructure_fetcher import InfrastructureFetcher
from .infrastructure_preparers import (
    prepare_for_frontend,
    prepare_upload_response,
    prepare_stats_response
)
from .data_sources.base import BoundingBox
from models import InfrastructureRequest

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["infrastructure"])

# Initialize fetcher (will use configured default source)
fetcher = InfrastructureFetcher(default_source='mongodb_aha')


@router.post("/get-infrastructure")
async def get_infrastructure(req: InfrastructureRequest):
    """
    Get infrastructure assets from MongoDB AHA Core data
    Filtered by bounding box and sector
    
    This endpoint uses MongoDB as the data source (AHA Core workflow)
    """
    try:
        logger.info(f"Infrastructure request: sector={req.sector}, hazard={req.hazard}")
        
        # Build bounding box from request or climate cache
        bbox = None
        use_explicit_bounds = all(
            v is not None 
            for v in [req.min_lat, req.max_lat, req.min_lon, req.max_lon]
        )
        
        if use_explicit_bounds:
            bbox = BoundingBox(
                min_lat=req.min_lat,
                max_lat=req.max_lat,
                min_lon=req.min_lon,
                max_lon=req.max_lon
            )
        else:
            # Try to get from cached climate data
            from cache_manager import cache
            prepared_data = cache.get("prepared", (req.hazard,))
            
            if not prepared_data or "bounding_box" not in prepared_data:
                raise HTTPException(
                    status_code=400,
                    detail="No bounding box provided and no climate data cached. "
                           "Either provide explicit bounds or run /api/get-climate first."
                )
            
            bbox_dict = prepared_data["bounding_box"]
            bbox = BoundingBox(
                min_lat=bbox_dict["min_lat"],
                max_lat=bbox_dict["max_lat"],
                min_lon=bbox_dict["min_lon"],
                max_lon=bbox_dict["max_lon"]
            )
        
        # Fetch from MongoDB AHA source
        result = await fetcher.fetch_infrastructure(
            bbox=bbox,
            sector=req.sector,
            source_name='mongodb_aha'  # Explicitly use MongoDB
        )
        
        # Prepare for frontend
        prepared = prepare_for_frontend(
            assets=result['infrastructure'],
            bbox={
                'min_lat': bbox.min_lat,
                'max_lat': bbox.max_lat,
                'min_lon': bbox.min_lon,
                'max_lon': bbox.max_lon
            },
            source_info=result
        )
        
        logger.info(f"Returning {prepared['count']} infrastructure assets")
        
        return JSONResponse(content=prepared)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_infrastructure: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload")
async def upload_custom_infrastructure(
    file: UploadFile = File(...),
    min_lat: Optional[float] = Form(None),
    max_lat: Optional[float] = Form(None),
    min_lon: Optional[float] = Form(None),
    max_lon: Optional[float] = Form(None),
    persist_to_mongo: bool = Form(False),
    persist_to_cache: bool = Form(True)
):
    """
    Upload and process custom infrastructure file (CSV/XLSX)
    
    This endpoint uses CustomUploadSource (custom upload workflow)
    
    Args:
        file: CSV or XLSX file
        min_lat, max_lat, min_lon, max_lon: Optional bounding box filter
        persist_to_mongo: Whether to save to MongoDB permanently
        persist_to_cache: Whether to cache for session
    
    Returns:
        Upload result with preview and filtered assets
    """
    try:
        logger.info(f"Custom upload: {file.filename}")
        
        # Read file content
        file_content = await file.read()
        
        # Upload via fetcher
        upload_result = await fetcher.upload_custom_file(
            file_content=file_content,
            filename=file.filename,
            persist_to_mongo=persist_to_mongo,
            persist_to_cache=persist_to_cache,
            required_columns=['latitude', 'longitude', 'name']
        )
        
        if not upload_result['success']:
            return JSONResponse(
                status_code=400,
                content=upload_result
            )
        
        # If bounding box provided, filter the uploaded data
        assets = None
        if all(v is not None for v in [min_lat, max_lat, min_lon, max_lon]):
            bbox = BoundingBox(
                min_lat=min_lat,
                max_lat=max_lat,
                min_lon=min_lon,
                max_lon=max_lon
            )
            
            # Fetch filtered assets
            result = await fetcher.fetch_infrastructure(
                bbox=bbox,
                sector="Custom Upload",  # Generic sector for uploads
                source_name='custom_upload'
            )
            
            assets = result['infrastructure']
        
        # Prepare response
        response = prepare_upload_response(
            upload_result=upload_result,
            assets=assets
        )
        
        logger.info(f"Upload successful: {upload_result['upload_id']}")
        
        return JSONResponse(content=response)
        
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/infrastructure-stats/{sector}")
async def get_infrastructure_stats(
    sector: str,
    source: str = 'mongodb_aha'
):
    """
    Get statistics about available infrastructure data
    
    Args:
        sector: Infrastructure sector
        source: Data source to query ('mongodb_aha' or 'custom_upload')
    
    Returns:
        Statistics including total count, component types, geographic coverage
    """
    try:
        # Use current fetcher's active source or specify one
        result = await fetcher.fetch_infrastructure(
            bbox=BoundingBox(min_lat=-90, max_lat=90, min_lon=-180, max_lon=180),
            sector=sector,
            source_name=source
        )
        
        stats = result.get('stats', {})
        
        response = prepare_stats_response(stats)
        
        return JSONResponse(content=response)
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/infrastructure-by-uuid/{uuid}")
async def get_infrastructure_by_uuid(
    uuid: str,
    source: str = 'mongodb_aha'
):
    """
    Get single infrastructure asset by UUID
    
    Args:
        uuid: Asset UUID
        source: Data source to query
    
    Returns:
        Asset details or 404 if not found
    """
    try:
        asset = await fetcher.fetch_by_uuid(uuid, source_name=source)
        
        if asset is None:
            raise HTTPException(status_code=404, detail=f"Asset {uuid} not found")
        
        return JSONResponse(content=asset)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching asset {uuid}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/infrastructure-sources")
async def list_infrastructure_sources():
    """
    List all available infrastructure data sources and their capabilities
    
    Returns:
        Dictionary of sources with metadata
    """
    try:
        sources = await fetcher.get_available_sources()
        return JSONResponse(content=sources)
        
    except Exception as e:
        logger.error(f"Error listing sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/switch-source/{source_name}")
async def switch_infrastructure_source(source_name: str):
    """
    Switch to a different infrastructure data source
    
    Args:
        source_name: Name of source to switch to
    
    Returns:
        Success status
    """
    try:
        success = await fetcher.switch_source(source_name)
        
        if not success:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot switch to source: {source_name}"
            )
        
        return JSONResponse(content={
            'success': True,
            'active_source': source_name
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error switching source: {e}")
        raise HTTPException(status_code=500, detail=str(e))