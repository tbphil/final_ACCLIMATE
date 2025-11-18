"""
Import Endpoint - FastAPI router for user asset import
Single endpoint POST /api/import/user_assets
"""

import logging
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
import json

try:
    # Try relative imports first (works in FastAPI context)
    from .importer import AssetImporter
    from . import config, field_mapper
except ImportError:
    # Fall back to absolute imports (works in standalone context)
    from projects.acclimate.importer import AssetImporter
    from projects.acclimate import config, field_mapper

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="",
    tags=["Asset Import"]
)


@router.post("/upload")
async def import_user_assets(
    file: UploadFile = File(...),
    column_mappings: Optional[str] = Form(None),
    component_mappings: Optional[str] = Form(None),
    target_collection: str = Form('energy_grid'),
    min_lat: Optional[float] = Form(None),
    max_lat: Optional[float] = Form(None),
    min_lon: Optional[float] = Form(None),
    max_lon: Optional[float] = Form(None)
):
    """
    Import user-uploaded asset data
    
    This endpoint handles the complete import workflow:
    1. Auto-detects file format (CSV, JSON, GeoJSON, Shapefile ZIP, Excel)
    2. Auto-maps columns to required fields (latitude, longitude, component_type)
    3. Auto-matches component types to canonical component library
    4. Returns success if everything auto-maps, or requests user input if needed
    
    Args:
        file: Uploaded file (CSV, JSON, GeoJSON, ZIP with shapefile, or Excel)
        column_mappings: Optional JSON string with PARTIAL column mapping overrides.
            Only specify fields you want to override from auto-detection.
            System will auto-detect other fields normally.
            e.g., '{"Longitude": "lng"}' - only fixes Longitude, auto-detects rest
        component_mappings: Optional JSON string with PARTIAL component type overrides.
            Only specify component types you want to override from auto-matching.
            System will auto-match other component types normally.
            e.g., '{"powerplant": "Natural Gas Generation Plant"}' - only fixes this one
        target_collection: MongoDB collection to import to (default: 'energy_grid')
    
    Returns:
        JSON response with:
        - success: bool
        - data: Enriched asset data
        - metadata: Field and component mappings with confidence scores
        - If success=false: Error message
    """
    
    try:
        # Read file content
        file_content = await file.read()
        filename = file.filename
        
        logger.info(f"Received upload: {filename} ({len(file_content)} bytes)")
        
        # Parse optional mappings
        parsed_column_mappings = None
        if column_mappings:
            try:
                parsed_column_mappings = json.loads(column_mappings)
            except json.JSONDecodeError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid column_mappings JSON"
                )
        
        parsed_component_mappings = None
        if component_mappings:
            try:
                parsed_component_mappings = json.loads(component_mappings)
            except json.JSONDecodeError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid component_mappings JSON"
                )
        
        # Create importer and process upload
        importer = AssetImporter(field_mapper)
        
        try:
            # Build bounding box if provided
            bbox = None
            if all(x is not None for x in [min_lat, max_lat, min_lon, max_lon]):
                bbox = {
                    'min_lat': min_lat,
                    'max_lat': max_lat,
                    'min_lon': min_lon,
                    'max_lon': max_lon
                }
                logger.info(f"Filtering assets by bounding box: {bbox}")
            
            result = await importer.process_upload(
                file_content=file_content,
                filename=filename,
                column_mappings=parsed_column_mappings,
                component_mappings=parsed_component_mappings,
                target_collection=target_collection,
                bounding_box=bbox
            )
            
            # Return appropriate status code
            if result.get('success'):
                return JSONResponse(content=result, status_code=200)
            elif result.get('needs_mapping'):
                return JSONResponse(content=result, status_code=200)  # Not an error, just needs input
            else:
                return JSONResponse(content=result, status_code=400)
                
        finally:
            importer.close()
    
    except ValueError as e:
        logger.error(f"Value error in import: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    
    except Exception as e:
        logger.error(f"Unexpected error in import endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/user_assets/status")
async def get_import_status():
    """
    Get status of import system
    
    Returns basic health check and available collections
    """
    from motor.motor_asyncio import AsyncIOMotorClient
    import os
    
    try:
        mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
        client = AsyncIOMotorClient(mongo_uri)
        db = client[config.MONGO_DATABASE]
        
        # Get collection names
        collections = await db.list_collection_names()
        
        # Check component library exists
        component_library_exists = config.COMPONENT_LIBRARY_COLLECTION in collections
        
        # Count components
        component_count = 0
        if component_library_exists:
            component_count = await db[config.COMPONENT_LIBRARY_COLLECTION].count_documents({})
        
        client.close()
        
        return {
            'status': 'ready',
            'available_collections': collections,
            'component_library_loaded': component_library_exists,
            'component_count': component_count,
            'supported_formats': ['csv', 'json', 'geojson', 'shapefile (zip)', 'excel']
        }
        
    except Exception as e:
        logger.error(f"Error checking import status: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }
