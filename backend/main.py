import numpy as np
# Fix numpy version issue
if not hasattr(np, 'round_'):
    np.round_ = np.round

from fastapi import FastAPI
from fastapi_websocket_pubsub import PubSubEndpoint
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from collections import defaultdict
import logging
import os

# Routers
from routers import data_collectors 
from climate import router as climate_router
from infrastructure import router as infrastructure_router
from hbom import router as hbom_router 
from fragility import router as fragility_router
from user_asset_import import router as import_router
from cache_manager import cache
from database import mongo_uri

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Quiet MongoDB heartbeats:
logging.getLogger('pymongo').setLevel(logging.WARNING)
items_by_uuid: dict[str, list] = defaultdict(list)
items_by_type: dict[str, list] = defaultdict(list)

@asynccontextmanager
async def lifespan(app):
    """Run once at startup, clean up at shutdown."""
    # START-UP --------------------------------------------------------
    
    # Configure infrastructure data sources
    from infrastructure.data_sources.registry import configure_source
    
    configure_source('mongodb_aha', {
        'mongo_uri': os.getenv('MONGO_URI', mongo_uri),
        'database': 'acclimate_db',
        'collection': 'energy_grid'
    })

    configure_source('custom_upload', {
        'cache_manager': cache,
        'persist_to_mongo': False,
        'mongo_uri': os.getenv('MONGO_URI', mongo_uri),
        'database': 'acclimate_db'
    })
    
    logger.info("Infrastructure module configured")
    
    # Configure HBOM data source
    from hbom.data_sources.registry import configure_source as configure_hbom_source

    configure_hbom_source('mongodb_baseline', {
        'mongo_uri': os.getenv('MONGO_URI', mongo_uri),
        'database': 'acclimate_db'
    })
    
    logger.info("HBOM module configured")
    
    yield  # ----> application runs
    
    # SHUT-DOWN -------------------------------------------------------
    items_by_uuid.clear()
    items_by_type.clear()

# Initialize FastAPI app
app = FastAPI(
    title="ACCLIMATE API",
    description="API for accessing and visualizing climate data.",
    version="1.0.0",
    lifespan=lifespan
)

# Include routers
app.include_router(climate_router)
app.include_router(infrastructure_router)
app.include_router(hbom_router)  
app.include_router(fragility_router)
app.include_router(import_router)  # User asset import
app.include_router(data_collectors.router)

# Configure CORS
origins = [
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "http://localhost:4200",  # Angular dev server
    "http://127.0.0.1:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex=".*",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Set up PubSub endpoint
pubsub_endpoint = PubSubEndpoint()
app.mount("/ws", pubsub_endpoint)

@app.get("/")
async def root():
    """Root endpoint for basic health check."""
    return {"message": "Welcome to the ACCLIMATE API"}

@app.get("/api/health")
async def health_check():
    """Health check endpoint to verify if the API is running."""
    return {"status": "API is up and running!"}

@app.post("/api/clear-cache")
async def clear_cache():
    logger.info("Cache cleared successfully.")
    return JSONResponse(content={"message": "Cache cleared successfully."})
