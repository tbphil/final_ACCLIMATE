"""
HBOM Module
Modular HBOM and fragility data retrieval system

Public exports:
- router: FastAPI router for HBOM endpoints
- HBOMFetcher: Main pipeline orchestrator
"""

from .hbom_router import router
from .hbom_fetcher import HBOMFetcher
from .hbom_preparers import reconstruct_tree, prepare_for_frontend

__all__ = [
    "router",
    "HBOMFetcher",
    "reconstruct_tree",
    "prepare_for_frontend",
]