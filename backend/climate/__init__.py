"""
Climate data pipeline package.

Public exports:
- router: FastAPI router for climate endpoints
- ClimateFetcher: Main pipeline orchestrator
- ClimateAnalyzer: Trend analysis engine
"""

from .climate_router import router
from .climate_fetcher import ClimateFetcher
from .climate_analyzers import ClimateAnalyzer
from .climate_hazards import get_hazard, list_hazards

__all__ = [
    "router",
    "ClimateFetcher", 
    "ClimateAnalyzer",
    "get_hazard",
    "list_hazards"
]