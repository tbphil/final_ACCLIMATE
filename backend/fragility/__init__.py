"""
Fragility Module
Fragility curve computation for HBOM trees

Public exports:
- router: FastAPI router for fragility endpoints
- FragilityComputer: Computation engine
"""

from .fragility_router import router
from .fragility_computer import FragilityComputer

__all__ = [
    "router",
    "FragilityComputer",
]