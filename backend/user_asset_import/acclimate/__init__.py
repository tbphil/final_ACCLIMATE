"""
ACCLIMATE Project - User Asset Import Configuration
"""

from . import config

__all__ = ['config']

# Conditional import of FastAPI endpoint
try:
    from .fastapi_endpoint import router
    __all__.append('router')
except ImportError:
    router = None
