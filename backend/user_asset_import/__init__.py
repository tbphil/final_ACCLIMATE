"""
User Asset Import Module

Handles importing user-uploaded asset data in various formats:
- CSV
- JSON/GeoJSON
- Shapefiles (ZIP)
- Excel

Supports multiple projects with different configurations:
- ACCLIMATE (FastAPI)
- Resilience Guard (Flask)

Auto-detects column mappings and component types, with fallback to user input.
"""

# ============================================================================
# PROJECT CONFIGURATION - Set which project to use
# ============================================================================
USE_ACCLIMATE = True        # ACCLIMATE (FastAPI) project
USE_RESILIENCE_GUARD = False  # Resilience Guard (Flask) project

# ============================================================================

__all__ = []

# Import ACCLIMATE (FastAPI)
if USE_ACCLIMATE:
    try:
        from . import acclimate
        from .acclimate import router
        __all__.extend(['acclimate', 'router'])
    except ImportError as e:
        raise ImportError(f"Failed to import acclimate module: {e}")

# Import Resilience Guard (Flask)
elif USE_RESILIENCE_GUARD:
    try:
        from . import resilience_guard
        __all__.append('resilience_guard')
    except ImportError as e:
        raise ImportError(f"Failed to import resilience_guard module: {e}")

else:
    raise RuntimeError(
        "No project configured. Set either USE_ACCLIMATE=True or USE_RESILIENCE_GUARD=True "
        "in backend/user_asset_import/__init__.py"
    )
