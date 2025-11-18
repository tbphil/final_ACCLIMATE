"""
Registry for composite variable calculations.
Each composite is a function that takes xr.Dataset and returns xr.DataArray.
"""
from typing import Dict, Callable
import xarray as xr

# Type alias
CompositeFunction = Callable[[xr.Dataset], xr.DataArray]

# Global registry
_COMPOSITES: Dict[str, CompositeFunction] = {}


def register_composite(name: str):
    """Decorator to register a composite function"""
    def decorator(func: CompositeFunction) -> CompositeFunction:
        _COMPOSITES[name] = func
        return func
    return decorator


def get_composite_function(name: str) -> CompositeFunction:
    """Get a registered composite function"""
    if name not in _COMPOSITES:
        raise ValueError(f"Unknown composite variable: {name}")
    return _COMPOSITES[name]


def list_composites() -> list[str]:
    """List all registered composite variables"""
    return list(_COMPOSITES.keys())


def has_composite(name: str) -> bool:
    """Check if a composite is registered"""
    return name in _COMPOSITES

def compute_composite(name: str, ds: xr.Dataset) -> xr.DataArray:
    """
    Compute a composite variable.
    
    Args:
        name: Composite variable name
        ds: Dataset containing required base variables
        
    Returns:
        DataArray with computed composite values
    """
    func = get_composite_function(name)
    return func(ds)