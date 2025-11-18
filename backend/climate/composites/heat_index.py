"""
Heat Index composite calculation.
"""
import numpy as np
import xarray as xr
from .registry import register_composite


@register_composite("hi")
def compute_heat_index(ds: xr.Dataset) -> xr.DataArray:
    """
    Compute Heat Index from temperature and relative humidity.
    
    Requires:
        - tas: temperature in Kelvin
        - hurs: relative humidity in %
    
    Returns:
        Heat Index in Fahrenheit
    """
    # Convert K to F
    T = (ds["tas"] - 273.15) * 9/5 + 32
    RH = ds["hurs"]
    
    # Initialize with temperature
    HI = T.copy()
    
    # Apply Rothfusz regression where T >= 80°F and RH >= 40%
    mask = (T >= 80) & (RH >= 40)
    
    HI_calculated = (
        -42.379
        + 2.04901523 * T
        + 10.14333127 * RH
        - 0.22475541 * T * RH
        - 0.00683783 * T**2
        - 0.05481717 * RH**2
        + 0.00122874 * T**2 * RH
        + 0.00085282 * T * RH**2
        - 0.00000199 * T**2 * RH**2
    )
    
    HI = xr.where(mask, HI_calculated, HI)
    
    # Create DataArray with metadata
    hi_da = xr.DataArray(
        HI,
        dims=T.dims,
        coords=T.coords,
        attrs={
            "units": "°F",
            "long_name": "Heat Index",
            "description": "Apparent temperature from temperature and humidity"
        }
    )
    
    return hi_da