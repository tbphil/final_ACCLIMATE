"""
Process raw climate data: unit conversions, aggregations, composites.
"""
import logging
from typing import List, Optional
import numpy as np
import xarray as xr

from .composites.registry import get_composite_function, has_composite

logger = logging.getLogger(__name__)


class ClimateProcessor:
    """Handles data transformations and composite calculations"""
    
    def __init__(self):
        self.conversions_applied = []
    
    def convert_units_for_display(self, ds: xr.Dataset, variables: List[str]) -> xr.Dataset:
        ds = ds.copy()
        
        for var in variables:
            if var not in ds.data_vars:
                continue
            
            # Temperature variables
            if var in ["tas", "tasmax", "tasmin"]:
                logger.debug(f"Converting {var}: K → °F")
                ds[var] = (ds[var] - 273.15) * 9.0 / 5.0 + 32.0
                ds[var].attrs["units"] = "°F"
            
            # Wind variables
            elif var in ["sfcWind", "uas", "vas"]:
                logger.debug(f"Converting {var}: m/s → mph")
                ds[var] = ds[var] * 2.23694
                ds[var].attrs["units"] = "mph"
            
            # Precipitation
            elif var == "pr":
                logger.debug(f"Converting {var}: kg m-2 s-1 → inches/day")
                ds[var] = ds[var] * 86400.0 / 25.4
                ds[var].attrs["units"] = "inches/day"
        
        return ds
    
    def aggregate_members(
        self,
        ds: xr.Dataset,
        method: str = "mean",
        q: Optional[float] = None
    ) -> xr.Dataset:
        """
        Aggregate across ensemble members.
        
        Args:
            ds: Dataset with member_id dimension
            method: 'mean', 'max', 'min', or 'percentile'
            q: Quantile (0-100) if method='percentile'
        """
        if "member_id" not in ds.dims:
            return ds
        
        logger.info(f"Aggregating {len(ds.member_id)} members using {method}")
        
        if method == "mean":
            return ds.mean(dim="member_id", skipna=True)
        elif method == "max":
            return ds.max(dim="member_id", skipna=True)
        elif method == "min":
            return ds.min(dim="member_id", skipna=True)
        elif method == "percentile":
            if q is None:
                raise ValueError("Must provide 'q' for percentile aggregation")
            return ds.quantile(q / 100, dim="member_id", skipna=True)
        else:
            raise ValueError(f"Unknown aggregation method: {method}")
    
    def compute_composites(
        self,
        ds: xr.Dataset,
        composite_vars: List[str]
    ) -> xr.Dataset:
        """
        Compute composite variables and add to dataset.
        
        Args:
            ds: Dataset with base variables
            composite_vars: List of composite variable names to compute
        
        Returns:
            Dataset with composites added
        """
        ds = ds.copy()
        
        for comp_var in composite_vars:
            if not has_composite(comp_var):
                logger.warning(f"No composite function registered for '{comp_var}'")
                continue
            
            try:
                comp_func = get_composite_function(comp_var)
                logger.info(f"Computing composite: {comp_var}")
                ds[comp_var] = comp_func(ds)
            except Exception as e:
                logger.error(f"Failed to compute composite '{comp_var}': {e}")
                continue
        
        return ds
    
    def compute_grid_around_point(
        self,
        ds: xr.Dataset,
        lat: float,
        lon: float,
        num_cells: int = 0
    ) -> xr.Dataset:
        """
        Select grid cells around a point.
        
        Args:
            lat, lon: Center point
            num_cells: Number of cells to expand (0 = single cell)
        """
        # Find lat/lon coordinate names
        lat_coords = [c for c in ds.coords if 'lat' in c.lower()]
        lon_coords = [c for c in ds.coords if 'lon' in c.lower()]
        
        if not lat_coords or not lon_coords:
            raise ValueError("Dataset does not contain lat/lon coordinates")
        
        lat_name = lat_coords[0]
        lon_name = lon_coords[0]
        
        # Get coordinate arrays
        lats = ds[lat_name].values
        lons = ds[lon_name].values
        
        # Find closest point
        lat_idx = np.abs(lats - lat).argmin()
        lon_idx = np.abs(lons - lon).argmin()
        
        # Define slice range
        lat_start = max(lat_idx - num_cells, 0)
        lat_end = min(lat_idx + num_cells + 1, ds.sizes[lat_name])
        lon_start = max(lon_idx - num_cells, 0)
        lon_end = min(lon_idx + num_cells + 1, ds.sizes[lon_name])
        
        return ds.isel({
            lat_name: slice(lat_start, lat_end),
            lon_name: slice(lon_start, lon_end)
        })
    
    def compute_grid_in_bbox(
        self,
        ds: xr.Dataset,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float
    ) -> xr.Dataset:
        """
        Select all grid cells that intersect with the bounding box.
        
        Args:
            ds: xarray Dataset with climate data
            min_lat, max_lat: Latitude bounds
            min_lon, max_lon: Longitude bounds
        
        Returns:
            Dataset containing only cells that intersect the bbox
        """
        # Find lat/lon coordinate names
        lat_coords = [c for c in ds.coords if 'lat' in c.lower()]
        lon_coords = [c for c in ds.coords if 'lon' in c.lower()]
        
        if not lat_coords or not lon_coords:
            raise ValueError("Dataset does not contain lat/lon coordinates")
        
        lat_name = lat_coords[0]
        lon_name = lon_coords[0]
        
        # Get coordinate arrays
        lats = ds[lat_name].values
        lons = ds[lon_name].values
        
        # Find all cells whose centers fall within the bbox
        lat_mask = (lats >= min_lat) & (lats <= max_lat)
        lon_mask = (lons >= min_lon) & (lons <= max_lon)
        
        # Get indices
        lat_indices = np.where(lat_mask)[0]
        lon_indices = np.where(lon_mask)[0]
        
        if len(lat_indices) == 0 or len(lon_indices) == 0:
            raise ValueError(
                f"No grid cells found within bounding box "
                f"[{min_lat}, {max_lat}] x [{min_lon}, {max_lon}]"
            )
        
        logger.info(
            f"Selected {len(lat_indices)}x{len(lon_indices)} grid cells "
            f"within bbox [{min_lat:.2f}, {max_lat:.2f}] x [{min_lon:.2f}, {max_lon:.2f}]"
        )
        
        # Select the cells
        return ds.isel({
            lat_name: lat_indices,
            lon_name: lon_indices
        })