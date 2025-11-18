"""
NA-CORDEX data source: S3/Zarr implementation.
FIXED: Uses grid cell intersection instead of center-point containment
"""
import logging
from typing import List, Dict, Tuple, Optional
import numpy as np
import xarray as xr
import s3fs
from functools import lru_cache

from .base import ClimateDataSource

logger = logging.getLogger(__name__)


class NACordexDataSource(ClimateDataSource):
    """NA-CORDEX climate data from S3 Zarr stores"""
    
    BASE_S3_URL = "s3://ncar-na-cordex/day"
    
    # Variable metadata
    VARIABLE_METADATA = {
        "tas": {
            "units": "K",
            "long_name": "Near-Surface Air Temperature",
            "display_units": "°F"
        },
        "hurs": {
            "units": "%",
            "long_name": "Near-Surface Relative Humidity",
            "display_units": "%"
        },
        "pr": {
            "units": "kg m-2 s-1",
            "long_name": "Precipitation",
            "display_units": "inches/day"
        },
        "rsds": {
            "units": "W m-2",
            "long_name": "Surface Downwelling Shortwave Radiation",
            "display_units": "W m-2"
        },
        "sfcWind": {
            "units": "m s-1",
            "long_name": "Near-Surface Wind Speed",
            "display_units": "mph"
        },
        "tasmax": {
            "units": "K",
            "long_name": "Daily Maximum Near-Surface Air Temperature",
            "display_units": "°F"
        },
        "tasmin": {
            "units": "K",
            "long_name": "Daily Minimum Near-Surface Air Temperature",
            "display_units": "°F"
        },
    }
    
    def __init__(self):
        """Initialize S3 connection"""
        try:
            self.fs = s3fs.S3FileSystem(anon=True)
            logger.info("Initialized NA-CORDEX data source")
        except Exception as e:
            logger.error(f"Failed to initialize S3 connection: {e}")
            raise
    
    @property
    def source_name(self) -> str:
        return "NA-CORDEX"
    
    def fetch_variables(
        self,
        variables: List[str],
        scenario: str,
        domain: str,
        lat_range: Tuple[float, float],
        lon_range: Tuple[float, float],
        time_range: Tuple[str, str],
        climate_model: Optional[str] = "all",
    ) -> xr.Dataset:
        """Fetch variables from S3 Zarr stores"""
        
        datasets = {}
        failed = []
        
        for var in variables:
            try:
                logger.info(f"Fetching variable: {var}")
                
                # Get S3 path (prefers bias-corrected)
                s3_path = self._get_s3_path(var, scenario, domain)
                
                # Open Zarr store
                ds = xr.open_zarr(
                    self.fs.get_mapper(s3_path),
                    consolidated=True,
                    chunks="auto"
                )
                
                # Filter by model if specified
                if climate_model not in ("all", "aggregate"):
                    if "member_id" in ds.coords:
                        ds = ds.sel(member_id=climate_model)
                    else:
                        logger.warning(
                            f"Variable {var} has no member_id dimension, "
                            f"cannot filter to model {climate_model}"
                        )
                
                # Temporal slice
                ds = ds.sel(time=slice(time_range[0], time_range[1]))
                
                # Spatial subset (CRITICAL for memory)
                ds = self._spatial_subset(ds, lat_range, lon_range)
                
                # Keep only this variable
                if var in ds.data_vars:
                    datasets[var] = ds[[var]]
                else:
                    logger.warning(f"Variable {var} not in dataset data_vars")
                    failed.append(var)
                
            except Exception as e:
                logger.error(f"Failed to fetch {var}: {e}")
                failed.append(var)
                continue
        
        if not datasets:
            raise RuntimeError(
                f"Failed to fetch any requested variables. "
                f"Requested: {variables}, Failed: {failed}"
            )
        
        if failed:
            logger.warning(f"Some variables could not be fetched: {failed}")
        
        # Merge all successfully fetched variables
        merged = xr.merge(list(datasets.values()), compat="no_conflicts")
        logger.info(f"Successfully fetched {len(datasets)} variables")
        
        return merged
    
    def list_available_models(
        self,
        variables: List[str],
        scenario: str,
        domain: str
    ) -> List[str]:
        """Find member_ids present in ALL variable Zarr stores"""
        return self._common_members(scenario, domain, tuple(variables))
    
    @lru_cache(maxsize=64)
    def _common_members(
        self,
        scenario: str,
        domain: str,
        variables: Tuple[str, ...]
    ) -> List[str]:
        """Cached implementation of member intersection"""
        member_sets = []
        
        for var in variables:
            try:
                s3_path = self._get_s3_path(var, scenario, domain)
                ds = xr.open_zarr(
                    self.fs.get_mapper(s3_path),
                    consolidated=True,
                    chunks="auto"
                )
                
                if "member_id" not in ds.coords:
                    logger.warning(f"Variable {var} has no member_id coordinate")
                    continue
                
                member_sets.append(
                    set(str(x) for x in ds.member_id.values)
                )
                
            except Exception as e:
                logger.warning(f"Could not read member_ids from {var}: {e}")
                continue
        
        if not member_sets:
            logger.warning(f"No member_ids found for variables: {variables}")
            return []
        
        # Return intersection of all sets
        common = set.intersection(*member_sets)
        result = sorted(common)
        
        logger.info(
            f"Found {len(result)} common members across "
            f"{len(variables)} variables"
        )
        
        return result
    
    def get_variable_metadata(self, variable: str) -> Dict[str, str]:
        """Get metadata for a variable"""
        return self.VARIABLE_METADATA.get(variable, {
            "units": "unknown",
            "long_name": variable,
            "display_units": "unknown"
        })
    
    def _get_s3_path(self, variable: str, scenario: str, domain: str) -> str:
        """Build S3 Zarr store path, preferring bias-corrected"""
        # Try bias-corrected first
        path_mbcn = (
            f"{self.BASE_S3_URL}/{variable}.hist-{scenario}.day."
            f"{domain}.mbcn-gridMET.zarr"
        )
        
        if self._s3_exists(path_mbcn):
            logger.debug(f"Using bias-corrected path: {path_mbcn}")
            return path_mbcn
        
        # Fall back to raw
        path_raw = (
            f"{self.BASE_S3_URL}/{variable}.hist-{scenario}.day."
            f"{domain}.raw.zarr"
        )
        
        logger.debug(f"Using raw path: {path_raw}")
        return path_raw
    
    def _s3_exists(self, path: str) -> bool:
        """Check if S3 path exists"""
        try:
            return self.fs.exists(path)
        except Exception as e:
            logger.debug(f"S3 exists check failed for {path}: {e}")
            return False
    
    def _spatial_subset(
        self,
        ds: xr.Dataset,
        lat_range: Tuple[float, float],
        lon_range: Tuple[float, float]
    ) -> xr.Dataset:
        """
        Subset dataset to include ANY grid cell that INTERSECTS the bounding box.
        
        Previously: Only selected cells whose CENTER fell within bbox.
        Now: Selects cells whose EXTENT overlaps with bbox.
        
        This ensures small drawn boxes capture at least some data.
        """
        lat_coords = [c for c in ds.coords if 'lat' in c.lower()]
        lon_coords = [c for c in ds.coords if 'lon' in c.lower()]
        
        if not lat_coords or not lon_coords:
            logger.warning("Could not find lat/lon coordinates for subsetting")
            return ds
        
        lat_name = lat_coords[0]
        lon_name = lon_coords[0]
        
        min_lat, max_lat = lat_range
        min_lon, max_lon = lon_range
        
        # Get coordinate arrays (these are cell CENTERS)
        lats = ds[lat_name].values
        lons = ds[lon_name].values
        
        # Calculate grid cell size (approximate, assumes regular grid)
        if len(lats) > 1:
            lat_spacing = abs(lats[1] - lats[0])
        else:
            lat_spacing = 0.22  # NA-CORDEX NAM-22i default
        
        if len(lons) > 1:
            lon_spacing = abs(lons[1] - lons[0])
        else:
            lon_spacing = 0.22  # NA-CORDEX NAM-22i default
        
        # Half-width of each grid cell
        lat_half = lat_spacing / 2
        lon_half = lon_spacing / 2
        
        logger.debug(f"Grid spacing: lat={lat_spacing:.4f}°, lon={lon_spacing:.4f}°")
        
        # Handle longitude wrapping (0-360 vs -180 to 180)
        if lons.min() >= 0 and min_lon < 0:
            logger.info("Converting longitude from -180:180 to 0:360 format")
            min_lon = min_lon % 360
            max_lon = max_lon % 360
        
        # INTERSECTION TEST: A cell intersects if:
        # cell_max >= bbox_min AND cell_min <= bbox_max
        lat_intersects = (lats + lat_half >= min_lat) & (lats - lat_half <= max_lat)
        lon_intersects = (lons + lon_half >= min_lon) & (lons - lon_half <= max_lon)
        
        lat_indices = np.where(lat_intersects)[0]
        lon_indices = np.where(lon_intersects)[0]
        
        # Diagnostics
        logger.info(f"Dataset lat range: [{lats.min():.4f}, {lats.max():.4f}]")
        logger.info(f"Dataset lon range: [{lons.min():.4f}, {lons.max():.4f}]")
        logger.info(f"Requested bbox: [{min_lat:.4f}, {max_lat:.4f}] x [{min_lon:.4f}, {max_lon:.4f}]")
        logger.info(f"Cells intersecting bbox: {len(lat_indices)} lat × {len(lon_indices)} lon")
        
        if len(lat_indices) == 0 or len(lon_indices) == 0:
            logger.error(
                f"NO GRID CELLS INTERSECT BOUNDING BOX!\n"
                f"  Bbox: [{min_lat:.4f}, {max_lat:.4f}] x [{min_lon:.4f}, {max_lon:.4f}]\n"
                f"  Dataset coverage: [{lats.min():.4f}, {lats.max():.4f}] x [{lons.min():.4f}, {lons.max():.4f}]\n"
                f"  Grid spacing: ~{lat_spacing:.4f}° × {lon_spacing:.4f}°\n"
                f"  Possible causes:\n"
                f"    - Bbox outside dataset coverage\n"
                f"    - Longitude format mismatch (check if dataset uses 0-360)"
            )
            # Return empty dataset rather than crash
            return ds.isel({lat_name: slice(0, 0), lon_name: slice(0, 0)})
        
        # Use isel (index selection) instead of sel (label selection)
        subset = ds.isel({
            lat_name: lat_indices,
            lon_name: lon_indices
        })
        
        logger.info(f"Successfully subset to {len(lat_indices)}×{len(lon_indices)} cells")
        
        return subset