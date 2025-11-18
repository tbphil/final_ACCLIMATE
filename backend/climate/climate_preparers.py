"""
Prepare climate data for frontend consumption.
Formats xarray.Dataset into the structure expected by Pinia store.
UPDATED: Now runs climate analysis automatically
"""
import logging
from typing import List, Dict, Any, Optional
import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)


class FrontendPreparer:
    """Formats processed climate data for frontend"""
    
    def __init__(self):
        # Lazy import to avoid circular dependencies
        self._analyzer = None
    
    @property
    def analyzer(self):
        """Lazy-load analyzer to avoid import issues"""
        if self._analyzer is None:
            from .climate_analyzers import ClimateAnalyzer
            self._analyzer = ClimateAnalyzer()
        return self._analyzer
    
    def prepare(
        self,
        ds: xr.Dataset,
        variables: List[str],
        variable_metadata: Dict[str, Dict],
        aggregate_over_members: bool = True,
        run_analysis: bool = True  # ← New parameter
    ) -> Dict[str, Any]:
        """
        Convert xarray.Dataset to frontend JSON structure.
        
        Returns structure matching ClimateData Pydantic model:
        {
            "variables": [...],
            "variable_long_names": [...],
            "times": [...],
            "bounding_box": {...},
            "data": [...],  # or "members": [...]
            "climate_analysis": {...}  # ← Added automatically
        }
        """
        # Extract times
        times = self._extract_times(ds)
        
        # Extract spatial bounds
        bounding_box = self._extract_bounding_box(ds)
        
        # Variable long names
        variable_long_names = [
            variable_metadata.get(v, {}).get("long_name", v)
            for v in variables
        ]
        
        # Build base response
        response = {
            "variables": variables,
            "variable_long_names": variable_long_names,
            "times": times,
            "bounding_box": bounding_box,
        }
        
        # Format data
        if aggregate_over_members or "member_id" not in ds.dims:
            response["data"] = self._format_aggregated_data(ds, variables)
        else:
            response["members"] = self._format_member_data(ds, variables)
            response["data"] = self._format_grid_data(ds, variables)
        
        # Run climate analysis automatically
        if run_analysis and response.get("data"):
            try:
                logger.info("Running climate trend analysis...")
                analysis_result = self.analyzer.analyze_all_variables(
                    response,  # Pass the prepared data structure
                    variables
                )
                response["climate_analysis"] = analysis_result
                logger.info("✓ Climate analysis complete")
            except Exception as e:
                logger.error(f"Climate analysis failed: {e}")
                # Don't fail the whole request, just skip analysis
                response["climate_analysis"] = {"analysis_results": {}}
        
        return response
    
    def _extract_times(self, ds: xr.Dataset) -> List[str]:
        """Extract time coordinates as ISO strings"""
        if "time" not in ds.coords:
            return []
        return ds["time"].values.astype(str).tolist()
    
    def _extract_bounding_box(self, ds: xr.Dataset) -> Dict[str, float]:
        """Extract spatial bounds"""
        lat_coords = [c for c in ds.coords if 'lat' in c.lower()]
        lon_coords = [c for c in ds.coords if 'lon' in c.lower()]
        
        if not lat_coords or not lon_coords:
            return {"min_lat": 0, "max_lat": 0, "min_lon": 0, "max_lon": 0}
        
        lat_name = lat_coords[0]
        lon_name = lon_coords[0]
        
        lats = ds[lat_name].values
        lons = ds[lon_name].values
        
        # Calculate offsets (half grid cell)
        lat_offset = abs(lats[1] - lats[0]) / 2 if len(lats) > 1 else 0.125
        lon_offset = abs(lons[1] - lons[0]) / 2 if len(lons) > 1 else 0.125
        
        return {
            "min_lat": float(lats.min() - lat_offset),
            "max_lat": float(lats.max() + lat_offset),
            "min_lon": float(lons.min() - lon_offset),
            "max_lon": float(lons.max() + lon_offset),
        }
    
    def _format_aggregated_data(
        self,
        ds: xr.Dataset,
        variables: List[str]
    ) -> List[Dict]:
        """Format aggregated (single value per timestep) data"""
        lat_coords = [c for c in ds.coords if 'lat' in c.lower()]
        lon_coords = [c for c in ds.coords if 'lon' in c.lower()]
        
        if not lat_coords or not lon_coords:
            logger.error("Cannot format data: missing lat/lon coordinates")
            return []
        
        lat_name = lat_coords[0]
        lon_name = lon_coords[0]
        
        lats = ds[lat_name].values
        lons = ds[lon_name].values
        
        lat_offset = abs(lats[1] - lats[0]) / 2 if len(lats) > 1 else 0.125
        lon_offset = abs(lons[1] - lons[0]) / 2 if len(lons) > 1 else 0.125
        
        grid_data = []
        
        for i, lat in enumerate(lats):
            for j, lon in enumerate(lons):
                bounds = {
                    "min_lat": float(lat - lat_offset),
                    "max_lat": float(lat + lat_offset),
                    "min_lon": float(lon - lon_offset),
                    "max_lon": float(lon + lon_offset),
                }
                
                # Extract timeseries for each variable at this grid point
                climate_data = {}
                for var in variables:
                    if var not in ds.data_vars:
                        climate_data[var] = None
                        continue
                    
                    arr = ds[var].values
                    # Shape: (time, lat, lon)
                    timeseries = arr[:, i, j].tolist()
                    # Convert NaN/Inf to None
                    climate_data[var] = [
                        None if (isinstance(v, float) and (np.isnan(v) or np.isinf(v)))
                        else float(v)
                        for v in timeseries
                    ]
                
                grid_data.append({
                    "grid_index": len(grid_data),
                    "bounds": bounds,
                    "climate": climate_data
                })
        
        return grid_data
    
    def _format_member_data(
        self,
        ds: xr.Dataset,
        variables: List[str]
    ) -> List[Dict]:
        """Format per-member timeseries"""
        if "member_id" not in ds.coords:
            return []
        
        member_ids = ds["member_id"].values
        
        members = []
        for member_id in member_ids:
            ds_member = ds.sel(member_id=member_id)
            
            member_data = {"member_id": str(member_id)}
            
            for var in variables:
                if var not in ds_member.data_vars:
                    member_data[var] = None
                    continue
                
                # Assume first grid point for member view
                arr = ds_member[var].values
                if len(arr.shape) == 3:  # (time, lat, lon)
                    timeseries = arr[:, 0, 0].tolist()
                else:  # (time,)
                    timeseries = arr.tolist()
                
                member_data[var] = [
                    None if (isinstance(v, float) and (np.isnan(v) or np.isinf(v)))
                    else float(v)
                    for v in timeseries
                ]
            
            members.append(member_data)
        
        return members
    
    def _format_grid_data(
        self,
        ds: xr.Dataset,
        variables: List[str]
    ) -> List[Dict]:
        """Format grid data with all members preserved"""
        # For per-member + per-grid-cell case
        return self._format_aggregated_data(ds, variables)