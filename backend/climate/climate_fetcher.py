"""
Main orchestrator for climate data fetching pipeline.
FIXED: Removed redundant spatial subsetting (now handled by data source)
"""
import logging
from typing import Dict, Any
import datetime

from .data_sources.base import ClimateDataSource
from models import DataRequest
from .climate_hazards import get_hazard
from .climate_processors import ClimateProcessor
from .climate_preparers import FrontendPreparer

logger = logging.getLogger(__name__)


class ClimateFetcher:
    """Orchestrates the climate data pipeline"""
    
    def __init__(self, data_source: ClimateDataSource):
        self.data_source = data_source
        self.processor = ClimateProcessor()
        self.preparer = FrontendPreparer()
    
    def fetch_for_request(self, request: DataRequest) -> Dict[str, Any]:
        """
        Main entry point: fetch, process, and format climate data.
        
        Args:
            request: DataRequest Pydantic model from API
            
        Returns:
            Dict ready for ClimateData Pydantic model
        """
        # 1. Get hazard definition
        hazard = get_hazard(request.hazard.value)
        logger.info(f"Fetching data for hazard: {hazard.name}")
        
        # 2. Calculate time range
        now_year = datetime.datetime.now().year
        start_year = now_year - (request.prior_years or 1)
        end_year = now_year + (request.future_years or 1)
        time_range = (f"{start_year}-01-01", f"{end_year}-12-31")
        
        # 3. Calculate spatial range
        cell_degrees = 0.22  # Approximate NA-CORDEX grid resolution
        
        # Determine if using point or bbox mode
        using_bbox = all(v is not None for v in [request.min_lat, request.max_lat, request.min_lon, request.max_lon])
        
        if using_bbox:
            # Bbox mode: use provided bounds directly
            logger.info(f"Using bounding box mode: [{request.min_lat}, {request.max_lat}] x [{request.min_lon}, {request.max_lon}]")
            lat_range = (request.min_lat, request.max_lat)
            lon_range = (request.min_lon, request.max_lon)
        else:
            # Point mode: calculate buffer around center point
            logger.info(f"Using point mode: ({request.lat}, {request.lon}) with {request.num_cells} cells")
            buffer = cell_degrees * (request.num_cells or 0)
            lat_range = (request.lat - buffer, request.lat + buffer)
            lon_range = (request.lon - buffer, request.lon + buffer)
        
        # 4. Fetch base variables only
        # NOTE: The data source (na_cordex.py) now handles spatial subsetting
        # using intersection-based selection, so we get the correct cells here.
        logger.info(f"Fetching base variables: {hazard.base_variables}")
        raw_data = self.data_source.fetch_variables(
            variables=hazard.base_variables,
            scenario=request.scenario.value,
            domain=request.domain,
            lat_range=lat_range,
            lon_range=lon_range,
            time_range=time_range,
            climate_model=request.climate_model
        )
        
        # 5. Verify we got data
        lat_coord = [c for c in raw_data.coords if 'lat' in c.lower()][0]
        lon_coord = [c for c in raw_data.coords if 'lon' in c.lower()][0]
        n_lat = len(raw_data.coords[lat_coord])
        n_lon = len(raw_data.coords[lon_coord])
        logger.info(f"Fetched {n_lat} × {n_lon} grid cells")
        
        if n_lat == 0 or n_lon == 0:
            raise ValueError(
                f"No grid cells found in spatial range. "
                f"Lat range: {lat_range}, Lon range: {lon_range}"
            )
        
        # 6. Aggregate members if requested
        needs_aggregation = (
            request.aggregate_over_member_id and
            request.climate_model in ("all", "aggregate")
        )
        
        if needs_aggregation:
            logger.info(f"Aggregating members using {request.aggregation_method}")
            agg_kwargs = {}
            if request.aggregation_method.value == "percentile":
                agg_kwargs["q"] = request.aggregation_q
            
            raw_data = self.processor.aggregate_members(
                raw_data,
                method=request.aggregation_method.value,
                **agg_kwargs
            )
        
        # 7. Compute composites
        if hazard.composite_variables:
            logger.info(f"Computing composites: {hazard.composite_variables}")
            raw_data = self.processor.compute_composites(
                raw_data,
                hazard.composite_variables
            )
        
        # 8. Convert units
        all_vars = hazard.all_variables()
        logger.info("Converting units for display")
        processed_data = self.processor.convert_units_for_display(raw_data, all_vars)
        
        # 9. Get metadata for response
        variable_metadata = {
            var: self.data_source.get_variable_metadata(var)
            for var in hazard.base_variables
        }
        # Add composite metadata
        for comp_var in hazard.composite_variables:
            if comp_var in processed_data.data_vars:
                variable_metadata[comp_var] = {
                    "units": processed_data[comp_var].attrs.get("units", ""),
                    "long_name": processed_data[comp_var].attrs.get("long_name", comp_var)
                }
        
        # 10. Format for frontend
        logger.info("Preparing data for frontend")
        response = self.preparer.prepare(
            processed_data,
            all_vars,
            variable_metadata,
            aggregate_over_members=needs_aggregation
        )
        
        logger.info("Climate data pipeline complete")
        return response