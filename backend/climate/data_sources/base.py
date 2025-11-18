"""
Abstract interface for climate data sources.
Any data provider must implement this interface.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Optional
import xarray as xr


class ClimateDataSource(ABC):
    """Abstract base class for climate data providers"""
    
    @abstractmethod
    def fetch_variables(
        self,
        variables: List[str],
        scenario: str,
        domain: str,
        lat_range: Tuple[float, float],
        lon_range: Tuple[float, float],
        time_range: Tuple[str, str],
        climate_model: Optional[str] = "all",
        **kwargs
    ) -> xr.Dataset:
        """
        Fetch climate variables and return as xarray Dataset.
        
        Args:
            variables: List of variable codes (e.g., ["tas", "hurs"])
            scenario: Climate scenario (e.g., "rcp85")
            domain: Grid domain (e.g., "NAM-22i")
            lat_range: (min_lat, max_lat)
            lon_range: (min_lon, max_lon)
            time_range: (start_date, end_date) as ISO strings
            climate_model: Model/member ID or "all"
            
        Returns:
            xarray.Dataset with requested variables
        """
        pass
    
    @abstractmethod
    def list_available_models(
        self,
        variables: List[str],
        scenario: str,
        domain: str
    ) -> List[str]:
        """
        List ensemble member IDs that have ALL requested variables.
        
        Returns:
            List of member_id strings
        """
        pass
    
    @abstractmethod
    def get_variable_metadata(self, variable: str) -> Dict:
        """
        Get metadata for a variable.
        
        Returns:
            Dict with keys: units, long_name, standard_name, etc.
        """
        pass
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Identifier for this data source"""
        pass