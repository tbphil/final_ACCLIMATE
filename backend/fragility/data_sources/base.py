"""
Abstract interface for climate data sources (fragility module)
Defines how fragility computation accesses climate data

Location: backend/fragility/data_sources/base.py
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class ClimateDataSource(ABC):
    """
    Abstract base class for climate data providers.
    
    Fragility computation needs prepared climate data with:
    - variables: List of climate variable names
    - times: Time axis
    - data: Grid cells with climate[var] time series
    """
    
    @abstractmethod
    def get_prepared_data(self, hazard: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve prepared climate data for a hazard.
        
        Args:
            hazard: Hazard type
        
        Returns:
            Dictionary with structure:
            {
                "variables": ["tas", "hurs", ...],
                "times": ["2025-01-01", ...],
                "data": [
                    {
                        "grid_index": 0,
                        "bounds": {...},
                        "climate": {
                            "tas": [temp_t0, temp_t1, ...],
                            "hurs": [rh_t0, rh_t1, ...]
                        }
                    }
                ]
            }
        """
        pass
    
    @abstractmethod
    def validate_cache(self, hazard: str) -> bool:
        """
        Check if climate data is available for hazard.
        
        Args:
            hazard: Hazard type
        
        Returns:
            True if data exists, False otherwise
        """
        pass
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Human-readable source name"""
        pass