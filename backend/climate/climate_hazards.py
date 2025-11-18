"""
Single source of truth for all hazard configurations.
Adding a new hazard = add one definition here.
"""
from typing import List
from dataclasses import dataclass
from models import HazardEnum
@dataclass
class HazardDefinition:
    """Defines what variables a hazard needs and how to compute them"""
    name: str
    display_name: str
    base_variables: List[str]        # From data source (NA-CORDEX)
    composite_variables: List[str]   # Computed locally
    description: str = ""
    
    def all_variables(self) -> List[str]:
        """All variables this hazard provides"""
        return self.base_variables + self.composite_variables
    
    def needs_fetching(self) -> List[str]:
        """Only base variables need to be fetched"""
        return self.base_variables


# Hazard Registry
HAZARDS = {
    HazardEnum.heat_stress: HazardDefinition(
        name="Heat Stress",
        display_name="Heat Stress",
        base_variables=["tas", "hurs"],
        composite_variables=["hi"],
        description="Temperature and humidity-based heat stress"
    ),
    
    HazardEnum.drought: HazardDefinition(
        name="Drought",
        display_name="Drought",
        base_variables=["pr", "rsds", "sfcWind"],
        composite_variables=[],
        description="Precipitation deficit and evapotranspiration"
    ),
    
    HazardEnum.wind: HazardDefinition(
        name="Wind",
        display_name="Extreme Wind",
        base_variables=["sfcWind"],
        composite_variables=[],
        description="Surface wind speed"
    ),
    
    # To add new hazards:
    # "Extreme Cold": HazardDefinition(
    #     name="Extreme Cold",
    #     display_name="Extreme Cold",
    #     base_variables=["tasmin"],
    #     composite_variables=["wind_chill"],
    #     description="Low temperature events"
    # ),
}


def get_hazard(hazard_name: str) -> HazardDefinition:
    """Get hazard definition by name"""
    if hazard_name not in HAZARDS:
        raise ValueError(f"Unknown hazard: {hazard_name}. Available: {list(HAZARDS.keys())}")
    return HAZARDS[hazard_name]


def list_hazards() -> List[str]:
    """List all available hazard names"""
    return list(HAZARDS.keys())