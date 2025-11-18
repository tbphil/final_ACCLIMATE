# backend/models.py  –– clean, unified version
from __future__ import annotations

import datetime
from enum import Enum
from typing import (
    Annotated,
    Dict,
    List,
    Literal,
    Optional,
    Union,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
import uuid
# ---------------------------------------------------------------------------#
#  0.  ENUMS
# ---------------------------------------------------------------------------#
class ScenarioEnum(str, Enum):
    rcp85 = "rcp85"
    rcp45 = "rcp45"


class AggregationEnum(str, Enum):
    mean = "mean"
    median = "median"
    max = "max"
    min = "min"
    percentile = "percentile"


class SectorEnum(str, Enum):
    energy_grid = "Energy Grid"
    agriculture = "Agriculture"


class HazardEnum(str, Enum):
    heat_stress = "Heat Stress"
    drought = "Drought"
    wind = "Wind"


# ---------------------------------------------------------------------------#
#  1.  REQUEST MODELS
# ---------------------------------------------------------------------------#
class DataRequest(BaseModel):
    hazard: HazardEnum = Field(HazardEnum.heat_stress, description="Climate hazard to query")
    scenario: ScenarioEnum = Field(ScenarioEnum.rcp85, description="Climate scenario")
    domain: str = Field("NAM-22i", description="Geographical domain / grid")
    
    # Point-based selection (optional - use with num_cells)
    lat: Optional[float] = Field(None, description="Latitude in decimal degrees (for point selection)")
    lon: Optional[float] = Field(None, description="Longitude in decimal degrees (for point selection)")
    num_cells: Optional[int] = Field(None, ge=0, le=10, description="How many cells to expand around the target grid point")
    
    # Bounding box selection (optional alternative to point)
    min_lat: Optional[float] = Field(None, description="Minimum latitude for bounding box")
    max_lat: Optional[float] = Field(None, description="Maximum latitude for bounding box")
    min_lon: Optional[float] = Field(None, description="Minimum longitude for bounding box")
    max_lon: Optional[float] = Field(None, description="Maximum longitude for bounding box")
    
    prior_years: Optional[int] = Field(1, ge=0, le=100, description="Years before current year")
    future_years: Optional[int] = Field(1, ge=0, le=100, description="Years after current year")
    climate_model: Optional[str] = Field("all", description="Which climate model to use (or 'all'/'aggregate')")
    aggregate_over_member_id: bool = Field(True, description="If true, collapse ensemble over member_id")
    aggregation_method: AggregationEnum = AggregationEnum.mean
    aggregation_q: Optional[int] = Field(None, ge=0, le=100, description="Quantile (0-100) if using percentile")
    sector: SectorEnum = Field(SectorEnum.energy_grid, description="Sector to filter infra data")
    
    @field_validator("climate_model", mode="after")
    def blank_means_all(cls, v: str) -> str:
        """
        Treat an empty string or None the same as the sentinel 'all'.
        This makes the API tolerant of UIs that send `""` instead of omitting
        the field or sending the literal 'all'.
        """
        return v or "all"
    
    @model_validator(mode="after")
    def _validate_spatial_selection(self):
        """Ensure either point OR bbox is provided, not both or neither"""
        has_point = all(v is not None for v in [self.lat, self.lon])
        has_bbox = all(v is not None for v in [self.min_lat, self.max_lat, self.min_lon, self.max_lon])
        
        if not has_point and not has_bbox:
            raise ValueError("Must provide either (lat, lon) for point selection OR (min_lat, max_lat, min_lon, max_lon) for bbox selection")
        
        if has_point and has_bbox:
            raise ValueError("Cannot provide both point and bbox - choose one selection method")
        
        # Validate bbox bounds if provided
        if has_bbox:
            if self.min_lat >= self.max_lat:
                raise ValueError("min_lat must be less than max_lat")
            if self.min_lon >= self.max_lon:
                raise ValueError("min_lon must be less than max_lon")
        
        return self

# ---------------------------------------------------------------------------#
#  2.  INFRASTRUCTURE MODELS
# ---------------------------------------------------------------------------#
class InfrastructureRequest(BaseModel):
    sector: str
    min_lat: Optional[float] = None
    max_lat: Optional[float] = None
    min_lon: Optional[float] = None
    max_lon: Optional[float] = None
    hazard: str | None = None

    class Config:
        from_attributes = True

class InfrastructureBase(BaseModel):
    id: str
    sector: str = Field(..., description="Sector identifier for the asset")
    name: str = Field(..., description="Facility or asset name")
    facilityTypeName: str = Field("", description="Facility type")
    county: str = Field("", description="County")
    state: str = Field("", description="State")
    latitude: float = Field(..., description="Latitude")
    longitude: float = Field(..., description="Longitude")
    source_sheet: Optional[str] = None
    source_workbook: Optional[str] = None


class EnergyGrid(InfrastructureBase):
    sector: Literal["Energy Grid"] = "Energy Grid"
    balancingauthority: Optional[str] = None
    eia_plant_id: Optional[str] = None
    lines: Optional[int] = None
    min_voltage: Optional[float] = None
    max_voltage: Optional[float] = None


InfrastructureUnion = Annotated[
    Union[EnergyGrid], Field(discriminator="sector")
]

# ---------------------------------------------------------------------------#
#  3.  CLIMATE-DATA MODELS (core of the current refactor)
# ---------------------------------------------------------------------------#
class GridBounds(BaseModel):
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float


class ClimateVariables(BaseModel):
    """
    Raw variables for **one** grid cell / timestep (aggregated ensemble)
    """

    model_config = ConfigDict(extra="allow")  # accept pr, tasmax, etc.

    tas: List[Optional[float]] # °C
    hurs: List[Optional[float]]  # % RH


class AnalysisResult(BaseModel):
    composite_metric: List[float]
    dates: List[str]
    trend_line: List[float]
    slope: float
    intercept: float
    histogram_counts: List[float]
    histogram_bins: List[float]
    mean_value: float
    median_value: float
    std_dev: float


class ClimateAnalysis(BaseModel):
    analysis_results: Dict[str, List[AnalysisResult]]


class GridData(BaseModel):
    """
    Single grid cell (already aggregated across ensemble members)
    """

    grid_index: int
    bounds: GridBounds
    climate: ClimateVariables


class MemberSeries(BaseModel):
    """
    One entire time-series for a single ensemble member (when the
    request sets aggregate_over_member_id=False)
    """

    model_config = ConfigDict(extra="allow")

    member_id: str
    tas: List[Optional[float]]
    hurs: List[Optional[float]]

class AOIDemographics(BaseModel):
    years: List[int]
    population: List[float] = []
    households: List[float] = []
    median_hhi: List[float] = []
    per_capita_income: List[float] = []

class ClimateData(BaseModel):
    # ----- meta -----
    variables: List[str]
    variable_long_names: List[str]
    times: List[str]
    bounding_box: GridBounds

    # ----- optional heavy payloads -----
    climate_analysis: Optional[ClimateAnalysis] = None

    # ----- mutually exclusive data payloads -----
    data: Optional[List[GridData]] = Field(
        default=None, description="Aggregated over member_id"
    )
    members: Optional[List[MemberSeries]] = Field(
        default=None, description="Separate series per ensemble member"
    )
    aoi_demographics: Optional[AOIDemographics] = None
    
    @model_validator(mode="after")
    def _either_data_or_members(self):
        if self.data is None and self.members is None:
            raise ValueError("Provide `data` (aggregated) or `members` (per-member).")
        return self


# ---------------------------------------------------------------------------#
#  4.  FRAGILITY & HBOM MODELS
# ---------------------------------------------------------------------------#
class FragilityDetails(BaseModel):
    fragility_model: Optional[str] = Field(
        None, description="e.g. Weibull, Lognormal, Logistic, inherit"
    )
    fragility_params: Optional[Dict[str, float]] = Field(
        default_factory=dict,
        description="Parameters for the chosen fragility model",
    )

    @field_validator("fragility_params", mode="after")
    def _params_required_if_not_inherit(cls, v, info):
        model = info.data.get("fragility_model")
        if model and model != "inherit" and not v:
            raise ValueError(
                "Provide fragility_params when fragility_model is not 'inherit'"
            )
        return v

    model_config = ConfigDict(extra="allow")


class HBOMComponent(BaseModel):
    uuid: str = Field(default_factory = lambda: str(uuid.uuid4()))
    label: str
    component_type: str
    hazards: Dict[str, FragilityDetails] = Field(default_factory=dict)
    subcomponents: Optional[List["HBOMComponent"]] = None

    # runtime annotations
    pof: Optional[float] = None
    replacement_cost: Optional[float] = None
    expected_annual_loss: Optional[float] = None

    class Config:
        from_attributes = True  # allow ORM mode


HBOMComponent.model_rebuild()


class HBOMDefinition(BaseModel):
    sector: str
    components: List[HBOMComponent]


# ---------------------------------------------------------------------------#
#  5.  COST DATA
# ---------------------------------------------------------------------------#
class CostCategory(str, Enum):
    replacement = "replacement"
    repair = "repair"
    o_and_m = "o&m"
    downtime = "downtime"


class CostSelector(BaseModel):
    field: Literal["max_voltage", "min_voltage", "lines", "capacity_mva"]
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    @model_validator(mode="after")
    def _min_lt_max(self):
        if (
            self.min_value is not None
            and self.max_value is not None
            and self.min_value >= self.max_value
        ):
            raise ValueError("min_value must be < max_value")
        return self


class CostItem(BaseModel):
    uuid: str
    component_type: str
    cost_category: CostCategory = CostCategory.replacement
    base_year: int = Field(2024, ge=1900)

    capex_usd: Optional[float] = None
    repair_usd: Optional[float] = None
    downtime_usd_per_hr: Optional[float] = None
    opex_usd_per_year: Optional[float] = None

    selector: Optional[CostSelector] = None
    scaling_formula: Optional[Dict[str, float]] = None

    region: Optional[str] = "US-Average"
    source: Optional[str] = None
    updated_at: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(
            datetime.timezone.utc
        )
    )

    @field_validator("uuid")
    def _uuid_not_blank(cls, v):
        if not v.strip():
            raise ValueError("uuid cannot be blank")
        return v

    @model_validator(mode="after")
    def _need_some_cost(self):
        if not any(
            getattr(self, k)
            for k in (
                "capex_usd",
                "repair_usd",
                "downtime_usd_per_hr",
                "opex_usd_per_year",
                "scaling_formula",
            )
        ):
            raise ValueError(
                "Provide at least one cost figure or a scaling_formula"
            )
        return self


# ---------------------------------------------------------------------------#
#  6.  INFRASTRUCTURE-LEVEL RISK SUMMARY
# ---------------------------------------------------------------------------#
class InfrastructureRiskSummary(BaseModel):
    sector: str
    hazard: str
    total_expected_annual_loss: float
    components_total_count: int
    components_at_risk_count: int
    percent_at_risk: float


def compute_infra_risk(hbom_tree: dict, pof_threshold: float = 0.5):
    """
    Utility that flattens the HBOM tree and compiles a quick headline
    risk summary – kept here so the model file is self-contained.
    """
    from utils import flatten  # local helper

    all_nodes = flatten(hbom_tree)
    total_eal = sum(n.get("expected_annual_loss", 0.0) for n in all_nodes)
    at_risk = [n for n in all_nodes if n.get("pof", 0.0) >= pof_threshold]

    return InfrastructureRiskSummary(
        sector=hbom_tree.get("sector", "Unknown"),
        hazard=hbom_tree.get("hazard", "Unknown"),
        total_expected_annual_loss=total_eal,
        components_total_count=len(all_nodes),
        components_at_risk_count=len(at_risk),
        percent_at_risk=(len(at_risk) / len(all_nodes) if all_nodes else 0),
    )