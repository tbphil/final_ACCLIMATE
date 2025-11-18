from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Literal

from models import GridBounds
from censusData import (
    get_demographics_timeseries_for_bbox,
    get_demographics_timeseries_with_projection_for_bbox,
    discover_latest_acs5_year,
)

router = APIRouter(prefix="/api", tags=["data_collectors"])

# Request model for AOI demographics aligned to the UI timeline
class CensusRequest(BaseModel):
    bbox: GridBounds
    years: List[int]
    # optional knobs (defaults are fine)
    project: bool = True
    method: Literal["cagr", "linear"] = "cagr"
    window: int = 5
    fill: Literal["ffill", "nearest"] = "ffill"

@router.post("/get-census-population")
async def get_census_population(req: CensusRequest):
    """
    Return ONLY the AOI demographics time series (no separate population_at_risk).
    The frontend will display Population at the current slider year directly from this series.
    """
    try:
        demo = get_demographics_timeseries_with_projection_for_bbox(
            min_lat=req.bbox.min_lat, max_lat=req.bbox.max_lat,
            min_lon=req.bbox.min_lon, max_lon=req.bbox.max_lon,
            years=req.years,
            fill=req.fill,
            project=req.project,
            method=req.method,
            window=req.window,
        )
        return {
            "aoi_demographics": {
                "years": req.years,
                "population": demo.get("population", []) or [],
                "households": demo.get("households", []) or [],
                "median_hhi": demo.get("median_household_income_proxy", []) or [],
                "per_capita_income": demo.get("per_capita_income", []) or [],
                "model": demo.get("model") or None,   # <-- pass-through
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in get_census_population: {e}")
        raise HTTPException(status_code=500, detail="Failed to get census data")