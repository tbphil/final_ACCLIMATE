# censusData.py
import requests
import pandas as pd
import numpy as np
from fastapi import HTTPException
from typing import List, Dict
import logging
import datetime
from functools import lru_cache

from config import settings
from cache_manager import cache  # ← Use the disk-backed cache

logger = logging.getLogger(__name__)

ACS_MIN_YEAR = 2009  # earliest ACS 5-year vintage generally available

@lru_cache(maxsize=1)
def discover_latest_acs5_year(max_year: int | None = None) -> int:
    """
    Find the newest ACS 5-year *data* year that actually responds.
    Cached in-memory since it rarely changes.
    """
    if max_year is None:
        max_year = datetime.datetime.utcnow().year
    for y in range(int(max_year), ACS_MIN_YEAR - 1, -1):
        try:
            url = ACS_ENDPOINT_TEMPLATE.format(year=y)
            resp = requests.get(
                url,
                params={"get": "NAME", "for": "us:1"},
                timeout=10,
            )
            if resp.ok:
                return y
        except requests.RequestException:
            continue
    return 2023 

TIGER_ENDPOINT = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Tracts_Blocks/MapServer/5/query"
ACS_ENDPOINT_TEMPLATE = "https://api.census.gov/data/{year}/acs/acs5"


def _get_bbox_tracts(min_lat: float, max_lat: float, min_lon: float, max_lon: float):
    """
    Return (tract_ids, state_counties) using TIGERweb for the given bbox.
    CACHED on disk since tract boundaries are stable.
    """
    if not settings.CENSUS_API_KEY:
        logger.error("Census API key is missing!")
        raise HTTPException(status_code=500, detail="Census API key not configured.")
    
    # Build cache key (rounded to avoid float precision issues)
    bbox_key = (
        round(min_lat, 6),
        round(max_lat, 6),
        round(min_lon, 6),
        round(max_lon, 6)
    )
    
    # Check cache first
    cached = cache.get("census_tracts", bbox_key)
    if cached is not None:
        logger.info(f"✓ Cache hit for tract lookup (bbox: {bbox_key})")
        return cached
    
    logger.info(f"Fetching tracts from TIGERweb for bbox: {bbox_key}")
    
    geometry_str = f"{min_lon},{min_lat},{max_lon},{max_lat}"
    params = {
        "where": "1=1",
        "geometry": geometry_str,
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "*",
        "f": "json",
    }
    
    r = requests.get(TIGER_ENDPOINT, params=params, timeout=30)
    if not r.ok:
        logger.error("TIGER request failed %s: %s", r.status_code, r.text)
        raise HTTPException(status_code=502, detail="Error querying TIGERweb")

    feats = (r.json() or {}).get("features", [])
    if not feats:
        result = ([], set())
        cache.set("census_tracts", bbox_key, result)
        return result

    tracts, state_counties = [], set()
    for f in feats:
        a = f.get("attributes", {})
        st = str(a.get("STATE", "")).zfill(2)
        co = str(a.get("COUNTY", "")).zfill(3)
        tr = str(a.get("TRACT", "")).zfill(6)
        if len(st) == 2 and len(co) == 3 and len(tr) == 6:
            tracts.append(st + co + tr)
            state_counties.add((st, co))
    
    result = (tracts, state_counties)
    
    # Cache the result (persists across restarts)
    cache.set("census_tracts", bbox_key, result)
    logger.info(f"✓ Cached {len(tracts)} tracts for bbox {bbox_key}")
    
    return result


def get_demographics_timeseries_for_bbox(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    years: list[int],
    fill: str = "ffill",
) -> dict:
    """
    For an AOI (bbox), return time series aligned to `years`.
    CACHED on disk by (bbox, years tuple).
    """
    if not years:
        return {
            "years": [],
            "population": [],
            "households": [],
            "median_household_income_proxy": [],
            "per_capita_income": []
        }
    
    # Build cache key
    bbox_key = (
        round(min_lat, 6),
        round(max_lat, 6),
        round(min_lon, 6),
        round(max_lon, 6)
    )
    years_key = tuple(sorted(set(int(y) for y in years)))
    demo_cache_key = (bbox_key, years_key, fill)
    
    # Check cache
    cached = cache.get("census_demographics", demo_cache_key)
    if cached is not None:
        logger.info(f"✓ Cache hit for demographics (bbox: {bbox_key}, {len(years)} years)")
        return cached
    
    logger.info(f"Fetching demographics from Census API for {len(years)} years")
    
    tract_ids, state_counties = _get_bbox_tracts(min_lat, max_lat, min_lon, max_lon)
    
    if not tract_ids:
        zeros = [0] * len(years)
        result = {
            "years": years,
            "population": zeros,
            "households": zeros,
            "median_household_income_proxy": zeros,
            "per_capita_income": zeros
        }
        cache.set("census_demographics", demo_cache_key, result)
        return result

    want_years = sorted(set(int(y) for y in years))
    by_year = {}
    VARS = ["B01003_001E", "B11001_001E", "B19013_001E", "B19301_001E"]

    for y in want_years:
        dfs = []
        for (st, co) in state_counties:
            params = {
                "get": ",".join(VARS),
                "for": "tract:*",
                "in": f"state:{st} county:{co}",
                "key": settings.CENSUS_API_KEY,
            }
            endpoint = ACS_ENDPOINT_TEMPLATE.format(year=y)
            resp = requests.get(endpoint, params=params, timeout=30)
            if not resp.ok:
                logger.warning("ACS failed (y=%s %s-%s): %s", y, st, co, resp.text)
                continue
            rows = resp.json()
            if len(rows) < 2:
                continue
            df = pd.DataFrame(rows[1:], columns=rows[0])
            df["TRACT_ID"] = (
                df["state"].astype(str).str.zfill(2)
                + df["county"].astype(str).str.zfill(3)
                + df["tract"].astype(str).str.zfill(6)
            )
            for col in VARS:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df[df["TRACT_ID"].isin(tract_ids)]
            if not df.empty:
                dfs.append(df)

        if not dfs:
            by_year[y] = {"pop": 0.0, "hh": 0.0, "hhi_wmean": 0.0, "pci_wmean": 0.0}
            continue

        all_df = pd.concat(dfs, ignore_index=True)
        pop = float(all_df["B01003_001E"].sum(numeric_only=True))
        hh  = float(all_df["B11001_001E"].sum(numeric_only=True))
        hhi_wmean = float((all_df["B19013_001E"] * all_df["B11001_001E"]).sum()) / hh if hh > 0 else 0.0
        pci_wmean = float((all_df["B19301_001E"] * all_df["B01003_001E"]).sum()) / pop if pop > 0 else 0.0
        by_year[y] = {"pop": pop, "hh": hh, "hhi_wmean": hhi_wmean, "pci_wmean": pci_wmean}

    known = sorted(by_year.keys())
    def _fill(y: int):
        if y in by_year: return by_year[y]
        if not known:    return {"pop": 0.0, "hh": 0.0, "hhi_wmean": 0.0, "pci_wmean": 0.0}
        if fill == "nearest":
            nearest = min(known, key=lambda ky: abs(ky - y))
            return by_year[nearest]
        prev = max((ky for ky in known if ky <= y), default=None)
        return by_year[prev] if prev is not None else by_year[known[0]]

    pop_ts, hh_ts, hhi_ts, pci_ts = [], [], [], []
    for y in years:
        v = _fill(int(y))
        pop_ts.append(int(round(v["pop"])))
        hh_ts.append(int(round(v["hh"])))
        hhi_ts.append(float(v["hhi_wmean"]))
        pci_ts.append(float(v["pci_wmean"]))

    result = {
        "years": years,
        "population": pop_ts,
        "households": hh_ts,
        "median_household_income_proxy": hhi_ts,
        "per_capita_income": pci_ts,
    }
    
    # Cache the result (persists to disk)
    cache.set("census_demographics", demo_cache_key, result)
    logger.info(f"✓ Cached demographics for {len(years)} years")
    
    return result


def get_demographics_timeseries_with_projection_for_bbox(
    *,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    years: List[int],
    fill: str = "ffill",
    project: bool = True,
    method: str = "cagr",
    window: int = 5,
) -> Dict[str, List[float]]:
    """
    Returns demographics with future projections.
    CACHED on disk by (bbox, years, projection params).
    """
    if not years:
        return {
            "years": [],
            "population": [],
            "households": [],
            "median_household_income_proxy": [],
            "per_capita_income": []
        }
    
    # Build cache key
    bbox_key = (
        round(min_lat, 6),
        round(max_lat, 6),
        round(min_lon, 6),
        round(max_lon, 6)
    )
    years_key = tuple(sorted(set(int(y) for y in years)))
    proj_cache_key = (bbox_key, years_key, fill, project, method, window)
    
    # Check cache
    cached = cache.get("census_projected", proj_cache_key)
    if cached is not None:
        logger.info(f"✓ Cache hit for projected demographics (bbox: {bbox_key}, {len(years)} years)")
        return cached
    
    logger.info(f"Computing projected demographics for {len(years)} years")
    
    # Normalize / sort years
    yrs = sorted({int(y) for y in years})

    # Determine latest ACS year
    try:
        latest_acs = discover_latest_acs5_year()
    except Exception:
        latest_acs = max([y for y in yrs if y <= (np.datetime64("today").astype(object).year - 2)], default=2022)

    hist_years = [y for y in yrs if y <= latest_acs]
    fut_years  = [y for y in yrs if y >  latest_acs]

    # Fetch historical ACS series
    if hist_years:
        hist = get_demographics_timeseries_for_bbox(
            min_lat=min_lat, max_lat=max_lat,
            min_lon=min_lon, max_lon=max_lon,
            years=hist_years,
        )
    else:
        # Seed with latest available year
        hist = get_demographics_timeseries_for_bbox(
            min_lat=min_lat, max_lat=max_lat,
            min_lon=min_lon, max_lon=max_lon,
            years=[latest_acs],
        )
        hist_years = [latest_acs]

    # Extract series
    pop_hist = np.array(hist.get("population", []), dtype=float)
    hh_hist  = np.array(hist.get("households", []), dtype=float)
    hhi_hist = np.array(hist.get("median_household_income_proxy", []), dtype=float)
    hhi_hist = np.array(hist.get("median_household_income_proxy", []), dtype=float)
    pci_hist = np.array(hist.get("per_capita_income", []), dtype=float)

    out = {
        "population": np.array(pop_hist, dtype=float),
        "households": np.array(hh_hist, dtype=float),
        "median_household_income_proxy": np.array(hhi_hist, dtype=float),
        "per_capita_income": np.array(pci_hist, dtype=float),
    }

    # Helper for projection params
    def _window_params(arr: np.ndarray):
        n_hist = len(hist_years)
        if n_hist == 0:
            return 0.0, 0.0, 1
        first_idx = max(0, n_hist - max(2, int(window)))
        first = float(arr[first_idx]) if arr.size else 0.0
        base  = float(arr[n_hist - 1]) if arr.size else 0.0
        span  = max(1, hist_years[-1] - hist_years[first_idx])
        return first, base, span

    # Pre-compute rates/slopes
    f_pop, b_pop, s_pop = _window_params(out["population"])
    f_hh,  b_hh,  s_hh  = _window_params(out["households"])
    f_hhi, b_hhi, s_hhi = _window_params(out["median_household_income_proxy"])
    f_pci, b_pci, s_pci = _window_params(out["per_capita_income"])

    def _cagr(first, base, span):
        if first > 0.0 and base > 0.0 and span > 0:
            try:
                return (base / first) ** (1.0 / span) - 1.0
            except Exception:
                return 0.0
        return 0.0

    def _slope(first, base, span):
        return (base - first) / span if span > 0 else 0.0

    pop_cagr = _cagr(f_pop, b_pop, s_pop)
    hh_cagr  = _cagr(f_hh,  b_hh,  s_hh)
    hhi_slp  = _slope(f_hhi, b_hhi, s_hhi)
    pci_slp  = _slope(f_pci, b_pci, s_pci)

    # Project future years if needed
    if project and fut_years:
        n_hist = len(hist_years)

        def _project(arr: np.ndarray, mode: str, rate: float, slope: float) -> np.ndarray:
            base = float(arr[-1]) if arr.size else 0.0
            if mode == "linear":
                incr = slope
                proj = []
                curr = base
                for _ in fut_years:
                    curr = max(0.0, curr + incr)
                    proj.append(curr)
            else:
                r = float(np.clip(rate, -0.2, 0.2))
                proj = []
                curr = base
                for _ in fut_years:
                    curr = max(0.0, curr * (1.0 + r))
                    proj.append(curr)
            return np.concatenate([arr, np.array(proj, dtype=float)]) if n_hist else np.array(proj, dtype=float)

        out["population"] = _project(out["population"], "cagr" if method == "cagr" else "linear", pop_cagr, hhi_slp)
        out["households"] = _project(out["households"], "cagr" if method == "cagr" else "linear", hh_cagr,  hhi_slp)
        out["median_household_income_proxy"] = _project(out["median_household_income_proxy"], "linear", 0.0, hhi_slp)
        out["per_capita_income"] = _project(out["per_capita_income"], "linear", 0.0, pci_slp)
    else:
        # No projection: repeat last value
        need = len(yrs) - len(hist_years)
        if need > 0:
            for k in list(out.keys()):
                last = float(out[k][-1]) if out[k].size else 0.0
                pad  = np.full((need,), last, dtype=float)
                out[k] = np.concatenate([out[k], pad]) if out[k].size else pad

    # Convert to lists
    pop_series = [float(x) for x in out["population"]]
    hh_series  = [float(x) for x in out["households"]]
    hhi_series = [float(x) for x in out["median_household_income_proxy"]]
    pci_series = [float(x) for x in out["per_capita_income"]]

    # Build model metadata
    last_hist_year = int(max(hist_years)) if hist_years else None
    model_meta = {
        "source": {"name": "US Census ACS 5-year"},
        "last_hist_year": last_hist_year,
        "series": {
            "population": {
                "method": "cagr" if method == "cagr" else "linear",
                "base_year": last_hist_year,
                "rate": float(pop_cagr)
            },
            "households": {
                "method": "cagr" if method == "cagr" else "linear",
                "base_year": last_hist_year,
                "rate": float(hh_cagr)
            },
            "median_hhi": {
                "method": "linear",
                "base_year": last_hist_year,
                "slope": float(hhi_slp)
            },
            "per_capita_income": {
                "method": "linear",
                "base_year": last_hist_year,
                "slope": float(pci_slp)
            },
        },
    }

    result = {
        "years": list(yrs),
        "population": pop_series,
        "households": hh_series,
        "median_hhi": hhi_series,
        "median_household_income_proxy": hhi_series,
        "per_capita_income": pci_series,
        "model": model_meta,
    }
    
    # Cache the result (disk-backed, persists across restarts)
    cache.set("census_projected", proj_cache_key, result)
    logger.info(f"✓ Cached projected demographics for {len(years)} years")
    
    return result