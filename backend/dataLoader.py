# dataLoader.py

import os
import openpyxl
import polars as pl
import xarray as xr
import s3fs
import logging
import datetime
from backend.routers.utils import (
    s3_path_exists,
    get_s3_path,
    limit_by_years,
    get_grid_around_point,
    prepare_data_for_frontend
)
from sectorDict import COLUMN_TYPE_MAPPING
# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def loadAHASector(
    workbook_names: list[str],
    sheet_names: list[list[str]] | None = None,
    base_directory: str = ".",
    selected_columns: list[str] | None = None,
) -> pl.DataFrame:
    """
    Load specific sheets/columns from one or more AHA Excel workbooks and
    return a single Polars DataFrame, guaranteeing uniform dtypes so the
    final concat cannot fail.
    """
    frames: list[pl.DataFrame] = []

    for i, wb in enumerate(workbook_names):
        path = os.path.normpath(os.path.join(base_directory, f"{wb}.xlsx"))

        # --- open workbook -------------------------------------------------
        try:
            wb_obj = openpyxl.load_workbook(path, data_only=True)
            logger.info("Loaded workbook: %s", wb)
        except FileNotFoundError:
            logger.warning("Workbook not found: %s", path)
            continue
        except Exception as e:
            logger.exception("Cannot open %s: %s", path, e)
            continue

        sheets_available = wb_obj.sheetnames
        sheets_wanted = (
            sheet_names[i] if sheet_names and len(sheet_names) > i else sheets_available
        )
        sheets_wanted = [str(s) for s in sheets_wanted]

        # --- per-sheet loop -------------------------------------------------
        for sh in sheets_wanted:
            if sh not in sheets_available:
                logger.warning("Sheet '%s' not found in %s", sh, wb)
                continue

            try:
                df = pl.read_excel(path, sheet_name=sh)

                # provenance tags
                df = df.with_columns(
                    pl.lit(sh).alias("source_sheet"),
                    pl.lit(wb).alias("source_workbook"),
                )

                # ---------------------------------------------------------
                # 1) Add placeholders ONLY if column truly missing
                # ---------------------------------------------------------
                if selected_columns:
                    missing = [c for c in selected_columns if c not in df.columns]
                    for col in missing:
                        if col == "lines":
                            df = df.with_columns(
                                pl.lit(None).cast(pl.Int64).alias(col)
                            )
                        elif col in ("min_voltage", "max_voltage"):
                            df = df.with_columns(
                                pl.lit(None).cast(pl.Float64).alias(col)
                            )
                        else:
                            df = df.with_columns(pl.lit(None).alias(col))

                    # reorder / select requested + provenance
                    df = df.select(selected_columns + ["source_sheet", "source_workbook"])

                # ---------------------------------------------------------
                # 2) Enforce canonical dtypes BEFORE concat
                # ---------------------------------------------------------
                for col, dtype in COLUMN_TYPE_MAPPING.items():
                    if col in df.columns and df[col].dtype != dtype:
                        df = df.with_columns(
                            pl.col(col).cast(dtype, strict=False)
                        )

                # ---------------------------------------------------------
                # 3) Normalise nulls
                # ---------------------------------------------------------
                df = df.with_columns(
                    [
                        pl.col(pl.Utf8).fill_null(""),
                        pl.col(pl.Float64).fill_null(0.0),
                        pl.col(pl.Int64).fill_null(0),
                    ]
                )

                logger.info("→ %s!%s  shape=%s", wb, sh, df.shape)
                frames.append(df)

            except Exception as e:
                logger.exception("Failed to load %s!%s: %s", wb, sh, e)

    # --- concat all frames --------------------------------------------------
    if not frames:
        logger.warning("No data loaded from %s", workbook_names)
        return pl.DataFrame()

    final_df = pl.concat(frames, how="vertical", rechunk=True)
    logger.info("Final dataframe schema: %s", final_df.schema)
    return final_df

def load_infrastructure_data(base_directory='.', workbook_names=['EnergyGrid'], sheet_names=None, selected_columns=None):
    """
    Load infrastructure data using the loadAHASector function.
    
    Parameters:
    - base_directory: str, the base directory where the infrastructure files are located.
    - workbook_names: list, list of workbook names (without .xlsx extension).
    - sheet_names: list or None, list of sheet names to load.
    - selected_columns: list or None, columns to select from each sheet.

    Returns:
    - pl.DataFrame: The loaded infrastructure data.
    """
    try:
        df = loadAHASector(workbook_names, sheet_names=sheet_names, base_directory=base_directory, selected_columns=selected_columns)
        return df
    except Exception as e:
        logger.error(f"Error loading infrastructure data: {e}")
        raise e

def load_data_from_aws(variables, scenario: str = "rcp85", domain: str = "NAM-22i"):
    """
    Load base-field climate data from the NCAR NA-CORDEX S3 bucket.

    Any variable whose Zarr store is missing or corrupted is skipped and
    will need to be computed later as a composite (e.g. 'hi').

    Returns
    -------
    dict[str, xr.Dataset]
        Only the variables that were successfully opened.
    """
    # ----- connect to S3 ---------------------------------------------------
    try:
        fs = s3fs.S3FileSystem(anon=True)
    except Exception as e:
        logger.error(f"S3FileSystem init failed: {e}")
        raise

    datasets: dict[str, xr.Dataset] = {}

    # ----- loop over requested vars ---------------------------------------
    for var in variables:
        s3_path = get_s3_path(fs, var, scenario, domain)
        logger.info(f"Loading data from: {s3_path}")

        try:
            ds = xr.open_zarr(
                fs.get_mapper(s3_path),
                consolidated=True,
                chunks="auto",
            )
            logger.info(f"Dataset loaded successfully for {var}.")
            datasets[var] = ds

        except Exception as e:
            # log-and-skip instead of hard-fail
            logger.warning(f"{var} not loaded ({e}); skipped.")
            continue

    # ----- sanity check ----------------------------------------------------
    if not datasets:
        raise RuntimeError(
            "None of the requested base variables could be loaded from S3."
        )

    return datasets

def load_and_prepare_data(
    variables: list[str],
    lat: float,
    lon: float,
    num_cells: int = 0,
    scenario: str = 'rcp85',
    domain: str = 'NAM-22i',
    prior_years: int = 1,
    future_years: int = 1,
    climate_model: str = 'all',
    aggregation_method: str = 'mean',
    aggregation_kwargs: dict | None = None,
    aggregate_over_member_id: bool = True
) -> dict:
    """
    Loads climate data for specified variables, spatial point, and temporal range,
    then prepares it for frontend visualization.

    Parameters
    ----------
    variables : list[str]
        Variables to fetch (e.g., ['tas', 'hurs']).
    lat : float
        Latitude of the point of interest.
    lon : float
        Longitude of the point of interest.
    num_cells : int, default=0
        Number of neighboring cells to include around the point (0 = just the nearest grid cell).
    scenario : str, default='rcp85'
        Climate scenario (e.g., 'rcp85' or 'rcp45').
    domain : str, default='NAM-22i'
        The grid/domain identifier.
    prior_years : int, default=1
        Number of years before the current year to include.
    future_years : int, default=1
        Number of years after the current year to include.
    climate_model : str, default='all'
        Which ensemble member to load: 
          - 'all' (load all members), 
          - 'aggregate' (load all members, then collapse over member_id), 
          - or a single member ID string (e.g. 'MPI-ESM-LR.CRCM5-UQAM') to slice to that member only.
    aggregation_method : str, default='mean'
        How to aggregate over member_id if aggregate_over_member_id is True.
        Options: 'mean', 'max', 'min', 'percentile'.
    aggregation_kwargs : dict, optional
        Additional kwargs for aggregation. E.g. {'q': 95} if aggregation_method=='percentile'.
    aggregate_over_member_id : bool, default=True
        If True, collapse the ensemble over member_id after loading. If False,
        keep each member_id separate (assuming climate_model == 'all').

    Returns
    -------
    dict
        A dictionary structured for frontend consumption, e.g.:
        {
          'variables': [...],
          'variable_long_names': [...],
          'times': [...],
          'bounding_box': {...},
          'data': [...],    # if aggregated
          'members': [...], # if not aggregated
          'climate_analysis': {...},  # if your compile step adds it
        }
    """
    # 0) Input validation
    if not isinstance(num_cells, int) or num_cells < 0:
        logger.error("num_cells must be a non‐negative integer.")
        raise ValueError("num_cells must be a non‐negative integer.")

    # 1) Compute actual start_year and end_year from prior_years/future_years
    now_year   = datetime.datetime.now().year
    start_year = now_year - (prior_years or 0)
    end_year   = now_year + (future_years or 0)
    logger.debug(f"[load_and_prepare_data] Year window: {start_year}–{end_year}")

    # 2) Load each variable’s full Dataset from S3/Zarr
    try:
        raw_datasets = load_data_from_aws(variables, scenario=scenario, domain=domain)
    except Exception as e:
        logger.error(f"Error loading datasets from AWS: {e}")
        raise

    limited_datasets: dict[str, xr.Dataset] = {}

    # 3) For each variable, optionally slice to a single member_id,
    #    then do early time subsetting, then spatial subsetting.
    for var, ds_full in raw_datasets.items():
        try:
            # 3a) If user asked for a single climate_model (not 'all' or 'aggregate'),
            #     slice down to only that member_id.
            if climate_model not in ("all", "aggregate"):
                if "member_id" not in ds_full.coords:
                    raise KeyError(f"Dataset for {var} has no 'member_id' coordinate.")
                ds_full = ds_full.sel(member_id=climate_model)

            # 3b) Early time‐slice using .sel(time=...)
            #     We assume ds_full.time is a pandas-like DatetimeIndex
            ds_time = ds_full.sel(
                time=slice(f"{start_year}-01-01", f"{end_year}-12-31")
            )

            # 3c) Early spatial subset: use get_grid_around_point (your helper)
            #     It should use ds_time.sel(lat=..., lon=...) internally or equivalent.
            grid_subset = get_grid_around_point(ds_time, lat, lon, num_cells=num_cells)

            limited_datasets[var] = grid_subset

        except Exception as e:
            logger.error(f"Error processing variable '{var}': {e}")
            raise

    # 4) Merge all limited datasets into one combined Dataset
    try:
        ds_combined = xr.merge(
            [limited_datasets[var] for var in variables if var in limited_datasets]
        )
    except Exception as e:
        logger.error(f"Error merging datasets: {e}")
        raise

    # 5) If `climate_model == 'aggregate'` OR (climate_model == 'all' AND aggregate_over_member_id=True),
    #    collapse over member_id here (using Xarray).
    needs_aggregation = aggregate_over_member_id and (climate_model in ("all", "aggregate"))
    if needs_aggregation:
        if "member_id" not in ds_combined.coords:
            logger.warning("Requested aggregation, but no 'member_id' dimension found.")
        else:
            try:
                if aggregation_method == "mean":
                    ds_collapsed = ds_combined.mean(dim="member_id", skipna=True)
                elif aggregation_method == "max":
                    ds_collapsed = ds_combined.max(dim="member_id", skipna=True)
                elif aggregation_method == "min":
                    ds_collapsed = ds_combined.min(dim="member_id", skipna=True)
                elif aggregation_method == "percentile":
                    # Expect aggregation_kwargs={'q': 95} for example
                    q = float(aggregation_kwargs.get("q", 50)) / 100.0
                    ds_collapsed = ds_combined.quantile(q, dim="member_id", skipna=True)
                else:
                    logger.error(f"Unknown aggregation method '{aggregation_method}'. Defaulting to mean.")
                    ds_collapsed = ds_combined.mean(dim="member_id", skipna=True)

                ds_to_prepare = ds_collapsed

            except Exception as e:
                logger.error(f"Error during Xarray aggregation: {e}")
                raise
    else:
        # No aggregation requested (either user asked for a single member_id,
        # or they unchecked aggregate_over_member_id). Just pass the combined DS through.
        ds_to_prepare = ds_combined

    # 6) Call your existing `prepare_data_for_frontend` to turn the Xarray Dataset
    #    into pure Python lists/dicts for the client. It should look at ds_to_prepare
    #    and build `times`, `bounding_box`, `data` (if aggregated) or `members` (if not).
    try:
        prepared_data = prepare_data_for_frontend(
            ds_to_prepare,
            variables,
            aggregation_method=aggregation_method,
            aggregation_kwargs=aggregation_kwargs,
            aggregate_over_member_id=(needs_aggregation)
        )
    except Exception as e:
        logger.error(f"Error preparing data for frontend: {e}")
        raise

     # 7) -------------------------------------------------------------- #
    # Guarantee member_ids is present even when only one member
    # -------------------------------------------------------------- #
    if ("member_ids" not in prepared_data
        and climate_model not in ("all", "aggregate")
        and not needs_aggregation):
        prepared_data["member_ids"] = [climate_model]

    # 8) -------------------------------------------------------------- #
    # AOI demographics aligned to the time axis (nested, no fallbacks)
    # -------------------------------------------------------------- #
    try:
        import datetime as dt
        from censusData import get_demographics_timeseries_with_projection_for_bbox

        def _to_year(v):
            if isinstance(v, (dt.datetime, dt.date)):
                return v.year
            if isinstance(v, (int, float)):
                try:
                    return dt.datetime.utcfromtimestamp(v / 1000.0 if v > 1e12 else v).year
                except Exception:
                    return dt.datetime.utcnow().year
            if isinstance(v, str):
                try:
                    return dt.datetime.fromisoformat(v.replace("Z", "")).year
                except Exception:
                    return dt.datetime.utcnow().year
            return dt.datetime.utcnow().year

        times = prepared_data.get("times", [])
        years = [_to_year(t) for t in times]

        bb = prepared_data.get("bounding_box") or {}
        min_lat = bb.get("min_lat"); max_lat = bb.get("max_lat")
        min_lon = bb.get("min_lon"); max_lon = bb.get("max_lon")

        if None not in (min_lat, max_lat, min_lon, max_lon) and years:
            demo = get_demographics_timeseries_with_projection_for_bbox(
                min_lat=float(min_lat), max_lat=float(max_lat),
                min_lon=float(min_lon), max_lon=float(max_lon),
                years=[int(y) for y in years],
                fill="ffill",
                project=True,
                method="cagr",
                window=5,
            )
            prepared_data["aoi_demographics"] = {
                "years": years,
                "population": demo.get("population", []),
                "households": demo.get("households", []),
                "median_hhi": demo.get("median_household_income_proxy", []),
                "per_capita_income": demo.get("per_capita_income", []),  # ← list, not dict
            }
        else:
            prepared_data["aoi_demographics"] = {
                "years": years,
                "population": [],
                "households": [],
                "median_hhi": [],
                "per_capita_income": [],
            }
    except Exception as _demo_err:
        try:
            logger.warning(f"AOI demographics unavailable: {_demo_err}")
        except Exception:
            pass

    return prepared_data