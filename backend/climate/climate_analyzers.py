"""
Climate trend analysis module.
Computes composite metrics, trends, and statistical analysis for climate variables.

Location: backend/climate/climate_analyzers.py
"""
import logging
from typing import Dict, List, Any
import pandas as pd
import numpy as np
from scipy.stats import zscore
from sklearn.linear_model import LinearRegression
from statsmodels.tsa.seasonal import seasonal_decompose
from typing import Optional
logger = logging.getLogger(__name__)


class ClimateAnalyzer:
    """Performs statistical analysis on climate time series"""
    
    def analyze_all_variables(
        self,
        prepared_data: Dict[str, Any],
        variables: List[str]
    ) -> Dict[str, Any]:
        """
        Run analysis on all variables and compile results.
        
        Args:
            prepared_data: Output from FrontendPreparer.prepare()
            variables: List of variable codes to analyze
            
        Returns:
            Dict with structure matching ClimateAnalysis Pydantic model
        """
        logger.info(f"Running climate analysis on {len(variables)} variables")
        
        analysis_results = {}
        
        for var in variables:
            try:
                var_results = self._analyze_single_variable(prepared_data, var)
                if var_results:
                    analysis_results[var] = var_results
                    logger.debug(f"Analyzed {var}: {len(var_results)} grid cells")
            except Exception as e:
                logger.warning(f"Failed to analyze variable {var}: {e}")
                continue
        
        if not analysis_results:
            logger.warning("No variables were successfully analyzed")
            return {"analysis_results": {}}
        
        compiled = {
            "analysis_results": analysis_results
        }
        
        logger.info(f"Successfully analyzed {len(analysis_results)} variables")
        return compiled
    
    def _analyze_single_variable(
        self,
        prepared_data: Dict[str, Any],
        variable: str
    ) -> List[Dict[str, Any]]:
        """
        Analyze a single variable across all grid cells.
        
        Returns list of analysis results, one per grid cell:
        [
            {
                'grid_index': 0,
                'dates': ['2025-01-01', ...],
                'composite_metric': [0.15, ...],
                'trend_line': [0.12, ...],
                'slope': 0.0607,
                'intercept': -0.5,
                'histogram_counts': [5, 12, ...],
                'histogram_bins': [0.0, 0.1, ...],
                'mean_value': 0.2,
                'median_value': 0.18,
                'std_dev': 0.5
            },
            ...
        ]
        """
        grid_data = prepared_data.get('data', [])
        times_str = prepared_data.get('times', [])
        
        if not times_str:
            logger.warning(f"No time data available for {variable}")
            return []
        
        # Parse times
        times = pd.to_datetime(times_str, errors='coerce').dropna()
        
        # Check for duplicate times
        if times.duplicated().any():
            logger.warning("Duplicate timestamps detected, dropping duplicates")
            _, unique_idx = np.unique(times, return_index=True)
            times = times[np.sort(unique_idx)]
        
        if len(times) == 0:
            logger.warning("No valid timestamps after parsing")
            return []
        
        analysis_results = []
        
        # Analyze each grid cell
        for idx, grid_point in enumerate(grid_data):
            try:
                result = self._analyze_grid_cell(
                    grid_point, variable, times, idx
                )
                if result:
                    analysis_results.append(result)
            except Exception as e:
                logger.debug(f"Failed to analyze grid {idx} for {variable}: {e}")
                continue
        
        return analysis_results
    
    def _analyze_grid_cell(
        self,
        grid_point: Dict[str, Any],
        variable: str,
        times: pd.DatetimeIndex,
        grid_index: int
    ) -> Optional[Dict[str, Any]]:
        """Analyze a single grid cell for a single variable"""
        
        # Extract variable data
        var_data = grid_point.get('climate', {}).get(variable, [])
        
        if var_data is None or len(var_data) == 0:
            return None
        
        # Align data length with times
        if len(var_data) != len(times):
            if len(var_data) > len(times):
                var_data = np.array(var_data)[:len(times)]
            else:
                return None
        
        # Create DataFrame
        df = pd.DataFrame({variable: var_data}, index=times)
        df = df.sort_index()
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df.dropna(subset=[variable], inplace=True)
        
        # Need at least ~1 year of data
        if len(df) < 365:
            return None
        
        # Time series decomposition
        try:
            decomposition = seasonal_decompose(
                df[variable],
                model='additive',
                period=365,
                extrapolate_trend='freq'
            )
            trend = decomposition.trend
            
            if trend is None:
                return None
            
            trend = trend.interpolate().dropna()
            
        except Exception as e:
            logger.debug(f"Decomposition failed for grid {grid_index}: {e}")
            return None
        
        if len(trend) < 3:
            return None
        
        # Compute derivatives
        time_deltas = trend.index.to_series().diff().dt.total_seconds() / 86400.0
        time_deltas = time_deltas.dropna()
        
        velocity = trend.diff() / time_deltas
        velocity = velocity.dropna()
        
        acceleration = velocity.diff() / time_deltas
        acceleration = acceleration.dropna()
        
        # Find common index
        common_index = trend.index.intersection(velocity.index).intersection(acceleration.index)
        
        if common_index.empty:
            return None
        
        trend_values = trend.loc[common_index].values
        velocity_values = velocity.loc[common_index].values
        acceleration_values = acceleration.loc[common_index].values
        
        # Check for NaNs
        if (np.isnan(trend_values).any() or 
            np.isnan(velocity_values).any() or 
            np.isnan(acceleration_values).any()):
            return None
        
        # Z-score normalization
        trend_z = zscore(trend_values)
        velocity_z = zscore(velocity_values)
        acceleration_z = zscore(acceleration_values)
        
        # Build metrics DataFrame
        metrics_df = pd.DataFrame({
            'Trend': trend_z,
            'Velocity': velocity_z,
            'Acceleration': acceleration_z
        }, index=common_index)
        
        # Composite metric (weighted combination)
        weights = {'Trend': 0.5, 'Velocity': 0.3, 'Acceleration': 0.2}
        metrics_df['CompositeMetric'] = (
            weights['Trend'] * metrics_df['Trend'] +
            weights['Velocity'] * metrics_df['Velocity'] +
            weights['Acceleration'] * metrics_df['Acceleration']
        )
        
        # Annual median for trend line
        metrics_df['Year'] = metrics_df.index.year
        annual_median = metrics_df.groupby('Year')['CompositeMetric'].median().reset_index()
        
        if len(annual_median) < 2:
            return None
        
        # Linear regression on annual medians
        X_median = annual_median['Year'].values.reshape(-1, 1)
        y_median = annual_median['CompositeMetric'].values
        
        if np.isnan(y_median).any():
            return None
        
        try:
            model_median = LinearRegression()
            model_median.fit(X_median, y_median)
            slope_median = model_median.coef_[0]
            intercept_median = model_median.intercept_
        except ValueError as e:
            logger.debug(f"Regression failed for grid {grid_index}: {e}")
            return None
        
        # Predict daily trend line
        daily_years = metrics_df['Year'].values.reshape(-1, 1)
        trend_line_daily = model_median.predict(daily_years)
        
        # Histogram
        composite_data = metrics_df['CompositeMetric']
        counts, bin_edges = np.histogram(composite_data, bins=50)
        
        # Return analysis result
        return {
            'grid_index': grid_index,
            'dates': metrics_df.index.strftime('%Y-%m-%d').tolist(),
            'composite_metric': composite_data.tolist(),
            'trend_line': trend_line_daily.tolist(),
            'slope': float(slope_median),
            'intercept': float(intercept_median),
            'histogram_counts': counts.tolist(),
            'histogram_bins': bin_edges.tolist(),
            'mean_value': float(composite_data.mean()),
            'median_value': float(composite_data.median()),
            'std_dev': float(composite_data.std())
        }