"""
Fragility Computer
Core fragility curve computation logic refactored from fragilityCurve.py

Location: backend/fragility/fragility_computer.py
"""

import logging
import numpy as np
from typing import Dict, Any, List
from math import prod
from scipy.stats import norm, weibull_min
from scipy.special import expit

logger = logging.getLogger(__name__)


class FragilityComputer:
    """
    Computes fragility curves for HBOM trees given climate data.
    
    Applies distribution functions (lognormal, weibull, logistic) to climate variables
    and generates probability of failure time series.
    """
    
    def compute_for_tree(
        self,
        hbom_tree: Dict[str, Any],
        hazard: str,
        prepared_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Main entry point: compute fragility curves for entire HBOM tree.
        
        Mutates tree in-place, adding:
        - node['hazards'][hazard]['fragility_curves'][var][grid]
        - node['pof_by_var']
        - node['pof']
        
        Args:
            hbom_tree: Nested HBOM tree (from hbom module)
            hazard: Hazard type to compute
            prepared_data: Climate data with variables, times, grid cells
        
        Returns:
            Mutated hbom_tree with fragility curves computed
        """
        climate_vars = prepared_data.get("variables", [])
        all_grid_data = prepared_data.get("data", [])
        
        logger.info(f"Computing fragility for hazard={hazard}, {len(climate_vars)} vars, {len(all_grid_data)} grids")
        
        # Process each root component
        components = hbom_tree.get("components", [])
        for comp in components:
            self._compute_for_component(comp, hazard, climate_vars, all_grid_data)
        
        return hbom_tree
    
    def _compute_for_component(
        self,
        component: Dict[str, Any],
        hazard: str,
        climate_vars: List[str],
        all_grid_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Recursively compute fragility for a component and its children.
        
        Leaf nodes with hazard data calculate curves.
        Parent nodes aggregate from children.
        """
        hazards_dict = component.setdefault("hazards", {})
        hazard_data = hazards_dict.get(hazard)
        
        # Initialize pof tracking
        component["pof_by_var"] = {}
        
        # 1. Leaf computation: if this node has fragility parameters
        if hazard_data and hazard_data.get("fragility_model"):
            model_name = hazard_data["fragility_model"]
            params = hazard_data.get("fragility_params", {})
            target_climate_var = hazard_data.get("climate_variable")  # Which variable this curve is for
            
            if model_name == "inherit":
                # Skip - will inherit from children or parent
                pass
            else:
                curves_by_var = {}
                pof_by_var = {}
                
                for var_name in climate_vars:
                    # Only compute if this curve applies to this variable
                    if target_climate_var and var_name != target_climate_var:
                        continue
                    
                    curves_by_var[var_name] = {}
                    curves_by_var[var_name] = {}
                    
                    for g_idx, grid_obj in enumerate(all_grid_data):
                        climate_array = grid_obj.get("climate", {}).get(var_name, [])
                        
                        if not climate_array:
                            fc_values = [0.0]
                            x_vals = [0.0]
                        else:
                            fc_values = self._compute_distribution_curve(
                                model_name, params, climate_array
                            )
                            x_vals = climate_array
                        
                        curves_by_var[var_name][g_idx] = {
                            "x_values": x_vals,
                            "fc_values": fc_values,
                            "final_pof": float(fc_values[-1]) if fc_values else 0.0
                        }
                    
                    # Max PoF across all grids for this variable
                    max_pof = max(
                        detail["final_pof"] 
                        for detail in curves_by_var[var_name].values()
                    ) if curves_by_var[var_name] else 0.0
                    
                    pof_by_var[var_name] = max_pof
                
                # Store computed curves
                hazard_data["fragility_curves"] = curves_by_var
                component["pof_by_var"] = pof_by_var
        
        # 2. Process children recursively
        child_pofs = []
        for child in component.get("subcomponents", []):
            self._compute_for_component(child, hazard, climate_vars, all_grid_data)
            child_pofs.append(child.get("pof_by_var", {}))
        
        # 3. Aggregate: combine own PoF with children (series logic)
        all_vars = set(component["pof_by_var"].keys())
        for child_pof in child_pofs:
            all_vars.update(child_pof.keys())
        
        combined_pof_by_var = {}
        for var_name in all_vars:
            own_pof = component["pof_by_var"].get(var_name, 0.0)
            child_var_pofs = [cp.get(var_name, 0.0) for cp in child_pofs]
            
            # Series failure: 1 - (1-own) × ∏(1-child)
            product_of_survival = prod((1.0 - p) for p in child_var_pofs)
            child_combined_pof = 1.0 - product_of_survival
            
            combined_survival = (1.0 - own_pof) * (1.0 - child_combined_pof)
            combined_pof = 1.0 - combined_survival
            
            combined_pof_by_var[var_name] = combined_pof
        
        component["pof_by_var"] = combined_pof_by_var
        
        # Top-level PoF: max across all variables
        if combined_pof_by_var:
            component["pof"] = max(combined_pof_by_var.values())
        else:
            component["pof"] = 0.0
        
        return component
    
    def _compute_distribution_curve(
        self,
        model_name: str,
        params: Dict[str, float],
        climate_array: List[float]
    ) -> List[float]:
        """
        Apply fragility distribution function to climate data.
        
        Args:
            model_name: "lognormal", "weibull", or "logistic"
            params: Distribution parameters (mu, sigma, scale, shape, etc.)
            climate_array: Climate variable values (time series)
        
        Returns:
            Probability of failure values (0-1) for each timestep
        """
        arr = np.array(climate_array, dtype=float)
        
        if model_name == "lognormal":
            median = params.get("median", 100.0)
            dispersion = params.get("dispersion", 0.3)
            
            # Handle mu/sigma if provided instead of median/dispersion
            if "mu" in params and "sigma" in params:
                mu = params["mu"]
                sigma = params["sigma"]
                log_vals = np.log(arr + 1e-9)
                z = (log_vals - mu) / sigma
            else:
                log_vals = np.log(arr + 1e-9)
                log_med = np.log(median)
                z = (log_vals - log_med) / dispersion
            
            return list(norm.cdf(z))
        
        elif model_name == "weibull":
            shape = params.get("shape", 2.0)
            scale = params.get("scale", 100.0)
            return list(weibull_min.cdf(arr, shape, scale=scale))
        
        elif model_name == "logistic":
            mid_pt = params.get("mid_point", 50.0)
            slope = params.get("slope", 0.5)
            return list(expit(slope * (arr - mid_pt)))
        
        else:
            logger.warning(f"Unknown fragility model: {model_name}")
            return [0.0] * len(arr)
    
    def compute_timeseries(
        self,
        hbom_tree: Dict[str, Any],
        hazard: str,
        prepared_data: Dict[str, Any]
    ) -> Dict[str, Dict[str, List[float]]]:
        """
        Compute PoF time series for every component.
        
        First computes full fragility curves, then extracts time series.
        
        Args:
            hbom_tree: HBOM tree (will be mutated with fragility_curves)
            hazard: Hazard type
            prepared_data: Climate data
        
        Returns:
            {uuid: {var: [pof_t0, pof_t1, ...]}}
        """
        # 1. Compute full curves
        self.compute_for_tree(hbom_tree, hazard, prepared_data)
        
        # 2. Extract time series
        times = prepared_data.get("times", [])
        frag_ts = {}
        
        def _extract_series(node: Dict[str, Any]):
            haz = node.get("hazards", {}).get(hazard, {})
            curves = haz.get("fragility_curves", {})
            
            if curves:
                # Build time series: max across grids at each timestep
                series = {}
                for var, grids in curves.items():
                    pof_series = []
                    for t in range(len(times)):
                        max_pof = max(
                            detail["fc_values"][t] if t < len(detail["fc_values"]) else 0.0
                            for detail in grids.values()
                        )
                        pof_series.append(max_pof)
                    series[var] = pof_series
                
                frag_ts[node["uuid"]] = series
            
            # Recurse to children
            for child in node.get("subcomponents", []):
                _extract_series(child)
        
        for root in hbom_tree.get("components", []):
            _extract_series(root)
        
        logger.info(f"Extracted time series for {len(frag_ts)} components")
        
        return frag_ts