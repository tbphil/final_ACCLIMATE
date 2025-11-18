"""
HBOM Preparers
Reconstructs nested trees from flat MongoDB nodes and formats for frontend

Location: backend/hbom/hbom_preparers.py
"""
import logging
import math
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


def _json_safe(obj):
    """
    Recursively clean object for JSON serialization.
    Replaces NaN, Infinity with None.
    """
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_json_safe(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj


def reconstruct_tree(
    flat_nodes: List[Dict[str, Any]],
    fragility_curves: Optional[List[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """
    Convert flat nodes with parent_uuid/children_uuids into nested tree structure.
    Optionally merge fragility curves into the tree.
    
    Args:
        flat_nodes: List of flat component nodes from hbom_baseline
        fragility_curves: Optional list of curves from fragility_db
    
    Returns:
        List of root nodes with nested subcomponents
    """
    if not flat_nodes:
        return []
    
    # Build lookup map: uuid → node
    node_map = {node["uuid"]: _prepare_node(node) for node in flat_nodes}
    
    # Merge fragility curves if provided
    if fragility_curves:
        _merge_fragilities(node_map, fragility_curves)
    
    # Build tree structure by linking children
    for uuid, node in node_map.items():
        children_uuids = node.pop("children_uuids", [])
        if children_uuids:
            # Replace UUIDs with actual child objects
            node["subcomponents"] = [
                node_map[child_uuid] 
                for child_uuid in children_uuids 
                if child_uuid in node_map
            ]
        else:
            node["subcomponents"] = []
    
    # Extract roots (parent_uuid is None)
    roots = [
        node for node in node_map.values() 
        if node.get("parent_uuid") is None
    ]
    
    # Clean up parent_uuid from final output (not needed by frontend)
    for node in node_map.values():
        node.pop("parent_uuid", None)
    
    logger.info(f"Reconstructed {len(roots)} root trees from {len(flat_nodes)} flat nodes")
    
    return roots


def _prepare_node(node: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare single node for tree structure.
    Converts MongoDB document to frontend-friendly format.
    """
    prepared = {
        "uuid": node["uuid"],
        "label": node["label"],
        "component_type": node.get("asset_type", "unknown"),
        "canonical_component_type": node.get("canonical_component_type"),
        "level": node.get("level"),
        "node_path": node.get("node_path", ""),
        "parent_uuid": node.get("parent_uuid"),  # Temporary, removed after tree build
        "children_uuids": node.get("children_uuids", []),  # Temporary
        "hazards": {},  # Will be populated by _merge_fragilities
    }
    
    # Preserve metadata if present
    if "metadata" in node:
        prepared["metadata"] = node["metadata"]
    
    return prepared


def _merge_fragilities(
    node_map: Dict[str, Dict[str, Any]],
    fragility_curves: List[Dict[str, Any]]
):
    """
    Merge fragility curves into component nodes.
    
    Multiple curves can exist for the same component (different hazards/conditions).
    Groups them by component_uuid + hazard.
    """
    # Group curves by component_uuid
    curves_by_component = {}
    for curve in fragility_curves:
        comp_uuid = curve.get("component_uuid")
        if not comp_uuid or comp_uuid not in node_map:
            continue
        
        if comp_uuid not in curves_by_component:
            curves_by_component[comp_uuid] = []
        curves_by_component[comp_uuid].append(curve)
    
    # Attach to nodes
    for comp_uuid, curves in curves_by_component.items():
        node = node_map[comp_uuid]
        
        # Group curves by hazard
        for curve in curves:
            hazard = curve.get("hazard", "Unknown")
            
            # For now, take first curve per hazard
            # TODO: Implement priority-based selection when conditions/attributes exist
            if hazard not in node["hazards"]:
                node["hazards"][hazard] = {
                    "fragility_model": curve.get("model"),
                    "fragility_params": curve.get("parameters", {}),
                    "climate_variable": curve.get("climate_variable"),  # Preserve which variable this curve applies to
                    "conditions": curve.get("conditions", {}),
                    "priority": curve.get("priority", 0),
                    "source": curve.get("provenance", {}).get("source", "Unknown"),
                }
    
    logger.info(f"Merged fragilities for {len(curves_by_component)} components")


async def prepare_for_frontend(
    roots: List[Dict[str, Any]],
    sector: str,
    hazard: Optional[str] = None,
    canonical_registry: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Format reconstructed tree for frontend consumption.
    Matches the HBOMDefinition Pydantic model structure.
    
    Args:
        roots: List of root nodes with nested subcomponents
        sector: Sector identifier
        hazard: Optional hazard filter
        canonical_registry: Optional dict mapping canonical_name -> canonical entry
    
    Returns:
        Dictionary matching frontend expectations
    """
    # Filter to only include nodes with the requested hazard (if specified)
    if hazard:
        roots = [_filter_hazard(root, hazard) for root in roots]
    
    # Enrich roots with aliases from canonical registry
    if canonical_registry:
        for root in roots:
            canonical_type = root.get("canonical_component_type")
            if canonical_type and canonical_type in canonical_registry:
                root["aliases"] = canonical_registry[canonical_type].get("aliases", [])
    
    response = {
        "sector": sector,
        "components": roots
    }
    
    # Clean NaN/Infinity values for JSON serialization
    response = _json_safe(response)
    
    logger.info(f"Prepared {len(roots)} root components for frontend")
    
    return response


def _filter_hazard(node: Dict[str, Any], hazard: str) -> Dict[str, Any]:
    """
    Recursively filter tree to only include specified hazard.
    Removes hazard entries that don't match.
    """
    # Filter this node's hazards
    if "hazards" in node:
        filtered_hazards = {
            h: details 
            for h, details in node["hazards"].items() 
            if h == hazard
        }
        node["hazards"] = filtered_hazards
    
    # Recursively filter children
    if "subcomponents" in node:
        node["subcomponents"] = [
            _filter_hazard(child, hazard) 
            for child in node["subcomponents"]
        ]
    
    return node


def get_component_by_uuid(
    flat_nodes: List[Dict[str, Any]],
    uuid: str
) -> Optional[Dict[str, Any]]:
    """
    Find and return a single component by UUID.
    Useful for asset-specific lookups.
    """
    for node in flat_nodes:
        if node["uuid"] == uuid:
            return _prepare_node(node)
    return None


def get_roots_for_sector(
    flat_nodes: List[Dict[str, Any]],
    sector: str
) -> List[Dict[str, Any]]:
    """
    Filter flat nodes to canonical roots for the sector.
    Returns all roots that have been linked to the canonical registry.
    """
    # Return all canonical roots (those with canonical_component_type)
    roots = [
        node for node in flat_nodes
        if node.get("parent_uuid") is None
        and node.get("canonical_component_type") is not None
    ]
    
    logger.info(f"Found {len(roots)} canonical roots for sector {sector}")
    
    return roots