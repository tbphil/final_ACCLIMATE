"""
Infrastructure Preparers
Formats raw infrastructure data for frontend visualization and analysis
"""

import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


def prepare_for_frontend(
    assets: List[Dict[str, Any]],
    bbox: Optional[Dict[str, float]] = None,
    source_info: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Prepare infrastructure data for frontend consumption
    
    Args:
        assets: Raw asset dictionaries from data source
        bbox: Bounding box used for query
        source_info: Information about data source
    
    Returns:
        Formatted response ready for frontend
    """
    try:
        prepared_assets = []
        
        for asset in assets:
            prepared = _prepare_single_asset(asset)
            if prepared:
                prepared_assets.append(prepared)
        
        logger.info(f"Prepared {len(prepared_assets)} assets for frontend")
        
        response = {
            'infrastructure': prepared_assets,
            'count': len(prepared_assets),
            'bounding_box': bbox,
            'source': source_info.get('source') if source_info else 'Unknown',
            'metadata': {
                'component_types': _extract_component_types(prepared_assets),
                'states': _extract_states(prepared_assets),
            }
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error preparing data for frontend: {e}")
        raise


def _prepare_single_asset(asset: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Prepare single asset for frontend"""
    try:
        if not all(k in asset for k in ['latitude', 'longitude']):
            return None
        
        prepared = {
            'id': asset.get('uuid') or asset.get('id') or 'unknown',
            'uuid': asset.get('uuid') or asset.get('id'),
            'name': asset.get('name') or 'Unnamed Asset',
            'facilityTypeName': asset.get('component_type') or asset.get('facilityTypeName') or 'Unknown',
            'component_type': asset.get('component_type') or asset.get('facilityTypeName') or 'Unknown',
            'sector': asset.get('sector') or 'Unknown',
            'latitude': float(asset['latitude']),
            'longitude': float(asset['longitude']),
            'state': asset.get('state') or '',
            'county': asset.get('county') or '',
            'owner': asset.get('owner') or '',
        }
        
        # Add energy grid specific fields
        for field in ['balancingauthority', 'eia_plant_id', 'lines', 'min_voltage', 'max_voltage']:
            if field in asset:
                prepared[field] = asset[field]
        
        # Preserve other fields
        for key, value in asset.items():
            if key not in prepared and key not in ['_id', 'location']:
                prepared[key] = value
        
        return prepared
        
    except Exception as e:
        logger.error(f"Error preparing asset: {e}")
        return None


def _extract_component_types(assets: List[Dict[str, Any]]) -> Dict[str, int]:
    """Extract component type counts"""
    types = {}
    for asset in assets:
        comp_type = asset.get('facilityTypeName') or 'Unknown'
        types[comp_type] = types.get(comp_type, 0) + 1
    return types


def _extract_states(assets: List[Dict[str, Any]]) -> Dict[str, int]:
    """Extract state counts"""
    states = {}
    for asset in assets:
        state = asset.get('state')
        if state:
            states[state] = states.get(state, 0) + 1
    return states


def prepare_upload_response(
    upload_result: Dict[str, Any],
    assets: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """Prepare custom upload response"""
    response = {
        'success': upload_result.get('success', False),
        'upload_id': upload_result.get('upload_id'),
        'filename': upload_result.get('filename'),
        'total_rows': upload_result.get('total_rows', 0),
    }
    
    if not upload_result['success']:
        response['error'] = upload_result.get('error')
    else:
        response['preview'] = upload_result.get('preview', [])
        if assets:
            response['infrastructure'] = assets
            response['count'] = len(assets)
    
    return response


def prepare_stats_response(stats: Dict[str, Any]) -> Dict[str, Any]:
    """Prepare infrastructure statistics"""
    return {
        'source': stats.get('source', 'Unknown'),
        'total_assets': stats.get('total_assets', 0),
        'component_types': stats.get('component_types', {}),
        'states': stats.get('top_states') or stats.get('states', {})
    }