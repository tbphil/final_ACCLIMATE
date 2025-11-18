"""
Field Mapper - Auto-detect and map user columns to required fields
Uses RapidFuzz for robust fuzzy matching to handle diverse naming conventions
"""

import logging
from typing import Dict, List, Optional, Set, Tuple
from rapidfuzz import process, fuzz

from . import config

logger = logging.getLogger(__name__)

# Common column name variations for auto-detection
#
# ADD NEW VARIATIONS HERE as you discover new column naming conventions
# Format: 'field': ['variation1', 'variation2', ...]
# Keep variations lowercase as they will be normalized during matching
# Keep this list organized by field type for easy maintenance
#
# CRITICAL FIELDS (latitude, longitude, component_type) have extensive variations
# to ensure robust matching across diverse data sources
#
# NOTE: Keys here match the ACTUAL field names from config.py
# Matching is done case-insensitively, but output uses these exact field names
FIELD_VARIATIONS = {
    'latitude': [
        'latitude', 'lat', 'y', 'ylat',
        'y_pos', 'ypos', 'y_position', 'y_coord', 'y_coordinate', 'lat_pos',
        'latitude_deg', 'lat_deg', 'latitude_decimal', 'lat_decimal', 'decimal_lat',
        'decimal_latitude', 'dd_lat', 'lat_dd', 'latitude_dd',
        'northing', 'geographic_latitude', 'geo_lat', 'geo_y', 'geolat',
        'wgs84_lat', 'lat_wgs84', 'epsg4326_lat', 'coord_y', 'coordy', 'point_y',
        'pointy', 'latitude_wgs84', 'wgs84_latitude',
        'lattitude', 'latitide', 'latitud', 'latd', 'lati', 'lt',
        'y pos', 'y coord', 'y coordinate', 'lat decimal', 'latitude degree',
        'lat_n', 'n', 'north', 'northing_y', 'y_north',
        'latitude_value', 'lat_value', 'y_value', 'latitude_coord', 'lat_coord'
    ],
    'longitude': [
        'longitude', 'lon', 'lng', 'long', 'x', 'xlon',
        'x_pos', 'xpos', 'x_position', 'x_coord', 'x_coordinate', 'lon_pos', 'lng_pos',
        'longitude_deg', 'lon_deg', 'lng_deg', 'longitude_decimal', 'lon_decimal',
        'lng_decimal', 'decimal_lon', 'decimal_lng', 'decimal_longitude', 'dd_lon',
        'lon_dd', 'lng_dd', 'longitude_dd',
        'easting', 'geographic_longitude', 'geo_lon', 'geo_lng', 'geo_x', 'geolon',
        'wgs84_lon', 'wgs84_lng', 'lon_wgs84', 'lng_wgs84', 'epsg4326_lon',
        'coord_x', 'coordx', 'point_x', 'pointx', 'longitude_wgs84', 'wgs84_longitude',
        'longitud', 'longitue', 'longd', 'lngi', 'lngt', 'lt',
        'x pos', 'x coord', 'x coordinate', 'lon decimal', 'lng decimal',
        'longitude degree',
        'lon_e', 'lng_e', 'e', 'east', 'easting_x', 'x_east',
        'longitude_value', 'lon_value', 'lng_value', 'x_value', 'longitude_coord',
        'lon_coord', 'lng_coord'
    ],
    'component_type': [
        'component_type', 'componenttype', 'type', 'component', 'comp_type', 'comptype',
        'asset_type', 'assettype', 'asset_class', 'assetclass', 'asset_category',
        'assetcategory', 'asset_kind', 'asset',
        'facility_type', 'facilitytype', 'facility_type_name', 'facilitytypename',
        'facility_name', 'facilityname', 'facility_category', 'facilitycategory', 
        'facility_class', 'facilityclass', 'facility_kind', 'facility',
        'equipment_type', 'equipmenttype', 'equipment_class', 'equipmentclass',
        'equipment_category', 'equipmentcategory', 'equipment_kind', 'equipment',
        'infrastructure_type', 'infrastructuretype', 'infrastructure_class',
        'structure_type', 'structuretype', 'structure_class', 'structure',
        'category', 'classification', 'class', 'kind', 'group', 'subtype', 'sub_type',
        'object_type', 'objecttype', 'object_class', 'feature_type', 'featuretype',
        'feature_class', 'feature',
        'plant_type', 'planttype', 'station_type', 'stationtype', 'source_type',
        'sourcetype', 'resource_type', 'resourcetype', 'generator_type', 'generatortype',
        'primsource', 'prim_source', 'primary_source', 'primarysource', 'prime_source',
        'type_description', 'typedescription', 'type_name', 'typename', 'component_name',
        'componentname', 'device_type', 'devicetype', 'system_type', 'systemtype',
        'utility_type', 'utilitytype', 'service_type', 'servicetype'
    ],
    'name': [
        'name', 'asset_name', 'assetname', 'facility_name', 'facilityname',
        'plant_name', 'plantname', 'site_name', 'sitename', 'station_name',
        'stationname', 'identifier', 'label', 'title', 'description',
        'facility_name', 'site_name', 'location_name', 'locationname'
    ],
    '_id': [
        '_id', 'id', 'uuid', 'asset_id', 'assetid', 'facility_id', 'facilityid',
        'identifier', 'rec_id', 'recid', 'record_id', 'recordid',
        'plant_code', 'plantcode', 'plant_id', 'plantid', 'site_id', 'siteid',
        'objectid', 'object_id', 'eia_plant_id', 'facility_code', 'facilitycode',
        'asset_number', 'assetnumber', 'facility_number', 'facilitynumber',
        'station_id', 'stationid', 'location_id', 'locationid', 'gis_id', 'gisid',
        'unique_id', 'uniqueid', 'key', 'primary_key', 'primarykey'
    ],
    'County': [
        'county', 'county_name', 'countyname', 'cnty', 'cnty_name',
        'parish', 'borough', 'county_code', 'countycode', 'cntyname',
        'admin2', 'admin_2', 'second_level', 'fips_county'
    ],
    'State': [
        'state', 'state_name', 'statename', 'province', 'state_code', 'statecode',
        'st', 'st_name', 'state_abbr', 'stateabbr', 'state_abbreviation',
        'region', 'territory', 'fips_state', 'state_fips'
    ],
    'City': [
        'city', 'municipality', 'town', 'city_name', 'cityname',
        'municipal', 'munic', 'locality', 'place', 'placename', 'place_name',
        'town_name', 'townname', 'village', 'settlement', 'urban_area',
        'incorporated_place', 'city_town'
    ],
    'Country': [
        'country', 'nation', 'country_name', 'countryname', 'country_code', 'countrycode',
        'ctry', 'cntry', 'nation_name', 'iso_country', 'iso3', 'iso2', 'country_iso'
    ],
    'Address': [
        'address', 'street', 'street_address', 'streetaddress', 'location_address',
        'locationaddress', 'physical_address', 'physicaladdress',
        'street_add', 'streetadd', 'street_addr', 'str_address', 'str_addr',
        'address1', 'address_1', 'addr', 'addr1', 'full_address', 'fulladdress',
        'mailing_address', 'mailingaddress', 'site_address', 'siteaddress'
    ],
    'Zip': [
        'zip', 'zipcode', 'zip_code', 'postal_code', 'postalcode', 'postal'
    ],
    'Critical': [
        'critical', 'is_critical', 'iscritical', 'criticality', 'critical_asset',
        'criticalasset', 'critical_infrastructure', 'critical_status',
        'importance', 'priority', 'essential', 'is_essential', 'isessential',
        'vital', 'is_vital', 'key_asset', 'keyasset', 'high_priority',
        'highpriority', 'mission_critical', 'missioncritical'
    ],
    'lines': [
        'lines', 'line', 'num_lines', 'numlines', 'number_of_lines', 'numberoflines',
        'line_count', 'linecount', 'line_number', 'linenumber', 'total_lines', 'totallines',
        'line_connections', 'lineconnections', 'connections', 'connection_count',
        'connectioncount', 'num_connections', 'numconnections', 'number_of_connections',
        'transmission_lines', 'transmissionlines', 'power_lines', 'powerlines',
        'circuit_count', 'circuitcount', 'circuits', 'num_circuits', 'numcircuits',
        'number_of_circuits', 'feeder_count', 'feedercount', 'feeders', 'num_feeders',
        'numfeeders', 'number_of_feeders', 'incoming_lines', 'incominglines',
        'outgoing_lines', 'outgoinglines', 'connected_lines', 'connectedlines',
        'attached_lines', 'attachedlines', 'line_attachments', 'lineattachments',
        'bay_count', 'baycount', 'bays', 'num_bays', 'numbays'
    ],
    'min_voltage': [
        'min_voltage', 'minvoltage', 'minimum_voltage', 'minimumvoltage',
        'voltage_min', 'voltagemin', 'min_volt', 'minvolt', 'minimum_volt',
        'min_kv', 'minkv', 'minimum_kv', 'minimumkv', 'kv_min', 'kvmin',
        'lower_voltage', 'lowervoltage', 'voltage_low', 'voltagelow',
        'low_voltage', 'lowvoltage', 'min_voltage_kv', 'minvoltagekv',
        'minimum_voltage_kv', 'minimumvoltagekv', 'operating_voltage_min',
        'operatingvoltagemin', 'op_voltage_min', 'opvoltagemin',
        'bulkminvoltage', 'bulk_min_voltage', 'distminvoltage', 'dist_min_voltage',
        'min_operating_voltage', 'minoperatingvoltage', 'voltage_minimum',
        'voltageminimum', 'min_rated_voltage', 'minratedvoltage',
        'rated_voltage_min', 'ratedvoltagemin', 'nominal_min_voltage',
        'nominalminvoltage', 'design_min_voltage', 'designminvoltage'
    ],
    'max_voltage': [
        'max_voltage', 'maxvoltage', 'maximum_voltage', 'maximumvoltage',
        'voltage_max', 'voltagemax', 'max_volt', 'maxvolt', 'maximum_volt',
        'max_kv', 'maxkv', 'maximum_kv', 'maximumkv', 'kv_max', 'kvmax',
        'upper_voltage', 'uppervoltage', 'voltage_high', 'voltagehigh',
        'high_voltage', 'highvoltage', 'max_voltage_kv', 'maxvoltagekv',
        'maximum_voltage_kv', 'maximumvoltagekv', 'operating_voltage_max',
        'operatingvoltagemax', 'op_voltage_max', 'opvoltagemax',
        'bulkmaxvoltage', 'bulk_max_voltage', 'distmaxvoltage', 'dist_max_voltage',
        'max_operating_voltage', 'maxoperatingvoltage', 'voltage_maximum',
        'voltagemaximum', 'max_rated_voltage', 'maxratedvoltage',
        'rated_voltage_max', 'ratedvoltagemax', 'nominal_max_voltage',
        'nominalmaxvoltage', 'design_max_voltage', 'designmaxvoltage',
        'gridvoltagekv', 'grid_voltage_kv', 'grid_voltage', 'gridvoltage',
        'voltage', 'volt', 'kv', 'rated_kv', 'ratedkv', 'nominal_voltage',
        'nominalvoltage', 'nominal_kv', 'nominalkv', 'operating_voltage',
        'operatingvoltage', 'rated_voltage', 'ratedvoltage'
    ]
}


def normalize_column_name(name: str) -> str:
    # Normalize column name for comparison
    return name.lower().replace(' ', '_').replace('-', '_')


def auto_map_fields(columns: List[str]) -> Dict[str, Optional[Dict[str, any]]]:
    """
    Automatically map user columns to model fields using exact and fuzzy matching
    Uses "best match wins" strategy: each column is assigned to the field it matches best
    
    Args:
        columns: List of column names from user's file
    
    Returns:
        Dictionary mapping model_field -> metadata dict with:
        {
            'mapped_to': detected_column,
            'match_type': 'exact' | 'known_variation' | 'fuzzy',
            'confidence': int (0 to 100)
        }
        Returns None for unmapped fields
    """
    # Normalize column names for comparison
    columns_normalized = {}
    for col in columns:
        normalized = normalize_column_name(col)
        columns_normalized[normalized] = col
    
    # Step 1: For each user column, find ALL possible field matches with scores
    # Structure: {user_column_normalized: [(field_name, confidence, match_type), ...]}
    column_to_fields = {}
    
    for user_col_norm, user_col_original in columns_normalized.items():
        potential_matches = []
        
        for field_name, variations in FIELD_VARIATIONS.items():
            match_type = None
            confidence = 0
            
            # First: Try exact match with field name
            field_normalized = normalize_column_name(field_name)
            if field_normalized == user_col_norm:
                match_type = 'exact'
                confidence = 100
            else:
                # Second: Try known variations
                for variation in variations:
                    variation_normalized = normalize_column_name(variation)
                    if variation_normalized == user_col_norm:
                        match_type = 'known_variation'
                        confidence = 100
                        break
                
                # Third: Try fuzzy matching if no exact match found
                if match_type is None and variations:
                    # Get cutoff from config (None means skip fuzzy matching)
                    cutoff = config.FUZZY_MATCH_CUTOFFS.get(field_name, 80)
                    
                    # Skip fuzzy matching if cutoff is None
                    if cutoff is None:
                        continue
                    
                    # Try fuzzy against field name
                    result = process.extractOne(
                        user_col_norm,
                        [field_normalized],
                        scorer=fuzz.ratio,
                        score_cutoff=cutoff
                    )
                    
                    if result:
                        _, score, _ = result
                        match_type = 'fuzzy'
                        confidence = score
                    else:
                        # Try fuzzy against variations
                        result = process.extractOne(
                            user_col_norm,
                            variations,
                            scorer=fuzz.ratio,
                            score_cutoff=cutoff
                        )
                        
                        if result:
                            _, score, _ = result
                            match_type = 'fuzzy'
                            confidence = score
            
            # If we found a match, add it to potential matches
            if match_type is not None:
                potential_matches.append((field_name, confidence, match_type))
        
        # Store all potential matches for this column
        if potential_matches:
            column_to_fields[user_col_norm] = potential_matches
    
    # Step 2: Assign each column to its BEST matching field
    # This prevents cross-field pollution (e.g., "county" going to "component_type")
    field_assignments = {}  # field_name -> (user_col, confidence, match_type)
    
    # Define match type priority (exact > known_variation > fuzzy)
    match_type_priority = {'exact': 3, 'known_variation': 2, 'fuzzy': 1}
    
    for user_col_norm, matches in column_to_fields.items():
        # Sort matches by confidence (highest first), then by match type priority
        matches_sorted = sorted(
            matches, 
            key=lambda x: (x[1], match_type_priority.get(x[2], 0)), 
            reverse=True
        )
        best_field, best_confidence, best_match_type = matches_sorted[0]
        
        # Assign this column to its best matching field
        # If the field already has an assignment, only replace if this is a better match
        # Priority: 1) Higher confidence, 2) Better match type (exact > variation > fuzzy)
        if best_field not in field_assignments:
            field_assignments[best_field] = (
                columns_normalized[user_col_norm],
                best_confidence,
                best_match_type
            )
        else:
            current_col, current_conf, current_type = field_assignments[best_field]
            # Replace if higher confidence, or same confidence but better match type
            if (best_confidence > current_conf or 
                (best_confidence == current_conf and 
                 match_type_priority.get(best_match_type, 0) > match_type_priority.get(current_type, 0))):
                field_assignments[best_field] = (
                    columns_normalized[user_col_norm],
                    best_confidence,
                    best_match_type
                )
    
    # Step 3: Build final mapping dict
    mapping = {}
    for field_name in FIELD_VARIATIONS.keys():
        if field_name in field_assignments:
            user_col, confidence, match_type = field_assignments[field_name]
            mapping[field_name] = {
                'mapped_to': user_col,
                'match_type': match_type,
                'confidence': confidence
            }
        else:
            mapping[field_name] = None
    
    return mapping


def get_missing_required_fields(mapping: Dict[str, Optional[Dict]]) -> List[str]:
    """
    Return fields that user must provide but weren't auto-mapped
    
    Args:
        mapping: Current field mapping
    
    Returns:
        List of missing required field names
    """
    missing = []
    
    for field in config.REQUIRED_FIELDS:
        if not mapping.get(field):
            missing.append(field)
    
    return missing


def validate_mapping(
    mapping: Dict[str, str],
    available_columns: Set[str]
) -> Dict[str, str]:
    """
    Validate user-provided mapping
    
    Args:
        mapping: User's field mapping (required_field -> column_name)
        available_columns: Set of available column names
    
    Returns:
        Dictionary of errors (field -> error_message), empty dict if valid
    """
    errors = {}
    
    # Check all required fields are mapped
    for field in config.REQUIRED_FIELDS:
        if field not in mapping or not mapping[field]:
            errors[field] = f"Required field '{field}' is not mapped"
    
    # Check mapped columns exist in file
    for field, column in mapping.items():
        if column and column not in available_columns:
            errors[field] = f"Column '{column}' not found in file"
    
    return errors


def apply_mapping(
    row: Dict[str, any],
    mapping: Dict[str, Optional[Dict[str, any]]]
) -> Dict[str, any]:
    """
    Apply field mapping to transform a row
    
    Args:
        row: Original row with user's column names
        mapping: Field mapping with metadata from auto_map_fields()
    
    Returns:
        Transformed row with standard field names and spec_overrides
    """
    transformed = {}
    
    # Map known fields
    for model_field, mapping_info in mapping.items():
        if mapping_info and mapping_info['mapped_to'] in row:
            user_column = mapping_info['mapped_to']
            value = row[user_column]
            # Only add if not None
            if value is not None:
                transformed[model_field] = value
    
    # Put unmapped fields in spec_overrides
    # Exclude columns that were mapped to our standard fields
    mapped_columns = set(
        m['mapped_to'] for m in mapping.values() if m is not None
    )
    
    # Also track which field names we've already used (case-insensitive)
    # to avoid duplicates like "Latitude" when we already have "latitude"
    used_field_names = set()
    for model_field, mapping_info in mapping.items():
        if mapping_info:
            used_field_names.add(model_field.lower())
    
    spec_overrides = {}
    
    for col, value in row.items():
        col_normalized = normalize_column_name(col)
        # Skip if this column was mapped, or if it's a duplicate of a mapped field
        if col not in mapped_columns and col_normalized not in used_field_names and value is not None:
            spec_overrides[col] = value
    
    if spec_overrides:
        transformed['spec_overrides'] = spec_overrides
    
    return transformed


def get_mapping_summary(mapping: Dict[str, Optional[Dict]]) -> Dict[str, any]:
    """
    Generate a summary of the mapping for user review
    
    Args:
        mapping: Field mapping with metadata
    
    Returns:
        Summary dictionary with statistics
    """
    mapped_count = sum(1 for v in mapping.values() if v is not None)
    unmapped_count = len(mapping) - mapped_count
    
    # Identify critical unmapped fields
    critical_unmapped = get_missing_required_fields(mapping)
    
    # Build simple mapping dict for display
    simple_mapping = {
        k: v['mapped_to'] for k, v in mapping.items() if v is not None
    }
    
    return {
        'total_fields': len(mapping),
        'mapped': mapped_count,
        'unmapped': unmapped_count,
        'critical_unmapped': critical_unmapped,
        'has_critical_unmapped': len(critical_unmapped) > 0,
        'mapping': simple_mapping
    }


def validate_required_fields(
    rows: List[Dict],
    mapping: Dict[str, Optional[Dict]]
) -> Tuple[bool, List[str]]:
    """
    Validate that required fields exist and have valid values
    Should be called after apply_mapping() transforms the rows
    
    Args:
        rows: Transformed data rows (with standard field names)
        mapping: Field mapping with metadata from auto_map_fields()
    
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    # Get coordinate ranges from config
    lat_min, lat_max = config.LATITUDE_RANGE
    lon_min, lon_max = config.LONGITUDE_RANGE
    
    # Check if required fields were mapped
    missing_fields = get_missing_required_fields(mapping)
    if missing_fields:
        errors.append(f"Required fields not found in data: {', '.join(missing_fields)}")
        return False, errors
    
    # Validate actual data values using the actual field names from config
    for i, row in enumerate(rows):
        row_num = i + 1
        
        # Check component_type
        component_type = row.get('component_type')
        if not component_type or component_type == '':
            errors.append(f"Row {row_num}: component_type is missing or empty")
        
        # Check latitude (lowercase)
        lat = row.get('latitude')
        if lat is None or lat == '':
            errors.append(f"Row {row_num}: latitude is missing or empty")
        else:
            try:
                lat_float = float(lat)
                if lat_float < lat_min or lat_float > lat_max:
                    errors.append(
                        f"Row {row_num}: latitude {lat_float} is out of valid range "
                        f"({lat_min} to {lat_max})"
                    )
            except (ValueError, TypeError):
                errors.append(f"Row {row_num}: latitude '{lat}' cannot be converted to a number")
        
        # Check longitude (lowercase)
        lon = row.get('longitude')
        if lon is None or lon == '':
            errors.append(f"Row {row_num}: longitude is missing or empty")
        else:
            try:
                lon_float = float(lon)
                if lon_float < lon_min or lon_float > lon_max:
                    errors.append(
                        f"Row {row_num}: longitude {lon_float} is out of valid range "
                        f"({lon_min} to {lon_max})"
                    )
            except (ValueError, TypeError):
                errors.append(f"Row {row_num}: longitude '{lon}' cannot be converted to a number")
        
        # Stop after first 100 errors to avoid overwhelming output
        if len(errors) >= 100:
            errors.append(f"... and more errors (stopped after 100)")
            break
    
    is_valid = len(errors) == 0
    return is_valid, errors
