"""
ACCLIMATE Project Configuration

This file is the SOURCE OF TRUTH for output field names and structure.
Field names defined here will appear exactly as specified in the final JSON output.

CAPITALIZATION MATTERS:
- 'Latitude' here → "Latitude" in output JSON
- 'component_type' here → "component_type" in output JSON

To add a new field:
1. Add to appropriate field list (REQUIRED or OPTIONAL) with exact output capitalization
2. Add fuzzy match cutoff to FUZZY_MATCH_CUTOFFS using the SAME field name
3. Add field variations to FIELD_VARIATIONS in field_mapper.py using the SAME field name as key
   (Internally, field_mapper normalizes everything to lowercase for matching, but outputs using these names)
"""

# ============================================================================
# MONGODB SETTINGS
# ============================================================================

MONGO_DATABASE = "acclimate_db"
COMPONENT_LIBRARY_COLLECTION = "component_library"


# ============================================================================
# IMPORT SETTINGS
# ============================================================================

DEFAULT_SECTOR = "Energy Grid"  # Default sector for imported assets
SAVE_TO_DATABASE_DEFAULT = False  # By default, just return data to client without saving


# ============================================================================
# VALIDATION SETTINGS
# ============================================================================

LATITUDE_RANGE = (-90, 90)
LONGITUDE_RANGE = (-180, 180)

# ============================================================================
# FIELD MAPPING CONFIGURATION
# ============================================================================

# Required fields - User MUST provide these (cannot auto-generate)
# These field names represent the EXACT output format in final JSON
REQUIRED_FIELDS = ['latitude', 'longitude', 'component_type']

# Optional fields that we try to find but can work without
# These enhance data quality but aren't critical for system operation
OPTIONAL_FIELDS = [
    'County', 'State', 'City', 'Country', 'Address', 'Zip',
    'Critical', 'lines', 'min_voltage', 'max_voltage'
]

# Fuzzy match cutoffs (0-100, where lower = more lenient, higher = stricter)
# Set to None to disable fuzzy matching for that field
#
# Guidelines:
# - Critical fields (Latitude, Longitude, component_type): 50 (lenient - MUST find these)
# - Important fields (name, _id, Critical): 70 (moderate)
# - Optional fields (County, State, etc): 80 (strict)
# - Generic fields (Address, Zip): 90 (very strict to avoid false positives)

FUZZY_MATCH_CUTOFFS = {
    # Required fields - be lenient, we MUST find these
    'latitude': 50,
    'longitude': 50,
    'component_type': 50,
    
    # Auto-generated fields - moderate matching
    'name': 70,
    '_id': 70,
    
    # Optional fields - important data quality fields
    'Critical': 70,
    'lines': 70,
    'min_voltage': 70,
    'max_voltage': 70,
    
    # Optional fields - address components (strict or very strict)
    'County': 80,
    'State': 80,
    'City': 80,
    'Country': 80,
    'Address': 90,  # Very strict to avoid false positives
    'Zip': 90,      # Very strict to avoid false positives
}


# ============================================================================
# COMPONENT MAPPING CONFIGURATION
# ============================================================================

# Fuzzy matching cutoff for component type matching (0-100)
# Lower = more lenient (fewer "Unknown" results), Higher = stricter
# For automatic operation, keep this lower to avoid blocking imports
# Default: 50 (matches with 50%+ similarity)
COMPONENT_FUZZY_CUTOFF = 50

# Minimum confidence threshold to accept a component match (0.0-1.0)
# Below this, component will be marked as "Unknown"
# Lower = accept more matches automatically, Higher = stricter matching
# For automatic operation, keep this lower to ensure imports succeed
# Default: 0.60 (60% confident)
COMPONENT_CONFIDENCE_THRESHOLD = 0.60

# Consensus boost per additional match (0.0-0.05 recommended)
# Each additional matching variation adds this to confidence
# Rewards multiple variations pointing to same component
# Capped at 10% total boost (5 extra matches * 0.02 = 0.10)
# Default: 0.02 (2% boost per additional match)
COMPONENT_CONSENSUS_BOOST = 0.02


# ============================================================================
# AUTO-GENERATED FIELDS
# ============================================================================
# These fields are ALWAYS added to output regardless of whether user provides them:
#
# NOTE: Fields '_id' and 'name' appear in FUZZY_MATCH_CUTOFFS so we attempt to find
#       user-provided values, but will be auto-generated if not found.
#
# Core Identification:
#   '_id' (str): ALWAYS generated as new UUID to ensure uniqueness across imports.
#                If user provides an ID field, their original ID value is preserved
#                in 'spec_overrides' using their actual column name (e.g., "OBJECTID": 1234).
#                The mapping is tracked in aliases.field (e.g., "_id": "OBJECTID").
#
#   'name' (str): Generated as "{component_type}_{_id}" if not found in user's data.
#                 If user provides a 'name' field, their value is used instead.
#
#   'sector' (str): Set from user selection during upload (defaults to config.DEFAULT_SECTOR)
#
# Geographic Data:
#   'coordinates' (array): [longitude, latitude] format (GeoJSON standard order)
#
#   'location' (object): GeoJSON Point object:
#                        {"type": "Point", "coordinates": [longitude, latitude]}
#
# Import Metadata:
#   'imported_at' (str): timestamp of when asset was imported
#
#   'import_source' (str): Source of import ("user_upload")
#
# Data Tracking:
#   'aliases' (object): Nested structure tracking all field/component transformations:
#                       {"field": {...}, "component": {...}}
#                       Preserves audit trail of how user data was mapped
#
#   'spec_overrides' (object): Contains unmapped user fields AND user's original ID
#                              (stored using their actual column name)
# ============================================================================
