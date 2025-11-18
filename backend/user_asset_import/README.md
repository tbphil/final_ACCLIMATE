# User Asset Import System

Handles user-uploaded infrastructure asset data in multiple formats with automatic field mapping and component type resolution.

## Overview

Users can upload infrastructure data in various formats (CSV, Excel, GeoJSON, Shapefiles) with inconsistent column names and component types. This system automatically maps them to ACCLIMATE's standard schema.

## Supported Formats

- **CSV** - UTF-8 encoded files
- **Excel** - .xlsx and .xls (multi-sheet support)
- **GeoJSON** - Extracts features automatically
- **Shapefiles** - From ZIP archives (auto-converts to WGS84)

## Import Pipeline

1. **Parse** - Detect format and extract data
2. **Map Fields** - Match user columns to standard fields using 200+ variations
3. **Transform** - Rename columns to ACCLIMATE schema
4. **Validate** - Verify required fields and coordinate ranges
5. **Match Components** - Resolve component types (100+ variations + MongoDB aliases)
6. **Enrich** - Generate UUIDs, GeoJSON, nested objects

## Module Structure

```
user_asset_import/
├── acclimate/
│   ├── config.py              # Settings and thresholds
│   ├── parsers.py             # Multi-format parser
│   ├── field_mapper.py        # Field name variations
│   ├── component_mapper.py    # Component type matching
│   ├── importer.py            # Import orchestration
│   ├── fastapi_endpoint.py    # REST API endpoint
│   ├── example_data/          # Test files
│   └── tests/                 # Unit tests
```

## Integration

Backend integration is complete. Added to `backend/main.py`:

```python
from user_asset_import import router as import_router
app.include_router(import_router)
```

Endpoint: `POST /api/import/user_assets`

## Database Requirements

Works with or without MongoDB component_library collection. With it:
- 83 canonical component types
- Enhanced matching accuracy
- Canonical UUIDs

Check status:
```bash
mongosh acclimate_db --eval "db.component_library.countDocuments()"
```

## API Endpoint

**POST** `/api/import/user_assets`

**Parameters:**
- `file` (required) - Upload file
- `column_mappings` (optional) - Manual field overrides (JSON string)
- `component_mappings` (optional) - Manual component overrides (JSON string)
- `target_collection` (optional) - MongoDB collection (default: 'energy_grid')
- `save_to_database` (optional) - Save or preview only (default: false)

**Response:**
```json
{
  "success": true,
  "data": [{
    "uuid": "generated-uuid-123",
    "latitude": 37.7749,
    "longitude": -122.4194,
    "component_type": "Solar Generation Facility",
    "location": {"type": "Point", "coordinates": [-122.4194, 37.7749]},
    "aliases": {"component_type": "pv"},
    "spec_overrides": {"capacity_mw": 50.5}
  }],
  "metadata": {
    "field_mappings": {
      "latitude": {"user_column": "LAT", "confidence": 1.0}
    },
    "component_mappings": {
      "pv": {"canonical_name": "Solar Generation Facility", "confidence": 1.0}
    }
  }
}
```

## Workflow

**Two-step process:**
1. Preview with `save_to_database=false` - Review mappings and confidence scores
2. Confirm with `save_to_database=true` - Save to database

**Manual overrides:**
```javascript
formData.append('column_mappings', JSON.stringify({
  "latitude": "Y_COORDINATE"
}));
formData.append('component_mappings', JSON.stringify({
  "pv": "Solar Generation Facility"
}));
```

## Field Mapping

Auto-detects columns using exact match, known variations, then fuzzy matching.

**Required fields:** latitude, longitude, component_type  
**Optional fields:** name, address, city, state, county, zip, year_in_use

Example variations for latitude:
- `latitude`, `lat`, `LAT`, `y`, `Y_COORD`, `gps_lat`, etc. (40+ variations)

## Component Matching

Three-layer matching:
1. Hardcoded variations (100+ mappings in code)
2. Database aliases (from component_library)
3. Fuzzy matching with consensus scoring

Examples:
- `pv`, `solar`, `photovoltaic` → Solar Generation Facility
- `wind turbine`, `wtg` → Wind Farm
- `sub`, `substation` → Substation

Low-confidence matches default to "Unknown" with suggestions provided.

## Configuration

Settings in `acclimate/config.py`:

```python
# Fuzzy match thresholds (0.0 to 1.0)
FUZZY_MATCH_THRESHOLDS = {
    'latitude': 0.60,
    'component_type': 0.60,
    'name': 0.70,
}

# Component matching
COMPONENT_FUZZY_CUTOFF = 0.60
COMPONENT_CONFIDENCE_THRESHOLD = 0.70
```

## Testing

```bash
cd backend/user_asset_import/acclimate/tests
python test_parser_field_mappings.py
python test_full_importer.py
```

Test files in `example_data/`: AHA_data.xlsx, Power_Plants.csv/geojson/zip

## Features

- Multi-format support (CSV, JSON, GeoJSON, Shapefile, Excel)
- 200+ field name variations with fuzzy matching
- 100+ component type variations with consensus scoring
- Confidence scores for all mappings
- Partial override support
- Complete data preservation via aliases
- GeoJSON generation for mapping
- Auto coordinate system conversion

## Frontend Integration

Remaining work (~2-3 hours):
1. Update upload handler to call new endpoint
2. Display mapping metadata and confidence scores
3. Implement preview/confirm workflow
4. Show alternatives for low-confidence matches
5. Refresh map after successful import

## Troubleshooting

**Field not detected:** Add variation to `FIELD_VARIATIONS` in config.py or use manual override

**Low confidence component match:** Returns "Unknown" with suggestions. Use manual override or add variation to component_mapper.py

**Invalid coordinates:** Verify lat/lon aren't swapped and coordinates are in valid ranges

---

Questions: Check code comments or run tests for examples.
