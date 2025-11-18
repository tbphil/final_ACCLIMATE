#!/usr/bin/env python3
"""
Facility Energy Systems → JSON Converter
Converts Excel workbook to HBOM schema-compatible JSON

Usage:
    python facility_to_json_converter.py input_file.xlsx output_file.json
    Files are read/written from: backend/infrastructure_data/AHACoreData/
"""

import sys
import json
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
from datetime import datetime
import uuid


# ============================================================================
# Configuration
# ============================================================================

# Hardcoded data directory path
DATA_DIR = Path("backend/infrastructure_data/AHACoreData")

print(f"🔧 DATA_DIR set to: {DATA_DIR.absolute()}")  # Debug line

# Required fields (must be present)
REQUIRED_FIELDS = [
    'id', 'name', 'facilityTypeName', 'owner', 
    'latitude', 'longitude', 'state', 'county', 'region'
]

# Universal fields (appear in all sheets - extract these)
UNIVERSAL_FIELDS = [
    'id', 'name', 'facilityTypeId', 'facilityTypeName', 'aliases',
    'address', 'city', 'zip', 'county', 'state', 'region', 'country',
    'confidence', 'operator', 'owner', 'critical', 'isContinuity',
    'latitude', 'longitude'
]

# Sheet-specific fields to capture in spec_overrides
SPEC_FIELDS = [
    'capacityMW', 'max_voltage', 'min_voltage', 'gridvoltageKV',
    'eia_plant_id', 'lines', 'balancingauthority', 'storage_capacityMWh',
    'blackstartcapable', 'battery', 'regulatorystatus', 'watersource'
]


# ============================================================================
# Helper Functions
# ============================================================================

def validate_coordinates(lat: Any, lon: Any) -> Optional[Dict[str, float]]:
    """Validate and return coordinates if valid"""
    try:
        lat_f = float(lat) if pd.notna(lat) else None
        lon_f = float(lon) if pd.notna(lon) else None
        
        if lat_f is None or lon_f is None:
            return None
            
        # Basic coordinate validation
        if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
            return {"latitude": lat_f, "longitude": lon_f}
    except (ValueError, TypeError):
        pass
    
    return None


def clean_value(value: Any) -> Any:
    """Clean and normalize field values"""
    if pd.isna(value) or value == '' or value == '[]':
        return None
    
    # Convert numpy types to native Python
    if hasattr(value, 'item'):
        value = value.item()
    
    # Clean string values
    if isinstance(value, str):
        value = value.strip()
        if value == '' or value.lower() == 'null':
            return None
    
    return value


def extract_spec_overrides(row: Dict[str, Any]) -> Dict[str, Any]:
    """Extract sheet-specific fields for spec_overrides"""
    overrides = {}
    
    for field in SPEC_FIELDS:
        if field in row:
            value = clean_value(row[field])
            if value is not None and value != 0:  # Exclude zeros
                overrides[field] = value
    
    # Handle voltage fields specially (convert to standard format)
    if 'max_voltage' in overrides:
        try:
            max_v = float(overrides['max_voltage'])
            if max_v > 0:
                overrides['voltage_class'] = f"{max_v}kV"
        except (ValueError, TypeError):
            pass
    elif 'gridvoltageKV' in overrides:
        try:
            voltage = float(overrides['gridvoltageKV'])
            if voltage > 0:
                overrides['voltage_class'] = f"{voltage}kV"
        except (ValueError, TypeError):
            pass
    
    return overrides


def convert_row_to_asset_instance(row: Dict[str, Any], sheet_name: str, 
                                  file_name: str, row_index: int) -> Optional[Dict[str, Any]]:
    """Convert a single row to asset_instance format"""
    
    # Validate coordinates (required)
    coordinates = validate_coordinates(row.get('latitude'), row.get('longitude'))
    if not coordinates:
        return None  # Skip rows without valid coordinates
    
    # Check required fields
    required_values = {}
    for field in REQUIRED_FIELDS:
        value = clean_value(row.get(field))
        if value is None and field in ['latitude', 'longitude']:
            return None  # Already handled above
        required_values[field] = value
    
    # Build asset instance
    asset_instance = {
        "uuid": clean_value(row.get('id', str(uuid.uuid4()))),  # Use existing ID or generate
        "component_type": clean_value(row.get('facilityTypeName')),
        "name": clean_value(row.get('name')),
        "location": {
            "type": "Point",
            "coordinates": [coordinates['longitude'], coordinates['latitude']]  # GeoJSON: [lon, lat]
        },
        "latitude": coordinates['latitude'],  # Also store flat for easy access
        "longitude": coordinates['longitude'],
        
        # Universal fields
        "owner": clean_value(row.get('owner')),
        "operator": clean_value(row.get('operator')),
        "region": clean_value(row.get('region')),
        "state": clean_value(row.get('state')),
        "county": clean_value(row.get('county')),
        "address": clean_value(row.get('address')),
        "city": clean_value(row.get('city')),
        "zip": clean_value(row.get('zip')),
        "country": clean_value(row.get('country', 'US')),
        
        # Metadata fields
        "critical": clean_value(row.get('critical')),
        "is_continuity": clean_value(row.get('isContinuity')),
        "confidence": clean_value(row.get('confidence')),
        "aliases": clean_value(row.get('aliases')),
        
        # Sheet-specific overrides
        "spec_overrides": extract_spec_overrides(row),
        
        # Source metadata
        "source_metadata": {
            "file": file_name,
            "sheet": sheet_name,
            "row_index": row_index + 2,  # Excel row number (1-indexed + header)
            "extracted_at": datetime.now().isoformat(),
            "facility_type_id": clean_value(row.get('facilityTypeId'))
        }
    }
    
    # Remove null values to keep JSON clean
    asset_instance = {k: v for k, v in asset_instance.items() if v is not None}
    if asset_instance.get('spec_overrides'):
        asset_instance['spec_overrides'] = {k: v for k, v in asset_instance['spec_overrides'].items() if v is not None}
    else:
        asset_instance.pop('spec_overrides', None)
    
    return asset_instance


# ============================================================================
# Main Converter Functions
# ============================================================================

def convert_excel_to_json(excel_path: Path) -> Dict[str, Any]:
    """Convert entire Excel workbook to JSON"""
    
    print(f"🚀 Converting: {excel_path.name}")
    
    try:
        # Read all sheets
        sheets = pd.read_excel(excel_path, sheet_name=None, engine='openpyxl')
        print(f"   📊 Found {len(sheets)} sheets")
        
    except Exception as e:
        print(f"   ❌ Error reading Excel file: {e}")
        return None
    
    # Statistics tracking
    stats = {
        'total_sheets': len(sheets),
        'processed_sheets': 0,
        'total_rows': 0,
        'valid_assets': 0,
        'skipped_no_coords': 0,
        'skipped_no_name': 0,
        'component_types': {},
        'sheets_processed': []
    }
    
    asset_instances = []
    component_library = set()  # Track unique component types
    
    # Process each sheet
    for sheet_name, df in sheets.items():
        if df.empty:
            print(f"   ⚠️  {sheet_name}: Empty sheet, skipping")
            continue
        
        print(f"   📄 Processing: {sheet_name} ({len(df)} rows)")
        stats['total_rows'] += len(df)
        stats['processed_sheets'] += 1
        
        sheet_assets = []
        sheet_skipped_coords = 0
        sheet_skipped_name = 0
        
        # Process each row
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Convert to asset instance
            asset = convert_row_to_asset_instance(
                row_dict, sheet_name, excel_path.name, idx
            )
            
            if asset is None:
                if not validate_coordinates(row_dict.get('latitude'), row_dict.get('longitude')):
                    sheet_skipped_coords += 1
                elif not clean_value(row_dict.get('name')):
                    sheet_skipped_name += 1
                continue
            
            sheet_assets.append(asset)
            
            # Track component types
            comp_type = asset.get('component_type')
            if comp_type:
                component_library.add(comp_type)
                stats['component_types'][comp_type] = stats['component_types'].get(comp_type, 0) + 1
        
        # Sheet summary
        valid_count = len(sheet_assets)
        print(f"      ✅ Valid: {valid_count}, ❌ No coords: {sheet_skipped_coords}, ❌ No name: {sheet_skipped_name}")
        
        asset_instances.extend(sheet_assets)
        stats['valid_assets'] += valid_count
        stats['skipped_no_coords'] += sheet_skipped_coords
        stats['skipped_no_name'] += sheet_skipped_name
        stats['sheets_processed'].append({
            'name': sheet_name,
            'total_rows': len(df),
            'valid_assets': valid_count,
            'skipped': sheet_skipped_coords + sheet_skipped_name
        })
    
    # Build component library
    component_library_docs = []
    for comp_type in sorted(component_library):
        component_library_docs.append({
            "canonical_name": comp_type,
            "component_type": comp_type,
            "sector": "Energy Grid",
            "aliases": [comp_type],
            "created_at": datetime.now().isoformat()
        })
    
    # Final output structure
    output = {
        "metadata": {
            "source_file": excel_path.name,
            "extraction_date": datetime.now().isoformat(),
            "total_sheets": stats['total_sheets'],
            "processed_sheets": stats['processed_sheets'],
            "total_source_rows": stats['total_rows'],
            "valid_asset_instances": stats['valid_assets'],
            "skipped_no_coordinates": stats['skipped_no_coords'],
            "skipped_no_name": stats['skipped_no_name'],
            "unique_component_types": len(component_library),
            "conversion_stats": stats
        },
        
        "component_library": component_library_docs,
        
        "asset_instances": asset_instances
    }
    
    return output


def generate_report(output_data: Dict[str, Any]) -> None:
    """Generate conversion report"""
    
    metadata = output_data['metadata']
    
    print(f"\n{'='*80}")
    print("CONVERSION REPORT")
    print(f"{'='*80}")
    
    print("\n📊 SUMMARY:")
    print(f"   Source file: {metadata['source_file']}")
    print(f"   Total sheets: {metadata['total_sheets']}")
    print(f"   Processed sheets: {metadata['processed_sheets']}")
    print(f"   Total source rows: {metadata['total_source_rows']:,}")
    print(f"   Valid asset instances: {metadata['valid_asset_instances']:,}")
    print(f"   Unique component types: {metadata['unique_component_types']}")
    
    success_rate = (metadata['valid_asset_instances'] / metadata['total_source_rows']) * 100
    print(f"   Success rate: {success_rate:.1f}%")
    
    print("\n❌ SKIPPED RECORDS:")
    print(f"   No coordinates: {metadata['skipped_no_coordinates']:,}")
    print(f"   No name: {metadata['skipped_no_name']:,}")
    
    print("\n🏗️  TOP COMPONENT TYPES:")
    component_counts = metadata['conversion_stats']['component_types']
    for comp_type, count in sorted(component_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"   {comp_type}: {count:,}")
    
    print("\n📄 SHEET BREAKDOWN:")
    for sheet in metadata['conversion_stats']['sheets_processed'][:10]:
        print(f"   {sheet['name']}: {sheet['valid_assets']:,} assets ({sheet['total_rows']} rows)")
    
    if len(metadata['conversion_stats']['sheets_processed']) > 10:
        remaining = len(metadata['conversion_stats']['sheets_processed']) - 10
        print(f"   ... and {remaining} more sheets")


# ============================================================================
# Main
# ============================================================================

def main(excel_filename: str, output_filename: str):
    """Main conversion pipeline"""
    
    # Use hardcoded data directory
    excel_file = DATA_DIR / excel_filename
    output_file = DATA_DIR / output_filename
    
    print(f"🔍 Looking for file at: {excel_file.absolute()}")  # Debug line
    
    if not excel_file.exists():
        print(f"❌ Error: Excel file not found: {excel_file}")
        print(f"   Expected location: {excel_file.absolute()}")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    print("🎯 Facility Energy Systems → JSON Converter")
    print(f"📁 Input: {excel_file}")
    print(f"📄 Output: {output_file}")
    
    # Convert
    output_data = convert_excel_to_json(excel_file)
    
    if not output_data:
        print("❌ Conversion failed!")
        sys.exit(1)
    
    # Generate report
    generate_report(output_data)
    
    # Write JSON
    print(f"\n💾 Writing JSON to: {output_file}")
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2, default=str)
    
    # File size info
    file_size_mb = output_file.stat().st_size / (1024 * 1024)
    print(f"✅ Complete! Output size: {file_size_mb:.1f} MB")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python facility_to_json_converter.py input_file.xlsx output_file.json")
        print("Files are read/written from: backend/infrastructure_data/AHACoreData/")
        sys.exit(1)
    
    main(sys.argv[1], sys.argv[2])