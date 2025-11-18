"""
Field Mapping Test - Test that we correctly map user columns to required fields
Focus: Test field_mapper.auto_map_fields() with real file data
Uses the actual parser and importer logic, not duplicated logic
"""

import sys
from pathlib import Path

# Add parent directories to path
# tests/ -> acclimate/ -> user_asset_import/ (need to go up 2 levels)
user_asset_import_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(user_asset_import_root))

from acclimate.parsers import (
    parse_file,
    detect_file_format,
    parse_excel
)
from acclimate import field_mapper, config


def print_header(title):
    """Print a section header"""
    print(f"\n{title}")
    print(f"{'-'*90}")


def format_match_type(mapping_info):
    """Format the match type for display"""
    if not mapping_info:
        return "NOT MAPPED"
    
    match_type = mapping_info['match_type']
    confidence = mapping_info['confidence']
    
    if match_type == 'exact':
        return "exact"
    elif match_type == 'known_variation':
        return "mapped"
    elif match_type == 'fuzzy':
        return f"fuzzy {int(confidence * 100)}%"
    else:
        return match_type


def test_field_mapping(filename, row_limit=None):
    """
    Test field mapping for a file using the actual parser functions.
    The test just calls parser functions and validates results - no business logic duplication.
    """
    
    # example_data is in parent directory (acclimate/) not in tests/
    example_path = Path(__file__).parent.parent / 'example_data' / filename
    
    if not example_path.exists():
        print(f"File not found: {example_path}")
        return False
    
    try:
        # Read file
        with open(example_path, 'rb') as f:
            file_content = f.read()
        
        # Print header
        print(f"{'='*90}")
        print("FIELD MAPPING RESULTS")
        print(f"Testing: {filename}")
        print(f"✓ File loaded: {len(file_content) / (1024*1024):.2f} MB")
        
        # Detect file format
        file_format = detect_file_format(filename, file_content)
        
        # Parse using appropriate parser function (NO logic duplication)
        # parse_excel now handles both single and multi-sheet automatically
        if file_format == 'excel':
            rows, columns = parse_excel(file_content, filename, row_limit=row_limit)
            print(f"✓ Parsed Excel: {len(rows):,} rows, {len(columns)} columns")
        else:
            rows, columns, _ = parse_file(file_content, filename)
            print(f"✓ Parsed: {len(rows):,} rows, {len(columns)} columns")
        
        print(f"{'-'*90}")
        
        # Run field mapping using the field_mapper module function
        field_mapping = field_mapper.auto_map_fields(columns)
        
        # Display results (this is just presentation, not business logic)
        display_mapping_results(field_mapping, columns)
        
        return True
        
    except Exception as e:
        print(f"\nTest failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def display_mapping_results(field_mapping, columns):
    """Display field mapping results - uses config.py for field lists (no logic duplication)"""
    
    # Get field categories from config.py - DON'T hardcode them!
    required_fields = config.REQUIRED_FIELDS
    optional_fields = config.OPTIONAL_FIELDS
    
    # Auto-generated fields that we try to find but will create if missing
    auto_gen_fields = ['name', '_id']
    
    # Show required fields first
    print(f"\nREQUIRED FIELDS (Must have for ACCLIMATE):")
    print(f"{'-'*90}")
    print(f"{'Field Name':<20} {'User Column':<25} {'Output As':<20} {'Match Type':<15}")
    print(f"{'-'*90}")
    
    for field in required_fields:
        mapping_info = field_mapping.get(field)
        
        if mapping_info:
            user_column = mapping_info['mapped_to']
            how_mapped = format_match_type(mapping_info)
            print(f"{field:<20} {user_column:<25} {field:<20} {how_mapped:<15}")
        else:
            print(f"{field:<20} {'<NOT FOUND>':<25} {'<UNMAPPED>':<20} {'NOT MAPPED':<15}")
    
    # Show auto-generated fields
    print(f"\nAUTO-GENERATED FIELDS (Auto-generated if missing):")
    print(f"{'-'*90}")
    print(f"{'Field Name':<20} {'User Column':<25} {'Output As':<20} {'Match Type':<15}")
    print(f"{'-'*90}")
    
    for field in auto_gen_fields:
        mapping_info = field_mapping.get(field)
        
        if mapping_info:
            user_column = mapping_info['mapped_to']
            how_mapped = format_match_type(mapping_info)
            print(f"{field:<20} {user_column:<25} {field:<20} {how_mapped:<15}")
        else:
            print(f"{field:<20} {'<NOT FOUND>':<25} {'<WILL GENERATE>':<20} {'N/A':<15}")
    
    # Show optional fields
    print(f"\nOPTIONAL FIELDS (Enhance data quality but not required):")
    print(f"{'-'*90}")
    print(f"{'Field Name':<20} {'User Column':<25} {'Output As':<20} {'Match Type':<15}")
    print(f"{'-'*90}")
    
    for field in optional_fields:
        mapping_info = field_mapping.get(field)
        
        if mapping_info:
            user_column = mapping_info['mapped_to']
            how_mapped = format_match_type(mapping_info)
            print(f"{field:<20} {user_column:<25} {field:<20} {how_mapped:<15}")
        else:
            print(f"{field:<20} {'NOT FOUND':<25} {'N/A':<20} {'N/A':<15}")
    
    # Show unmapped columns from source file
    mapped_columns = set(m['mapped_to'] for m in field_mapping.values() if m)
    unmapped_columns = [col for col in columns if col not in mapped_columns]
    
    if unmapped_columns:
        print(f"\nUNMAPPED COLUMNS FROM SOURCE FILE:")
        print(f"   These columns will be stored in spec_overrides:")
        for col in unmapped_columns:
            print(f"   - {col}")
    
    # Use field_mapper's get_missing_required_fields() function
    missing = field_mapper.get_missing_required_fields(field_mapping)
    if missing:
        print(f"\n⚠️  WARNING: Missing required fields: {', '.join(missing)}")
        print(f"   Import will fail without these fields!")
    else:
        print(f"\n✅ All required fields found!")
    
    print(f"\n{'='*90}")


def main():
    """Run field mapping tests"""
    import sys
    
    # Check for CLI arguments
    if len(sys.argv) < 2:
        print("\n  Error: No filename provided")
        print("\nUsage: python test_parser_field_mappings.py <filename>")
        print("\nExamples:")
        print("  python test_parser_field_mappings.py Power_Plants.zip")
        print("  python test_parser_field_mappings.py AHA_data.xlsx")
        print("  python test_parser_field_mappings.py Power_Plants.csv")
        print("  python test_parser_field_mappings.py Power_Plants.geojson")
        print("\nNote: Files should be in the example_data/ directory")
        sys.exit(1)
    
    filename = sys.argv[1]
    
    # Optional: Set row limit for Excel files to speed up testing
    row_limit = 10 if filename.endswith(('.xlsx', '.xls')) else None
    
    # Run test using actual parser logic
    success = test_field_mapping(filename, row_limit)
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
