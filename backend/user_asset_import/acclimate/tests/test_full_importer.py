"""
Full Import Test - Tests complete import flow from file to JSON output
Uses actual importer module - no logic duplication
Shows the actual JSON output that will be generated
"""

import asyncio
import json
import sys
from pathlib import Path

# Add parent directories to path
# tests/ -> acclimate/ -> user_asset_import/ (need to go up 2 levels)
user_asset_import_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(user_asset_import_root))

from acclimate.importer import AssetImporter
from acclimate import field_mapper


async def test_full_import(filename: str, row_limit: int = None, save_to_db: bool = False):
    """
    Test complete import flow using actual importer module
    
    Args:
        filename: Name of file in example_data/
        row_limit: Optional limit on rows to process
        save_to_db: Whether to actually save to MongoDB (default: False)
    """
    
    print(f"\n{'='*90}")
    print(f"FULL IMPORT TEST")
    print(f"File: {filename}")
    if row_limit:
        print(f"Row Limit: {row_limit}")
    print(f"Save to DB: {save_to_db}")
    print('='*90)
    
    # example_data is in parent directory (acclimate/) not in tests/
    example_path = Path(__file__).parent.parent / 'example_data' / filename
    
    if not example_path.exists():
        print(f"❌ File not found: {example_path}")
        return False
    
    # Read file
    with open(example_path, 'rb') as f:
        file_content = f.read()
    
    print(f"✓ File loaded: {len(file_content) / (1024*1024):.2f} MB")
    
    # Create importer and process
    importer = AssetImporter(field_mapper)
    
    try:
        result = await importer.process_upload(
            file_content=file_content,
            filename=filename,
            sector='Energy Grid',
            save_to_database=save_to_db,
            target_collection='energy_grid',
            row_limit=row_limit
        )
        
        if not result.get('success'):
            print(f"\n❌ Import failed!")
            print(f"Error: {result.get('error')}")
            if result.get('validation_errors'):
                print(f"\nValidation Errors:")
                for error in result['validation_errors'][:10]:  # Show first 10
                    print(f"  - {error}")
            return False
        
        # Display results
        metadata = result.get('metadata', {})
        
        print(f"\n{'='*90}")
        print("FIELD MAPPINGS")
        print('='*90)
        
        field_mappings = metadata.get('field_mappings', {})
        if field_mappings:
            print(f"{'Field':<20} {'User Column':<30} {'Match Type':<25}")
            print('-'*90)
            for field, details in field_mappings.items():
                match_type = details['match_type']
                confidence = details['confidence']
                
                # Format match type: exact, mapped, fuzzy 85%
                if match_type == 'exact':
                    match_display = 'exact'
                elif match_type == 'known_variation':
                    match_display = 'mapped'
                elif match_type == 'fuzzy':
                    match_display = f"fuzzy {int(confidence * 100)}%"
                else:
                    match_display = match_type
                
                print(f"{field:<20} {details['mapped_from']:<30} {match_display:<25}")
        
        print(f"\n{'='*90}")
        print("COMPONENT MAPPINGS")
        print('='*90)
        
        comp_mappings = metadata.get('component_mappings', {})
        if comp_mappings:
            print(f"{'Mapped To':<40} {'User Value':<30} {'Match Type':<25}")
            print('-'*90)
            for user_val, details in comp_mappings.items():
                match_type = details['match_type']
                confidence = details['confidence']
                
                # Format match type: exact, mapped, fuzzy 85%
                if match_type == 'exact':
                    match_display = 'exact'
                elif match_type in ['alias', 'known_variation']:
                    match_display = 'mapped'
                elif match_type == 'fuzzy_consensus':
                    match_display = f"fuzzy {int(confidence * 100)}%"
                else:
                    match_display = match_type
                
                print(f"{details['mapped_to']:<40} {user_val:<30} {match_display:<25}")
        
        print(f"\n{'='*90}")
        print("IMPORT SUMMARY")
        print('='*90)
        print(f"Total Rows: {metadata.get('total_rows')}")
        print(f"Valid Rows: {metadata.get('valid_rows')}")
        print(f"File Format: {metadata.get('file_format')}")
        print(f"Saved to DB: {result.get('saved_to_database')}")
        if result.get('saved_to_database'):
            print(f"Collection: {result.get('collection')}")
            print(f"Inserted: {result.get('import_result', {}).get('inserted', 0)}")
        
        # Show first few assets (full JSON output)
        data = result.get('data', [])
        num_to_show = min(3, len(data))
        
        print(f"\n{'='*90}")
        print(f"JSON OUTPUT PREVIEW (showing first {num_to_show} of {len(data)} assets)")
        print('='*90)
        
        for i in range(num_to_show):
            print(f"\n--- Asset {i+1} ---")
            print(json.dumps(data[i], indent=2, default=str))
        
        if len(data) > num_to_show:
            print(f"\n... and {len(data) - num_to_show} more assets")
        
        print(f"\n{'='*90}")
        print("✅ Import test completed successfully!")
        print('='*90)
        
        return True
    
    except Exception as e:
        print(f"\n❌ Exception during import:")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        importer.close()


async def main():
    """Run import test based on CLI arguments"""
    
    if len(sys.argv) < 2:
        print("\n  Error: No filename provided")
        print("\nUsage: python test_full_importer.py <filename> [row_limit] [--save]")
        print("\nExamples:")
        print("  python test_full_importer.py Power_Plants.csv")
        print("  python test_full_importer.py Power_Plants.zip")
        print("  python test_full_importer.py AHA_data.xlsx 100")
        print("  python test_full_importer.py Power_Plants.csv --save  (save to MongoDB)")
        print("\nNote: Files should be in the example_data/ directory")
        print("      Make sure MongoDB is running if using --save")
        sys.exit(1)
    
    filename = sys.argv[1]
    
    # Parse optional row limit
    row_limit = None
    save_to_db = False
    
    if len(sys.argv) > 2:
        for arg in sys.argv[2:]:
            if arg == '--save':
                save_to_db = True
            else:
                try:
                    row_limit = int(arg)
                except ValueError:
                    print(f"Warning: Invalid row limit '{arg}', ignoring")
    
    # Run test
    success = await test_full_import(filename, row_limit, save_to_db)
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
