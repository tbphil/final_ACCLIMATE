"""
ACCLIMATE Importer - Main orchestrator for asset import process
Coordinates parsing, field mapping, component matching, validation, and persistence
"""

import logging
import uuid
from typing import Dict, List, Optional
from datetime import datetime

from .parsers import parse_file, detect_file_format
from .component_mapper import ComponentMapper
from . import config

logger = logging.getLogger(__name__)


class AssetImporter:
    """
    Orchestrates the asset import process for ACCLIMATE
    """
    
    def __init__(self, field_mapper_module, mongo_uri: Optional[str] = None, database: str = 'acclimate_db'):
        """
        Initialize importer
        
        Args:
            field_mapper_module: Module containing project-specific field mapping functions
                                 Must have: auto_map_fields, apply_mapping, validate_mapping
            mongo_uri: MongoDB connection string
            database: Database name
        """
        self.field_mapper = field_mapper_module
        self.component_mapper = ComponentMapper(mongo_uri, database)
    
    async def process_upload(
        self,
        file_content: bytes,
        filename: str,
        sector: str = config.DEFAULT_SECTOR,
        column_mappings: Optional[Dict[str, str]] = None,
        component_mappings: Optional[Dict[str, str]] = None,
        save_to_database: bool = config.SAVE_TO_DATABASE_DEFAULT,
        target_collection: str = 'energy_grid',
        row_limit: Optional[int] = None,
        bounding_box: Optional[Dict[str, float]] = None
    ) -> Dict:
        """
        Process uploaded file with optional user-provided mappings
        
        Args:
            file_content: Raw file bytes
            filename: Original filename
            sector: Sector for all assets in this upload (default: from config.DEFAULT_SECTOR)
            column_mappings: Optional user-provided column mappings
            component_mappings: Optional user-provided component type mappings
            save_to_database: If True, save data to MongoDB (default: from config.SAVE_TO_DATABASE_DEFAULT)
            target_collection: MongoDB collection to import to (only used if save_to_database=True)
            row_limit: Optional limit on number of rows to read (useful for testing with large files)
        
        Returns:
            Dictionary with processed data and mapping metadata
        """
        try:
            # Step 1: Parse file (parse_excel handles both single and multi-sheet automatically)
            logger.info(f"Step 1: Parsing file {filename}")
            rows, columns, file_format = parse_file(file_content, filename)
            
            if not rows:
                return {
                    'success': False,
                    'error': 'No data rows found in file'
                }
            
            logger.info(f"Parsed {len(rows)} rows with {len(columns)} columns")
            
            # Step 2: Auto-detect field mappings (always run)
            logger.info("Step 2: Field mapping")
            
            # Always start with auto-detection
            field_mapping = self.field_mapper.auto_map_fields(columns)
            logger.info(f"Auto-detected field mappings: {field_mapping}")
            
            # If user provided overrides, merge them in
            if column_mappings:
                logger.info(f"Applying user field mapping overrides: {column_mappings}")
                
                # Validate user's column names exist in the file
                for model_field, user_column in column_mappings.items():
                    if user_column not in columns:
                        return {
                            'success': False,
                            'error': f'Invalid column mapping: Column "{user_column}" not found in file',
                            'available_columns': list(columns)
                        }
                
                # Override auto-detected mappings with user's choices
                for model_field, user_column in column_mappings.items():
                    field_mapping[model_field] = {
                        'mapped_to': user_column,
                        'confidence': 1.0,
                        'match_type': 'user_provided'
                    }
                
                logger.info(f"Final field mappings (with user overrides): {field_mapping}")
            
            # Build field mapping metadata for response
            field_mapping_metadata = {}
            for model_field, mapping_info in field_mapping.items():
                if mapping_info:
                    field_mapping_metadata[model_field] = {
                        'mapped_from': mapping_info['mapped_to'],
                        'confidence': mapping_info['confidence'],
                        'match_type': mapping_info['match_type']
                    }
            
            # Step 3: Apply field mapping to transform rows
            logger.info("Step 3: Transforming rows")
            transformed_rows = []
            original_rows = []  # Keep original rows for ID tracking
            for row in rows:
                original_rows.append(row.copy())  # Store original before transformation
                transformed = self.field_mapper.apply_mapping(row, field_mapping)
                transformed_rows.append(transformed)
            
            # Step 3a: Filter by bounding box if provided (before expensive operations)
            if bounding_box:
                initial_count = len(transformed_rows)
                filtered_transformed = []
                filtered_original = []
                for i, row in enumerate(transformed_rows):
                    lat = row.get('latitude')
                    lon = row.get('longitude')
                    if (lat is not None and lon is not None):
                        try:
                            lat_float = float(lat)
                            lon_float = float(lon)
                            if (bounding_box['min_lat'] <= lat_float <= bounding_box['max_lat'] and
                                bounding_box['min_lon'] <= lon_float <= bounding_box['max_lon']):
                                filtered_transformed.append(row)
                                filtered_original.append(original_rows[i])
                        except (ValueError, TypeError):
                            pass  # Skip rows with invalid coordinates
                
                transformed_rows = filtered_transformed
                original_rows = filtered_original
                filtered_count = len(transformed_rows)
                logger.info(f"Filtered {initial_count} assets to {filtered_count} within bounding box (before enrichment)")
            
            # Step 3b: Validate required fields early (fail fast)
            logger.info("Step 3b: Validating required fields")
            is_valid, validation_errors = self.field_mapper.validate_required_fields(
                transformed_rows,
                field_mapping
            )
            
            if not is_valid:
                return {
                    'success': False,
                    'error': 'Validation failed',
                    'validation_errors': validation_errors
                }
            
            # Step 4: Component type matching (only on filtered data)
            logger.info("Step 4: Component type matching")
            
            # Get unique component types from data
            component_mapping_info = field_mapping.get('component_type')
            component_col = component_mapping_info['mapped_to'] if component_mapping_info else None
            unique_components = set()
            for row in rows:
                if component_col:
                    comp_val = row.get(component_col)
                    if comp_val:
                        unique_components.add(str(comp_val))
            
            logger.info(f"Found {len(unique_components)} unique component types")
            
            # Match components
            if component_mappings:
                # User provided component mappings
                component_matches = {}
                for user_val, canonical in component_mappings.items():
                    component_matches[user_val] = {
                        'matched': True,
                        'canonical_name': canonical,
                        'match_type': 'user_provided',
                        'confidence': 1.0
                    }
                logger.info("Using user-provided component mappings")
            else:
                # Auto-match components
                component_matches = await self.component_mapper.batch_map_components(
                    list(unique_components)
                )
            
            # Build component mapping metadata
            component_mapping_metadata = {}
            for user_val, match_result in component_matches.items():
                if match_result['matched']:
                    component_mapping_metadata[user_val] = {
                        'mapped_to': match_result['canonical_name'],
                        'confidence': match_result['confidence'],
                        'match_type': match_result['match_type'],
                        'alternatives': match_result.get('suggestions', [])
                    }
                else:
                    # For unmatched components, use "Unknown"
                    component_mapping_metadata[user_val] = {
                        'mapped_to': 'Unknown',
                        'confidence': 0.0,
                        'match_type': 'no_match',
                        'alternatives': match_result.get('suggestions', [])
                    }
                    # Update the match result so enrichment works
                    component_matches[user_val] = {
                        'matched': True,
                        'canonical_name': 'Unknown',
                        'match_type': 'no_match',
                        'confidence': 0.0,
                        'sector': 'Energy Grid'
                    }
            
            # Step 5: Enrich data with matched components and sector
            logger.info("Step 5: Enriching data")
            enriched_rows = await self._enrich_rows(
                transformed_rows,
                original_rows,
                field_mapping,
                component_matches,
                sector
            )
            
            # Step 6: Optionally save to MongoDB
            saved_to_database = False
            import_result = None
            
            if save_to_database:
                logger.info(f"Step 6: Saving {len(enriched_rows)} rows to {target_collection}")
                import_result = await self._import_to_mongo(
                    enriched_rows,
                    target_collection
                )
                saved_to_database = True
            else:
                logger.info(f"Step 6: Skipping database save (save_to_database=False)")
            
            return {
                'success': True,
                'data': enriched_rows,  # Return the actual data to client
                'metadata': {
                    'total_rows': len(rows),
                    'valid_rows': len(enriched_rows),
                    'file_format': file_format,
                    'filename': filename,
                    'field_mappings': field_mapping_metadata,
                    'component_mappings': component_mapping_metadata
                },
                'saved_to_database': saved_to_database,
                'collection': target_collection if saved_to_database else None,
                'import_result': import_result
            }
            
        except ValueError as e:
            logger.error(f"Value error during import: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error during import: {e}", exc_info=True)
            return {
                'success': False,
                'error': f"Unexpected error: {str(e)}"
            }
    
    def _parse_boolean(self, value) -> bool:
        """
        Parse various boolean representations to Python bool
        Handles: true/false, yes/no, 1/0, t/f, y/n (case-insensitive)
        
        Args:
            value: Value to parse as boolean
            
        Returns:
            Boolean value (defaults to False if cannot parse)
        """
        if value is None:
            return False
        
        # If already a boolean, return it
        if isinstance(value, bool):
            return value
        
        # Convert to string and normalize
        str_value = str(value).lower().strip()
        
        # True values
        if str_value in ('true', 'yes', '1', 't', 'y', 'on'):
            return True
        
        # False values
        if str_value in ('false', 'no', '0', 'f', 'n', 'off', ''):
            return False
        
        # Default to False for unparseable values
        logger.warning(f"Could not parse boolean value '{value}', defaulting to False")
        return False
    
    async def _enrich_rows(
        self,
        rows: List[Dict],
        original_rows: List[Dict],
        field_mapping: Dict[str, Optional[Dict]],
        component_matches: Dict[str, Dict],
        sector: str
    ) -> List[Dict]:
        """
        Enrich rows with auto-generated fields and matched components
        ACCLIMATE-specific: Uses flat address structure
        
        Args:
            rows: Transformed data rows
            original_rows: Original data rows before field mapping (for ID tracking)
            field_mapping: Field mapping with metadata from auto_map_fields()
            component_matches: Component match results
            sector: Sector to assign to all assets
        
        Returns:
            Enriched rows with aliases tracking all transformations
        """
        enriched = []
        
        for idx, row in enumerate(rows):
            original_row = original_rows[idx]
            enriched_row = row.copy()
            
            # Initialize aliases dict to track all transformations
            field_aliases = {}
            component_aliases = {}
            
            # Track field mappings (column name changes)
            for field_name, mapping_info in field_mapping.items():
                if mapping_info:
                    user_column = mapping_info['mapped_to']
                    if user_column != field_name:
                        field_aliases[field_name] = user_column
            
            # Store user's original ID if present (we always generate new)
            # For multi-sheet Excel, different sheets may have different ID column names
            # We need to find which column in THIS row actually contains an ID
            user_provided_id = None
            user_id_column_name = None
            
            # Check the transformed row first
            if row.get('_id'):
                user_provided_id = row.get('_id')
                # Find which original column had this value
                for col_name, col_value in original_row.items():
                    if col_value == user_provided_id:
                        user_id_column_name = col_name
                        break
            else:
                # Transformed row doesn't have _id, so check original row for any ID-like column
                # Common ID column names in order of preference
                id_column_candidates = ['eia_plant_id', 'id', '_id', 'uuid', 'asset_id', 'facility_id', 'objectid', 'rec_id']
                for col_candidate in id_column_candidates:
                    if col_candidate in original_row:
                        value = original_row[col_candidate]
                        if value is not None and value != '':
                            user_provided_id = value
                            user_id_column_name = col_candidate
                            break
            
            # Update alias to reflect the actual column name from THIS row
            if user_id_column_name and user_id_column_name != '_id':
                field_aliases['_id'] = user_id_column_name
            
            # Generate new UUID for all assets to avoid collisions
            new_id = str(uuid.uuid4())
            enriched_row['_id'] = new_id
            
            # Store user's original ID value in spec_overrides using THEIR column name
            # This preserves both the column name and value (e.g., "OBJECTID": 1234 or "eia_plant_id": 5678)
            # This way we can trace back: aliases shows "_id": "OBJECTID", spec_overrides shows "OBJECTID": 1234
            if user_provided_id is not None and user_provided_id != '':
                if 'spec_overrides' not in enriched_row:
                    enriched_row['spec_overrides'] = {}
                # Use their actual column name
                enriched_row['spec_overrides'][user_id_column_name] = user_provided_id
            
            # Add ALL unmapped fields to spec_overrides (preserve user's custom data)
            # Get list of columns that were mapped to known fields
            mapped_columns = set()
            for field_name, mapping_info in field_mapping.items():
                if mapping_info:
                    mapped_columns.add(mapping_info['mapped_to'])
            
            # Find unmapped columns from original row
            for col_name, col_value in original_row.items():
                if col_name not in mapped_columns:
                    # This field wasn't mapped to anything - preserve it
                    # Exclude empty/null values
                    if col_value is not None and col_value != '':
                        if 'spec_overrides' not in enriched_row:
                            enriched_row['spec_overrides'] = {}
                        # Only add if not already present (e.g., from ID handling above)
                        if col_name not in enriched_row['spec_overrides']:
                            enriched_row['spec_overrides'][col_name] = col_value
            
            # Parse Critical field to boolean if present (already capitalized by field_mapper)
            if 'Critical' in row:
                critical_value = row['Critical']
                enriched_row['Critical'] = self._parse_boolean(critical_value)
            
            # Get user's component type value (before matching)
            user_component_type = row.get('component_type')
            
            if user_component_type and user_component_type in component_matches:
                match = component_matches[user_component_type]
                
                if match['matched']:
                    canonical_component_type = match['canonical_name']
                    match_type = match.get('match_type', 'exact')
                    
                    # Set canonical component type for system use
                    enriched_row['component_type'] = canonical_component_type
                    
                    # Track component type mapping in component_aliases for ALL non-exact matches
                    # This includes: alias, fuzzy, mapped variations, etc.
                    # Only skip if it's an exact canonical name match (case-insensitive)
                    is_exact_match = (match_type == 'exact' and 
                                     str(user_component_type).lower().strip() == str(canonical_component_type).lower().strip())
                    
                    if not is_exact_match:
                        component_aliases[canonical_component_type] = str(user_component_type)
            
            # Set sector from parameter (user-selected during upload)
            enriched_row['sector'] = sector
            
            # Add aliases to enriched row with nested structure
            aliases = {}
            if field_aliases:
                aliases['field'] = field_aliases
            if component_aliases:
                aliases['component'] = component_aliases
            
            if aliases:
                enriched_row['aliases'] = aliases
            
            # Generate name if not present (component_type_fullID format)
            if 'name' not in enriched_row or not enriched_row['name']:
                component = enriched_row.get('component_type', 'Asset')
                asset_id = enriched_row.get('_id', '')
                enriched_row['name'] = f"{component}_{asset_id}"
            
            # Add import metadata
            enriched_row['imported_at'] = datetime.now().isoformat()
            enriched_row['import_source'] = 'user_upload'
            
            # Ensure coordinates are floats (field_mapper uses lowercase)
            if 'latitude' in enriched_row and enriched_row['latitude']:
                try:
                    enriched_row['latitude'] = float(enriched_row['latitude'])
                except (ValueError, TypeError) as e:
                    logger.error(f"Could not convert latitude to float: {enriched_row['latitude']}")
                    raise ValueError(f"Invalid latitude value: {enriched_row['latitude']}")
            
            if 'longitude' in enriched_row and enriched_row['longitude']:
                try:
                    enriched_row['longitude'] = float(enriched_row['longitude'])
                except (ValueError, TypeError) as e:
                    logger.error(f"Could not convert longitude to float: {enriched_row['longitude']}")
                    raise ValueError(f"Invalid longitude value: {enriched_row['longitude']}")
            
            # Create both coordinates array and GeoJSON location object
            if 'latitude' in enriched_row and 'longitude' in enriched_row:
                # Top-level coordinates array [longitude, latitude]
                enriched_row['coordinates'] = [
                    enriched_row['longitude'],
                    enriched_row['latitude']
                ]
                # GeoJSON location object for MongoDB geospatial queries
                enriched_row['location'] = {
                    'type': 'Point',
                    'coordinates': [
                        enriched_row['longitude'],
                        enriched_row['latitude']
                    ]
                }
            
            # ACCLIMATE-specific: Keep address fields flat at main level
            # Ensure address fields have empty string defaults if not present (already capitalized by field_mapper)
            address_fields = ['Address', 'City', 'State', 'Zip', 'County', 'Country']
            for field in address_fields:
                if field not in enriched_row:
                    enriched_row[field] = ""
            
            # Build final output in specific order for consistency
            ordered_row = {}
            
            # 1. Core identification fields
            ordered_row['_id'] = enriched_row['_id']
            ordered_row['id'] = enriched_row['_id']  # Map expects 'id', not '_id'
            ordered_row['uuid'] = enriched_row['_id']  # Also add uuid for compatibility
            ordered_row['component_type'] = enriched_row.get('component_type', '')
            ordered_row['facilityTypeName'] = enriched_row.get('component_type', '')  # Map expects this
            ordered_row['name'] = enriched_row.get('name', '')
            
            # 2. Sector
            ordered_row['sector'] = enriched_row.get('sector', '')
            
            # 3. Geographic coordinates (lowercase - matches infrastructure system)
            ordered_row['latitude'] = enriched_row.get('latitude')
            ordered_row['longitude'] = enriched_row.get('longitude')
            
            # 4. Address fields (capitalized)
            ordered_row['City'] = enriched_row.get('City', '')
            ordered_row['State'] = enriched_row.get('State', '')
            ordered_row['County'] = enriched_row.get('County', '')
            ordered_row['Country'] = enriched_row.get('Country', '')
            ordered_row['Address'] = enriched_row.get('Address', '')
            
            # Convert Zip - remove .0 suffix but PRESERVE leading zeros (keep as string)
            # Example: "01234.0" → "01234" (NOT 1234)
            zip_value = enriched_row.get('Zip', '')
            if zip_value and str(zip_value).strip():
                str_zip = str(zip_value).strip()
                # Remove trailing .0 if present (from Excel float conversion)
                if str_zip.endswith('.0'):
                    str_zip = str_zip[:-2]
                ordered_row['Zip'] = str_zip
            else:
                ordered_row['Zip'] = ''
            
            # 5. Coordinate arrays and location objects
            ordered_row['coordinates'] = enriched_row.get('coordinates', [])
            ordered_row['location'] = enriched_row.get('location', {})
            
            # 6. Critical field if present
            if 'Critical' in enriched_row:
                ordered_row['Critical'] = enriched_row['Critical']
            
            # 7. Optional fields (lowercase) - convert to appropriate types
            if 'lines' in enriched_row and enriched_row['lines']:
                try:
                    # Convert to int (handles "3.0" → 3)
                    ordered_row['lines'] = int(float(enriched_row['lines']))
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert lines to int: {enriched_row['lines']}")
                    ordered_row['lines'] = enriched_row['lines']
            
            if 'min_voltage' in enriched_row and enriched_row['min_voltage']:
                try:
                    ordered_row['min_voltage'] = float(enriched_row['min_voltage'])
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert min_voltage to float: {enriched_row['min_voltage']}")
                    ordered_row['min_voltage'] = enriched_row['min_voltage']
            
            if 'max_voltage' in enriched_row and enriched_row['max_voltage']:
                try:
                    ordered_row['max_voltage'] = float(enriched_row['max_voltage'])
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert max_voltage to float: {enriched_row['max_voltage']}")
                    ordered_row['max_voltage'] = enriched_row['max_voltage']
            
            # 8. Aliases
            if 'aliases' in enriched_row:
                ordered_row['aliases'] = enriched_row['aliases']
            
            # 9. Spec overrides (all unmapped fields + original ID if present)
            if 'spec_overrides' in enriched_row:
                ordered_row['spec_overrides'] = enriched_row['spec_overrides']
            
            # 10. Import metadata
            ordered_row['imported_at'] = enriched_row.get('imported_at')
            ordered_row['import_source'] = enriched_row.get('import_source', 'user_upload')
            
            enriched.append(ordered_row)
        
        return enriched
    
    async def _import_to_mongo(
        self,
        rows: List[Dict],
        collection_name: str
    ) -> Dict:
        """
        Import enriched rows to MongoDB
        
        Args:
            rows: Enriched and validated rows
            collection_name: Target collection
        
        Returns:
            Import statistics
        """
        from motor.motor_asyncio import AsyncIOMotorClient
        import os
        
        mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
        client = AsyncIOMotorClient(mongo_uri)
        db = client['acclimate_db']
        collection = db[collection_name]
        
        try:
            # Insert documents
            if rows:
                result = await collection.insert_many(rows)
                inserted_count = len(result.inserted_ids)
                
                # Create indexes
                await collection.create_index('_id')
                await collection.create_index('component_type')
                await collection.create_index('location', name='geo_index')
                
                logger.info(f"Inserted {inserted_count} documents to {collection_name}")
                
                return {
                    'inserted': inserted_count,
                    'collection': collection_name
                }
            else:
                return {
                    'inserted': 0,
                    'collection': collection_name
                }
                
        finally:
            client.close()
    
    def close(self):
        """Close component mapper connection"""
        self.component_mapper.close()
