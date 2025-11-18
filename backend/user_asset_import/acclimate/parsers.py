"""
File Parsers - Handle different file formats for asset import
Supports CSV, JSON, GeoJSON, and Shapefiles (ZIP)
"""

import logging
import io
import json
import zipfile
from typing import List, Dict, Tuple, Optional
import pandas as pd

logger = logging.getLogger(__name__)


def detect_file_format(filename: str, file_content: bytes) -> str:
    """
    Detect file format from filename and content
    
    Args:
        filename: Original filename
        file_content: Raw file bytes
    
    Returns:
        Format string: 'csv', 'json', 'geojson', 'shapefile', or 'unknown'
    """
    filename_lower = filename.lower()
    
    # Check extension first
    if filename_lower.endswith('.csv'):
        return 'csv'
    elif filename_lower.endswith('.json'):
        # Need to check if it's GeoJSON
        try:
            data = json.loads(file_content.decode('utf-8'))
            if 'type' in data and data['type'] in ['FeatureCollection', 'Feature']:
                return 'geojson'
            return 'json'
        except:
            return 'json'
    elif filename_lower.endswith('.geojson'):
        return 'geojson'
    elif filename_lower.endswith('.zip'):
        # Check if it contains shapefiles
        try:
            with zipfile.ZipFile(io.BytesIO(file_content)) as zf:
                files = zf.namelist()
                # Look for .shp file
                if any(f.endswith('.shp') for f in files):
                    return 'shapefile'
        except:
            pass
        return 'unknown'
    elif filename_lower.endswith(('.xlsx', '.xls')):
        return 'excel'
    
    return 'unknown'


def parse_csv(file_content: bytes, filename: str) -> Tuple[List[Dict], List[str]]:
    """
    Parse CSV file
    
    Args:
        file_content: Raw CSV bytes
        filename: Original filename
    
    Returns:
        Tuple of (list of row dicts, list of column names)
    """
    try:
        # Decode as UTF-8 (standard for modern data)
        try:
            text = file_content.decode('utf-8')
        except UnicodeDecodeError as e:
            raise ValueError(
                "CSV file is not UTF-8 encoded."
            )
        
        # Use pandas for robust CSV parsing
        df = pd.read_csv(
            io.StringIO(text),
            dtype=str,  # Read everything as strings initially
            na_filter=False  # Don't convert empty strings to NaN
        )
        
        # Convert to list of dicts
        rows = df.to_dict('records')
        columns = list(df.columns)
        
        # Clean up values - keep everything as strings (no type conversion)
        # Type handling will be done downstream where we know the field types
        for row in rows:
            for key, value in row.items():
                if value == '' or value is None:
                    row[key] = None
                elif isinstance(value, str):
                    row[key] = value.strip()
        
        logger.info(f"Parsed CSV: {len(rows)} rows, {len(columns)} columns")
        return rows, columns
        
    except Exception as e:
        logger.error(f"Error parsing CSV: {e}")
        raise ValueError(f"Failed to parse CSV file: {str(e)}")


def parse_json(file_content: bytes, filename: str) -> Tuple[List[Dict], List[str]]:
    """
    Parse JSON file (array of objects or single object)
    
    Args:
        file_content: Raw JSON bytes
        filename: Original filename
    
    Returns:
        Tuple of (list of row dicts, list of column names)
    """
    try:
        text = file_content.decode('utf-8')
        data = json.loads(text)
        
        # Handle different JSON structures
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            # Single object - wrap in list
            rows = [data]
        else:
            raise ValueError("JSON must be an array of objects or a single object")
        
        # Get all unique column names
        columns = set()
        for row in rows:
            if isinstance(row, dict):
                columns.update(row.keys())
            else:
                raise ValueError("JSON array must contain objects")
        
        columns = sorted(list(columns))
        
        logger.info(f"Parsed JSON: {len(rows)} rows, {len(columns)} columns")
        return rows, columns
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON: {e}")
        raise ValueError(f"Failed to parse JSON file: {str(e)}")
    except Exception as e:
        logger.error(f"Error parsing JSON: {e}")
        raise ValueError(f"Failed to parse JSON file: {str(e)}")


def parse_geojson(file_content: bytes, filename: str) -> Tuple[List[Dict], List[str]]:
    """
    Parse GeoJSON file (FeatureCollection or Feature)
    
    Args:
        file_content: Raw GeoJSON bytes
        filename: Original filename
    
    Returns:
        Tuple of (list of row dicts, list of column names)
    """
    try:
        text = file_content.decode('utf-8')
        data = json.loads(text)
        
        features = []
        
        # Handle FeatureCollection or single Feature
        if data.get('type') == 'FeatureCollection':
            features = data.get('features', [])
        elif data.get('type') == 'Feature':
            features = [data]
        else:
            raise ValueError("GeoJSON must be a FeatureCollection or Feature")
        
        # Extract properties and geometry
        rows = []
        columns = set()
        
        for feature in features:
            if not isinstance(feature, dict):
                continue
            
            row = {}
            
            # Get properties
            properties = feature.get('properties', {})
            if properties:
                row.update(properties)
                columns.update(properties.keys())
            
            # Extract coordinates from geometry
            geometry = feature.get('geometry', {})
            if geometry:
                geom_type = geometry.get('type')
                coordinates = geometry.get('coordinates')
                
                # Extract lat/lon based on geometry type
                if geom_type == 'Point' and coordinates:
                    # Point: [lon, lat] or [lon, lat, elevation]
                    row['longitude'] = coordinates[0]
                    row['latitude'] = coordinates[1]
                    columns.add('longitude')
                    columns.add('latitude')
                elif geom_type in ['MultiPoint', 'LineString', 'Polygon'] and coordinates:
                    # Use first coordinate
                    if geom_type == 'Polygon':
                        # Polygon coordinates are nested one more level
                        coords = coordinates[0][0] if coordinates and coordinates[0] else None
                    else:
                        coords = coordinates[0] if coordinates else None
                    
                    if coords:
                        row['longitude'] = coords[0]
                        row['latitude'] = coords[1]
                        columns.add('longitude')
                        columns.add('latitude')
                
                # Store full geometry in special_properties for reference
                row['_geometry'] = geometry
                columns.add('_geometry')
            
            if row:
                rows.append(row)
        
        columns = sorted(list(columns))
        
        logger.info(f"Parsed GeoJSON: {len(rows)} features, {len(columns)} columns")
        return rows, columns
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid GeoJSON: {e}")
        raise ValueError(f"Failed to parse GeoJSON file: {str(e)}")
    except Exception as e:
        logger.error(f"Error parsing GeoJSON: {e}")
        raise ValueError(f"Failed to parse GeoJSON file: {str(e)}")


def parse_shapefile(file_content: bytes, filename: str) -> Tuple[List[Dict], List[str]]:
    """
    Parse Shapefile from ZIP archive
    
    Args:
        file_content: Raw ZIP bytes containing shapefile
        filename: Original filename
    
    Returns:
        Tuple of (list of row dicts, list of column names)
    """
    try:
        import geopandas as gpd
        import tempfile
        import os
        
        # Extract ZIP to temporary directory
        with tempfile.TemporaryDirectory() as tmpdir:
            # Extract all files
            with zipfile.ZipFile(io.BytesIO(file_content)) as zf:
                zf.extractall(tmpdir)
            
            # Find .shp file
            shp_file = None
            for root, dirs, files in os.walk(tmpdir):
                for file in files:
                    if file.endswith('.shp'):
                        shp_file = os.path.join(root, file)
                        break
                if shp_file:
                    break
            
            if not shp_file:
                raise ValueError("No .shp file found in ZIP archive")
            
            # Read shapefile
            gdf = gpd.read_file(shp_file)
            
            # Convert to WGS84 if needed
            if gdf.crs and gdf.crs.to_epsg() != 4326:
                logger.info(f"Converting from {gdf.crs} to EPSG:4326 (WGS84)")
                gdf = gdf.to_crs(epsg=4326)
            
            # Extract coordinates
            gdf['longitude'] = gdf.geometry.x
            gdf['latitude'] = gdf.geometry.y
            
            # Convert to list of dicts
            # Drop geometry column but keep the coordinate columns
            gdf_no_geom = gdf.drop(columns=['geometry'])
            rows = gdf_no_geom.to_dict('records')
            columns = list(gdf_no_geom.columns)
            
            # Clean up values
            for row in rows:
                for key, value in list(row.items()):
                    # Handle None and NaN
                    if pd.isna(value):
                        row[key] = None
                    # Convert numpy types to Python types
                    elif hasattr(value, 'item'):
                        row[key] = value.item()
            
            logger.info(f"Parsed Shapefile: {len(rows)} features, {len(columns)} columns")
            return rows, columns
            
    except ImportError:
        raise ValueError("geopandas is required to parse shapefiles. Install with: pip install geopandas")
    except Exception as e:
        logger.error(f"Error parsing shapefile: {e}")
        raise ValueError(f"Failed to parse shapefile: {str(e)}")


def get_excel_sheet_names(file_content: bytes) -> List[str]:
    """
    Get list of sheet names from Excel file
    
    Args:
        file_content: Raw Excel bytes
    
    Returns:
        List of sheet names
    """
    try:
        excel_buffer = io.BytesIO(file_content)
        xl = pd.ExcelFile(excel_buffer)
        return xl.sheet_names
    except Exception as e:
        logger.error(f"Error reading Excel sheet names: {e}")
        raise ValueError(f"Failed to read Excel sheet names: {str(e)}")


def parse_excel(file_content: bytes, filename: str, row_limit: Optional[int] = None) -> Tuple[List[Dict], List[str]]:
    """
    Parse Excel file (.xlsx or .xls) - automatically handles single and multi-sheet
    
    - Single sheet: Parse normally like CSV
    - Multi-sheet: Combine all sheets, inject sheet name as component_type
    
    Args:
        file_content: Raw Excel bytes
        filename: Original filename
        row_limit: Optional limit on number of rows to read (per sheet for multi-sheet)
    
    Returns:
        Tuple of (list of row dicts, list of column names)
    """
    try:
        # Get sheet names to determine single vs multi-sheet
        sheet_names = get_excel_sheet_names(file_content)
        logger.info(f"Excel file has {len(sheet_names)} sheet(s)")
        
        if len(sheet_names) == 1:
            # Single sheet - parse like standard CSV
            excel_buffer = io.BytesIO(file_content)
            df = pd.read_excel(excel_buffer, dtype=str, na_filter=False, nrows=row_limit)
            
            # Convert to list of dicts
            rows = df.to_dict('records')
            columns = list(df.columns)
            
            # Clean up values - keep everything as strings (no type conversion)
            for row in rows:
                for key, value in row.items():
                    if value == '' or value is None:
                        row[key] = None
                    elif isinstance(value, str):
                        row[key] = value.strip()
            
            logger.info(f"Parsed single sheet: {len(rows)} rows, {len(columns)} columns")
            return rows, columns
        
        else:
            # Multi-sheet - combine all sheets with component_type from sheet name
            # Skip common non-data sheet names
            NON_DATA_SHEET_NAMES = {
                'summary', 'notes', 'readme', 'metadata', 'info', 'information',
                'legend', 'glossary', 'instructions'
            }
            
            all_rows = []
            all_columns = set()
            
            for sheet_name in sheet_names:
                # Skip non-data sheets
                if sheet_name.lower().strip() in NON_DATA_SHEET_NAMES:
                    logger.info(f"Skipping non-data sheet: '{sheet_name}'")
                    continue
                try:
                    excel_buffer = io.BytesIO(file_content)
                    df = pd.read_excel(
                        excel_buffer, 
                        sheet_name=sheet_name, 
                        dtype=str, 
                        na_filter=False, 
                        nrows=row_limit
                    )
                    
                    # Convert to list of dicts
                    sheet_rows = df.to_dict('records')
                    sheet_columns = list(df.columns)
                    
                    # Clean up values - keep everything as strings (no type conversion)
                    for row in sheet_rows:
                        for key, value in row.items():
                            if value == '' or value is None:
                                row[key] = None
                            elif isinstance(value, str):
                                row[key] = value.strip()
                        
                        # Inject component_type from sheet name
                        row['component_type'] = sheet_name
                    
                    # Add to combined data
                    all_rows.extend(sheet_rows)
                    all_columns.update(sheet_columns)
                    all_columns.add('component_type')
                    
                    logger.info(f"Parsed sheet '{sheet_name}': {len(sheet_rows)} rows")
                    
                except Exception as e:
                    logger.warning(f"Failed to parse sheet '{sheet_name}': {e}")
                    continue
            
            combined_columns = sorted(list(all_columns))
            
            logger.info(f"Combined multi-sheet Excel: {len(all_rows)} total rows, {len(combined_columns)} unique columns from {len(sheet_names)} sheets")
            
            return all_rows, combined_columns
            
    except Exception as e:
        logger.error(f"Error parsing Excel: {e}")
        raise ValueError(f"Failed to parse Excel file: {str(e)}")


def parse_file(file_content: bytes, filename: str) -> Tuple[List[Dict], List[str], str]:
    """
    Auto-detect and parse file based on format
    
    Args:
        file_content: Raw file bytes
        filename: Original filename
    
    Returns:
        Tuple of (list of row dicts, list of column names, detected format)
    """
    # Detect format
    file_format = detect_file_format(filename, file_content)
    
    logger.info(f"Detected format: {file_format} for file: {filename}")
    
    # Parse based on format
    if file_format == 'csv':
        rows, columns = parse_csv(file_content, filename)
    elif file_format == 'json':
        rows, columns = parse_json(file_content, filename)
    elif file_format == 'geojson':
        rows, columns = parse_geojson(file_content, filename)
    elif file_format == 'shapefile':
        rows, columns = parse_shapefile(file_content, filename)
    elif file_format == 'excel':
        rows, columns = parse_excel(file_content, filename)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    return rows, columns, file_format
