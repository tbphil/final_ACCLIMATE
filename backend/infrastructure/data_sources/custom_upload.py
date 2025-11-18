"""
Custom Upload Data Source
Handles user-uploaded CSV/Excel files for infrastructure data
Supports in-memory processing, cache persistence, and optional MongoDB persistence
"""

import logging
import io
from typing import List, Dict, Any, Optional
import polars as pl
import pandas as pd
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from database import mongo_uri
from .base import InfrastructureDataSource, BoundingBox

logger = logging.getLogger(__name__)


class CustomUploadSource(InfrastructureDataSource):
    """
    In-memory data source for user-uploaded infrastructure files
    
    Supports CSV and Excel (XLSX) formats
    Filters by bounding box in-memory using Polars for performance
    Can persist to cache or MongoDB for later sessions
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize custom upload source
        
        Args:
            config: Optional config with:
                - cache_manager: CacheManager instance for persistence
                - mongo_uri: MongoDB connection string (if persisting to DB)
                - database: MongoDB database name
                - persist_to_mongo: Whether to save uploads to MongoDB
        """
        super().__init__(config)
        
        # In-memory storage for uploaded data
        self._data: Optional[pl.DataFrame] = None
        self._source_filename: Optional[str] = None
        self._upload_id: Optional[str] = None
        
        # Cache manager for persistence
        self.cache_manager = self.config.get('cache_manager')
        
        # MongoDB settings (optional)
        self.persist_to_mongo = self.config.get('persist_to_mongo', False)
        self.mongo_uri = self.config.get('mongo_uri', mongo_uri)
        self.database_name = self.config.get('database', 'acclimate_db')
        self.custom_collection_name = 'custom_uploads'
        
        # Initialize MongoDB if persistence enabled
        if self.persist_to_mongo:
            self.client = AsyncIOMotorClient(self.mongo_uri)
            self.db = self.client[self.database_name]
            self.collection = self.db[self.custom_collection_name]
        else:
            self.client = None
        
        logger.info(f"Initialized CustomUpload source (persist_to_mongo={self.persist_to_mongo})")
    
    @property
    def source_name(self) -> str:
        if self._source_filename:
            return f"Custom Upload: {self._source_filename}"
        return "Custom Upload"
    
    @property
    def supports_realtime_updates(self) -> bool:
        return self.persist_to_mongo  # True if persisted to MongoDB
    
    async def validate_connection(self) -> bool:
        """Check if data is loaded"""
        is_valid = self._data is not None and len(self._data) > 0
        logger.info(f"Custom upload validation: {is_valid}")
        return is_valid
    
    async def load_file(
        self,
        file_content: bytes,
        filename: str,
        required_columns: Optional[List[str]] = None,
        persist: bool = True,
        upload_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Load and validate uploaded file
        
        Args:
            file_content: Raw file bytes
            filename: Original filename (for format detection)
            required_columns: Columns that must be present
            persist: Whether to persist to cache/MongoDB
            upload_id: Optional ID for this upload (generated if not provided)
        
        Returns:
            Dictionary with load results (success, errors, row_count, upload_id, etc.)
        """
        try:
            logger.info(f"Loading custom file: {filename}")
            
            # Generate upload ID if not provided
            if upload_id is None:
                upload_id = f"upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}"
            
            self._upload_id = upload_id
            
            # Detect file format
            if filename.lower().endswith('.csv'):
                df = self._load_csv(file_content)
            elif filename.lower().endswith(('.xlsx', '.xls')):
                df = self._load_excel(file_content)
            else:
                return {
                    'success': False,
                    'error': f"Unsupported file format. Please upload CSV or XLSX files."
                }
            
            # Validate required columns
            required_cols = required_columns or ['latitude', 'longitude', 'name']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                return {
                    'success': False,
                    'error': f"Missing required columns: {', '.join(missing_cols)}",
                    'found_columns': df.columns,
                    'required_columns': required_cols
                }
            
            # Validate coordinates
            valid_coords = (
                (df['latitude'] >= -90) & (df['latitude'] <= 90) &
                (df['longitude'] >= -180) & (df['longitude'] <= 180)
            )
            
            invalid_count = (~valid_coords).sum()
            
            if invalid_count > 0:
                logger.warning(f"Found {invalid_count} rows with invalid coordinates")
                df = df.filter(valid_coords)
            
            # Store in memory
            self._data = df
            self._source_filename = filename
            
            # Persist if requested
            persisted_to = []
            
            if persist:
                # Always persist to cache if available
                if self.cache_manager:
                    try:
                        cache_key = ('custom_upload', upload_id)
                        cache_data = {
                            'dataframe': df.to_dicts(),
                            'filename': filename,
                            'upload_id': upload_id,
                            'uploaded_at': datetime.now().isoformat(),
                            'row_count': len(df),
                            'columns': df.columns
                        }
                        self.cache_manager.set('custom_upload', cache_key, cache_data)
                        persisted_to.append('cache')
                        logger.info(f"Persisted upload {upload_id} to cache")
                    except Exception as e:
                        logger.error(f"Failed to persist to cache: {e}")
                
                # Optionally persist to MongoDB
                if self.persist_to_mongo:
                    try:
                        await self._persist_to_mongodb(df, filename, upload_id)
                        persisted_to.append('mongodb')
                        logger.info(f"Persisted upload {upload_id} to MongoDB")
                    except Exception as e:
                        logger.error(f"Failed to persist to MongoDB: {e}")
            
            result = {
                'success': True,
                'upload_id': upload_id,
                'filename': filename,
                'total_rows': len(df),
                'invalid_coords_removed': invalid_count,
                'columns': df.columns,
                'preview': df.head(5).to_dicts(),
                'persisted_to': persisted_to
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error loading file {filename}: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def load_from_cache(self, upload_id: str) -> bool:
        """
        Load previously uploaded data from cache
        
        Args:
            upload_id: ID of the upload to retrieve
        
        Returns:
            True if successfully loaded, False otherwise
        """
        if not self.cache_manager:
            logger.warning("No cache manager available")
            return False
        
        try:
            cache_key = ('custom_upload', upload_id)
            cached_data = self.cache_manager.get('custom_upload', cache_key)
            
            if cached_data:
                # Reconstruct DataFrame
                self._data = pl.DataFrame(cached_data['dataframe'])
                self._source_filename = cached_data['filename']
                self._upload_id = upload_id
                
                logger.info(f"Loaded upload {upload_id} from cache ({len(self._data)} rows)")
                return True
            else:
                logger.warning(f"Upload {upload_id} not found in cache")
                return False
                
        except Exception as e:
            logger.error(f"Error loading from cache: {e}")
            return False
    
    async def load_from_mongodb(self, upload_id: str) -> bool:
        """
        Load previously uploaded data from MongoDB
        
        Args:
            upload_id: ID of the upload to retrieve
        
        Returns:
            True if successfully loaded, False otherwise
        """
        if not self.persist_to_mongo or not self.client:
            logger.warning("MongoDB persistence not enabled")
            return False
        
        try:
            # Query for assets with this upload_id
            cursor = self.collection.find({'upload_id': upload_id})
            assets = await cursor.to_list(length=None)
            
            if not assets:
                logger.warning(f"Upload {upload_id} not found in MongoDB")
                return False
            
            # Convert to DataFrame
            # Clean MongoDB _id fields
            for asset in assets:
                if '_id' in asset:
                    del asset['_id']
            
            self._data = pl.DataFrame(assets)
            self._upload_id = upload_id
            self._source_filename = assets[0].get('source_filename', upload_id)
            
            logger.info(f"Loaded upload {upload_id} from MongoDB ({len(assets)} rows)")
            return True
            
        except Exception as e:
            logger.error(f"Error loading from MongoDB: {e}")
            return False
    
    async def _persist_to_mongodb(
        self,
        df: pl.DataFrame,
        filename: str,
        upload_id: str
    ):
        """
        Persist uploaded data to MongoDB
        
        Args:
            df: Polars DataFrame to persist
            filename: Original filename
            upload_id: Upload identifier
        """
        # Convert to list of dicts
        records = df.to_dicts()
        
        # Add metadata to each record
        for record in records:
            record['upload_id'] = upload_id
            record['source_filename'] = filename
            record['uploaded_at'] = datetime.now().isoformat()
            
            # Create GeoJSON location for geospatial queries
            if 'latitude' in record and 'longitude' in record:
                record['location'] = {
                    'type': 'Point',
                    'coordinates': [record['longitude'], record['latitude']]
                }
        
        # Delete existing records with this upload_id (replace)
        await self.collection.delete_many({'upload_id': upload_id})
        
        # Insert new records
        if records:
            await self.collection.insert_many(records)
        
        # Create indexes
        await self.collection.create_index('upload_id')
        await self.collection.create_index('location', name='geo_index')
        
        logger.info(f"Persisted {len(records)} records to MongoDB")
    
    def _load_csv(self, file_content: bytes) -> pl.DataFrame:
        """Load CSV file using Polars"""
        try:
            # Try UTF-8 first
            text = file_content.decode('utf-8')
        except UnicodeDecodeError:
            # Fallback to latin-1
            text = file_content.decode('latin-1')
        
        df = pl.read_csv(
            io.StringIO(text),
            infer_schema_length=1000,
            ignore_errors=True
        )
        
        logger.info(f"Loaded CSV: {len(df)} rows, {len(df.columns)} columns")
        return df
    
    def _load_excel(self, file_content: bytes) -> pl.DataFrame:
        """Load Excel file using Pandas then convert to Polars"""
        # Use pandas for Excel (better support), then convert to Polars
        excel_buffer = io.BytesIO(file_content)
        
        # Read first sheet
        pdf = pd.read_excel(excel_buffer, engine='openpyxl')
        
        # Convert to Polars
        df = pl.from_pandas(pdf)
        
        logger.info(f"Loaded Excel: {len(df)} rows, {len(df.columns)} columns")
        return df
    
    async def fetch(
        self,
        bbox: BoundingBox,
        sector: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Filter in-memory data by bounding box
        
        If no data in memory but persist_to_mongo=True, tries MongoDB query
        
        Args:
            bbox: Geographic bounding box
            sector: Infrastructure sector (not used for custom uploads)
            filters: Additional column filters
        
        Returns:
            List of matching asset dictionaries
        """
        # If no in-memory data but we have MongoDB, try querying there
        if self._data is None and self.persist_to_mongo and self._upload_id:
            return await self._fetch_from_mongodb(bbox, filters)
        
        if self._data is None:
            logger.warning("No data loaded for custom upload")
            return []
        
        try:
            # Apply bounding box filter
            filtered = self._data.filter(
                (pl.col('latitude') >= bbox.min_lat) &
                (pl.col('latitude') <= bbox.max_lat) &
                (pl.col('longitude') >= bbox.min_lon) &
                (pl.col('longitude') <= bbox.max_lon)
            )
            
            # Apply additional filters if provided
            if filters:
                for column, value in filters.items():
                    if column in filtered.columns:
                        filtered = filtered.filter(pl.col(column) == value)
            
            logger.info(f"Custom upload filtered: {len(filtered)} assets in bbox")
            
            # Convert to list of dicts
            return filtered.to_dicts()
            
        except Exception as e:
            logger.error(f"Error filtering custom upload data: {e}")
            raise
    
    async def _fetch_from_mongodb(
        self,
        bbox: BoundingBox,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch from MongoDB when in-memory data not available"""
        try:
            query = {
                'upload_id': self._upload_id,
                'location': {
                    '$geoWithin': {
                        '$box': [
                            [bbox.min_lon, bbox.min_lat],
                            [bbox.max_lon, bbox.max_lat]
                        ]
                    }
                }
            }
            
            if filters:
                query.update(filters)
            
            cursor = self.collection.find(query)
            assets = await cursor.to_list(length=None)
            
            # Clean MongoDB _id
            for asset in assets:
                if '_id' in asset:
                    asset['_id'] = str(asset['_id'])
            
            logger.info(f"Fetched {len(assets)} assets from MongoDB")
            return assets
            
        except Exception as e:
            logger.error(f"Error fetching from MongoDB: {e}")
            raise
    
    async def get_stats(self, sector: str) -> Dict[str, Any]:
        """Get statistics about loaded data"""
        if self._data is None:
            return {
                'source': self.source_name,
                'loaded': False,
                'total_assets': 0
            }
        
        try:
            stats = {
                'source': self.source_name,
                'loaded': True,
                'upload_id': self._upload_id,
                'filename': self._source_filename,
                'total_assets': len(self._data),
                'columns': self._data.columns,
                'geographic_bounds': {
                    'min_lat': float(self._data['latitude'].min()),
                    'max_lat': float(self._data['latitude'].max()),
                    'min_lon': float(self._data['longitude'].min()),
                    'max_lon': float(self._data['longitude'].max())
                }
            }
            
            # Component type distribution if column exists
            if 'component_type' in self._data.columns:
                type_counts = self._data.group_by('component_type').count()
                stats['component_types'] = {
                    row['component_type']: row['count']
                    for row in type_counts.to_dicts()
                }
            
            # State distribution if column exists
            if 'state' in self._data.columns:
                state_counts = self._data.group_by('state').count()
                stats['states'] = {
                    row['state']: row['count']
                    for row in state_counts.to_dicts()
                }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting custom upload stats: {e}")
            raise
    
    def clear(self):
        """Clear loaded data from memory (does not affect persisted data)"""
        self._data = None
        self._source_filename = None
        self._upload_id = None
        logger.info("Custom upload data cleared from memory")
    
    async def close(self):
        """Close MongoDB connection if open"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")