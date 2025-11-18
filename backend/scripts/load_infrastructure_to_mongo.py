#!/usr/bin/env python3
"""
Load Infrastructure JSON to MongoDB
Supports initial seeding and incremental updates without duplication

Usage:
    python load_infrastructure_to_mongo.py energy_grid.json energy_grid
    python load_infrastructure_to_mongo.py nuclear_facilities.json energy_grid
    
Arguments:
    json_file: Name of JSON file in backend/infrastructure_data/AHACoreData/
    collection_name: MongoDB collection name (e.g., 'energy_grid')
"""

import sys
import json
import asyncio
from pathlib import Path
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from database import mongo_uri

# Configuration
DB_NAME = "acclimate_db"
DATA_DIR = Path("backend/infrastructure_data/AHACoreData")

async def load_to_mongo(json_filename: str, collection_name: str):
    """Load infrastructure JSON into specified MongoDB collection"""
    
    json_file = DATA_DIR / json_filename
    
    print(f"📂 Loading: {json_file}")
    
    if not json_file.exists():
        print(f"❌ File not found: {json_file}")
        sys.exit(1)
    
    # Read JSON
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    metadata = data.get('metadata', {})
    asset_instances = data.get('asset_instances', [])
    component_library = data.get('component_library', [])
    
    print(f"📊 Found {len(asset_instances)} asset instances")
    print(f"📚 Found {len(component_library)} component types")
    
    # Connect to MongoDB
    print(f"🔌 Connecting to MongoDB: {mongo_uri}")
    client = AsyncIOMotorClient(mongo_uri)
    db = client[DB_NAME]
    
    # Asset instances collection
    assets_collection = db[collection_name]
    
    # Component library collection (shared across all infrastructure)
    components_collection = db['component_library']
    
    # Stats tracking
    stats = {
        'inserted': 0,
        'updated': 0,
        'skipped': 0,
        'components_added': 0
    }
    
    # Insert/Update asset instances (upsert by UUID to avoid duplicates)
    print(f"\n🔄 Upserting assets to '{collection_name}' collection...")
    
    for asset in asset_instances:
        result = await assets_collection.replace_one(
            {'uuid': asset['uuid']},  # Match by UUID
            {
                **asset,
                'last_updated': datetime.now().isoformat()
            },
            upsert=True
        )
        
        if result.upserted_id:
            stats['inserted'] += 1
        elif result.modified_count > 0:
            stats['updated'] += 1
        else:
            stats['skipped'] += 1
    
    # Insert/Update component library (shared reference)
    print(f"📚 Updating component library...")
    
    for component in component_library:
        result = await components_collection.replace_one(
            {'component_type': component['component_type']},
            {
                **component,
                'last_updated': datetime.now().isoformat()
            },
            upsert=True
        )
        
        if result.upserted_id or result.modified_count > 0:
            stats['components_added'] += 1
    
    # Store load metadata
    metadata_collection = db['load_metadata']
    await metadata_collection.insert_one({
        'collection': collection_name,
        'source_file': json_filename,
        'loaded_at': datetime.now().isoformat(),
        'source_metadata': metadata,
        'stats': stats
    })
    
    # Create indexes for performance
    print(f"🔍 Creating indexes...")
    await assets_collection.create_index('uuid', unique=True)
    await assets_collection.create_index('component_type')
    await assets_collection.create_index('location', name='geo_index')
    await assets_collection.create_index([('state', 1), ('county', 1)])
    
    # Print summary
    print(f"\n{'='*80}")
    print("LOAD SUMMARY")
    print(f"{'='*80}")
    print(f"Collection: {collection_name}")
    print(f"Source: {json_filename}")
    print(f"\n📊 Asset Instances:")
    print(f"   Inserted (new): {stats['inserted']}")
    print(f"   Updated (existing): {stats['updated']}")
    print(f"   Skipped (unchanged): {stats['skipped']}")
    print(f"   Total processed: {len(asset_instances)}")
    print(f"\n📚 Component Library:")
    print(f"   Added/Updated: {stats['components_added']}")
    print(f"\n✅ Load complete!")
    
    # Close connection
    client.close()


async def show_collection_stats(collection_name: str):
    """Show current collection statistics"""
    
    client = AsyncIOMotorClient(mongo_uri)
    db = client[DB_NAME]
    collection = db[collection_name]
    
    total_count = await collection.count_documents({})
    
    # Count by component type
    pipeline = [
        {'$group': {'_id': '$component_type', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 10}
    ]
    
    top_types = await collection.aggregate(pipeline).to_list(length=10)
    
    print(f"\n📊 Current '{collection_name}' Collection Stats:")
    print(f"   Total assets: {total_count}")
    print(f"\n   Top component types:")
    for item in top_types:
        print(f"     {item['_id']}: {item['count']}")
    
    client.close()


def main():
    """Main entry point"""
    
    if len(sys.argv) < 3:
        print("Usage: python load_infrastructure_to_mongo.py <json_file> <collection_name>")
        print("\nExamples:")
        print("  python load_infrastructure_to_mongo.py energy_grid.json energy_grid")
        print("  python load_infrastructure_to_mongo.py nuclear_facilities.json energy_grid")
        print("\nThis will upsert data (no duplicates) based on UUID matching.")
        sys.exit(1)
    
    json_filename = sys.argv[1]
    collection_name = sys.argv[2]
    
    print("🚀 Infrastructure to MongoDB Loader")
    print(f"{'='*80}\n")
    
    # Load the data
    asyncio.run(load_to_mongo(json_filename, collection_name))
    
    # Show updated stats
    asyncio.run(show_collection_stats(collection_name))


if __name__ == "__main__":
    main()