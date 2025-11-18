#!/usr/bin/env python3
"""
Load HBOM Baseline JSON to MongoDB
Upserts nodes by UUID to preserve existing canonical_component_type fields

Usage:
    python load_hbom_baseline.py
"""

import asyncio
import json
from pathlib import Path
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from database import mongo_uri

DB_NAME = "acclimate_db"
BASELINE_FILE = Path("backend/fragility_data/processed/hbom_baseline.json")


async def load_baseline():
    """Load HBOM baseline JSON into MongoDB"""
    
    print("=" * 80)
    print("HBOM BASELINE LOADER")
    print("=" * 80)
    
    # 1. Load JSON
    print(f"\n[1/3] Loading {BASELINE_FILE}...")
    
    if not BASELINE_FILE.exists():
        print(f"ERROR: File not found: {BASELINE_FILE}")
        print("Run generate_hbom_baseline.py first")
        return
    
    with open(BASELINE_FILE, 'r') as f:
        data = json.load(f)
    
    nodes = data.get('nodes', [])
    metadata = data.get('metadata', {})
    
    print(f"      Loaded {len(nodes)} nodes from JSON")
    print(f"      Created: {metadata.get('created_date')}")
    
    # 2. Connect to MongoDB
    client = AsyncIOMotorClient(mongo_uri)
    db = client[DB_NAME]
    baseline = db['hbom_baseline']
    
    # 3. Upsert nodes (preserves existing fields like canonical_component_type)
    print(f"\n[2/3] Upserting {len(nodes)} nodes to MongoDB...")
    
    stats = {"inserted": 0, "updated": 0, "preserved_canonical": 0}
    
    for node in nodes:
        # Check if node already exists
        existing = await baseline.find_one({"uuid": node['uuid']})
        
        # Preserve canonical_component_type if it exists
        if existing and 'canonical_component_type' in existing:
            node['canonical_component_type'] = existing['canonical_component_type']
            stats['preserved_canonical'] += 1
        
        # Add load timestamp
        node['loaded_at'] = datetime.now().isoformat()
        node['loaded_by'] = 'hbom_baseline_loader'
        
        # Upsert by UUID
        result = await baseline.replace_one(
            {"uuid": node['uuid']},
            node,
            upsert=True
        )
        
        if result.upserted_id:
            stats['inserted'] += 1
        else:
            stats['updated'] += 1
    
    print(f"      Inserted (new): {stats['inserted']}")
    print(f"      Updated (existing): {stats['updated']}")
    print(f"      Preserved canonical links: {stats['preserved_canonical']}")
    
    # 4. Create indexes
    print(f"\n[3/3] Creating indexes...")
    await baseline.create_index("uuid", unique=True)
    await baseline.create_index("parent_uuid")
    await baseline.create_index("canonical_component_type")
    await baseline.create_index("asset_type")
    await baseline.create_index("label")
    print("      Indexes created")
    
    # Summary
    print("\n" + "=" * 80)
    print("LOAD SUMMARY")
    print("=" * 80)
    print(f"  Total nodes processed: {len(nodes)}")
    print(f"  New nodes added: {stats['inserted']}")
    print(f"  Existing nodes updated: {stats['updated']}")
    print(f"  Canonical links preserved: {stats['preserved_canonical']}")
    
    # Count current total
    total = await baseline.count_documents({})
    print(f"\n  Total nodes in hbom_baseline: {total}")
    
    print("\n" + "=" * 80)
    
    client.close()


if __name__ == "__main__":
    asyncio.run(load_baseline())