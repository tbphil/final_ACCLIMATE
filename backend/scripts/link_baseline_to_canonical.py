#!/usr/bin/env python3
"""
Link HBOM Baseline to Canonical Registry

Adds canonical_component_type field to all hbom_baseline nodes.
Root nodes get it from matching canonical registry.
Child nodes inherit from their root.

Usage:
    python link_baseline_to_canonical.py
"""

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from database import mongo_uri

DB_NAME = "acclimate_db"


async def link_baseline():
    """Add canonical_component_type to all hbom_baseline nodes"""
    
    client = AsyncIOMotorClient(mongo_uri)
    db = client[DB_NAME]
    baseline = db['hbom_baseline']
    comp_lib = db['component_library']
    
    print("=" * 80)
    print("LINKING HBOM BASELINE TO CANONICAL REGISTRY")
    print("=" * 80)
    
    # 1. Build mapping: label → canonical_component_type
    print("\n[1/3] Building canonical mapping from component_library...")
    
    cursor = comp_lib.find({"canonical_uuid": {"$exists": True}})
    canonical_entries = await cursor.to_list(length=None)
    
    # Map: canonical_name → component_type
    canonical_map = {
        entry['canonical_name']: entry['component_type']
        for entry in canonical_entries
    }
    
    print(f"      Found {len(canonical_map)} canonical components")
    for name in sorted(canonical_map.keys())[:5]:
        print(f"        {name}")
    if len(canonical_map) > 5:
        print(f"        ... and {len(canonical_map) - 5} more")
    
    # 2. Update root nodes
    print("\n[2/3] Updating root nodes with canonical_component_type...")
    
    cursor = baseline.find({"parent_uuid": None})
    roots = await cursor.to_list(length=None)
    
    root_stats = {"updated": 0, "skipped": 0}
    root_mapping = {}  # uuid → canonical_component_type
    
    for root in roots:
        label = root['label']
        
        if label in canonical_map:
            canonical_type = canonical_map[label]
            
            # Update this root node
            await baseline.update_one(
                {"uuid": root['uuid']},
                {
                    "$set": {
                        "canonical_component_type": canonical_type,
                        "updated_at": datetime.now().isoformat()
                    }
                }
            )
            
            # Track for child propagation
            root_mapping[root['uuid']] = canonical_type
            
            print(f"  {label} → canonical_type: {canonical_type}")
            root_stats["updated"] += 1
        else:
            print(f"  SKIP: {label} (not in canonical registry)")
            root_stats["skipped"] += 1
    
    print(f"\n  Updated {root_stats['updated']} roots")
    print(f"  Skipped {root_stats['skipped']} roots (not canonical)")
    
    # 3. Update all child nodes (inherit from root)
    print("\n[3/3] Propagating canonical_component_type to child nodes...")
    
    child_stats = {"updated": 0, "skipped": 0}
    
    for root_uuid, canonical_type in root_mapping.items():
        # Get all descendants of this root
        descendants = await get_all_descendants(baseline, root_uuid)
        
        if descendants:
            # Update all descendants with inherited canonical_component_type
            result = await baseline.update_many(
                {"uuid": {"$in": descendants}},
                {
                    "$set": {
                        "canonical_component_type": canonical_type,
                        "updated_at": datetime.now().isoformat()
                    }
                }
            )
            
            child_stats["updated"] += result.modified_count
    
    print(f"  Updated {child_stats['updated']} child nodes")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"  Root nodes updated: {root_stats['updated']}")
    print(f"  Child nodes updated: {child_stats['updated']}")
    print(f"  Total updated: {root_stats['updated'] + child_stats['updated']}")
    print(f"  Skipped (not canonical): {root_stats['skipped']}")
    
    print("\n" + "=" * 80)
    print("CANONICAL LINKING COMPLETE")
    print("=" * 80)
    print("\nAll hbom_baseline nodes now have canonical_component_type field.")
    print("This links them to the canonical component registry.")
    print("\nNext: Test with a query to verify the links work.")
    
    client.close()


async def get_all_descendants(collection, parent_uuid: str) -> list:
    """Recursively get all descendant UUIDs"""
    descendants = []
    
    cursor = collection.find({"parent_uuid": parent_uuid})
    children = await cursor.to_list(length=None)
    
    for child in children:
        descendants.append(child['uuid'])
        # Recurse for grandchildren
        grandchildren = await get_all_descendants(collection, child['uuid'])
        descendants.extend(grandchildren)
    
    return descendants


if __name__ == "__main__":
    asyncio.run(link_baseline())