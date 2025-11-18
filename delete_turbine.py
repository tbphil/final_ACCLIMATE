#!/usr/bin/env python3
"""
Delete Standalone Wind Turbine from HBOM Baseline

Removes the old standalone Wind Turbine root and all its descendants.
Wind Turbine now exists as a component under Wind Farm.

Usage:
    python delete_wind_turbine.py [--dry-run]
"""

import asyncio
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from backend.database import mongo_uri

DB_NAME = "acclimate_db"

# Old Wind Turbine UUID (from diagnostic output)
OLD_WIND_TURBINE_UUID = "98b89e84-de31-548a-845e-177aff3b637d"


async def get_all_descendants(collection, parent_uuid: str) -> list:
    """Recursively collect all descendant UUIDs"""
    descendants = []
    
    cursor = collection.find({"parent_uuid": parent_uuid})
    children = await cursor.to_list(length=None)
    
    for child in children:
        descendants.append(child['uuid'])
        # Recurse for grandchildren
        grandchildren = await get_all_descendants(collection, child['uuid'])
        descendants.extend(grandchildren)
    
    return descendants


async def delete_wind_turbine(dry_run: bool = False):
    """Delete standalone Wind Turbine and all descendants"""
    
    client = AsyncIOMotorClient(mongo_uri)
    db = client[DB_NAME]
    baseline = db['hbom_baseline']
    
    print("=" * 80)
    print("DELETE STANDALONE WIND TURBINE")
    print("=" * 80)
    
    # 1. Find the old Wind Turbine root
    print(f"\n[1/3] Finding standalone Wind Turbine...")
    
    wind_turbine = await baseline.find_one({"uuid": OLD_WIND_TURBINE_UUID})
    
    if not wind_turbine:
        print(f"      Wind Turbine not found (already deleted?)")
        print(f"      UUID: {OLD_WIND_TURBINE_UUID}")
        client.close()
        return
    
    print(f"      Found: {wind_turbine['label']}")
    print(f"      UUID: {wind_turbine['uuid']}")
    print(f"      Path: {wind_turbine.get('node_path', 'N/A')}")
    
    # 2. Collect all descendants
    print(f"\n[2/3] Collecting descendants...")
    
    descendants = await get_all_descendants(baseline, OLD_WIND_TURBINE_UUID)
    
    print(f"      Found {len(descendants)} descendant nodes")
    
    # Show sample descendants
    if descendants:
        print(f"      Sample descendants:")
        for desc_uuid in descendants[:5]:
            desc = await baseline.find_one({"uuid": desc_uuid})
            if desc:
                print(f"        - {desc.get('label')} ({desc.get('node_path', 'N/A')})")
        if len(descendants) > 5:
            print(f"        ... and {len(descendants) - 5} more")
    
    # 3. Delete root + descendants
    all_to_delete = [OLD_WIND_TURBINE_UUID] + descendants
    
    print(f"\n[3/3] Deleting {len(all_to_delete)} nodes...")
    
    if dry_run:
        print("      DRY RUN - No changes made")
        print(f"      Would delete:")
        print(f"        - {wind_turbine['label']} (root)")
        print(f"        - {len(descendants)} descendants")
    else:
        result = await baseline.delete_many({"uuid": {"$in": all_to_delete}})
        print(f"      Deleted {result.deleted_count} nodes")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    if dry_run:
        print(f"  DRY RUN - Would delete {len(all_to_delete)} nodes")
    else:
        print(f"  Deleted {len(all_to_delete)} nodes")
        
        # Verify
        remaining = await baseline.count_documents({})
        print(f"  Remaining nodes in hbom_baseline: {remaining}")
    
    print("\n" + "=" * 80)
    
    client.close()


if __name__ == "__main__":
    # Check for --dry-run flag
    dry_run = "--dry-run" in sys.argv
    
    if dry_run:
        print("Running in DRY RUN mode (no changes will be made)")
        print()
    
    asyncio.run(delete_wind_turbine(dry_run))