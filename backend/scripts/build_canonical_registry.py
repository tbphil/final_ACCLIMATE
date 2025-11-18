#!/usr/bin/env python3
"""
Build Canonical Component Registry from Intersection

Creates/updates component_library entries ONLY for components that have:
1. HBOM decomposition (exist in hbom_baseline as roots)
2. Real-world assets (exist in energy_grid collection)

Usage:
    python build_canonical_registry.py
"""

import asyncio
import uuid
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from database import mongo_uri

DB_NAME = "acclimate_db"

# Manual alias mappings (expand as needed)
ALIAS_MAP = {
    "Substation": ["Power Station", "Electrical Substation", "Switching Station"],
    "Natural Gas Generation Plant": ["Gas Power Plant", "NGCC", "Gas Plant"],
    "Coal Fired Generation Plant": ["Coal Power Plant", "Coal Plant"],
    "Wind Farm": ["Wind Power Plant", "Wind Generation Facility"],
    "Solar Generation Facility": ["Solar Farm", "Solar Plant", "Solar Power Plant"],
    "Photovoltaic (PV) Generation Facility": ["PV Plant", "Solar PV"],
    "Hydroelectric Power Generation Facility": ["Hydro Plant", "Hydroelectric Facility"],
    "Nuclear Generation Plant": ["Nuclear Plant", "Nuclear Power Plant"],
}


def generate_canonical_uuid(component_name: str) -> str:
    """Generate stable UUID5 for canonical component"""
    namespace = uuid.UUID("10000000-0000-0000-0000-000000000000")
    return str(uuid.uuid5(namespace, f"canonical:{component_name}"))


async def build_registry():
    """Build canonical registry from intersection of hbom_baseline and energy_grid"""
    
    client = AsyncIOMotorClient(mongo_uri)
    db = client[DB_NAME]
    baseline = db['hbom_baseline']
    energy_grid = db['energy_grid']
    comp_lib = db['component_library']
    
    print("=" * 80)
    print("CANONICAL COMPONENT REGISTRY BUILDER")
    print("=" * 80)
    
    # 1. Get asset types that exist in real infrastructure
    print("\n[1/4] Getting asset types from energy_grid...")
    asset_types = set(await energy_grid.distinct("component_type"))
    print(f"      Found {len(asset_types)} unique asset types in infrastructure data")
    
    # 2. Get decompositions from hbom_baseline
    print("\n[2/4] Getting decompositions from hbom_baseline...")
    cursor = baseline.find({"parent_uuid": None})
    roots = await cursor.to_list(length=None)
    print(f"      Found {len(roots)} root decompositions")
    
    # Group by label (handle duplicates)
    roots_by_label = {}
    for root in roots:
        label = root['label']
        if label not in roots_by_label:
            roots_by_label[label] = []
        roots_by_label[label].append(root)
    
    print(f"      Unique decomposition labels: {len(roots_by_label)}")
    
    # 3. Find intersection and update component_library
    print("\n[3/4] Building canonical registry from intersection...")
    
    stats = {"created": 0, "updated": 0, "skipped": 0}
    
    for label, root_list in sorted(roots_by_label.items()):
        # Check if this decomposition has real assets (exact match OR via aliases)
        aliases = ALIAS_MAP.get(label, [label])
        
        # Check if the label itself OR any of its aliases exist in asset_types
        has_assets = label in asset_types or any(alias in asset_types for alias in aliases)
        
        if not has_assets:
            print(f"  SKIP: '{label}' - decomposition exists but no real assets")
            stats["skipped"] += 1
            continue
        
        # Generate canonical UUID
        canonical_uuid = generate_canonical_uuid(label)
        
        # Get aliases
        aliases = ALIAS_MAP.get(label, [label])
        
        # Link to first HBOM decomposition (they should be functionally equivalent)
        hbom_uuid = root_list[0]['uuid']
        
        # Check if entry already exists
        existing = await comp_lib.find_one({"component_type": label})
        
        doc = {
            "canonical_uuid": canonical_uuid,
            "canonical_name": label,
            "component_type": label,
            "sector": "Energy Grid",
            "aliases": aliases,
            "hbom_baseline_uuid": hbom_uuid,
            "hbom_duplicate_count": len(root_list),
            "created_at": existing.get("created_at") if existing else datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "updated_by": "canonical_registry_builder"
        }
        
        # Upsert
        result = await comp_lib.replace_one(
            {"component_type": label},
            doc,
            upsert=True
        )
        
        if result.upserted_id:
            print(f"  CREATE: '{label}' → {canonical_uuid[:16]}... → hbom:{hbom_uuid[:16]}...")
            stats["created"] += 1
        else:
            print(f"  UPDATE: '{label}' → {canonical_uuid[:16]}... → hbom:{hbom_uuid[:16]}...")
            stats["updated"] += 1
    
    # 4. Create indexes (drop existing first to avoid conflicts)
    print(f"\n[4/4] Creating indexes...")
    
    # Drop existing indexes that might conflict
    try:
        await comp_lib.drop_index("canonical_uuid_1")
    except:
        pass  # Index might not exist
    
    try:
        await comp_lib.drop_index("component_type_1")
    except:
        pass
    
    # Create new indexes
    await comp_lib.create_index("canonical_uuid", unique=True, sparse=True)  # sparse=True ignores nulls
    await comp_lib.create_index("component_type", unique=True)
    await comp_lib.create_index("aliases")
    print("      Indexes created")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"  Created: {stats['created']}")
    print(f"  Updated: {stats['updated']}")
    print(f"  Skipped: {stats['skipped']} (decomposition exists but no real assets)")
    
    print("\n" + "=" * 80)
    print("NEXT: Update hbom_baseline to add canonical_component_type field")
    print("=" * 80)
    
    client.close()


if __name__ == "__main__":
    asyncio.run(build_registry())