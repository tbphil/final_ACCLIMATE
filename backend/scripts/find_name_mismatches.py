#!/usr/bin/env python3
"""
Find naming mismatches between energy_grid and hbom_baseline

Identifies where asset component_type doesn't match HBOM label.
These need to be added as aliases in the canonical registry.

Usage:
    python find_name_mismatches.py
"""

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from difflib import SequenceMatcher
from database import mongo_uri

DB_NAME = "acclimate_db"


def similarity(a: str, b: str) -> float:
    """Calculate string similarity (0-1)"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


async def find_mismatches():
    """Find naming mismatches between collections"""
    
    client = AsyncIOMotorClient(mongo_uri)
    db = client[DB_NAME]
    
    print("=" * 80)
    print("FINDING NAME MISMATCHES")
    print("=" * 80)
    
    # 1. Get asset types from energy_grid
    print("\n[1/3] Getting asset types from energy_grid...")
    asset_types = set(await db.energy_grid.distinct("component_type"))
    print(f"      Found {len(asset_types)} unique asset types")
    
    # 2. Get HBOM labels from hbom_baseline roots
    print("\n[2/3] Getting decomposition labels from hbom_baseline...")
    cursor = db.hbom_baseline.find({"parent_uuid": None})
    roots = await cursor.to_list(length=None)
    hbom_labels = {root['label'] for root in roots}
    print(f"      Found {len(hbom_labels)} HBOM decomposition labels")
    
    # 3. Find mismatches
    print("\n[3/3] Analyzing mismatches...")
    
    # Exact matches
    exact_matches = asset_types & hbom_labels
    print(f"\n   Exact matches: {len(exact_matches)}")
    for name in sorted(exact_matches):
        print(f"      ✓ {name}")
    
    # Assets without HBOM
    assets_no_hbom = asset_types - hbom_labels
    print(f"\n   Asset types WITHOUT HBOM decomposition: {len(assets_no_hbom)}")
    for name in sorted(assets_no_hbom)[:10]:
        print(f"      • {name}")
    if len(assets_no_hbom) > 10:
        print(f"      ... and {len(assets_no_hbom) - 10} more")
    
    # HBOMs without assets
    hbom_no_assets = hbom_labels - asset_types
    print(f"\n   HBOM decompositions WITHOUT real assets: {len(hbom_no_assets)}")
    for name in sorted(hbom_no_assets):
        print(f"      • {name}")
    
    # Fuzzy matching for likely aliases
    print(f"\n   LIKELY ALIASES (similar names):")
    print("   " + "-" * 76)
    
    aliases_needed = []
    
    for asset_name in sorted(assets_no_hbom):
        # Find similar HBOM labels
        matches = []
        for hbom_name in hbom_no_assets:
            sim = similarity(asset_name, hbom_name)
            if sim > 0.6:  # 60% similar
                matches.append((hbom_name, sim))
        
        if matches:
            matches.sort(key=lambda x: -x[1])
            best_match, score = matches[0]
            aliases_needed.append((asset_name, best_match, score))
            print(f"   {asset_name}")
            print(f"      → {best_match} ({score:.1%} similar)")
    
    # Generate alias mapping code
    if aliases_needed:
        print("\n" + "=" * 80)
        print("SUGGESTED ALIAS ADDITIONS")
        print("=" * 80)
        print("\nAdd these to ALIAS_MAP in build_canonical_registry.py:\n")
        
        for asset_name, hbom_name, score in sorted(aliases_needed, key=lambda x: x[1]):
            print(f'    "{hbom_name}": [..., "{asset_name}"],')
    
    print("\n" + "=" * 80)
    
    client.close()


if __name__ == "__main__":
    asyncio.run(find_mismatches())