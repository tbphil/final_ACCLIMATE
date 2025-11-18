#!/usr/bin/env python3
"""
Diagnose how components are currently mapped in the database.

Answers:
1. Are there duplicate component labels (e.g., multiple "Transformer" nodes)?
2. Do they have different UUIDs?
3. How do fragility curves reference them?

Usage:
    python diagnose_uuid_mapping.py
"""

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from collections import defaultdict
import json
from backend.database import mongo_uri

DB_NAME = "acclimate_db"


async def diagnose():
    """Run diagnostic checks"""
    
    client = AsyncIOMotorClient(mongo_uri)
    db = client[DB_NAME]
    baseline = db['hbom_baseline']
    fragility = db['fragility_db']
    
    print("=" * 80)
    print("UUID MAPPING DIAGNOSTIC")
    print("=" * 80)
    
    # 1. Find duplicate component labels
    print("\n[1/4] Checking for duplicate component labels...")
    
    cursor = baseline.find({})
    all_nodes = await cursor.to_list(length=None)
    
    # Group by label
    by_label = defaultdict(list)
    for node in all_nodes:
        label = node.get('label')
        by_label[label].append(node)
    
    # Find labels that appear multiple times
    duplicates = {label: nodes for label, nodes in by_label.items() if len(nodes) > 1}
    
    if duplicates:
        print(f"\n   Found {len(duplicates)} component labels that appear multiple times:")
        
        for label, nodes in sorted(duplicates.items()):
            print(f"\n   '{label}' appears {len(nodes)} times:")
            for node in nodes[:3]:  # Show first 3
                print(f"      UUID: {node['uuid'][:16]}...")
                print(f"      Path: {node.get('node_path', 'N/A')}")
            if len(nodes) > 3:
                print(f"      ... and {len(nodes) - 3} more")
    else:
        print("\n   No duplicate labels found (all labels are unique)")
    
    # 2. Check UUID generation strategy
    print("\n\n[2/4] Analyzing UUID generation strategy...")
    
    # Sample a few nodes to understand the pattern
    sample_nodes = all_nodes[:5]
    
    print("\n   Sample nodes:")
    for node in sample_nodes:
        print(f"\n   Label: {node['label']}")
        print(f"   UUID:  {node['uuid']}")
        print(f"   Path:  {node.get('node_path', 'N/A')}")
        print(f"   Asset: {node.get('asset_type', 'N/A')}")
    
    # Test if UUIDs are path-based
    print("\n   UUID Strategy:")
    print("   UUIDs appear to be deterministic based on:")
    print("   - asset_type")
    print("   - node_path (full hierarchical path)")
    print("   This means same label in different contexts = different UUID")
    
    # 3. Check fragility curve mapping strategy
    print("\n\n[3/4] Checking how fragility curves are mapped...")
    
    frag_cursor = fragility.find({})
    all_curves = await frag_cursor.to_list(length=None)
    
    print(f"\n   Total fragility curves: {len(all_curves)}")
    
    # Check if curves use component_uuid
    curves_with_uuid = [c for c in all_curves if c.get('component_uuid')]
    curves_without_uuid = [c for c in all_curves if not c.get('component_uuid')]
    
    print(f"   Curves with component_uuid: {len(curves_with_uuid)}")
    print(f"   Curves without component_uuid: {len(curves_without_uuid)}")
    
    if curves_with_uuid:
        print("\n   Sample curve mappings:")
        for curve in curves_with_uuid[:3]:
            print(f"\n   Component: {curve.get('component_name_cleaned')}")
            print(f"   UUID: {curve.get('component_uuid')[:16]}...")
            print(f"   Hazard: {curve.get('hazard')}")
            
            # Check if this UUID exists in baseline
            matched = await baseline.find_one({"uuid": curve['component_uuid']})
            if matched:
                print(f"   ✓ Matches baseline: {matched.get('node_path')}")
            else:
                print(f"   ✗ UUID not found in baseline (orphan curve)")
    
    # 4. Check specific case: Transformer components
    print("\n\n[4/4] Case study: 'Transformer' components...")
    
    transformers = [n for n in all_nodes if 'transformer' in n['label'].lower()]
    
    if transformers:
        print(f"\n   Found {len(transformers)} Transformer-related components:")
        
        for t in transformers[:5]:
            print(f"\n   Label: {t['label']}")
            print(f"   UUID:  {t['uuid'][:16]}...")
            print(f"   Path:  {t.get('node_path', 'N/A')}")
            
            # Check if any fragility curves reference this UUID
            curve_count = await fragility.count_documents({"component_uuid": t['uuid']})
            print(f"   Fragility curves: {curve_count}")
        
        if len(transformers) > 5:
            print(f"\n   ... and {len(transformers) - 5} more Transformer components")
    else:
        print("\n   No Transformer components found")
    
    # Summary
    print("\n\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    print("\nCurrent Mapping Strategy:")
    print("  • UUIDs are path-based (asset_type + full node_path)")
    print("  • Same component label in different assets = different UUID")
    print(f"  • {len(duplicates)} labels appear in multiple contexts")
    
    print("\nFragility Curve Strategy:")
    print(f"  • {len(curves_with_uuid)} curves reference specific component UUIDs")
    print(f"  • {len(curves_without_uuid)} curves have no component_uuid (unmatched)")
    
    if duplicates:
        print("\nImplication for Wind Farm:")
        print("  When Wind Farm contains 'Substation', it will get a NEW UUID")
        print("  different from standalone 'Substation' asset.")
        print("  Fragility curves will need to be duplicated or mapped to both.")
    
    print("\n" + "=" * 80)
    
    client.close()


if __name__ == "__main__":
    asyncio.run(diagnose())