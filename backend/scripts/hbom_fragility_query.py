#!/usr/bin/env python3
"""
Query HBOM and Fragility data for any asset type

# List all available assets
python hbom_fragility_query.py --list-all

# Query specific asset
python hbom_fragility_query.py "Wind Turbine"
python hbom_fragility_query.py "Substation"
python hbom_fragility_query.py "Natural Gas Generation Plant"

# Export to JSON for further analysis
python qhbom_fragility_query.py "Wind Turbine" --export

# Compare two assets
python hbom_fragility_query.py "Wind Turbine" --compare "Substation"

"""

import asyncio
import sys
import json
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, List, Optional
from database import mongo_uri

DB_NAME = "acclimate_db"

class AssetHBOMQuery:
    """Query HBOM and fragility data for any asset"""
    
    def __init__(self):
        self.client = AsyncIOMotorClient(mongo_uri)
        self.db = self.client[DB_NAME]
        self.baseline_coll = self.db['hbom_baseline']
        self.frag_coll = self.db['fragility_db']
        self.comp_coll = self.db['hbom_components']
        self.def_coll = self.db['hbom_definitions']
    
    async def list_all_assets(self):
        """List all root-level assets in the baseline"""
        print("\n" + "=" * 80)
        print("ALL ROOT ASSETS IN HBOM BASELINE")
        print("=" * 80 + "\n")
        
        roots = self.baseline_coll.find({"parent_uuid": None})
        roots_list = await roots.to_list(length=None)
        
        if not roots_list:
            print("No root assets found in hbom_baseline collection")
            return []
        
        print(f"Found {len(roots_list)} root assets:\n")
        
        for idx, root in enumerate(roots_list, 1):
            print(f"{idx:2}. {root['label']}")
            print(f"    UUID: {root['uuid']}")
            print(f"    Asset Type: {root.get('asset_type', 'N/A')}")
            print(f"    Children: {len(root.get('children_uuids', []))}")
            print()
        
        return [r['label'] for r in roots_list]
    
    async def query_asset(self, asset_name: str, export_json: bool = False):
        """Query HBOM and fragility data for specific asset"""
        
        print("\n" + "=" * 80)
        print(f"QUERYING: {asset_name}")
        print("=" * 80)
        
        # Find root node (case-insensitive)
        root = await self.baseline_coll.find_one({
            "label": {"$regex": f"^{asset_name}$", "$options": "i"},
            "parent_uuid": None
        })
        
        if not root:
            print(f"\nâŒ Asset '{asset_name}' not found in hbom_baseline")
            print("\nTry one of these:")
            await self.list_all_assets()
            return None
        
        result = {
            "asset_name": root['label'],
            "root_uuid": root['uuid'],
            "asset_type": root.get('asset_type'),
            "decomposition": {},
            "fragility_curves": {},
            "gaps": []
        }
        
        # 1. Get full decomposition hierarchy
        print("\n1. DECOMPOSITION HIERARCHY")
        print("-" * 80)
        await self._print_and_collect_hierarchy(root['uuid'], result, indent=0)
        
        # 2. Get fragility curves
        print("\n\n2. FRAGILITY CURVES")
        print("-" * 80)
        await self._query_fragility_curves(root['uuid'], result)
        
        # 3. Identify gaps
        print("\n\n3. COVERAGE GAPS")
        print("-" * 80)
        await self._identify_gaps(result)
        
        # 4. Summary
        print("\n\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"\nAsset: {result['asset_name']}")
        print(f"Root UUID: {result['root_uuid']}")
        print(f"Total components: {len(result['decomposition'])}")
        print(f"Components with curves: {len(result['fragility_curves'])}")
        print(f"Components without curves: {len(result['gaps'])}")
        
        if result['fragility_curves']:
            hazards = set()
            for curves in result['fragility_curves'].values():
                hazards.update(c['hazard'] for c in curves)
            print(f"Hazards covered: {', '.join(sorted(hazards))}")
        
        # Export if requested
        if export_json:
            filename = f"{asset_name.lower().replace(' ', '_')}_hbom_report.json"
            with open(filename, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            print(f"\nâœ… Exported to: {filename}")
        
        print("\n" + "=" * 80)
        
        return result
    
    async def _print_and_collect_hierarchy(self, parent_uuid: str, result: Dict, 
                                          indent: int = 0, path: str = ""):
        """Recursively print and collect component hierarchy"""
        
        children = self.baseline_coll.find({"parent_uuid": parent_uuid})
        children_list = await children.to_list(length=None)
        
        for child in children_list:
            label = child['label']
            uuid = child['uuid']
            level = child.get('level', 0)
            
            # Store in result
            full_path = f"{path} > {label}" if path else label
            result['decomposition'][uuid] = {
                "label": label,
                "path": full_path,
                "level": level,
                "parent_uuid": parent_uuid
            }
            
            # Print
            prefix = "   " * indent + "├─"
            print(f"{prefix} {label}")
            print(f"{'   ' * indent}   UUID: {uuid[:16]}...")
            
            # Recurse
            if child.get('children_uuids'):
                await self._print_and_collect_hierarchy(
                    uuid, result, indent + 1, full_path
                )
    
    async def _query_fragility_curves(self, root_uuid: str, result: Dict):
        """Find fragility curves for all components in this asset"""
        
        # Get all component UUIDs
        comp_uuids = list(result['decomposition'].keys())
        comp_uuids.append(root_uuid)  # Include root
        
        # Query fragility curves
        curves = self.frag_coll.find({
            "component_uuid": {"$in": comp_uuids}
        })
        
        curves_list = await curves.to_list(length=None)
        
        if not curves_list:
            print("\nNo fragility curves found for any components")
            return
        
        # Group by component UUID
        by_component = {}
        for curve in curves_list:
            uuid = curve['component_uuid']
            if uuid not in by_component:
                by_component[uuid] = []
            by_component[uuid].append(curve)
        
        result['fragility_curves'] = by_component
        
        # Print organized by component
        for uuid, curves in by_component.items():
            comp_info = result['decomposition'].get(uuid, {"label": "Root", "path": root_uuid})
            
            print(f"\n{comp_info['path']}:")
            
            for curve in curves:
                print(f"   • {curve['hazard']}: {curve['model']} "
                      f"(mu={curve['parameters']['mu']:.3f}, "
                      f"sigma={curve['parameters']['sigma']:.3f})")
                
                if curve.get('conditions'):
                    print(f"     Conditions: {curve['conditions']}")
                
                print(f"     Climate var: {curve['climate_variable']}")
                print(f"     Priority: {curve.get('priority', 0)}")
    
    async def _identify_gaps(self, result: Dict):
        """Identify components without fragility curves"""
        
        all_uuids = set(result['decomposition'].keys())
        covered_uuids = set(result['fragility_curves'].keys())
        gap_uuids = all_uuids - covered_uuids
        
        if not gap_uuids:
            print("\nâœ… All components have fragility curves!")
            return
        
        print(f"\nFound {len(gap_uuids)} components without fragility curves:\n")
        
        gaps = []
        for uuid in gap_uuids:
            comp = result['decomposition'][uuid]
            gaps.append(comp)
            print(f"   • {comp['path']}")
            print(f"     UUID: {uuid}")
        
        result['gaps'] = gaps
    
    async def compare_assets(self, asset1: str, asset2: str):
        """Compare HBOM structure and fragility coverage between two assets"""
        
        print("\n" + "=" * 80)
        print(f"COMPARING: {asset1} vs {asset2}")
        print("=" * 80)
        
        r1 = await self.query_asset(asset1, export_json=False)
        r2 = await self.query_asset(asset2, export_json=False)
        
        if not r1 or not r2:
            return
        
        print("\n\nCOMPARISON SUMMARY")
        print("-" * 80)
        
        print(f"\n{asset1}:")
        print(f"   Components: {len(r1['decomposition'])}")
        print(f"   With curves: {len(r1['fragility_curves'])}")
        print(f"   Gaps: {len(r1['gaps'])}")
        
        print(f"\n{asset2}:")
        print(f"   Components: {len(r2['decomposition'])}")
        print(f"   With curves: {len(r2['fragility_curves'])}")
        print(f"   Gaps: {len(r2['gaps'])}")
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()


async def main():
    """Main entry point with CLI argument handling"""
    
    query = AssetHBOMQuery()
    
    try:
        if len(sys.argv) < 2:
            print("Usage:")
            print("  python query_asset_hbom.py 'Asset Name'")
            print("  python query_asset_hbom.py --list-all")
            print("  python query_asset_hbom.py 'Asset 1' --compare 'Asset 2'")
            print("  python query_asset_hbom.py 'Asset Name' --export")
            sys.exit(1)
        
        if sys.argv[1] == "--list-all":
            await query.list_all_assets()
        
        elif "--compare" in sys.argv:
            idx = sys.argv.index("--compare")
            asset1 = sys.argv[1]
            asset2 = sys.argv[idx + 1]
            await query.compare_assets(asset1, asset2)
        
        else:
            asset_name = sys.argv[1]
            export = "--export" in sys.argv
            await query.query_asset(asset_name, export_json=export)
    
    finally:
        query.close()


if __name__ == "__main__":
    asyncio.run(main())