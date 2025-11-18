#!/usr/bin/env python3
"""
Fragility Database Loader
Loads fragility_database.json into MongoDB with smart upsert logic

Usage:
    python scripts/load_fragility_database.py
"""

import asyncio
import json
from pathlib import Path
from collections import defaultdict
from motor.motor_asyncio import AsyncIOMotorClient
import os
from datetime import datetime
from database import mongo_uri

# Paths
FRAGILITY_FILE = Path("backend/fragility_data/processed/fragility_database.json")

# MongoDB connection
DB_NAME = "acclimate_db"


class FragilityLoader:
    def __init__(self):
        self.client = AsyncIOMotorClient(mongo_uri)
        self.db = self.client[DB_NAME]
        self.fragility_db = self.db["fragility_db"]
        self.hbom_baseline = self.db["hbom_baseline"]
        
        # Stats tracking
        self.stats = {
            "total_curves": 0,
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
            "matched": 0,
            "unmatched": 0,
        }
        self.warnings = []
    
    async def load(self):
        """Main loading process"""
        print("=" * 80)
        print("FRAGILITY DATABASE LOADER")
        print("=" * 80)
        
        # 1. Load JSON
        print("\n[1/4] Loading fragility_database.json...")
        frag_data = self._load_json(FRAGILITY_FILE)
        curves = frag_data.get("fragility_curves", [])
        self.stats["total_curves"] = len(curves)
        print(f"      Loaded {len(curves)} fragility curves from JSON")
        
        # 2. Get baseline UUIDs for validation
        print("\n[2/4] Loading component UUIDs from hbom_baseline...")
        baseline_uuids = await self._get_baseline_uuids()
        print(f"      Found {len(baseline_uuids)} components in baseline")
        
        # 3. Process and upsert curves
        print("\n[3/4] Upserting fragility curves to MongoDB...")
        await self._upsert_curves(curves, baseline_uuids)
        
        # 4. Create indexes
        print("\n[4/4] Creating indexes...")
        await self._create_indexes()
        
        # 5. Report
        self._print_report()
        
        self.client.close()
    
    def _load_json(self, path: Path) -> dict:
        """Load and parse JSON file"""
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
        with open(path, 'r') as f:
            return json.load(f)
    
    async def _get_baseline_uuids(self) -> set:
        """Get all component UUIDs from hbom_baseline collection"""
        cursor = self.hbom_baseline.find({}, {"uuid": 1})
        nodes = await cursor.to_list(None)
        return {node["uuid"] for node in nodes}
    
    async def _upsert_curves(self, curves: list, baseline_uuids: set):
        """Upsert curves with smart duplicate handling"""
        
        # Track what we've seen to detect duplicates within the JSON
        seen_keys = {}  # (component_uuid, hazard, conditions_str) → curve
        
        for curve in curves:
            comp_uuid = curve.get("component_uuid")
            hazard = curve.get("hazard")
            conditions = curve.get("conditions", {})
            
            # Create a hashable key from conditions
            conditions_str = json.dumps(conditions, sort_keys=True)
            dedupe_key = (comp_uuid, hazard, conditions_str)
            
            # Check for duplicates in JSON
            if dedupe_key in seen_keys:
                existing = seen_keys[dedupe_key]
                # Compare parameters to see if they're truly identical
                if curve["parameters"] == existing["parameters"]:
                    self.stats["skipped"] += 1
                    continue  # Skip exact duplicate
                else:
                    # Same component+hazard+conditions but different parameters
                    # This is a conflict - warn and take higher priority
                    if curve.get("priority", 0) > existing.get("priority", 0):
                        seen_keys[dedupe_key] = curve
                        self.warnings.append({
                            "type": "parameter_conflict",
                            "component": curve.get("component_name_cleaned"),
                            "hazard": hazard,
                            "action": "Using higher priority curve"
                        })
                    else:
                        self.stats["skipped"] += 1
                        continue
            else:
                seen_keys[dedupe_key] = curve
            
            # Validate component_uuid exists in baseline
            if comp_uuid:
                if comp_uuid in baseline_uuids:
                    self.stats["matched"] += 1
                else:
                    self.stats["unmatched"] += 1
                    self.warnings.append({
                        "type": "orphan_curve",
                        "component_uuid": comp_uuid,
                        "component_name": curve.get("component_name_cleaned"),
                        "hazard": hazard,
                        "reason": "component_uuid not found in baseline"
                    })
            else:
                self.stats["unmatched"] += 1
            
            # Prepare document for MongoDB
            doc = {
                **curve,
                "loaded_at": datetime.now().isoformat(),
                "loaded_by": "load_script"
            }
            
            # Upsert by curve UUID
            result = await self.fragility_db.replace_one(
                {"uuid": curve["uuid"]},
                doc,
                upsert=True
            )
            
            if result.upserted_id:
                self.stats["inserted"] += 1
            else:
                self.stats["updated"] += 1
        
        print(f"      ✓ Inserted: {self.stats['inserted']}")
        print(f"      ✓ Updated:  {self.stats['updated']}")
        print(f"      ⊘ Skipped:  {self.stats['skipped']} (exact duplicates)")
    
    async def _create_indexes(self):
        """Create MongoDB indexes for fast queries"""
        
        # Primary lookup: by curve UUID
        await self.fragility_db.create_index("uuid")
        
        # Lookup by component
        await self.fragility_db.create_index("component_uuid")
        
        # Compound: component + hazard (most common query)
        await self.fragility_db.create_index([
            ("component_uuid", 1), 
            ("hazard", 1)
        ])
        
        # Filter by hazard
        await self.fragility_db.create_index("hazard")
        
        # Sort by priority for curve selection
        await self.fragility_db.create_index("priority")
        
        # Filter by match status
        await self.fragility_db.create_index("applies_to_level")
        
        print("      ✓ Created indexes: uuid, component_uuid, hazard, priority")
    
    def _print_report(self):
        """Print final summary"""
        print("\n" + "=" * 80)
        print("LOADING COMPLETE")
        print("=" * 80)
        
        print(f"\nTotal curves processed: {self.stats['total_curves']}")
        print(f"  Inserted (new):       {self.stats['inserted']}")
        print(f"  Updated (existing):   {self.stats['updated']}")
        print(f"  Skipped (duplicates): {self.stats['skipped']}")
        
        print(f"\nComponent Matching:")
        print(f"  ✓ Matched:   {self.stats['matched']} curves linked to baseline components")
        print(f"  ⚠ Unmatched: {self.stats['unmatched']} curves need baseline expansion")
        
        if self.warnings:
            print(f"\n⚠ WARNINGS: {len(self.warnings)}")
            
            # Group warnings by type
            by_type = defaultdict(list)
            for w in self.warnings:
                by_type[w["type"]].append(w)
            
            # Show orphan curves (no matching component)
            orphans = by_type.get("orphan_curve", [])
            if orphans:
                print(f"\n  Orphan Curves ({len(orphans)}):")
                print("  These have component_uuid but component doesn't exist in baseline:")
                for w in orphans[:5]:
                    print(f"    - {w['component_name']} ({w['hazard']}): UUID {w['component_uuid']}")
                if len(orphans) > 5:
                    print(f"    ... and {len(orphans) - 5} more")
            
            # Show unmatched curves (null component_uuid)
            unmatched = [w for w in self.warnings if w["type"] == "unmatched"]
            if unmatched:
                print(f"\n  Unmatched to Baseline ({len(unmatched)}):")
                print("  These are valid research but no baseline component exists:")
                
                # Group by component name
                by_comp = defaultdict(set)
                for w in self.warnings:
                    if "component_name" in w:
                        by_comp[w["component_name"]].add(w.get("hazard", "?"))
                
                for comp, hazards in sorted(by_comp.items())[:10]:
                    print(f"    - {comp}: {', '.join(hazards)}")
                
                if len(by_comp) > 10:
                    print(f"    ... and {len(by_comp) - 10} more components")
            
            # Show conflicts
            conflicts = by_type.get("parameter_conflict", [])
            if conflicts:
                print(f"\n  Parameter Conflicts ({len(conflicts)}):")
                for w in conflicts:
                    print(f"    - {w['component']} ({w['hazard']}): {w['action']}")
        
        print("\n" + "=" * 80)
        print("MongoDB Collections Ready:")
        print("  • fragility_db")
        print("\nNext Steps:")
        print("  1. Review warnings above")
        print("  2. Add missing components to baseline (if needed)")
        print("  3. Re-run fragility_research_processor.py with updated baseline")
        print("  4. Re-run this script to update matches")
        print("=" * 80 + "\n")


async def main():
    """Entry point"""
    loader = FragilityLoader()
    await loader.load()


if __name__ == "__main__":
    asyncio.run(main())