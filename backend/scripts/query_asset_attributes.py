async def check_specific_fields(self, field_names: list):
        """Check if specific fields exist anywhere in the collection"""
        
        print("\n" + "=" * 80)
        print("FIELD EXISTENCE CHECK")
        print("=" * 80 + "\n")
        
        for field in field_names:
            # Check top-level field
            count = await self.coll.count_documents({field: {"$exists": True}})
            print(f"Documents with '{field}' field (top-level): {count}")
            
            # Check nested in spec_overrides
            nested_field = f"spec_overrides.{field}"
            nested_count = await self.coll.count_documents({nested_field: {"$exists": True}})
            print(f"Documents with '{nested_field}': {nested_count}")
            
            if count > 0 or nested_count > 0:
                # Show sample values
                if count > 0:
                    cursor = self.coll.find({field: {"$exists": True}}).limit(3)
                    samples = await cursor.to_list(length=3)
                    print(f"  Top-level samples:")
                    for s in samples:
                        print(f"    {s.get('name', 'N/A')}: {field}={s.get(field)}")
                
                if nested_count > 0:
                    cursor = self.coll.find({nested_field: {"$exists": True}}).limit(3)
                    samples = await cursor.to_list(length=3)
                    print(f"  Nested (spec_overrides) samples:")
                    for s in samples:
                        val = s.get('spec_overrides', {}).get(field)
                        print(f"    {s.get('name', 'N/A')}: spec_overrides.{field}={val}")
            print()#!/usr/bin/env python3
"""
Query Asset Attributes from MongoDB
Generic tool to inspect real-world infrastructure assets and their attributes

Usage:
    python query_asset_attributes.py "Substation"
    python query_asset_attributes.py "Generation Plant" --attribute capacity
    python query_asset_attributes.py --list-types
"""

import asyncio
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from collections import defaultdict
import json
from database import mongo_uri

DB_NAME = "acclimate_db"
COLLECTION = "energy_grid"


class AssetAttributeQuery:
    """Query real-world asset instances and their attributes"""
    
    def __init__(self):
        self.client = AsyncIOMotorClient(mongo_uri)
        self.db = self.client[DB_NAME]
        self.coll = self.db[COLLECTION]
    
    async def list_asset_types(self):
        """List all unique asset types in the collection"""
        print("\n" + "=" * 80)
        print("ASSET TYPES IN energy_grid COLLECTION")
        print("=" * 80 + "\n")
        
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "component_type": "$component_type",
                        "facilityTypeName": "$facilityTypeName"
                    },
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"count": -1}}
        ]
        
        cursor = self.coll.aggregate(pipeline)
        results = await cursor.to_list(length=None)
        
        print(f"Found {len(results)} unique asset type combinations:\n")
        
        for r in results:
            ct = r['_id'].get('component_type') or 'N/A'
            ft = r['_id'].get('facilityTypeName') or 'N/A'
            count = r['count']
            print(f"  {ct:40} | {ft:40} | {count:>6} assets")
    
    async def query_assets(
        self,
        asset_type: str,
        attribute: str = None,
        limit: int = 10
    ):
        """Query assets of a specific type and show their attributes"""
        
        print("\n" + "=" * 80)
        print(f"QUERYING: {asset_type}")
        print("=" * 80)
        
        # Build query - match on either component_type or facilityTypeName
        base_query = {
            "$or": [
                {"component_type": {"$regex": asset_type, "$options": "i"}},
                {"facilityTypeName": {"$regex": asset_type, "$options": "i"}}
            ]
        }
        
        # If specific attribute requested, add to query
        # Note: Check for attribute existence AND non-zero/non-null values
        # Handle both numeric 0 and string "0"
        if attribute:
            query = {
                "$and": [
                    base_query,
                    {
                        "$or": [
                            # Top-level field
                            {attribute: {"$exists": True, "$nin": [None, 0, "0", ""]}},
                            # Nested in spec_overrides
                            {f"spec_overrides.{attribute}": {"$exists": True, "$nin": [None, 0, "0", ""]}}
                        ]
                    }
                ]
            }
        else:
            query = base_query
        
        # Count total matching base query
        total = await self.coll.count_documents(base_query)
        print(f"\nTotal assets matching '{asset_type}': {total}")
        
        if attribute:
            # Count with specific attribute
            with_attr = await self.coll.count_documents(query)
            print(f"With attribute '{attribute}': {with_attr}")
        
        # Sample assets - use filtered query if attribute specified, otherwise base
        sample_query = query if attribute else base_query
        
        print(f"\nSample assets (limit {limit}):")
        if attribute:
            print(f"Showing only assets with non-zero '{attribute}'")
        print("-" * 80)
        
        cursor = self.coll.find(sample_query).limit(limit)
        assets = await cursor.to_list(length=limit)
        
        if not assets:
            print("No assets found matching query")
            self.close()
            return
        
        for idx, asset in enumerate(assets, 1):
            print(f"\n{idx}. {asset.get('name', 'Unnamed')}")
            print(f"   UUID: {asset.get('uuid', 'N/A')}")
            print(f"   Type: {asset.get('component_type') or asset.get('facilityTypeName')}")
            print(f"   Location: {asset.get('state', 'N/A')}, {asset.get('county', 'N/A')}")
            
            # Show ALL fields to see what actually exists
            print(f"   ALL FIELDS IN THIS DOCUMENT:")
            for k, v in sorted(asset.items()):
                if k != '_id':  # Skip MongoDB internal ID
                    val_str = str(v)[:60]  # Truncate long values
                    print(f"      {k}: {val_str}")
        
        # Analyze attribute distribution
        if total > 0:
            await self._analyze_attributes(base_query, asset_type)
    
    async def _analyze_attributes(self, query: dict, asset_type: str):
        """Analyze what attributes exist across all matching assets"""
        
        print("\n" + "=" * 80)
        print(f"ATTRIBUTE ANALYSIS FOR: {asset_type}")
        print("=" * 80 + "\n")
        
        # Get all assets (or sample if too many)
        cursor = self.coll.find(query).limit(1000)
        assets = await cursor.to_list(length=1000)
        
        # Collect all unique attributes
        attr_counts = defaultdict(int)
        attr_values = defaultdict(set)
        
        standard = {'_id', 'uuid', 'name', 'component_type', 'facilityTypeName',
                   'latitude', 'longitude', 'state', 'county', 'location', 
                   'sector', 'source_sheet', 'source_workbook'}
        
        for asset in assets:
            for key, val in asset.items():
                if key not in standard and val is not None:
                    attr_counts[key] += 1
                    # Store unique values (limit to prevent memory issues)
                    if len(attr_values[key]) < 50:
                        attr_values[key].add(str(val))
        
        # Print attribute summary
        if attr_counts:
            print("Attributes found (sorted by frequency):\n")
            
            for attr, count in sorted(attr_counts.items(), key=lambda x: -x[1]):
                pct = (count / len(assets)) * 100
                print(f"  {attr:30} | {count:>4}/{len(assets):<4} assets ({pct:>5.1f}%)")
                
                # Show unique values if reasonable number
                if len(attr_values[attr]) <= 10:
                    vals = sorted(attr_values[attr])
                    print(f"      Values: {', '.join(vals)}")
                elif len(attr_values[attr]) < 50:
                    print(f"      ({len(attr_values[attr])} unique values)")
        else:
            print("No non-standard attributes found")
        
        # Specific voltage analysis if applicable
        if 'max_voltage' in attr_counts or 'min_voltage' in attr_counts:
            await self._analyze_voltage_distribution(query)
    
    async def _analyze_voltage_distribution(self, query: dict):
        """Detailed voltage distribution analysis"""
        
        print("\n" + "=" * 80)
        print("VOLTAGE CLASS DISTRIBUTION")
        print("=" * 80 + "\n")
        
        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": "$max_voltage",
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        cursor = self.coll.aggregate(pipeline)
        results = await cursor.to_list(length=None)
        
        print("Distribution by max_voltage:\n")
        for r in results:
            voltage = r['_id'] if r['_id'] is not None else 'null'
            count = r['count']
            print(f"  {str(voltage):>10} kV: {count:>4} assets")
    
    def close(self):
        """Close connection"""
        self.client.close()


async def main():
    """Main entry point"""
    
    query = AssetAttributeQuery()
    
    try:
        if len(sys.argv) < 2 or sys.argv[1] == "--list-types":
            await query.list_asset_types()
        else:
            asset_type = sys.argv[1]
            
            # Check for --attribute flag
            attribute = None
            if "--attribute" in sys.argv:
                idx = sys.argv.index("--attribute")
                if idx + 1 < len(sys.argv):
                    attribute = sys.argv[idx + 1]
            
            # Check for --limit flag
            limit = 10
            if "--limit" in sys.argv:
                idx = sys.argv.index("--limit")
                if idx + 1 < len(sys.argv):
                    limit = int(sys.argv[idx + 1])
            
            await query.query_assets(asset_type, attribute, limit)
    
    finally:
        query.close()


if __name__ == "__main__":
    asyncio.run(main())