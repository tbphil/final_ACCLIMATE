#!/usr/bin/env python3
"""
HBOM Baseline Builder
Reads decomposition files from backend/hbom/decomposition_files and outputs hbom_baseline.json

Handles unlimited hierarchy depth by auto-detecting columns before attribute columns.

Usage:
    python generate_hbom_baseline.py
"""

import json
import uuid
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime


# ============================================================================
# Configuration
# ============================================================================

DECOMPOSITION_DIR = Path("backend/hbom/decomposition_files")
OUTPUT_FILE = Path("backend/fragility_data/processed/hbom_baseline.json")

# Columns that mark the start of attributes (not hierarchy)
ATTRIBUTE_MARKERS = {
    "hazard", "fragilitymodel", "scale", "shape", "sector", 
    "fragility_model", "fragility_params", "median", "dispersion",
    "mid_point", "slope"
}


# ============================================================================
# UUID Generation
# ============================================================================

def generate_node_uuid(asset_type: str, node_path: str) -> str:
    """Generate stable UUID5 for HBOM node"""
    namespace = uuid.UUID("a3d2e5f7-8b9c-4d1e-a2f3-5c6d7e8f9a0b")
    key = f"{asset_type}|{node_path}"
    return str(uuid.uuid5(namespace, key))


# ============================================================================
# File Parsers
# ============================================================================

def detect_hierarchy_columns(df: pd.DataFrame) -> List[str]:
    """
    Auto-detect hierarchy columns by finding all columns before attribute markers.
    
    Handles unlimited depth (not limited to Asset/Component/Sub-Component).
    """
    hierarchy_cols = []
    
    for col in df.columns:
        col_lower = str(col).strip().lower()
        
        # Stop at first attribute column
        if col_lower in ATTRIBUTE_MARKERS:
            break
        
        # Skip empty column names
        if not col or col_lower == 'unnamed':
            continue
        
        hierarchy_cols.append(col)
    
    return hierarchy_cols


def parse_file_hierarchical(file_path: Path, asset_type: str) -> List[Dict[str, Any]]:
    """Parse Excel/CSV/TXT file with auto-detected hierarchical columns"""
    
    print(f"  Reading {file_path.name}...")
    
    # Read based on file type
    if file_path.suffix == '.csv':
        df = pd.read_csv(file_path)
    elif file_path.suffix == '.txt':
        df = pd.read_csv(file_path)
    elif file_path.suffix == '.xlsx':
        # Read first sheet only (or we could iterate all sheets if needed)
        df = pd.read_excel(file_path, engine='openpyxl', sheet_name=0)
    else:
        print(f"    Warning: Unsupported file type: {file_path.suffix}")
        return []
    
    # Auto-detect hierarchy columns
    hierarchy_cols = detect_hierarchy_columns(df)
    
    if not hierarchy_cols:
        print("    Warning: No hierarchy columns detected")
        return []
    
    print(f"    Detected {len(hierarchy_cols)} hierarchy columns: {hierarchy_cols}")
    
    # Extract rows
    rows = []
    for idx, row in df.iterrows():
        row_data = {col: row.get(col) for col in hierarchy_cols}
        row_data = {k: v for k, v in row_data.items() if pd.notna(v) and str(v).strip()}
        if row_data:
            rows.append(row_data)
    
    print(f"    Found {len(rows)} rows")
    return rows


# ============================================================================
# Tree Builder
# ============================================================================

def build_tree_from_rows(rows: List[Dict[str, Any]], asset_type: str, hierarchy_cols: List[str]) -> Optional[Dict[str, Any]]:
    """
    Convert flat rows into hierarchical tree structure.
    Handles unlimited depth dynamically.
    """
    
    if not rows or not hierarchy_cols:
        return None
    
    # Root is always first hierarchy column
    root_col = hierarchy_cols[0]
    root_name = rows[0].get(root_col, asset_type)
    
    tree = {
        'asset_type': asset_type,
        'label': root_name,
        'level': 1,
        'children': []
    }
    
    # Track current node at each level
    current_path = {1: tree}
    
    for row in rows:
        # Process each hierarchy level in order
        for level_idx, col in enumerate(hierarchy_cols, start=1):
            label = row.get(col)
            
            # Skip empty cells
            if not label or not str(label).strip():
                break  # Stop processing deeper levels if this level is empty
            
            label = str(label).strip()
            
            # Get parent (previous level)
            parent_level = level_idx - 1
            parent = current_path.get(parent_level)
            
            if not parent:
                break  # Can't add child without parent
            
            # Check if this child already exists under parent
            existing = None
            for child in parent.get('children', []):
                if child['label'] == label and child['level'] == level_idx:
                    existing = child
                    break
            
            if existing:
                # Reuse existing node
                current_path[level_idx] = existing
            else:
                # Create new node
                new_node = {
                    'asset_type': asset_type,
                    'label': label,
                    'level': level_idx,
                    'children': []
                }
                parent['children'].append(new_node)
                current_path[level_idx] = new_node
    
    return tree


# ============================================================================
# Node Flattener
# ============================================================================

def flatten_tree_to_nodes(tree: Dict[str, Any],
                          parent_path: str = "",
                          parent_uuid: Optional[str] = None) -> List[Dict[str, Any]]:
    """Flatten tree into node list with UUIDs"""
    
    nodes = []
    
    current_path = f"{parent_path} > {tree['label']}" if parent_path else tree['label']
    node_uuid = generate_node_uuid(tree['asset_type'], current_path)
    
    node = {
        'uuid': node_uuid,
        'asset_type': tree['asset_type'],
        'label': tree['label'],
        'level': tree['level'],
        'node_path': current_path,
        'parent_uuid': parent_uuid,
        'children_uuids': [],
        'metadata': {
            'created_date': datetime.now().isoformat(),
            'source': 'baseline_decomposition'
        }
    }
    
    nodes.append(node)
    
    # Process children
    for child in tree.get('children', []):
        child_nodes = flatten_tree_to_nodes(child, current_path, node_uuid)
        nodes.extend(child_nodes)
        if child_nodes:
            node['children_uuids'].append(child_nodes[0]['uuid'])
    
    return nodes


# ============================================================================
# Main Processing
# ============================================================================

def load_all_decompositions(decomp_dir: Path) -> Dict[str, Dict]:
    """Load all decomposition files from directory"""
    
    if not decomp_dir.exists():
        print(f"ERROR: Directory not found: {decomp_dir}")
        print(f"Expected location: {decomp_dir.absolute()}")
        print(f"\nCreating directory...")
        decomp_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created: {decomp_dir}")
        print(f"\nPlace your decomposition Excel files here, then re-run this script.")
        return {}
    
    print(f"\nScanning {decomp_dir} for decomposition files...")
    
    # Find all files
    files = (list(decomp_dir.glob("*.xlsx")) + 
             list(decomp_dir.glob("*.csv")) + 
             list(decomp_dir.glob("*.txt")))
    
    # Exclude fragility research file
    files = [f for f in files if 'Fragility_Curve' not in f.name]
    
    if not files:
        print(f"No decomposition files found in {decomp_dir}")
        print("\nPlace Excel/CSV files with hierarchical structure in this directory.")
        return {}
    
    print(f"Found {len(files)} files\n")
    
    decompositions = {}
    
    for file_path in sorted(files):
        asset_type = file_path.stem.lower().replace(' ', '_').replace('-', '_')
        
        print(f"Processing: {file_path.name}")
        print(f"  Asset type: {asset_type}")
        
        # Read file to detect hierarchy columns
        if file_path.suffix == '.xlsx':
            df = pd.read_excel(file_path, engine='openpyxl', sheet_name=0)
        elif file_path.suffix == '.csv':
            df = pd.read_csv(file_path)
        else:
            df = pd.read_csv(file_path)
        
        hierarchy_cols = detect_hierarchy_columns(df)
        
        if not hierarchy_cols:
            print("    Warning: No hierarchy columns detected")
            print()
            continue
        
        print(f"    Detected {len(hierarchy_cols)} hierarchy columns: {hierarchy_cols}")
        
        # Extract rows
        rows = []
        for idx, row in df.iterrows():
            row_data = {col: row.get(col) for col in hierarchy_cols}
            row_data = {k: v for k, v in row_data.items() if pd.notna(v) and str(v).strip()}
            if row_data:
                rows.append(row_data)
        
        print(f"    Found {len(rows)} data rows")
        
        if not rows:
            print()
            continue
        
        tree = build_tree_from_rows(rows, asset_type, hierarchy_cols)
        
        if tree:
            print(f"  Built tree: {tree['label']}")
            print(f"    Depth: {len(hierarchy_cols)} levels")
            print(f"    Direct children: {len(tree.get('children', []))}")
            decompositions[asset_type] = tree
        
        print()
    
    return decompositions


def generate_all_nodes(decompositions: Dict[str, Dict]) -> List[Dict]:
    """Convert all trees to flat node list"""
    
    print("\nFlattening trees to nodes...")
    
    all_nodes = []
    
    for asset_type, tree in decompositions.items():
        print(f"  Processing: {tree['label']}")
        nodes = flatten_tree_to_nodes(tree)
        print(f"    Generated {len(nodes)} nodes")
        all_nodes.extend(nodes)
    
    return all_nodes


def write_output(nodes: List[Dict], output_path: Path):
    """Write nodes to JSON file"""
    
    print(f"\nWriting output to: {output_path}")
    
    # Ensure directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    output = {
        'metadata': {
            'created_date': datetime.now().isoformat(),
            'total_nodes': len(nodes),
            'source': 'baseline_decomposition_files',
            'note': 'HBOM baseline from decomposition files'
        },
        'nodes': nodes
    }
    
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)
    
    file_size_kb = output_path.stat().st_size / 1024
    print(f"  Wrote {len(nodes)} nodes ({file_size_kb:.1f} KB)")


def print_summary(nodes: List[Dict]):
    """Print summary statistics"""
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    # Group by asset type
    by_type = {}
    for node in nodes:
        asset_type = node['asset_type']
        if asset_type not in by_type:
            by_type[asset_type] = []
        by_type[asset_type].append(node)
    
    print(f"\nTotal nodes: {len(nodes)}")
    print(f"Asset types: {len(by_type)}")
    
    for asset_type, type_nodes in sorted(by_type.items()):
        root_nodes = [n for n in type_nodes if n['parent_uuid'] is None]
        print(f"  {asset_type}: {len(type_nodes)} nodes ({len(root_nodes)} roots)")
    
    # Show sample paths from each asset type
    print("\nSample component paths:")
    for asset_type, type_nodes in sorted(by_type.items()):
        sample = type_nodes[min(5, len(type_nodes)-1)]
        print(f"  {asset_type}:")
        print(f"    {sample['node_path']}")
    
    print("\n" + "="*80)


# ============================================================================
# Main
# ============================================================================

def main():
    """Main entry point"""
    
    print("HBOM Baseline Builder")
    print("="*80)
    print(f"Reading from: {DECOMPOSITION_DIR.absolute()}")
    print(f"Output to:    {OUTPUT_FILE.absolute()}")
    print()
    
    # Load decomposition files
    decompositions = load_all_decompositions(DECOMPOSITION_DIR)
    
    if not decompositions:
        print("\nNo decompositions loaded - exiting")
        return
    
    # Generate nodes
    nodes = generate_all_nodes(decompositions)
    
    # Print summary
    print_summary(nodes)
    
    # Write output
    write_output(nodes, OUTPUT_FILE)
    
    print("\nBaseline building complete!")
    print(f"\nNext steps:")
    print(f"  1. Review the generated baseline: {OUTPUT_FILE}")
    print(f"  2. Load to MongoDB: python load_hbom_baseline.py")


if __name__ == "__main__":
    main()