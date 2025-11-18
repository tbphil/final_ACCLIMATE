#!/usr/bin/env python3
"""
Generate Component Reference Guide for Researchers
Extracts all component names from HBOM decomposition baseline
Outputs researcher-friendly reference showing what components exist in the system
"""

import json
from pathlib import Path
from collections import defaultdict

# Paths
HBOM_BASELINE = Path("backend/fragility_data/processed/hbom_baseline.json")
OUTPUT_FILE = Path("backend/fragility_data/processed/component_reference_guide.txt")

def generate_reference_guide():
    """Extract and organize component names for research reference"""
    
    print("Loading HBOM baseline...")
    
    with open(HBOM_BASELINE, 'r') as f:
        hbom_data = json.load(f)
    
    nodes = hbom_data.get('nodes', [])
    print(f"Found {len(nodes)} components\n")
    
    # Organize by asset type
    by_asset_type = defaultdict(set)
    all_labels = set()
    
    for node in nodes:
        label = node.get('label')
        asset_type = node.get('asset_type', 'unknown')
        
        if label:
            by_asset_type[asset_type].add(label)
            all_labels.add(label)
    
    # Generate output
    output = []
    output.append("="*80)
    output.append("COMPONENT REFERENCE GUIDE FOR FRAGILITY CURVE RESEARCHERS")
    output.append("="*80)
    output.append("")
    output.append("This document lists all component names that exist in the HBOM decomposition")
    output.append("baseline. When creating fragility curves, use these exact component names")
    output.append("to ensure proper matching.")
    output.append("")
    output.append(f"Total unique components: {len(all_labels)}")
    output.append(f"Asset types: {len(by_asset_type)}")
    output.append("")
    
    # Section 1: Alphabetical list
    output.append("="*80)
    output.append("SECTION 1: ALL COMPONENTS (ALPHABETICAL)")
    output.append("="*80)
    output.append("")
    
    for label in sorted(all_labels):
        output.append(f"  • {label}")
    
    output.append("")
    output.append("")
    
    # Section 2: By asset type
    output.append("="*80)
    output.append("SECTION 2: COMPONENTS BY ASSET TYPE")
    output.append("="*80)
    output.append("")
    
    for asset_type in sorted(by_asset_type.keys()):
        components = sorted(by_asset_type[asset_type])
        output.append(f"\n{'─'*80}")
        output.append(f"{asset_type.upper().replace('_', ' ')}")
        output.append(f"{'─'*80}")
        output.append(f"Components: {len(components)}\n")
        
        for comp in components:
            output.append(f"  • {comp}")
    
    output.append("")
    output.append("")
    
    # Section 3: Common patterns
    output.append("="*80)
    output.append("SECTION 3: NAMING GUIDELINES FOR RESEARCHERS")
    output.append("="*80)
    output.append("")
    output.append("When naming fragility curves:")
    output.append("")
    output.append("1. USE EXACT NAMES from this list when possible")
    output.append("   Example: Use 'Blades' not 'Wind Turbine Blade 1'")
    output.append("")
    output.append("2. If you need component-level detail not in this list:")
    output.append("   - Request the component be added to decomposition files")
    output.append("   - Or map your detailed curves to the broader component")
    output.append("")
    output.append("3. Avoid including conditions in component names:")
    output.append("   - BAD:  'Wood Pole 40 Years'")
    output.append("   - GOOD: 'Wood Pole' with age condition = '40_years'")
    output.append("")
    output.append("4. Use singular form where possible:")
    output.append("   - Prefer: 'Blade' over 'Blades'")
    output.append("   - Unless plural is the standard term in the list above")
    output.append("")
    output.append("5. Common substitutions that work:")
    output.append("   - 'Substation' matches 'High Voltage Substation'")
    output.append("   - 'Generation Plant' matches power plant types")
    output.append("")
    
    # Section 4: Missing components
    output.append("")
    output.append("="*80)
    output.append("SECTION 4: COMMONLY REQUESTED COMPONENTS NOT YET IN SYSTEM")
    output.append("="*80)
    output.append("")
    output.append("The following component types appear in fragility research but")
    output.append("are not yet in the decomposition baseline:")
    output.append("")
    output.append("  • Utility Poles (Wood, Steel)")
    output.append("  • Individual Wind Turbine Blades (1, 2, 3)")
    output.append("  • Transmission Tower variants (#1, #2)")
    output.append("  • Cable types (Thermoplastic, Thermoset, Kerite)")
    output.append("  • Nuclear Power Plant components")
    output.append("")
    output.append("To add these components, contact the infrastructure data team.")
    output.append("")
    
    # Write to file
    output_text = "\n".join(output)
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(output_text)
    
    print(f"Reference guide written to: {OUTPUT_FILE}")
    print(f"Total components documented: {len(all_labels)}")
    print("\nSample components:")
    for label in sorted(all_labels)[:10]:
        print(f"  • {label}")
    print("  ...")
    
    return output_text


if __name__ == "__main__":
    guide_text = generate_reference_guide()
    print("\n" + "="*80)
    print("Reference guide complete!")
    print("="*80)