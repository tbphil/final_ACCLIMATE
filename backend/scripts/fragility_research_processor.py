#!/usr/bin/env python3
"""
Fragility Research Processor
Converts research Excel (Fragility_Curve_Compilation.xlsx) into production database

Creates both human-readable (research sharing) and machine-readable (system consumption) outputs
Matches fragility curves to decomposition component UUIDs for multi-level analysis

Usage:
    python fragility_research_processor.py Fragility_Curve_Compilation.xlsx fragility_database.json
"""

import sys
import json
import uuid
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
from datetime import datetime


# ============================================================================
# Configuration
# ============================================================================

# Hardcoded data directory paths
FRAGILITY_DATA_DIR = Path("backend/fragility_data")
RESEARCH_DIR = FRAGILITY_DATA_DIR / "research"
PROCESSED_DIR = FRAGILITY_DATA_DIR / "processed"

# HBOM baseline (decomposition components)
HBOM_BASELINE_PATH = PROCESSED_DIR / "hbom_baseline.json"

# Map hazard variables to climate variables
HAZARD_CLIMATE_MAP = {
    "Wind Gust Speed (m/s)": "sfcWind",
    "Wind Gust Speed (mph)": "sfcWind", 
    "Wind speed (m/s)": "sfcWind",
    "Temperature (degC)": "tas",
    "Ice Thickness (mm)": "ice_thickness",  # Derived
    "Depth of flood (ft)": "flood_depth",   # External
    "Depth of flood (m)": "flood_depth"
}

# Normalize conditional variable names
CONDITION_NORMALIZER = {
    "Terrain type 1": {"field": "terrain_type", "value": "type_1"},
    "Terrain type 2": {"field": "terrain_type", "value": "type_2"},
    "Terrain type 3": {"field": "terrain_type", "value": "type_3"},
    "Terrain type 4": {"field": "terrain_type", "value": "type_4"},
    "Moderate": {"field": "severity_level", "value": "moderate"},
    "Severe": {"field": "severity_level", "value": "severe"},
    "Complete": {"field": "severity_level", "value": "complete"},
    "Uniform balance load": {"field": "load_condition", "value": "uniform_balance"},
    "Longitudinal Bending": {"field": "load_condition", "value": "longitudinal_bending"},
    "no wind": {"field": "wind_condition", "value": "no_wind"},
    "New": {"field": "age_category", "value": "new"},
    "40 Years": {"field": "age_category", "value": "40_years"},
    "class 1": {"field": "pole_class", "value": "class_1"},
    "class 7": {"field": "pole_class", "value": "class_7"}
}

# Operating state patterns to extract from component names
OPERATING_STATE_PATTERNS = {
    "in Parked Condition": {"field": "operating_state", "value": "parked"},
    "in Operating Condition": {"field": "operating_state", "value": "operating"},
    "Parked": {"field": "operating_state", "value": "parked"},
    "Operating": {"field": "operating_state", "value": "operating"}
}


# ============================================================================
# Decomposition Loading from JSON
# ============================================================================

def load_decomposition_components() -> Dict[str, str]:
    """Load HBOM baseline to build component label → UUID lookup"""
    
    print("📚 Loading decomposition components from hbom_baseline.json...")
    
    if not HBOM_BASELINE_PATH.exists():
        print(f"   ⚠️  HBOM baseline not found: {HBOM_BASELINE_PATH}")
        return {}
    
    try:
        with open(HBOM_BASELINE_PATH, 'r') as f:
            hbom_data = json.load(f)
        
        nodes = hbom_data.get('nodes', [])
        print(f"   📊 Found {len(nodes)} decomposition components")
        
        component_lookup = {}
        
        for node in nodes:
            label = node.get('label')
            uuid_val = node.get('uuid')
            
            if label and uuid_val:
                # Store exact label
                component_lookup[label] = uuid_val
                
                # Store lowercase for case-insensitive matching
                component_lookup[label.lower()] = uuid_val
                
                # Store simplified (no special chars) for fuzzy matching
                label_simple = ''.join(c for c in label.lower() if c.isalnum() or c.isspace())
                component_lookup[label_simple.strip()] = uuid_val
        
        print(f"   ✅ Built lookup with {len(component_lookup)} entries")
        return component_lookup
        
    except Exception as e:
        print(f"   ❌ Error loading HBOM baseline: {e}")
        return {}


# ============================================================================
# Helper Functions
# ============================================================================

def fuzzy_match_component(component_name: str, decomp_lookup: Dict[str, str]) -> tuple[Optional[str], str]:
    """
    Try progressively simpler versions of component name to find a match
    Returns: (uuid, match_method)
    """
    if not component_name:
        return None, "no_name"
    
    # Try exact match first
    if component_name in decomp_lookup:
        return decomp_lookup[component_name], "exact"
    
    # Try case-insensitive
    if component_name.lower() in decomp_lookup:
        return decomp_lookup[component_name.lower()], "case_insensitive"
    
    # Strip common suffixes/patterns
    simplified = component_name
    
    # Remove age indicators: "40 Years", "60 Years", "New"
    simplified = simplified.replace(" 40 Years", "").replace(" 60 Years", "")
    simplified = simplified.replace("New ", "").replace(" New", "")
    
    # Remove numbers and # symbols: "Tower #1" → "Tower"
    import re
    simplified = re.sub(r'\s*#?\d+', '', simplified)
    
    # Remove condition descriptors
    conditions_to_remove = [
        " in Parked Condition",
        " in Operating Condition", 
        " (Greece)",
        " 1", " 2", " 3"  # Trailing numbers
    ]
    for cond in conditions_to_remove:
        simplified = simplified.replace(cond, "")
    
    simplified = simplified.strip()
    
    # Try simplified exact
    if simplified in decomp_lookup:
        return decomp_lookup[simplified], "simplified"
    
    # Try simplified case-insensitive
    if simplified.lower() in decomp_lookup:
        return decomp_lookup[simplified.lower()], "simplified_case"
    
    # Normalize common variations
    normalizations = {
        "gas power plant": "natural gas generation plant",
        "coal power plant": "coal fired generation plant",
        "solar power plant": "solar generation facility",
        "wind power plant": "wind turbine",
        "solar substation": "substation",
        "high voltage substation": "substation",
        "medium voltage substation": "substation",
        "utility pole": "pole",
        "steel tower": "transmission tower",
        "solar pv panel": "pv array",
        "solar panels": "pv array",
        "wind turbine blade": "blades",  # Wind turbine specific
        "wind turbine tower": "tower"     # Wind turbine specific
    }
    
    normalized = normalizations.get(simplified.lower(), simplified.lower())
    if normalized in decomp_lookup:
        return decomp_lookup[normalized], "normalized"
    
    # Try removing "s" plurals
    if simplified.endswith('s'):
        singular = simplified[:-1]
        if singular.lower() in decomp_lookup:
            return decomp_lookup[singular.lower()], "singular"
    
    # No match found
    return None, "no_match"


def normalize_component_name(name: str) -> Optional[str]:
    """Normalize component name variations"""
    if pd.isna(name) or not name:
        return None
    
    name = str(name).strip()
    
    # Handle common variations
    normalizations = {
        "Transmission Tower ": "Transmission Tower",
        "Cables: Thermoplastic Insulation": "Cable (Thermoplastic)",
        "Cables: Thermoset Insulation": "Cable (Thermoset)",
        "Nuclear Research and Test Reactors": "Research Reactor",
        "Nuclear Waste Processing and Disposal Facility (Storage)": "Nuclear Waste Facility"
    }
    
    return normalizations.get(name, name)


def extract_operating_state_from_name(component_name: str) -> tuple[str, Dict[str, str]]:
    """
    Extract operating state from component name
    Returns: (cleaned_name, conditions_dict)
    """
    conditions = {}
    cleaned_name = component_name
    
    # Check for operating state patterns in name
    for pattern, condition in OPERATING_STATE_PATTERNS.items():
        if pattern in cleaned_name:
            conditions[condition["field"]] = condition["value"]
            cleaned_name = cleaned_name.replace(pattern, "").strip()
    
    return cleaned_name, conditions


def parse_conditions(row: Dict[str, Any]) -> Dict[str, str]:
    """Parse conditional variables from Additional Variable columns"""
    conditions = {}
    
    # Column mapping (with typos from source)
    condition_columns = [
        "1st Additional Variables",
        "2nd Additional Vriable",  # Note: typo in source
        "3rd Additional Variable", 
        "4th Additional Variable"
    ]
    
    for col in condition_columns:
        value = row.get(col)
        if pd.notna(value) and value:
            value = str(value).strip()
            
            # Normalize the condition
            if value in CONDITION_NORMALIZER:
                normalized = CONDITION_NORMALIZER[value]
                conditions[normalized["field"]] = normalized["value"]
            else:
                # Store unknown conditions as-is for manual review
                conditions[f"unknown_{len(conditions)}"] = value
    
    return conditions


def calculate_priority(conditions: Dict[str, str], best_fit: bool) -> int:
    """Calculate curve priority for selection logic"""
    priority = 10  # Base priority
    
    if best_fit:
        priority += 50  # Best fit curves get highest priority
    
    # Add points for each specific condition
    priority += len(conditions) * 10
    
    return priority


def generate_curve_uuid(component_name: str, hazard: str, conditions: Dict[str, str]) -> str:
    """Generate deterministic UUID for fragility curve"""
    # Create a stable key for UUID generation
    key_parts = [
        component_name or "unknown",
        hazard,
        json.dumps(conditions, sort_keys=True)
    ]
    key_string = "|".join(key_parts)
    
    # Generate UUID5 (namespace + name based)
    namespace = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")  # Fixed namespace
    return str(uuid.uuid5(namespace, key_string))


# ============================================================================
# Main Processing Functions
# ============================================================================

def process_research_excel(excel_path: Path) -> Dict[str, Any]:
    """Process research Excel file into structured fragility database"""
    
    print(f"📊 Processing research file: {excel_path.name}")
    
    # Load decomposition components for UUID matching
    decomp_lookup = load_decomposition_components()
    
    try:
        sheets = pd.read_excel(excel_path, sheet_name=None, engine='openpyxl')
        print(f"   Found {len(sheets)} hazard sheets")
    except Exception as e:
        print(f"   ❌ Error reading Excel: {e}")
        return None
    
    # Initialize output structure
    result = {
        "research_metadata": {
            "source_file": excel_path.name,
            "processed_date": datetime.now().isoformat(),
            "total_curves": 0,
            "hazards_processed": list(sheets.keys()),
            "contributors": ["University Research Consortium", "Idaho National Laboratory"],
            "citation": "Climate Risk Assessment for Critical Infrastructure (2024)",
            "license": "CC BY 4.0 - Attribution Required"
        },
        
        "curves_by_component": {},  # Human-readable section
        "fragility_curves": [],     # Machine-readable section
        "taxonomy_gaps": [],        # Components not in taxonomy
        "unknown_conditions": [],   # Conditions needing manual mapping
        "matching_stats": {
            "total_matched": 0,
            "component_level": 0,
            "archetype_level": 0,
            "unmatched": 0
        }
    }
    
    all_curves = []
    taxonomy_gaps = set()
    unknown_conditions = set()
    matching_stats = {"total_matched": 0, "component_level": 0, "archetype_level": 0, "unmatched": 0}
    
    # Process each hazard sheet
    for hazard, df in sheets.items():
        if df.empty:
            continue
            
        print(f"   📄 Processing {hazard} sheet ({len(df)} rows)")
        
        current_hazard_var = None
        current_component = None
        sheet_curves = 0
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Track current hazard variable (inherited from above)
            if pd.notna(row_dict.get("Hazard Variable")):
                current_hazard_var = row_dict["Hazard Variable"]
            
            # Track current component (inherited from above)
            if pd.notna(row_dict.get("Infrastructure Component")):
                raw_component = normalize_component_name(
                    row_dict["Infrastructure Component"]
                )
                if raw_component:  # Check it's not empty after normalization
                    # Extract operating state from component name
                    current_component, name_conditions = extract_operating_state_from_name(raw_component)
                else:
                    current_component = None
                    name_conditions = {}
            else:
                name_conditions = {}
            
            # Skip if we don't have a valid component name
            if not current_component:
                continue
            
            # Skip if no lognormal distribution
            lognormal_flag = row_dict.get("Lognormal Distribution (y/n)", "")
            if not lognormal_flag or str(lognormal_flag).lower() != "y":
                continue
                
            # Skip if no component or parameters
            if not current_component:
                continue
            
            # Try to parse parameters - skip if they're formulas
            try:
                mu_val = row_dict.get("Mu (if lognormal distribution)")
                sigma_val = row_dict.get("Sigma (if lognormal distribution)")
                
                if pd.isna(mu_val) or pd.isna(sigma_val):
                    continue
                
                mu = float(mu_val)
                sigma = float(sigma_val)
            except (ValueError, TypeError):
                # Skip parametric curves with formulas for now
                continue
            best_fit_val = row_dict.get("Best Fit (y/n)", "")
            best_fit = str(best_fit_val).lower() == "y" if best_fit_val else False
            source = row_dict.get("Source", "Unknown")
            
            # Parse conditions from Additional Variable columns
            conditions = parse_conditions(row_dict)
            
            # Merge with conditions extracted from component name
            conditions.update(name_conditions)
            
            # Track unknown conditions
            for k, v in conditions.items():
                if k.startswith("unknown_"):
                    unknown_conditions.add(v)
            
            # Match component to UUID using fuzzy matching
            component_uuid, match_method = fuzzy_match_component(current_component, decomp_lookup)
            
            if component_uuid:
                applies_to_level = "component"
                matching_stats["component_level"] += 1
            else:
                # If no component match, might be archetype-level
                archetype_names = ["substation", "generation plant", "transmission tower", "transmission line"]
                if current_component.lower() in archetype_names:
                    applies_to_level = "archetype"
                    match_method = "archetype"
                    matching_stats["archetype_level"] += 1
                else:
                    applies_to_level = "unknown"
                    match_method = "no_match"
                    matching_stats["unmatched"] += 1
            
            # Track gaps
            if not component_uuid and applies_to_level == "unknown":
                taxonomy_gaps.add(current_component)
            
            if component_uuid or applies_to_level == "archetype":
                matching_stats["total_matched"] += 1
            
            # Build curve entry
            curve = {
                "uuid": generate_curve_uuid(current_component, hazard, conditions),
                "component_name": row_dict.get("Infrastructure Component", ""),  # Original name
                "component_name_cleaned": current_component,  # Cleaned for matching
                "component_uuid": component_uuid,
                "applies_to_level": applies_to_level,
                "match_method": match_method,
                "hazard": hazard,
                "hazard_variable": current_hazard_var,
                "climate_variable": HAZARD_CLIMATE_MAP.get(current_hazard_var, current_hazard_var),
                "conditions": conditions,
                "model": "lognormal",
                "parameters": {"mu": mu, "sigma": sigma},
                "priority": calculate_priority(conditions, best_fit),
                "is_best_fit": best_fit,
                "units": {
                    "intensity": current_hazard_var.split("(")[-1].rstrip(")") if "(" in current_hazard_var else "unknown",
                    "probability": "0-1"
                },
                "provenance": {
                    "source": source,
                    "sheet": hazard,
                    "row": idx + 2,
                    "extracted_date": datetime.now().isoformat()
                },
                "active": True,
                "version": 1
            }
            
            all_curves.append(curve)
            sheet_curves += 1
        
        print(f"      ✅ Extracted {sheet_curves} curves")
    
    # Build human-readable section (organized by component)
    curves_by_component = {}
    for curve in all_curves:
        comp_name = curve["component_name"]  # Use original name for human readability
        hazard = curve["hazard"]
        
        if comp_name not in curves_by_component:
            curves_by_component[comp_name] = {}
        if hazard not in curves_by_component[comp_name]:
            curves_by_component[comp_name][hazard] = []
        
        # Simplified entry for researchers
        human_curve = {
            "conditions": curve["conditions"],
            "variable": curve["hazard_variable"],
            "distribution": curve["model"],
            "parameters": curve["parameters"],
            "is_best_fit": curve["is_best_fit"],
            "source": curve["provenance"]["source"],
            "matched_uuid": curve["component_uuid"],
            "level": curve["applies_to_level"]
        }
        
        curves_by_component[comp_name][hazard].append(human_curve)
    
    # Update result
    result["curves_by_component"] = curves_by_component
    result["fragility_curves"] = all_curves
    result["taxonomy_gaps"] = list(taxonomy_gaps)
    result["unknown_conditions"] = list(unknown_conditions)
    result["research_metadata"]["total_curves"] = len(all_curves)
    result["matching_stats"] = matching_stats
    
    print("\n📊 Processing Complete:")
    print(f"   Total curves: {len(all_curves)}")
    print(f"   Components: {len(curves_by_component)}")
    print(f"   Matched to decomposition UUIDs: {matching_stats['component_level']}")
    print(f"   Archetype-level curves: {matching_stats['archetype_level']}")
    print(f"   Unmatched: {matching_stats['unmatched']}")
    print(f"   Taxonomy gaps: {len(taxonomy_gaps)}")
    print(f"   Unknown conditions: {len(unknown_conditions)}")
    
    return result


def generate_summary_report(data: Dict[str, Any]):
    """Generate human-readable summary of processing results"""
    
    metadata = data["research_metadata"]
    stats = data["matching_stats"]
    
    print(f"\n{'='*80}")
    print("FRAGILITY RESEARCH DATABASE SUMMARY")
    print(f"{'='*80}")
    
    print("\n📊 OVERVIEW:")
    print(f"   Source: {metadata['source_file']}")
    print(f"   Total curves: {metadata['total_curves']}")
    print(f"   Hazard types: {len(metadata['hazards_processed'])}")
    print(f"   Component types: {len(data['curves_by_component'])}")
    
    print("\n🎯 MATCHING RESULTS:")
    print(f"   Successfully matched: {stats['total_matched']} ({stats['total_matched']/metadata['total_curves']*100:.1f}%)")
    print(f"   Component-level: {stats['component_level']}")
    print(f"   Archetype-level: {stats['archetype_level']}")
    print(f"   Unmatched: {stats['unmatched']}")
    
    print("\n🗃️  COMPONENT COVERAGE:")
    for comp_name, hazards in data["curves_by_component"].items():
        hazard_counts = {h: len(curves) for h, curves in hazards.items()}
        total = sum(hazard_counts.values())
        matched = sum(1 for h in hazards.values() for c in h if c.get('matched_uuid'))
        print(f"   {comp_name}: {total} curves, {matched} matched")
    
    print("\n⚠️  TAXONOMY GAPS (need UUID mapping):")
    for gap in data["taxonomy_gaps"]:
        print(f"   - {gap}")
    
    print("\n❓ UNKNOWN CONDITIONS (need normalization):")
    for condition in data["unknown_conditions"]:
        print(f"   - {condition}")
    
    print("\n📋 NEXT STEPS:")
    print("   1. Review unmatched components")
    print("   2. Add missing components to decomposition files")
    print("   3. Normalize unknown conditions")
    print("   4. Load to MongoDB fragility_curves collection")
    print("   5. Test curve matching with infrastructure assets")
    
    print("\n✅ OUTPUT STRUCTURE:")
    print("   research_metadata: Research sharing info")
    print("   curves_by_component: Human-readable curves")
    print("   fragility_curves: Machine-readable with UUIDs")
    print("   matching_stats: Matching success metrics")
    print("   taxonomy_gaps: Components needing mapping")


# ============================================================================
# Main
# ============================================================================

def main(excel_filename: str, output_filename: str):
    """Main processing pipeline"""
    
    # Use hardcoded directory paths
    excel_file = RESEARCH_DIR / excel_filename
    output_file = PROCESSED_DIR / output_filename
    
    # Ensure directories exist
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    if not excel_file.exists():
        print(f"❌ Excel file not found: {excel_file}")
        print(f"   Expected location: {excel_file.absolute()}")
        sys.exit(1)
    
    print("🔬 Fragility Research → Production Database Processor")
    print("="*60)
    
    # Process research Excel
    result = process_research_excel(excel_file)
    
    if not result:
        print("❌ Processing failed!")
        sys.exit(1)
    
    # Generate summary
    generate_summary_report(result)
    
    # Write output
    print(f"\n💾 Writing database to: {output_file}")
    with open(output_file, 'w') as f:
        json.dump(result, f, indent=2, default=str)
    
    # File size info
    file_size_kb = output_file.stat().st_size / 1024
    print(f"✅ Complete! Database size: {file_size_kb:.1f} KB")
    
    print("\n🎯 Ready for:")
    print("   - Research collaboration (curves_by_component section)")
    print("   - Production system integration (fragility_curves section)")
    print("   - MongoDB loading via fragility curve loader")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python fragility_research_processor.py Fragility_Curve_Compilation.xlsx fragility_database.json")
        sys.exit(1)
    
    main(sys.argv[1], sys.argv[2])