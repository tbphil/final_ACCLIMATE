"""
Component Mapper - Map user component types to canonical component library
Handles exact matching, alias matching, fuzzy matching, and common mappings
"""

import logging
from typing import Dict, List, Optional, Tuple
from motor.motor_asyncio import AsyncIOMotorClient
from rapidfuzz import fuzz
import os

from . import config

logger = logging.getLogger(__name__)


# ============================================================================
# AVAILABLE COMPONENTS - What we map user input TO
# ============================================================================
# User inputs (like "pv", "wind turbine", "sub") get mapped to these canonical names.
# Canonical components loaded from MongoDB component_library collection.
# Example: ['Battery', 'Solar Generation Facility', 'Wind Farm', 'Substation', ...]


# Built-in mappings for common component types
# Maps canonical component names → list of user term variations
# 
# ADD NEW VARIATIONS HERE as you discover new user terminology or spelling variations
# Format: 'Canonical Component Name': ['variation1', 'variation2', 'VARIATION3', ...]
# Include common abbreviations, misspellings, case variations, etc.
# Keep this list alphabetically organized for easy maintenance
#
COMPONENT_VARIATIONS = {
    # Battery/Storage
    'Battery': [
        'battery', 'BATTERY', 'bat', 'BAT', 
        'battery storage', 'energy storage', 'bess', 'BESS',
        'battery energy storage', 'battery system', 'storage system',
        'lithium battery', 'li-ion', 'battery bank'
    ],
    
    # Coal
    'Coal Fired Generation Plant': [
        'coal', 'COAL', 'coal plant', 'coal power', 
        'coal fired', 'coal generation', 'coal-fired',
        'coal power plant', 'coal steam', 'pulverized coal'
    ],
    
    # Distillate/Oil
    'Distillate Fuel Oil Generation Plant': [
        'petroleum', 'PETROLEUM', 'oil', 'OIL', 
        'diesel', 'DIESEL', 'fuel oil', 'distillate', 
        'oil plant', 'diesel generator', 'diesel gen',
        'oil fired', 'petroleum plant'
    ],
    
    # Geothermal
    'Geothermal Generation Plant': [
        'geothermal', 'GEOTHERMAL', 'geo', 'GEO',
        'geothermal power', 'geothermal plant',
        'geothermal energy', 'geo thermal'
    ],
    
    # Hydro
    'Hydroelectric Power Generation Facility': [
        'hydro', 'HYDRO', 'hydroelectric', 'HYDROELECTRIC',
        'hydropower', 'HYDROPOWER', 'hydro power', 
        'hydro plant', 'water power', 'hydroelec',
        'hydro electric', 'hydro generation', 'dam'
    ],
    'Hydroelectric Pump Storage Facility': [
        'pumped storage', 'PUMPED STORAGE', 'pumped hydro',
        'pump storage', 'psh', 'PSH', 'pumped-storage',
        'pumped hydro storage'
    ],
    
    # Natural Gas
    'Natural Gas Generation Plant': [
        'natural gas', 'NATURAL GAS', 'gas', 'GAS',
        'ng', 'NG', 'ngcc', 'NGCC',
        'gas turbine', 'combined cycle', 'gas plant',
        'gas power', 'gas fired', 'gas generation',
        'cc', 'CC', 'gas-fired', 'nat gas', 'natgas'
    ],
    
    # Nuclear
    'Nuclear Generation Plant': [
        'nuclear', 'NUCLEAR', 'nuclear power', 
        'nuclear plant', 'nuc', 'NUC', 'atomic',
        'nuclear generation', 'nuclear reactor', 'npp'
    ],
    
    # Solar
    'Solar Generation Facility': [
        'solar', 'SOLAR', 'pv', 'PV',
        'photovoltaic', 'PHOTOVOLTAIC', 'solar pv',
        'solar panel', 'solar power', 'solar plant',
        'solar farm', 'solar gen', 'sol gen',
        'solar generation', 'spv', 'solar power generation',
        'solar array', 'solar field', 'pv system',
        'photovoltaic system', 'solar energy'
    ],
    'Concentrated Solar': [
        'csp', 'CSP', 'concentrated solar',
        'solar thermal', 'concentrating solar',
        'solar tower', 'parabolic trough', 'solar thermal power'
    ],
    
    # Biomass/Waste
    'Solid Waste Generation Plant': [
        'biomass', 'BIOMASS', 'solid waste', 'waste',
        'waste-to-energy', 'wte', 'WTE', 'bio mass',
        'biomass plant', 'wood', 'organic waste'
    ],
    'Landfill Gas Plant': [
        'biogas', 'BIOGAS', 'landfill gas', 'landfill',
        'lfg', 'LFG', 'methane', 'landfill methane',
        'bio gas', 'landfill power'
    ],
    
    # Wind
    'Wind Farm': [
        'wind', 'WIND', 'wind turbine', 'wind power',
        'wind plant', 'wind farm', 'wind generation',
        'wnd', 'WND', 'offshore wind', 'onshore wind',
        'wind energy', 'wind gen', 'wind turbine generator',
        'wtg', 'WTG', 'wind energy system'
    ],
    
    # Substations
    'Substation': [
        'substation', 'SUBSTATION', 'sub', 'SUB',
        'switching station', 'transformer station',
        'substa', 'ss', 'SS', 'switchyard',
        'switch yard', 'electrical substation'
    ],
    
    # Transmission & Distribution
    'Transmission Line': [
        'transmission line', 'trans line', 'tx line',
        't-line', 'transmission', 'high voltage line',
        'hv line', 'transmission tower', 'power line',
        'transmission system', 'overhead line'
    ],
    'Distribution Line': [
        'distribution line', 'dist line', 'distribution',
        'd-line', 'feeder', 'distribution feeder',
        'distrib', 'medium voltage', 'mv line'
    ],
    
    # Transformers
    'Transformer': [
        'transformer', 'xfmr', 'xformer', 'trans',
        'power transformer', 'substation transformer'
    ],
    
    # Generic Generation
    'Generation Plant': [
        'generation', 'GENERATION', 'generation plant',
        'power plant', 'gen', 'GEN', 'generating station',
        'power station', 'generator', 'generating facility',
        'power generation', 'electric generation'
    ],
    
    # Unknown/Unspecified (for truly unknown types, empty values, or "other")
    'Unknown': [
        'unknown', 'UNKNOWN', 'other', 'OTHER', '',
        'unspecified', 'UNSPECIFIED', 'not specified',
        'na', 'NA', 'N/A', 'n/a', 'nan', 'NaN',
        'none', 'NONE', 'null', 'NULL', 'undefined',
        '?', 'tbd', 'TBD', 'misc', 'miscellaneous'
    ],
}

# Build reverse lookup dictionary for fast access: user_term -> canonical_name
# This is built automatically from COMPONENT_VARIATIONS
_COMPONENT_LOOKUP = {}
for canonical_name, variations in COMPONENT_VARIATIONS.items():
    for variation in variations:
        _COMPONENT_LOOKUP[variation.lower()] = canonical_name


class ComponentMapper:
    """
    Maps user component type values to canonical component library
    Uses MongoDB component_library collection for lookups
    """
    
    def __init__(
        self, 
        mongo_uri: Optional[str] = None, 
        database: str = 'acclimate_db'
    ):
        """
        Initialize component mapper
        
        Args:
            mongo_uri: MongoDB connection string
            database: Database name
        """
        self.mongo_uri = mongo_uri or os.getenv('MONGO_URI', 'mongodb://localhost:27017')
        self.database_name = database
        self.client = None
        self.db = None
        self.collection = None
        
        # Cache for component library (loaded once)
        self._component_cache = None
        self._aliases_cache = None
    
    async def connect(self):
        """Connect to MongoDB"""
        if not self.client:
            self.client = AsyncIOMotorClient(self.mongo_uri)
            self.db = self.client[self.database_name]
            self.collection = self.db['component_library']
            logger.info("Connected to component library")
    
    async def load_component_library(self):
        """Load component library into memory for fast lookups"""
        if self._component_cache is not None:
            return  # Already loaded
        
        await self.connect()
        
        # Load all components
        cursor = self.collection.find({})
        components = await cursor.to_list(length=None)
        
        # Build lookup dictionaries
        self._component_cache = {}
        self._aliases_cache = {}
        
        for comp in components:
            canonical_name = comp.get('canonical_name', '')
            if not canonical_name:
                continue
            
            # Store by canonical name (lowercase for case-insensitive matching)
            canonical_lower = canonical_name.lower()
            self._component_cache[canonical_lower] = comp
            
            # Also store by aliases
            aliases = comp.get('aliases', [])
            for alias in aliases:
                if alias:
                    alias_lower = alias.lower()
                    self._aliases_cache[alias_lower] = canonical_name
        
        logger.info(f"Loaded {len(self._component_cache)} components with {len(self._aliases_cache)} aliases")
    
    async def map_component(
        self,
        user_value: str,
        sector: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Map a user component type value to canonical component
        
        Args:
            user_value: User's component type value (e.g., "pv", "Substation")
            sector: Optional sector filter
        
        Returns:
            Dictionary with:
                - matched: bool (True if match found)
                - canonical_name: str (mapped canonical name)
                - match_type: str (mapped, exact, alias, fuzzy, none)
                - confidence: float (0.0-1.0)
                - sector: str (matched component's sector)
                - suggestions: List[str] (if no exact match)
        """
        await self.load_component_library()
        
        if not user_value:
            return {
                'matched': False,
                'canonical_name': None,
                'match_type': 'none',
                'confidence': 0.0,
                'sector': None,
                'suggestions': []
            }
        
        user_value_lower = user_value.lower().strip()
        
        # 1. Try exact canonical name match FIRST (highest priority - 100%)
        if user_value_lower in self._component_cache:
            comp = self._component_cache[user_value_lower]
            comp_sector = comp.get('sector', 'Energy Grid')
            
            # If sector filter provided, check it matches
            if sector and comp_sector != sector:
                return await self._no_match_result(user_value, sector)
            
            return {
                'matched': True,
                'canonical_name': comp['canonical_name'],
                'match_type': 'exact',
                'confidence': 1.0,
                'sector': comp_sector,
                'canonical_uuid': comp.get('canonical_uuid'),
                'suggestions': []
            }
        
        # 2. Try alias match (second priority - 95%)
        if user_value_lower in self._aliases_cache:
            canonical_name = self._aliases_cache[user_value_lower]
            comp = self._component_cache[canonical_name.lower()]
            comp_sector = comp.get('sector', 'Energy Grid')
            
            if sector and comp_sector != sector:
                return await self._no_match_result(user_value, sector)
            
            return {
                'matched': True,
                'canonical_name': canonical_name,
                'match_type': 'alias',
                'confidence': 0.95,
                'sector': comp_sector,
                'canonical_uuid': comp.get('canonical_uuid'),
                'suggestions': []
            }
        
        # 3. Try built-in component variations lookup (third priority - 100%)
        # These are explicitly defined mappings (known variations)
        if user_value_lower in _COMPONENT_LOOKUP:
            canonical_name = _COMPONENT_LOOKUP[user_value_lower]
            
            # Special handling for "Unknown" - return immediately without MongoDB lookup
            if canonical_name == 'Unknown':
                return {
                    'matched': True,
                    'canonical_name': 'Unknown',
                    'match_type': 'exact',
                    'confidence': 1.0,
                    'sector': 'Energy Grid',
                    'canonical_uuid': None,
                    'suggestions': []
                }
            
            # For other mapped components, verify in MongoDB
            comp = self._component_cache.get(canonical_name.lower())
            
            if comp:
                comp_sector = comp.get('sector', 'Energy Grid')
                
                if sector and comp_sector != sector:
                    return await self._no_match_result(user_value, sector)
                
                # Check if user value is exact match of canonical name (case-insensitive)
                # If not, it's a known variation that should be tracked in aliases
                is_exact_canonical = (user_value_lower == canonical_name.lower())
                
                return {
                    'matched': True,
                    'canonical_name': comp['canonical_name'],
                    'match_type': 'exact' if is_exact_canonical else 'known_variation',
                    'confidence': 1.0,
                    'sector': comp_sector,
                    'canonical_uuid': comp.get('canonical_uuid'),
                    'suggestions': []
                }
        
        # 4. Try consensus-based fuzzy matching (last resort)
        # Match against ALL possible terms: database names, aliases, AND hardcoded variations
        all_names = (
            list(self._component_cache.keys()) +      # Database canonical names
            list(self._aliases_cache.keys()) +        # Database aliases
            list(_COMPONENT_LOOKUP.keys())            # All COMPONENT_VARIATIONS terms
        )
        
        # Get fuzzy matches using config cutoff
        from rapidfuzz import process as rf_process
        fuzzy_results = rf_process.extract(
            user_value_lower, 
            all_names, 
            scorer=fuzz.ratio,
            score_cutoff=config.COMPONENT_FUZZY_CUTOFF,  # Use config value
            limit=20  # Get top matches to find consensus
        )
        
        if fuzzy_results:
            # Group matches by canonical component
            component_matches = {}  # canonical_name -> [(variation, score), ...]
            
            for match_text, score, _ in fuzzy_results:
                # Find which canonical component this match belongs to
                canonical_name = None
                
                if match_text in self._component_cache:
                    canonical_name = self._component_cache[match_text]['canonical_name']
                elif match_text in self._aliases_cache:
                    canonical_name = self._aliases_cache[match_text]
                elif match_text in _COMPONENT_LOOKUP:
                    canonical_name = _COMPONENT_LOOKUP[match_text]
                
                if canonical_name:
                    if canonical_name not in component_matches:
                        component_matches[canonical_name] = []
                    component_matches[canonical_name].append((match_text, score))
            
            # Filter by sector if specified
            if sector:
                component_matches = {
                    name: matches for name, matches in component_matches.items()
                    if self._component_cache.get(name.lower(), {}).get('sector') == sector
                }
            
            if component_matches:
                # Calculate confidence for each component using simplified formula:
                # confidence = (best_match_score / 100) + (consensus_boost * extra_matches)
                component_scores = []
                for canonical_name, matches in component_matches.items():
                    match_count = len(matches)
                    scores = [score for _, score in matches]
                    max_score = max(scores)
                    
                    # Simple confidence calculation
                    base_confidence = max_score / 100
                    consensus_boost = min(
                        (match_count - 1) * config.COMPONENT_CONSENSUS_BOOST,
                        0.10  # Cap at 10% boost
                    )
                    confidence = min(base_confidence + consensus_boost, 1.0)
                    
                    # Log low confidence matches for monitoring
                    if confidence < 0.75:
                        logger.info(
                            f"Low confidence component match: '{user_value}' → '{canonical_name}' "
                            f"(confidence: {confidence:.2f}, best_score: {max_score}, matches: {match_count})"
                        )
                    
                    component_scores.append((canonical_name, confidence, match_count, max_score, matches))
                
                # Sort by confidence first, then by match count, then by max score
                component_scores.sort(key=lambda x: (x[1], x[2], x[3]), reverse=True)
                
                # Best match
                best_component, best_confidence, best_count, best_max_score, best_matches = component_scores[0]
                
                # Get suggestions (top 5 unique components)
                suggestions = [name for name, _, _, _, _ in component_scores[:5]]
                
                # If confidence is below threshold, return as Unknown with suggestions
                if best_confidence < config.COMPONENT_CONFIDENCE_THRESHOLD:
                    logger.warning(
                        f"Component match below threshold: '{user_value}' → 'Unknown' "
                        f"(best match: '{best_component}' with {best_confidence:.2f}, "
                        f"threshold: {config.COMPONENT_CONFIDENCE_THRESHOLD})"
                    )
                    return {
                        'matched': True,
                        'canonical_name': 'Unknown',
                        'match_type': 'low_confidence',
                        'confidence': best_confidence,
                        'sector': 'Energy Grid',
                        'canonical_uuid': None,
                        'suggestions': suggestions
                    }
                
                # Return best match
                comp = self._component_cache.get(best_component.lower(), {})
                return {
                    'matched': True,
                    'canonical_name': best_component,
                    'match_type': 'fuzzy_consensus',
                    'confidence': best_confidence,
                    'sector': comp.get('sector', 'Energy Grid'),
                    'canonical_uuid': comp.get('canonical_uuid'),
                    'suggestions': suggestions[1:] if len(suggestions) > 1 else []
                }
        
        # 5. No match found
        return await self._no_match_result(user_value, sector)
    
    async def _no_match_result(
        self,
        user_value: str,
        sector: Optional[str] = None
    ) -> Dict[str, any]:
        """Generate a 'no match' result with suggestions"""
        # Try to suggest based on partial string matching
        suggestions = []
        user_lower = user_value.lower()
        
        for canonical_lower, comp in self._component_cache.items():
            if sector and comp.get('sector') != sector:
                continue
            
            # Check if user value is substring of canonical or vice versa
            if user_lower in canonical_lower or canonical_lower in user_lower:
                suggestions.append(comp['canonical_name'])
                if len(suggestions) >= 5:
                    break
        
        return {
            'matched': False,
            'canonical_name': None,
            'match_type': 'none',
            'confidence': 0.0,
            'sector': sector,
            'suggestions': suggestions[:5]
        }
    
    async def batch_map_components(
        self,
        user_values: List[str],
        sector: Optional[str] = None
    ) -> Dict[str, Dict[str, any]]:
        """
        Map multiple component type values at once
        
        Args:
            user_values: List of unique user component type values
            sector: Optional sector filter
        
        Returns:
            Dictionary mapping user_value -> match_result
        """
        results = {}
        
        for user_value in user_values:
            results[user_value] = await self.map_component(user_value, sector)
        
        return results
    
    async def get_sector_from_component(self, canonical_name: str) -> Optional[str]:
        """
        Get sector for a canonical component name
        
        Args:
            canonical_name: Canonical component name
        
        Returns:
            Sector string or None
        """
        await self.load_component_library()
        
        canonical_lower = canonical_name.lower()
        if canonical_lower in self._component_cache:
            return self._component_cache[canonical_lower].get('sector', 'Energy Grid')
        
        return None
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("Component mapper connection closed")


# Convenience function for single-use mapping
async def map_component_types(
    user_values: List[str],
    sector: Optional[str] = None
) -> Dict[str, Dict[str, any]]:
    """
    Convenience function to map component types without managing mapper instance
    
    Args:
        user_values: List of user component type values
        sector: Optional sector filter
    
    Returns:
        Dictionary mapping user_value -> match_result
    """
    mapper = ComponentMapper()
    try:
        results = await mapper.batch_map_components(user_values, sector)
        return results
    finally:
        mapper.close()
