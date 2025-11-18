"""
Composite variable calculations.
Import all composite modules to register them.
"""
from .registry import (
    register_composite,
    get_composite_function,
    list_composites,
    has_composite,
    compute_composite
)

# Import composite modules to trigger registration
from . import heat_index

__all__ = [
    "register_composite",
    "get_composite_function",
    "list_composites",
    "has_composite",
    "compute_composite",
    heat_index,
]