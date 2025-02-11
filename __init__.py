"""
Chunky CSV – A minimal CSV transformation library for streaming CSV processing.

This package provides functions such as:
    - streaming_pivot_table
    - process_csv_remove_parentheses
    - generate_column_analytics
    - join_large_csvs
    - unique_filter
    - infer_dtypes
    - read_csv_in_chunks
... and any additional functions defined in chunky.py.
"""

# Import everything from the chunky.py module.
from .chunky import *

# Optionally, you can define __all__ to restrict what is exported via wildcard import.
# If your chunky.py file already defines __all__, you can re-export that.
try:
    from .chunky import __all__ as _all
except ImportError:
    # Otherwise, default to exporting every public name (i.e. names not starting with an underscore)
    _all = [name for name in dir() if not name.startswith("_")]
__all__ = _all
