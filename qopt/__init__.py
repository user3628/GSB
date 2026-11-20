"""Query planning and index selection component for local SQLite data.

The package estimates predicate selectivity and join cardinality from
statistics snapshots (row counts, distinct-value counts, equi-depth
histograms, null fractions) and chooses indexes and a left-deep join
order under a documented numerical precision contract.
"""

__all__ = ["__version__"]

__version__ = "1.0.0"
