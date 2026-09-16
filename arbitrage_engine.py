"""Deprecated compatibility entry point.

The historical arbitrage engine is retired. Dragon v7 + pricing.py are the
single supported calculation path, preventing multiple mathematical models
from producing conflicting opportunities.
"""
from arbitrage_engine_v7 import _tri_diag, _basis_diag, live_scan, stats_v7

__all__ = ["_tri_diag", "_basis_diag", "live_scan", "stats_v7"]

if __name__ == "__main__":
    import dragon_core as core
    core.stats = stats_v7
    core.scan = live_scan
    core.main()
