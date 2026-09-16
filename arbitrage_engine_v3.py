"""Deprecated compatibility entry point.

Dragon v7 is the supported arbitrage engine. All arbitrage mathematics is
centralized in pricing.py; this module contains no independent pricing model.
"""
from arbitrage_engine_v7 import _tri_diag, _basis_diag, live_scan, stats_v7

__all__ = ["_tri_diag", "_basis_diag", "live_scan", "stats_v7"]

if __name__ == "__main__":
    import dragon_core as core
    core.stats = stats_v7
    core.scan = live_scan
    core.main()
