"""Deprecated compatibility entry point.

All arbitrage pricing is centralized in pricing.py and Dragon v7. No local
edge, fee, slippage, funding, or notional formulas are maintained here.
"""
from arbitrage_engine_v7 import _tri_diag, _basis_diag, live_scan, stats_v7

__all__ = ["_tri_diag", "_basis_diag", "live_scan", "stats_v7"]

if __name__ == "__main__":
    import dragon_core as core
    core.stats = stats_v7
    core.scan = live_scan
    core.main()
