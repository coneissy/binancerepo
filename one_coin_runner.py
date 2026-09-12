"""Single-symbol launcher for the HFT-inspired dry-run engine.

Pins the existing engine to LSKUSDT without changing its core execution/risk code.
Live execution remains disabled by hft_scalper.py.
"""
import hft_scalper

TARGET = "lskusdt"

# Replace only the universe selector; all existing scoring, WebSocket,
# position-management and safety logic remains in hft_scalper.py.
hft_scalper.universe = lambda: [TARGET]
hft_scalper.UNIVERSE_SIZE = 1
hft_scalper.WS_SHARDS = 1

if not hft_scalper.DRY_RUN:
    raise RuntimeError("One-coin launcher requires DRY_RUN=true")

if __name__ == "__main__":
    hft_scalper.main()
