import asyncio, time, statistics
from cryptofeed import FeedHandler
from cryptofeed.exchanges import Binance
from cryptofeed.defines import TRADES

def pct(xs, p):
    if not xs: return float("nan")
    xs = sorted(xs); k = max(0, min(len(xs)-1, int(round((p/100.0)*(len(xs)-1)))))
    return xs[k]

class Stats:
    def __init__(self):
        self.e_lat_ns, self.t_lat_ns, self.et_gap_ms, self.n = [], [], [], 0
    def add(self, now_ns, E_ms, T_ms):
        if E_ms is not None: self.e_lat_ns.append(now_ns - int(E_ms)*1_000_000)
        if T_ms is not None: self.t_lat_ns.append(now_ns - int(T_ms)*1_000_000_000)
        if (E_ms is not None) and (T_ms is not None): self.et_gap_ms.append(int(E_ms) - int(T_ms * 1_000))
        self.n += 1
    def summ(self, name, arr, ms=True):
        if not arr: return f"{name}: no data"
        a = [x/1_000_000 for x in arr] if ms else arr
        return f"{name} n={len(a)} p50={pct(a,50):.1f} p95={pct(a,95):.1f} p99={pct(a,99):.1f} min={min(a):.1f} max={max(a):.1f}"

def main(seconds=120, symbol="BTC-USDT"):
    stats = Stats()

    async def on_trade(t, receipt_timestamp):
        now = time.time_ns()
        E = t.raw.get("E") if getattr(t, "raw", None) else None  # ms
        T = getattr(t, "timestamp", None)                        # ms
        stats.add(now, E, T)

    fh = FeedHandler()
    fh.add_feed(Binance(symbols=[symbol], channels=[TRADES], callbacks={TRADES: on_trade}))

    loop = asyncio.get_event_loop()
    # Schedule loop.stop() after `seconds`. FH.run(start_loop=True) will run_forever,
    # then in its finally block it will stop and close feeds/loop cleanly. :contentReference[oaicite:2]{index=2}
    loop.call_later(seconds, loop.stop)

    # Must run on the main thread’s loop; this starts feeds and runs the loop. :contentReference[oaicite:3]{index=3}
    fh.run(start_loop=True, install_signal_handlers=True)

    print(stats.summ("now - eventTime(E) (ms)", stats.e_lat_ns))
    print(stats.summ("now - tradeTime(T) (ms)", stats.t_lat_ns))
    print(stats.summ("eventTime - tradeTime (ms)", stats.et_gap_ms, ms=False))
    print(f"msg_count={stats.n}")

if __name__ == "__main__":
    # plain sync entrypoint (no asyncio.run)
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=120)
    ap.add_argument("--symbol", default="BTC-USDT")
    args = ap.parse_args()
    main(args.seconds, args.symbol)