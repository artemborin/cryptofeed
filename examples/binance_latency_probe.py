# file: binance_latency_probe.py
import asyncio, json, statistics, time
import orjson as fastjson

import uvloop
uvloop.install()

import websockets

WS_URL = "wss://stream.binance.com:9443/ws"

def parse(msg_bytes):
    if fastjson:
        return fastjson.loads(msg_bytes)
    # websockets gives str by default, but we request bytes for speed
    return json.loads(msg_bytes.decode("utf-8"))

def ns():
    return time.time_ns()

def pct(xs, p):
    # fast approximate percentile without numpy
    if not xs:
        return float("nan")
    k = max(0, min(len(xs)-1, int(round((p/100.0)*(len(xs)-1)))))
    return sorted(xs)[k]

async def run(symbol="btcusdt", seconds=60, disable_compression=True):
    stream = f"{symbol}@trade"
    extra = {}
    if disable_compression:
        extra["compression"] = None  # reduce CPU variance from permessage-deflate

    url = f"{WS_URL}/{stream}"
    print(f"Connecting to {url} for {seconds}s …")

    e_lat_ns, t_lat_ns, et_gap_ns = [], [], []
    count = 0
    start = ns()

    async with websockets.connect(url, max_size=None, **extra) as ws:
        while True:
            # Receive the next complete WS message as bytes (fewer copies)
            msg = await ws.recv()
            now = ns()
            if isinstance(msg, str):
                msg = msg.encode("utf-8")
            data = parse(msg)

            # Binance trade payload includes: E (event time ms), T (trade time ms)
            E = data.get("E")  # eventTime
            T = data.get("T")  # tradeTime
            if E is not None:
                e_lat_ns.append(now - (int(E) * 1_000_000))
            if T is not None:
                t_lat_ns.append(now - (int(T) * 1_000_000))
            if E is not None and T is not None: 
                et_gap_ns.append(1_000_000 * (int(E) - int(T)))


            count += 1
            if now - start >= seconds * 1_000_000_000:
                break

    def summarize(label, arr):
        if not arr:
            return f"{label}: no data"
        # convert to ms for readability
        arr_ms = [x / 1_000_000 for x in arr]
        return (f"{label} (ms)  n={len(arr_ms)} | "
                f"p50={pct(arr_ms,50):.1f}  p95={pct(arr_ms,95):.1f}  p99={pct(arr_ms,99):.1f}  "
                f"min={min(arr_ms):.1f}  max={max(arr_ms):.1f}")

    print(summarize("now - eventTime(E)", e_lat_ns))
    print(summarize("now - tradeTime(T)", t_lat_ns))
    print(summarize("eventTime(E) - tradeTime(T)", et_gap_ns))
    print(f"msg_count={count}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="btcusdt", help="e.g. btcusdt, ethusdt")
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--compression", action="store_true", help="enable permessage-deflate")
    args = ap.parse_args()
    asyncio.run(run(symbol=args.symbol.lower(),
                    seconds=args.seconds,
                    disable_compression=not args.compression))