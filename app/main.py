import asyncio
import json
import logging
import os
import sqlite3
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

# Gemini SDK 임포트
try:
    from google import genai
    from google.genai import types
except Exception:
    genai = None

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mexc-monitor")

REST = os.getenv("MEXC_REST_BASE", "https://api.mexc.com").rstrip("/")
WS_URL = os.getenv("MEXC_WS_URL", "wss://contract.mexc.com/edge")
DB_PATH = os.getenv("DB_PATH", "./data/market.db")
DETAIL_LIMIT = int(os.getenv("DETAIL_SYMBOL_LIMIT", "60"))
INTERVALS = [x.strip() for x in os.getenv("KLINE_INTERVALS", "Min1,Min5,Min15,Min60,Hour4").split(",") if x.strip()]
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "120"))
AI_ENABLED = os.getenv("AI_ENABLED", "true").lower() == "true"
AI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
AI_COOLDOWN = int(os.getenv("AI_MIN_SECONDS_BETWEEN_CALLS_PER_SYMBOL", "300"))
AI_HOURLY_LIMIT = int(os.getenv("AI_MAX_CALLS_PER_HOUR", "120"))

# 텔레그램 및 제미나이 환경 변수 설정
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
db_lock = asyncio.Lock()
state_lock = asyncio.Lock()

tickers: dict[str, dict[str, Any]] = {}
candles: dict[str, dict[str, deque]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=500)))
analysis_state: dict[str, dict[str, Any]] = {}
events: deque = deque(maxlen=250)
connected_clients: set[WebSocket] = set()
last_ai_call: dict[str, float] = {}
ai_call_times: deque = deque()
detail_symbols: set[str] = set()
contracts: set[str] = set()
ws_status = {"connected": False, "last_message": None, "last_error": None}
client = httpx.AsyncClient(timeout=20.0, headers={"User-Agent": "MEXC-AI-Market-Monitor/1.0"})

# Gemini 클라이언트 초기화
ai_client = genai.Client(api_key=GEMINI_API_KEY) if (genai and GEMINI_API_KEY) else None


async def send_telegram_message(text: str):
    """텔레그램 봇을 통해 메시지를 전송하는 함수"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram bot token or chat ID is not set.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        response = await client.post(url, json=payload)
        if response.status_code != 200:
            log.warning("Failed to send Telegram message: %s", response.text)
    except Exception as exc:
        log.error("Telegram request error: %s", exc)


def db():
    conn = sqlite3.connect(DB_PATH, timeout=20)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    con = db()
    con.executescript("""
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS ticker_snapshots(
      symbol TEXT NOT NULL, ts INTEGER NOT NULL, last_price REAL, amount24 REAL,
      volume24 REAL, rise_fall_rate REAL, hold_vol REAL, funding_rate REAL,
      PRIMARY KEY(symbol, ts)
    );
    CREATE INDEX IF NOT EXISTS idx_ticker_ts ON ticker_snapshots(ts);
    CREATE TABLE IF NOT EXISTS candles(
      symbol TEXT NOT NULL, interval TEXT NOT NULL, t INTEGER NOT NULL,
      o REAL, h REAL, l REAL, c REAL, v REAL, amount REAL,
      PRIMARY KEY(symbol, interval, t)
    );
    CREATE TABLE IF NOT EXISTS ai_analyses(
      symbol TEXT PRIMARY KEY, updated_at INTEGER, payload TEXT
    );
    """)
    con.close()


async def broadcast(payload: dict):
    dead = []
    message = json.dumps(payload, ensure_ascii=False)
    for ws in list(connected_clients):
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        connected_clients.discard(ws)


async def rest_get(path: str, params: dict | None = None):
    response = await client.get(f"{REST}{path}", params=params)
    response.raise_for_status()
    body = response.json()
    if isinstance(body, dict) and body.get("success") is False:
        raise RuntimeError(f"MEXC API error: {body}")
    return body.get("data", body) if isinstance(body, dict) else body


async def load_contracts():
    global contracts
    data = await rest_get("/api/v1/contract/detail")
    rows = data if isinstance(data, list) else data.get("data", []) if isinstance(data, dict) else []
    found = set()
    for row in rows:
        symbol = row.get("symbol", "")
        if (symbol.endswith("_USDT")
            and row.get("quoteCoin") == "USDT"
            and int(row.get("futureType", 1)) == 1
            and int(row.get("state", 0)) == 0
            and not row.get("isHidden", False)):
            found.add(symbol)
    contracts = found
    log.info("Loaded %s active USDT perpetual contracts", len(contracts))


def normalize_ticker(row: dict) -> dict:
    return {
        "symbol": row.get("symbol"),
        "lastPrice": _num(row.get("lastPrice")),
        "bid1": _num(row.get("bid1")),
        "ask1": _num(row.get("ask1")),
        "amount24": _num(row.get("amount24")),
        "volume24": _num(row.get("volume24")),
        "riseFallRate": _num(row.get("riseFallRate")),
        "holdVol": _num(row.get("holdVol")),
        "fundingRate": _num(row.get("fundingRate")),
        "fairPrice": _num(row.get("fairPrice")),
        "indexPrice": _num(row.get("indexPrice")),
        "high24Price": _num(row.get("high24Price")),
        "lower24Price": _num(row.get("lower24Price")),
        "timestamp": int(row.get("timestamp") or time.time() * 1000),
        "receivedAt": int(time.time() * 1000),
    }


def _num(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


async def save_tickers(rows: list[dict]):
    now = int(time.time() * 1000)
    values = []
    for raw in rows:
        row = normalize_ticker(raw)
        symbol = row.get("symbol")
        if not symbol or not symbol.endswith("_USDT"):
            continue
        previous = tickers.get(symbol, {})
        for field in ("holdVol", "fundingRate", "bid1", "ask1", "indexPrice", "fairPrice"):
            if row.get(field) is None and previous.get(field) is not None:
                row[field] = previous[field]
        tickers[symbol] = row
        values.append((symbol, now, row["lastPrice"], row["amount24"], row["volume24"],
                       row["riseFallRate"], row["holdVol"], row["fundingRate"]))
    if values:
        ws_status["last_message"] = int(time.time() * 1000)
        async with db_lock:
            con = db()
            con.executemany("INSERT OR REPLACE INTO ticker_snapshots VALUES(?,?,?,?,?,?,?,?)", values)
            con.commit()
            con.close()
    await broadcast({"type": "tickers", "data": [tickers[x] for x in tickers if x in {v[0] for v in values}]})


async def load_initial_tickers():
    data = await rest_get("/api/v1/contract/ticker")
    rows = data if isinstance(data, list) else [data] if isinstance(data, dict) and data.get("symbol") else []
    await save_tickers(rows)


def select_detail_symbols():
    ranked = sorted(
        ((s, t.get("amount24") or 0) for s, t in tickers.items() if s in contracts),
        key=lambda x: x[1], reverse=True
    )
    return {s for s, _ in ranked[:DETAIL_LIMIT]}


def parse_kline_payload(symbol: str, interval: str, data: dict):
    ts = int(data.get("t") or time.time())
    return {
        "symbol": symbol, "interval": interval, "t": ts,
        "o": _num(data.get("o")), "h": _num(data.get("h")),
        "l": _num(data.get("l")), "c": _num(data.get("c")),
        "v": _num(data.get("v", data.get("q"))),
        "amount": _num(data.get("a")),
    }


async def save_candle(c: dict):
    if not c["symbol"] or c["c"] is None:
        return
    bucket = candles[c["symbol"]][c["interval"]]
    if bucket and bucket[-1]["t"] == c["t"]:
        bucket[-1] = c
    elif not bucket or c["t"] > bucket[-1]["t"]:
        bucket.append(c)
    async with db_lock:
        con = db()
        con.execute("INSERT OR REPLACE INTO candles VALUES(?,?,?,?,?,?,?,?,?)",
                    (c["symbol"], c["interval"], c["t"], c["o"], c["h"], c["l"], c["c"], c["v"], c["amount"]))
        con.commit()
        con.close()
    await evaluate_symbol(c["symbol"], c["interval"])


async def seed_history(symbol: str, interval: str):
    data = await rest_get(f"/api/v1/contract/kline/{symbol}", {"interval": interval, "limit": HISTORY_LIMIT})
    if not isinstance(data, dict) or "time" not in data:
        return
    times = data.get("time", [])
    keys = ("open", "high", "low", "close", "vol", "amount")
    arrays = [data.get(k, []) for k in keys]
    batch = []
    for i, ts in enumerate(times):
        def at(arr):
            return _num(arr[i]) if i < len(arr) else None
        c = {"symbol": symbol, "interval": interval, "t": int(ts),
             "o": at(arrays[0]), "h": at(arrays[1]), "l": at(arrays[2]),
             "c": at(arrays[3]), "v": at(arrays[4]), "amount": at(arrays[5])}
        if c["c"] is not None:
            batch.append(c)
    if not batch:
        return
    bucket = candles[symbol][interval]
    for c in batch[-500:]:
        if not bucket or c["t"] > bucket[-1]["t"]:
            bucket.append(c)
    async with db_lock:
        con = db()
        con.executemany("INSERT OR REPLACE INTO candles VALUES(?,?,?,?,?,?,?,?,?)",
            [(c["symbol"], c["interval"], c["t"], c["o"], c["h"], c["l"], c["c"], c["v"], c["amount"]) for c in batch])
        con.commit()
        con.close()


def structure_for(symbol: str, interval: str):
    rows = list(candles[symbol][interval])
    if len(rows) < 25:
        return None
    closed = rows[:-1] if len(rows) > 1 else rows
    if len(closed) < 22:
        return None
    last = closed[-1]
    prior = closed[-21:-1]
    high = max(x["h"] for x in prior if x["h"] is not None)
    low = min(x["l"] for x in prior if x["l"] is not None)
    prev_close = closed[-2]["c"]
    direction = "상승" if last["c"] > high else "하락" if last["c"] < low else "범위 내"
    avg_vol = sum((x["v"] or 0) for x in prior) / max(1, len(prior))
    vol_ratio = (last["v"] or 0) / avg_vol if avg_vol else None
    return {
        "interval": interval, "close": last["c"], "high20": high, "low20": low,
        "previousClose": prev_close, "direction": direction,
        "volume": last["v"], "volumeRatio20": round(vol_ratio, 2) if vol_ratio is not None else None,
        "candleTime": last["t"],
    }


async def evaluate_symbol(symbol: str, interval: str):
    if interval not in ("Min1", "Min5", "Min15", "Min60", "Hour4"):
        return
    structures = {tf: structure_for(symbol, tf) for tf in ("Min1", "Min5", "Min15", "Min60", "Hour4")}
    structures = {k: v for k, v in structures.items() if v}
    if not structures:
        return
    signals = [v for v in structures.values() if v["direction"] in ("상승", "하락")]
    if not signals:
        return
    latest = max(v["candleTime"] for v in structures.values())
    previous = analysis_state.get(symbol, {})
    signature = "|".join(f"{k}:{v['direction']}:{v['high20']}:{v['low20']}" for k, v in sorted(structures.items()))
    if previous.get("signature") == signature:
        return
    analysis_state[symbol] = {"signature": signature, "updatedAt": int(time.time()), "structures": structures}
    await maybe_ai_analyze(symbol, structures, latest)


async def maybe_ai_analyze(symbol: str, structures: dict, candle_time: int):
    if not AI_ENABLED or ai_client is None:
        return
    now = time.time()
    while ai_call_times and now - ai_call_times[0] > 3600:
        ai_call_times.popleft()
    if now - last_ai_call.get(symbol, 0) < AI_COOLDOWN or len(ai_call_times) >= AI_HOURLY_LIMIT:
        return
    last_ai_call[symbol] = now
    ai_call_times.append(now)
    ticker = tickers.get(symbol, {})
    payload = {
        "symbol": symbol,
        "ticker": {k: ticker.get(k) for k in ("lastPrice", "amount24", "riseFallRate", "holdVol", "fundingRate")},
        "timeframes": structures,
        "rule": "Use only supplied market facts. Do not invent levels. Distinguish confirmed candle closes from live candle values. No moving averages or pivot indicators. Analysis only; never issue orders."
    }
    try:
        # Gemini 비동기 API 호출
        response = await ai_client.aio.models.generate_content(
            model=AI_MODEL,
            contents=json.dumps(payload, ensure_ascii=False),
            config=types.GenerateContentConfig(
                system_instruction=(
                    "You are a cautious crypto futures market-structure analyst. "
                    "Return Korean concise JSON with keys: bias, summary, supports, resistances, "
                    "bullish_scenario, bearish_scenario, invalidation, confidence, evidence, caveats. "
                    "supports/resistances must be arrays of numeric prices derived only from supplied levels. "
                    "If data is insufficient, say so. Do not provide trade execution instructions or promise outcomes."
                ),
                max_output_tokens=700,
            ),
        )
        text_out = response.text
        result = {"symbol": symbol, "updatedAt": int(time.time()), "raw": text_out,
                  "market": payload, "source": "Gemini"}
        
        # 텔레그램으로 분석 결과 전송
        tg_message = f"🚨 **[Gemini Market Analysis]**\n- 종목: `{symbol}`\n- 결과:\n{text_out}"
        await send_telegram_message(tg_message)

    except Exception as exc:
        log.exception("AI analysis failed for %s", symbol)
        result = {"symbol": symbol, "updatedAt": int(time.time()), "raw": None,
                  "error": str(exc), "market": payload, "source": "AI error"}
        
        # 에러 발생 시 텔레그램 전송
        await send_telegram_message(f"⚠️ **[Gemini Analysis Error]**\n- 종목: `{symbol}`\n- 에러: {exc}")

    analysis_state.setdefault(symbol, {}).update({"ai": result})
    async with db_lock:
        con = db()
        con.execute("INSERT OR REPLACE INTO ai_analyses(symbol,updated_at,payload) VALUES(?,?,?)",
                    (symbol, result["updatedAt"], json.dumps(result, ensure_ascii=False)))
        con.commit()
        con.close()
    event = {"type": "ai_analysis", "data": result}
    events.appendleft(event)
    await broadcast(event)


async def websocket_market_loop():
    backoff = 2
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=None, close_timeout=5, max_size=8_000_000) as ws:
                ws_status["connected"] = True
                ws_status["last_error"] = None
                backoff = 2
                await ws.send(json.dumps({"method": "sub.tickers", "param": {}, "gzip": False}))
                log.info("Subscribed to MEXC global futures tickers")
                async def heartbeat():
                    while True:
                        await asyncio.sleep(15)
                        await ws.send(json.dumps({"method": "ping"}))
                ping_task = asyncio.create_task(heartbeat())
                try:
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            import gzip
                            try:
                                raw = gzip.decompress(raw).decode("utf-8")
                            except Exception:
                                raw = raw.decode("utf-8", errors="ignore")
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        channel = msg.get("channel", "")
                        if channel == "pong":
                            continue
                        if channel == "push.tickers":
                            data = msg.get("data", [])
                            if isinstance(data, list):
                                await save_tickers(data)
                        elif channel == "push.kline":
                            d = msg.get("data", {})
                            symbol = d.get("symbol") or msg.get("symbol")
                            interval = d.get("interval")
                            if symbol and interval:
                                await save_candle(parse_kline_payload(symbol, interval, d))
                finally:
                    ping_task.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            ws_status["connected"] = False
            ws_status["last_error"] = str(exc)
            log.warning("MEXC websocket disconnected: %s", exc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        finally:
            ws_status["connected"] = False


async def candle_websocket_loop():
    backoff = 3
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=None, close_timeout=5, max_size=8_000_000) as ws:
                subscribed: set[tuple[str, str]] = set()

                async def update_subscriptions():
                    while True:
                        desired = select_detail_symbols()
                        detail_symbols.clear()
                        detail_symbols.update(desired)
                        desired_pairs = {(symbol, interval) for symbol in desired for interval in INTERVALS}
                        additions = sorted(desired_pairs - subscribed)
                        removals = sorted(subscribed - desired_pairs)
                        for symbol, interval in removals:
                            await ws.send(json.dumps({
                                "method": "unsub.kline",
                                "param": {"symbol": symbol, "interval": interval},
                                "gzip": False
                            }))
                            subscribed.discard((symbol, interval))
                        for symbol, interval in additions:
                            try:
                                await seed_history(symbol, interval)
                            except Exception as exc:
                                log.debug("History seed failed %s %s: %s", symbol, interval, exc)
                            await ws.send(json.dumps({
                                "method": "sub.kline",
                                "param": {"symbol": symbol, "interval": interval},
                                "gzip": False
                            }))
                            subscribed.add((symbol, interval))
                            await asyncio.sleep(0.025)
                        log.info("Detailed candle subscriptions active: %s symbols / %s streams",
                                 len(desired), len(subscribed))
                        await asyncio.sleep(30)

                async def heartbeat():
                    while True:
                        await asyncio.sleep(15)
                        await ws.send(json.dumps({"method": "ping"}))

                manager_task = asyncio.create_task(update_subscriptions())
                ping_task = asyncio.create_task(heartbeat())
                try:
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            import gzip
                            try:
                                raw = gzip.decompress(raw).decode("utf-8")
                            except Exception:
                                raw = raw.decode("utf-8", errors="ignore")
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if msg.get("channel") == "push.kline":
                            d = msg.get("data", {})
                            symbol = d.get("symbol") or msg.get("symbol")
                            interval = d.get("interval")
                            if symbol and interval:
                                await save_candle(parse_kline_payload(symbol, interval, d))
                finally:
                    manager_task.cancel()
                    ping_task.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Candle websocket reconnect: %s", exc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def contract_refresh_loop():
    while True:
        try:
            await load_contracts()
        except Exception as exc:
            log.warning("Contract refresh failed: %s", exc)
        await asyncio.sleep(1800)


async def ticker_fallback_loop():
    while True:
        await asyncio.sleep(60)
        try:
            await load_initial_tickers()
        except Exception as exc:
            log.warning("Ticker REST refresh failed: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    try:
        await load_contracts()
        await load_initial_tickers()
    except Exception as exc:
        log.warning("Startup market-data warmup failed: %s", exc)
    tasks = [
        asyncio.create_task(websocket_market_loop()),
        asyncio.create_task(candle_websocket_loop()),
        asyncio.create_task(contract_refresh_loop()),
        asyncio.create_task(ticker_fallback_loop()),
    ]
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await client.aclose()


app = FastAPI(title="MEXC AI Market Monitor", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def home():
    return (Path("app/static/index.html")).read_text(encoding="utf-8")


@app.get("/api/status")
async def status():
    return {
        "ws": ws_status,
        "contracts": len(contracts),
        "ticker_symbols": len(tickers),
        "detailed_symbols": len(detail_symbols),
        "ai_enabled": bool(AI_ENABLED and ai_client),
        "ai_model": AI_MODEL if ai_client else None,
        "ai_calls_last_hour": len(ai_call_times),
    }


@app.get("/api/markets")
async def markets(limit: int = 500):
    rows = sorted(tickers.values(), key=lambda x: x.get("amount24") or 0, reverse=True)
    return {"count": len(rows), "data": rows[:max(1, min(limit, 2000))]}


@app.get("/api/market/{symbol}")
async def market_detail(symbol: str):
    symbol = symbol.upper()
    tf_data = {}
    for tf in INTERVALS:
        tf_data[tf] = list(candles[symbol][tf])[-200:]
    return {
        "ticker": tickers.get(symbol),
        "timeframes": tf_data,
        "analysis": analysis_state.get(symbol),
    }


@app.get("/api/analyses")
async def analyses(limit: int = 100):
    con = db()
    rows = con.execute("SELECT symbol,updated_at,payload FROM ai_analyses ORDER BY updated_at DESC LIMIT ?", (limit,)).fetchall()
    con.close()
    return [{"symbol": r["symbol"], "updatedAt": r["updated_at"], "data": json.loads(r["payload"])} for r in rows]


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        await websocket.send_json({"type": "snapshot", "markets": sorted(tickers.values(), key=lambda x: x.get("amount24") or 0, reverse=True)[:500],
                                   "status": await status()})
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        connected_clients.discard(websocket)
