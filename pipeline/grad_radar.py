"""Pump.fun radar with two alert lanes, both behind a RugCheck safety gate.

  EARLY lane (pre-graduation): low-cap coins still on the bonding curve with
      broad, accelerating buyer demand. Earlier and cheaper, but riskier.
  GRADUATED lane: coins that just moved to PumpSwap with real momentum.
      Later and pricier, but liquidity and LP lock are verifiable.

A coin can alert in both lanes: an Early alert followed by a Graduated alert
is confirmation that the early read was right.

Each run (every ~5 min on GitHub Actions, or `--loop SECONDS` on an always-on host):
  1. DISCOVER  GeckoTerminal (free, no key): newest Solana pools plus the most
               active Pump.fun bonding-curve and PumpSwap pools. Coins showing
               early buyers go on a watchlist (kept for `watch_hours`).
  2. GRADUATE  DexScreener (free, no key) prices every watched coin in batches
               of 30. A coin counts as graduated once it has a PumpSwap pair;
               that pair's creation time is the graduation time.
  3. MOMENTUM  Graduated within `max_minutes_since_graduation`, market cap in
               band, enough liquidity, buyers leading sellers, not dumping.
  4. SAFETY    RugCheck report must pass every hard gate (mint/freeze revoked,
               no transfer fee, not rugged, no danger-level risk, LP locked,
               top-10 concentration and insider limits). Soft flags, such as a
               cluster of similar-sized top wallets, ride along in the alert.
  5. ALERT     Telegram message with the numbers, flags and quick links.
               Each coin alerts once.

Run:  python -m pipeline.grad_radar            (one scan)
      python -m pipeline.grad_radar --dry-run  (print alerts, send nothing)
      python -m pipeline.grad_radar --loop 30  (scan every 30s, always-on host)
      python -m pipeline.grad_radar --check <MINT>  (score one coin now)

Rules are hypotheses to tune from the outcome log, not backtested signals.
"""
from __future__ import annotations

import argparse
import calendar
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RADAR_DIR = ROOT / ".radar"
STATE_PATH = RADAR_DIR / "grad_state.json"

GECKO = "https://api.geckoterminal.com/api/v2"
DEXS = "https://api.dexscreener.com"
RUGCHECK = "https://api.rugcheck.xyz/v1"
UA = {"Accept": "application/json", "User-Agent": "signal-audit-grad-radar/1.0"}

DEFAULTS = {
    "enabled": True,
    # Discovery
    "watch_min_buyers_h1": 25,          # bonding-curve coin needs this many unique buyers to be watched
    "watch_hours": 6,                   # drop a watched coin that has not graduated after this long
    # Graduation + momentum
    "max_minutes_since_graduation": 45,
    "min_market_cap": 20000,
    "max_market_cap": 150000,
    "min_liquidity_usd": 8000,
    "min_liquidity_to_mc": 0.10,        # exit depth: pool liquidity / market cap
    "min_buys_since_graduation": 150,   # DexScreener h1 buys on the graduated pair
    "min_buy_sell_ratio": 1.15,
    "max_drop_5m_pct": -15,             # skip coins already dumping
    # RugCheck hard gates (any failure blocks the alert)
    "max_top10_pct": 30,                # top 10 holders, excluding pools/AMMs
    "max_single_holder_pct": 10,
    "min_lp_locked_pct": 90,
    "max_insider_wallets": 0,           # RugCheck graphInsidersDetected
    "block_on_danger_risk": True,
    # RugCheck soft flags (shown in alert, do not block unless set)
    "cluster_min_pct": 2.5,             # holders at or above this size...
    "cluster_band_pct": 0.75,           # ...within this spread of each other...
    "cluster_min_count": 4,             # ...this many of them = similar-size cluster flag
    "block_on_cluster": False,
    "require_socials": False,
    # EARLY lane (pre-graduation, bonding curve)
    "early_enabled": True,
    "early_max_age_minutes": 120,       # coin age since launch
    "early_min_market_cap": 8000,
    "early_max_market_cap": 45000,      # stay well below graduation
    "early_min_unique_buyers_h1": 60,   # distinct wallets, not trade count
    "early_min_buyers_15m": 20,         # demand still active right now
    "early_min_buyer_seller_ratio": 1.3,
    "early_min_volume_h1": 5000,
    "early_max_drop_5m_pct": -15,
    "early_require_buyer_growth": True, # unique buyers rising vs the previous scan
    "early_max_top10_pct": 25,          # stricter than graduated lane
    "early_max_single_holder_pct": 8,
    "early_max_dev_pct": 5,             # dev still holding a big bag = dump risk
    "early_require_socials": False,
    # Outcome log (feeds tuning)
    "track_hours": 24,
}


# --------------------------------------------------------------------------- utils
def log(msg: str) -> None:
    print(f"[grad-radar {time.strftime('%H:%M:%S')}] {msg}", flush=True)


class RateLimited(Exception):
    pass


def http_json(url: str, retries: int = 3, backoff: float = 10.0):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                if attempt < retries:
                    wait = fnum(e.headers.get("Retry-After"), 0) or backoff * (attempt + 1)
                    time.sleep(min(wait, 60))
                    continue
                raise RateLimited(url.split("?")[0])
            if e.code in (400, 404):
                return None
            raise
        except (urllib.error.URLError, TimeoutError):
            if attempt < retries:
                time.sleep(backoff)
                continue
            raise
    return None


def fnum(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def load_config() -> dict:
    cfg = dict(DEFAULTS)
    path = ROOT / "config.yaml"
    if path.exists():
        try:
            import yaml  # already in signal-audit requirements
            data = yaml.safe_load(path.read_text()) or {}
            cfg.update(data.get("grad_radar") or {})
        except Exception as e:  # never let a config typo kill the scan
            log(f"config.yaml grad_radar section unreadable, using defaults ({e})")
    return cfg


def load_state() -> dict:
    RADAR_DIR.mkdir(exist_ok=True)
    if STATE_PATH.exists():
        try:
            return _upgrade(json.loads(STATE_PATH.read_text()))
        except json.JSONDecodeError:
            pass
    return {"watch": {}, "alerted": {}, "alerted_early": {}, "blocked": {}, "last_scan": None, "last_error": None}


def _upgrade(state: dict) -> dict:
    state.setdefault("alerted_early", {})
    return state


def save_state(state: dict) -> None:
    RADAR_DIR.mkdir(exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=1))


# ----------------------------------------------------------------------- discover
def discover(cfg: dict, state: dict) -> None:
    """Add promising bonding-curve coins and fresh PumpSwap coins to the watchlist."""
    now = time.time()
    feeds = [
        f"{GECKO}/networks/solana/new_pools?page=1",
        f"{GECKO}/networks/solana/new_pools?page=2",
        f"{GECKO}/networks/solana/dexes/pump-fun/pools?page=1&sort=h24_volume_usd_desc",
        f"{GECKO}/networks/solana/dexes/pump-fun/pools?page=2&sort=h24_volume_usd_desc",
        f"{GECKO}/networks/solana/dexes/pumpswap/pools?page=1&sort=h24_volume_usd_desc",
        f"{GECKO}/networks/solana/dexes/pumpswap/pools?page=2&sort=h24_volume_usd_desc",
    ]
    added = 0
    skipped = 0
    for url in feeds:
        try:
            data = http_json(url) or {}
        except RateLimited:
            skipped += 1
            time.sleep(15)
            continue
        for pool in data.get("data", []):
            try:
                dex = pool["relationships"]["dex"]["data"]["id"]
                mint = pool["relationships"]["base_token"]["data"]["id"].split("_", 1)[1]
                a = pool["attributes"]
            except (KeyError, IndexError, TypeError):
                continue
            if dex not in ("pump-fun", "pumpswap"):
                continue
            buyers = int(fnum(a.get("transactions", {}).get("h1", {}).get("buyers")))
            created = a.get("pool_created_at") or ""
            fresh_swap = dex == "pumpswap" and _iso_age_min(created, now) <= cfg["max_minutes_since_graduation"]
            if (dex == "pump-fun" and buyers >= cfg["watch_min_buyers_h1"]) or fresh_swap:
                if mint not in state["watch"] and mint not in state["alerted"]:
                    state["watch"][mint] = {"first_seen": now, "name": a.get("name", "")}
                    added += 1
            if dex == "pump-fun" and mint in state["watch"]:
                w = state["watch"][mint]
                prev = (w.get("early") or {}).get("buyers_h1")
                w["early"] = _early_snapshot(a, now)
                if prev is not None:
                    w["early"]["prev_buyers_h1"] = prev
        time.sleep(2.2)  # GeckoTerminal free tier: ~30 requests/minute
    # expire stale watch entries
    cutoff = now - cfg["watch_hours"] * 3600
    for mint in [m for m, w in state["watch"].items() if w["first_seen"] < cutoff]:
        state["watch"].pop(mint, None)
    log(f"discovery: +{added} watched, {len(state['watch'])} on watchlist"
        + (f" ({skipped} feed(s) rate-limited, skipped)" if skipped else ""))


def _early_snapshot(a: dict, now: float) -> dict:
    tx = a.get("transactions") or {}
    return {
        "at": now,
        "mc": fnum(a.get("market_cap_usd") or a.get("fdv_usd")),
        "age_min": _iso_age_min(a.get("pool_created_at") or "", now),
        "buyers_h1": int(fnum(tx.get("h1", {}).get("buyers"))),
        "sellers_h1": int(fnum(tx.get("h1", {}).get("sellers"))),
        "buyers_15m": int(fnum(tx.get("m15", {}).get("buyers"))),
        "buys_5m": int(fnum(tx.get("m5", {}).get("buys"))),
        "sells_5m": int(fnum(tx.get("m5", {}).get("sells"))),
        "chg_5m": fnum((a.get("price_change_percentage") or {}).get("m5")),
        "chg_1h": fnum((a.get("price_change_percentage") or {}).get("h1")),
        "vol_1h": fnum((a.get("volume_usd") or {}).get("h1")),
        "name": a.get("name", ""),
        "pool": a.get("address", ""),
    }


def _iso_age_min(iso: str, now: float) -> float:
    try:
        t = calendar.timegm(time.strptime(iso.replace("Z", "")[:19], "%Y-%m-%dT%H:%M:%S"))
        return (now - t) / 60
    except ValueError:
        return 1e9


# ---------------------------------------------------------------------- graduation
def dexscreener_pairs(mints: list[str]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for i in range(0, len(mints), 30):
        chunk = mints[i:i + 30]
        try:
            data = http_json(f"{DEXS}/tokens/v1/solana/{','.join(chunk)}") or []
        except RateLimited:
            data = []
        for pair in data if isinstance(data, list) else []:
            mint = pair.get("baseToken", {}).get("address")
            if mint:
                out.setdefault(mint, []).append(pair)
        time.sleep(0.3)
    return out


def graduated_pair(pairs: list[dict]) -> dict | None:
    """The PumpSwap pair with the most liquidity, or None if not graduated yet."""
    swap = [p for p in pairs if p.get("dexId") == "pumpswap"]
    if not swap:
        return None
    return max(swap, key=lambda p: fnum((p.get("liquidity") or {}).get("usd")))


def momentum_check(pair: dict, cfg: dict, now: float) -> tuple[bool, list[str], dict]:
    m = {
        "mc": fnum(pair.get("marketCap") or pair.get("fdv")),
        "liq": fnum((pair.get("liquidity") or {}).get("usd")),
        "buys": int(fnum(pair.get("txns", {}).get("h1", {}).get("buys"))),
        "sells": int(fnum(pair.get("txns", {}).get("h1", {}).get("sells"))),
        "buys_5m": int(fnum(pair.get("txns", {}).get("m5", {}).get("buys"))),
        "sells_5m": int(fnum(pair.get("txns", {}).get("m5", {}).get("sells"))),
        "chg_5m": fnum(pair.get("priceChange", {}).get("m5")),
        "chg_1h": fnum(pair.get("priceChange", {}).get("h1")),
        "vol_1h": fnum(pair.get("volume", {}).get("h1")),
        "grad_min": (now * 1000 - fnum(pair.get("pairCreatedAt"), now * 1000)) / 60000,
        "quote": pair.get("quoteToken", {}).get("symbol", "?"),
        "socials": len((pair.get("info") or {}).get("socials") or []) + len((pair.get("info") or {}).get("websites") or []),
        "pair_url": pair.get("url", ""),
        "name": pair.get("baseToken", {}).get("name", ""),
        "symbol": pair.get("baseToken", {}).get("symbol", ""),
    }
    fails = []
    if m["grad_min"] > cfg["max_minutes_since_graduation"]:
        fails.append(f"graduated {m['grad_min']:.0f}m ago")
    if not cfg["min_market_cap"] <= m["mc"] <= cfg["max_market_cap"]:
        fails.append(f"MC ${m['mc']:,.0f} out of band")
    if m["liq"] < cfg["min_liquidity_usd"]:
        fails.append(f"liquidity ${m['liq']:,.0f}")
    if m["mc"] and m["liq"] / m["mc"] < cfg["min_liquidity_to_mc"]:
        fails.append(f"liq/MC {m['liq'] / m['mc']:.0%}")
    if m["buys"] < cfg["min_buys_since_graduation"]:
        fails.append(f"{m['buys']} buys")
    if m["buys"] < cfg["min_buy_sell_ratio"] * max(m["sells"], 1):
        fails.append(f"buys/sells {m['buys']}/{m['sells']}")
    if m["sells"] == 0 and m["buys"] > 50:
        fails.append("zero sells (possible sell block)")
    if m["chg_5m"] < cfg["max_drop_5m_pct"]:
        fails.append(f"5m {m['chg_5m']:.0f}%")
    if cfg["require_socials"] and not m["socials"]:
        fails.append("no socials")
    return (not fails), fails, m


def early_check(e: dict, cfg: dict) -> tuple[bool, list[str]]:
    fails = []
    if e["age_min"] > cfg["early_max_age_minutes"]:
        fails.append(f"age {e['age_min']:.0f}m")
    if not cfg["early_min_market_cap"] <= e["mc"] <= cfg["early_max_market_cap"]:
        fails.append(f"MC ${e['mc']:,.0f} out of band")
    if e["buyers_h1"] < cfg["early_min_unique_buyers_h1"]:
        fails.append(f"{e['buyers_h1']} unique buyers")
    if e["buyers_15m"] < cfg["early_min_buyers_15m"]:
        fails.append(f"{e['buyers_15m']} buyers in 15m")
    if e["buyers_h1"] < cfg["early_min_buyer_seller_ratio"] * max(e["sellers_h1"], 1):
        fails.append(f"buyers/sellers {e['buyers_h1']}/{e['sellers_h1']}")
    if e["sellers_h1"] == 0 and e["buyers_h1"] > 30:
        fails.append("zero sellers (possible sell block)")
    if e["vol_1h"] < cfg["early_min_volume_h1"]:
        fails.append(f"1h volume ${e['vol_1h']:,.0f}")
    if e["chg_5m"] < cfg["early_max_drop_5m_pct"]:
        fails.append(f"5m {e['chg_5m']:.0f}%")
    if cfg["early_require_buyer_growth"]:
        prev = e.get("prev_buyers_h1")
        if prev is None:
            fails.append("waiting for a second snapshot")
        elif e["buyers_h1"] <= prev:
            fails.append(f"buyers flat ({prev}->{e['buyers_h1']})")
    return (not fails), fails


# -------------------------------------------------------------------------- safety
def rugcheck_gate(mint: str, cfg: dict, report: dict | None = None,
                  lane: str = "grad") -> tuple[bool, list[str], list[str], dict]:
    """Returns (passed, hard_failures, soft_flags, facts). lane = 'grad' or 'early'."""
    early = lane == "early"
    max_top10 = cfg["early_max_top10_pct"] if early else cfg["max_top10_pct"]
    max_single = cfg["early_max_single_holder_pct"] if early else cfg["max_single_holder_pct"]
    if report is None:
        try:
            report = http_json(f"{RUGCHECK}/tokens/{mint}/report")
        except RateLimited:
            report = None
    if not report:
        return False, ["RugCheck report unavailable"], [], {}

    hard, soft = [], []
    token = report.get("token") or {}
    if token.get("mintAuthority") or report.get("mintAuthority"):
        hard.append("mint authority active")
    if token.get("freezeAuthority") or report.get("freezeAuthority"):
        hard.append("freeze authority active")
    if report.get("rugged"):
        hard.append("flagged as rugged")
    if fnum((report.get("transferFee") or {}).get("pct")) > 0:
        hard.append("transfer fee")
    ext = report.get("token_extensions") or {}
    if ext.get("permanentDelegate") or ext.get("transferHook") or ext.get("pausableConfig"):
        hard.append("dangerous token extension")
    danger = [r.get("name", "?") for r in report.get("risks") or [] if r.get("level") == "danger"]
    if danger and cfg["block_on_danger_risk"]:
        hard.append("danger: " + ", ".join(danger[:3]))
    for r in report.get("risks") or []:
        if r.get("level") == "warn":
            soft.append(r.get("name", "warning"))

    # Exclude pools/AMMs/lockers from holder concentration
    market_ids = {m.get("pubkey") for m in report.get("markets") or []}
    known_amm = {k for k, v in (report.get("knownAccounts") or {}).items() if v.get("type") in ("AMM", "LOCKER")}
    excluded = market_ids | known_amm
    holders = [h for h in report.get("topHolders") or []
               if h.get("owner") not in excluded and h.get("address") not in excluded]
    pcts = sorted((fnum(h.get("pct")) for h in holders), reverse=True)
    top10 = sum(pcts[:10])
    if top10 > max_top10:
        hard.append(f"top 10 hold {top10:.1f}%")
    if pcts and pcts[0] > max_single:
        hard.append(f"one wallet holds {pcts[0]:.1f}%")

    insiders = int(fnum(report.get("graphInsidersDetected")))
    if insiders > cfg["max_insider_wallets"]:
        hard.append(f"{insiders} insider wallets linked")

    # LP lock on the PumpSwap (pump_fun_amm) market
    lp_pct = None
    for m in report.get("markets") or []:
        if m.get("marketType") == "pump_fun_amm":
            lp_pct = fnum((m.get("lp") or {}).get("lpLockedPct"))
            break
    if early:
        pass  # bonding curve: no LP yet; Pump.fun locks it at graduation
    elif lp_pct is None:
        soft.append("LP lock not reported")
    elif lp_pct < cfg["min_lp_locked_pct"]:
        hard.append(f"LP only {lp_pct:.0f}% locked")

    # Similar-size cluster (possible bundled snipers)
    cluster = _largest_cluster([p for p in pcts[:20] if p >= cfg["cluster_min_pct"]], cfg["cluster_band_pct"])
    if len(cluster) >= cfg["cluster_min_count"]:
        msg = f"{len(cluster)} wallets at {min(cluster):.1f}-{max(cluster):.1f}% ({sum(cluster):.0f}% combined)"
        (hard if cfg["block_on_cluster"] else soft).append("similar-size cluster: " + msg)

    supply = fnum(token.get("supply"))
    dev_pct = 100 * fnum(report.get("creatorBalance")) / supply if supply else 0.0
    if fnum(report.get("creatorBalance")) == 0:
        soft.append("dev wallet holds 0 (already exited)")
    elif early and dev_pct > cfg["early_max_dev_pct"]:
        hard.append(f"dev still holds {dev_pct:.1f}%")
    elif dev_pct > 0:
        soft.append(f"dev holds {dev_pct:.1f}%")

    prior = report.get("creatorTokens") or []
    if isinstance(prior, list) and prior:
        soft.append(f"dev has launched {len(prior)} other token(s): check their history on GMGN")

    facts = {
        "holders": int(fnum(report.get("totalHolders"))),
        "top10": top10,
        "lp_pct": lp_pct,
        "score": report.get("score_normalised", report.get("score")),
        "creator": report.get("creator", ""),
        "dev_pct": dev_pct,
    }
    return (not hard), hard, soft, facts


def _largest_cluster(values: list[float], band: float) -> list[float]:
    values = sorted(values)
    best: list[float] = []
    j = 0
    for i in range(len(values)):
        while values[i] - values[j] > band:
            j += 1
        if i - j + 1 > len(best):
            best = values[j:i + 1]
    return best


# --------------------------------------------------------------------------- alert
def format_alert(mint: str, m: dict, facts: dict, soft: list[str]) -> str:
    flags = "\n".join(f"  ⚠️ {s}" for s in soft) or "  none"
    return (
        f"🎓 GRADUATED: {m['name']} (${m['symbol']})\n"
        f"Graduated {m['grad_min']:.0f}m ago · paired with {m['quote']}\n\n"
        f"MC ${m['mc']:,.0f} · Liq ${m['liq']:,.0f} ({m['liq'] / max(m['mc'], 1):.0%})\n"
        f"Buys/Sells since grad {m['buys']}/{m['sells']} · 5m {m['buys_5m']}/{m['sells_5m']}\n"
        f"Price 5m {m['chg_5m']:+.0f}% · 1h {m['chg_1h']:+.0f}% · Vol 1h ${m['vol_1h']:,.0f}\n"
        f"Holders {facts.get('holders', 0):,} · Top10 {facts.get('top10', 0):.1f}% · LP locked {facts.get('lp_pct') or 0:.0f}%\n\n"
        f"✅ RugCheck gate passed\nFlags:\n{flags}\n\n"
        f"CA: {mint}\n"
        f"GMGN: https://gmgn.ai/sol/token/{mint}\n"
        f"RugCheck: https://rugcheck.xyz/tokens/{mint}\n"
        f"Bubblemaps: https://v2.bubblemaps.io/map?address={mint}&chain=solana\n"
        f"Chart: {m['pair_url']}\n\n"
        f"Alert, not advice. Check Bundle % on GMGN before acting."
    )


def send_telegram(text: str) -> bool:
    token, chat = os.environ.get("TG_BOT_TOKEN"), os.environ.get("TG_CHAT_ID")
    if not token or not chat:
        log("Telegram bot secrets missing (TG_BOT_TOKEN / TG_CHAT_ID); alert printed only")
        print(text)
        return False
    body = urllib.parse.urlencode({"chat_id": chat, "text": text, "disable_web_page_preview": "true"}).encode()
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=body)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r).get("ok", False)
    except urllib.error.HTTPError as e:
        log(f"Telegram error {e.code}: {e.read()[:200]!r}")
        return False


def format_early_alert(mint: str, e: dict, facts: dict, soft: list[str]) -> str:
    flags = "\n".join(f"  ⚠️ {s}" for s in soft) or "  none"
    grow = f" (was {e['prev_buyers_h1']})" if e.get("prev_buyers_h1") is not None else ""
    return (
        f"🌱 EARLY (pre-graduation): {e['name']}\n"
        f"Age {e['age_min']:.0f}m · still on the Pump.fun bonding curve\n\n"
        f"MC ${e['mc']:,.0f}\n"
        f"Unique buyers 1h {e['buyers_h1']}{grow} vs sellers {e['sellers_h1']} · 15m buyers {e['buyers_15m']}\n"
        f"Price 5m {e['chg_5m']:+.0f}% · 1h {e['chg_1h']:+.0f}% · Vol 1h ${e['vol_1h']:,.0f}\n"
        f"Holders {facts.get('holders', 0):,} · Top10 {facts.get('top10', 0):.1f}% · Dev {facts.get('dev_pct', 0):.1f}%\n\n"
        f"✅ RugCheck gate passed (early rules)\nFlags:\n{flags}\n\n"
        f"CA: {mint}\n"
        f"GMGN (check Dev tab + Bundle %): https://gmgn.ai/sol/token/{mint}\n"
        f"RugCheck: https://rugcheck.xyz/tokens/{mint}\n"
        f"Bubblemaps: https://v2.bubblemaps.io/map?address={mint}&chain=solana\n\n"
        f"Higher risk than a graduated alert. Size small. Alert, not advice."
    )


# ---------------------------------------------------------------------------- main
def evaluate(mint: str, pairs: list[dict], cfg: dict, now: float, report: dict | None = None) -> dict:
    pair = graduated_pair(pairs)
    if not pair:
        return {"status": "not_graduated"}
    ok, fails, m = momentum_check(pair, cfg, now)
    if not ok:
        return {"status": "momentum_fail", "reasons": fails, "metrics": m}
    passed, hard, soft, facts = rugcheck_gate(mint, cfg, report)
    if not passed:
        return {"status": "safety_fail", "reasons": hard, "metrics": m}
    return {"status": "alert", "metrics": m, "facts": facts, "soft": soft}


def track_outcomes(state: dict, pairs_by_mint: dict, cfg: dict, now: float) -> None:
    """Log peak and latest MC after each alert so thresholds can be tuned from real data."""
    for mint, a in state.get("alerted_early", {}).items():
        if a.get("blocked") or now - a["at"] > cfg["track_hours"] * 3600:
            continue
        pairs = pairs_by_mint.get(mint, [])
        if pairs:
            mc = max(fnum(p.get("marketCap") or p.get("fdv")) for p in pairs)
            a["last_mc"], a["peak_mc"] = mc, max(a.get("peak_mc", 0), mc)
            a["graduated"] = a.get("graduated") or bool(graduated_pair(pairs))
    for mint, a in state["alerted"].items():
        if now - a["at"] > cfg["track_hours"] * 3600 or mint not in pairs_by_mint:
            continue
        pair = graduated_pair(pairs_by_mint[mint])
        if pair:
            mc = fnum(pair.get("marketCap") or pair.get("fdv"))
            a["last_mc"] = mc
            a["peak_mc"] = max(a.get("peak_mc", 0), mc)


def scan(cfg: dict, dry_run: bool = False) -> int:
    state = load_state()
    now = time.time()
    try:
        discover(cfg, state)
        tracked = [m for m, a in list(state["alerted"].items()) + list(state["alerted_early"].items())
                   if not a.get("blocked") and now - a["at"] < cfg["track_hours"] * 3600]
        mints = list(dict.fromkeys(list(state["watch"]) + tracked))
        pairs_by_mint = dexscreener_pairs(mints)
        track_outcomes(state, pairs_by_mint, cfg, now)

        sent = 0
        if cfg.get("early_enabled", True):
            sent += scan_early(state, pairs_by_mint, cfg, now, dry_run)
        for mint in list(state["watch"]):
            res = evaluate(mint, pairs_by_mint.get(mint, []), cfg, now)
            if res["status"] == "not_graduated":
                continue
            if res["status"] == "alert":
                text = format_alert(mint, res["metrics"], res["facts"], res["soft"])
                if dry_run:
                    print(text)
                    delivered = True
                else:
                    delivered = send_telegram(text)
                log(f"ALERT grad {mint} {res['metrics']['symbol']} mc={res['metrics']['mc']:.0f} delivered={bool(delivered)}")
                state["alerted"][mint] = {"at": now, "mc": res["metrics"]["mc"], "peak_mc": res["metrics"]["mc"],
                                          "symbol": res["metrics"]["symbol"], "delivered": bool(delivered)}
                state["watch"].pop(mint, None)
                sent += 1
                time.sleep(1.2 if not dry_run else 0)
            elif res["status"] == "safety_fail" and res["reasons"] == ["RugCheck report unavailable"]:
                continue  # temporary outage or rate limit: retry next scan
            elif res["status"] == "safety_fail":
                state["blocked"][mint] = {"at": now, "reasons": res["reasons"], "symbol": res["metrics"]["symbol"]}
                state["watch"].pop(mint, None)  # a failed safety gate is final
            elif res["metrics"]["grad_min"] > cfg["max_minutes_since_graduation"]:
                state["watch"].pop(mint, None)  # window closed
        # keep the blocked log small
        state["blocked"] = dict(sorted(state["blocked"].items(), key=lambda kv: -kv[1]["at"])[:200])
        state["last_scan"], state["last_error"] = now, None
        log(f"scan done: {sent} alert(s), {len(state['blocked'])} blocked on file")
        return sent
    except Exception as e:  # record, don't crash the schedule
        state["last_error"] = f"{type(e).__name__}: {e}"
        log(f"scan error: {state['last_error']}")
        return 0
    finally:
        save_state(state)


def scan_early(state: dict, pairs_by_mint: dict, cfg: dict, now: float, dry_run: bool) -> int:
    sent = 0
    for mint, w in list(state["watch"].items()):
        e = w.get("early")
        if not e or mint in state["alerted_early"] or now - e["at"] > 600:
            continue
        if graduated_pair(pairs_by_mint.get(mint, [])):
            continue  # already graduated: the graduated lane handles it
        ok, _ = early_check(e, cfg)
        if not ok:
            continue
        passed, hard, soft, facts = rugcheck_gate(mint, cfg, lane="early")
        if hard == ["RugCheck report unavailable"]:
            continue  # temporary: retry next scan
        if not passed:
            state["blocked"][mint] = {"at": now, "reasons": hard, "symbol": e["name"], "lane": "early"}
            state["alerted_early"][mint] = {"at": now, "mc": e["mc"], "blocked": True}  # don't recheck
            continue
        if cfg["early_require_socials"]:
            pairs = pairs_by_mint.get(mint, [])
            if not any((p.get("info") or {}).get("socials") for p in pairs):
                continue
        text = format_early_alert(mint, e, facts, soft)
        if dry_run:
            print(text); delivered = True
        else:
            delivered = send_telegram(text)
        log(f"ALERT early {mint} {e['name']} mc={e['mc']:.0f} delivered={bool(delivered)}")
        state["alerted_early"][mint] = {"at": now, "mc": e["mc"], "peak_mc": e["mc"], "symbol": e["name"],
                                        "delivered": bool(delivered)}
        sent += 1
        time.sleep(0 if dry_run else 1.2)
    return sent


def check_one(mint: str, cfg: dict) -> None:
    pairs = dexscreener_pairs([mint]).get(mint, [])
    if graduated_pair(pairs):
        res = evaluate(mint, pairs, dict(cfg, max_minutes_since_graduation=10**9), time.time())
    else:
        data = http_json(f"{GECKO}/networks/solana/tokens/{mint}/pools") or {}
        pools = [p for p in data.get("data", []) if p.get("relationships", {}).get("dex", {}).get("data", {}).get("id") == "pump-fun"]
        if not pools:
            print(json.dumps({"status": "no bonding-curve pool found"})); return
        e = _early_snapshot(pools[0]["attributes"], time.time())
        ok, fails = early_check(e, dict(cfg, early_require_buyer_growth=False))
        passed, hard, soft, facts = rugcheck_gate(mint, cfg, lane="early")
        res = {"lane": "early", "momentum_ok": ok, "momentum_fails": fails,
               "safety_ok": passed, "safety_fails": hard, "flags": soft, "metrics": e, "facts": facts}
    print(json.dumps(res, indent=1, default=str))


def summarise() -> dict:
    """For the daily report: alerts, hit rate, and top block reasons."""
    s = load_state()
    early = [a for a in s.get("alerted_early", {}).values() if not a.get("blocked")]
    alerts = list(s["alerted"].values())
    doubled = [a for a in alerts if a.get("peak_mc", 0) >= 2 * a.get("mc", 1)]
    reasons: dict[str, int] = {}
    for b in s["blocked"].values():
        for r in b["reasons"]:
            key = r.split(":")[0].split(" ")[0:3]
            reasons[" ".join(key)] = reasons.get(" ".join(key), 0) + 1
    return {"last_scan": s["last_scan"], "last_error": s["last_error"],
            "early_alerts": len(early),
            "early_hit_2x": sum(1 for a in early if a.get("peak_mc", 0) >= 2 * a.get("mc", 1)),
            "early_graduated": sum(1 for a in early if a.get("graduated")),
            "grad_alerts": len(alerts),
            "grad_hit_2x": len(doubled), "watching": len(s["watch"]),
            "top_block_reasons": sorted(reasons.items(), key=lambda kv: -kv[1])[:5]}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--loop", type=int, default=0, help="seconds between scans")
    ap.add_argument("--max-minutes", type=float, default=0, help="stop looping after this many minutes")
    ap.add_argument("--check", metavar="MINT")
    ap.add_argument("--summary", action="store_true")
    ap.add_argument("--test-alert", action="store_true", help="send one test message to confirm Telegram works")
    args = ap.parse_args()
    cfg = load_config()
    if not cfg.get("enabled", True):
        log("grad_radar disabled in config.yaml"); sys.exit(0)
    if args.check:
        check_one(args.check, cfg); sys.exit(0)
    if args.test_alert:
        ok = send_telegram("✅ Pump.fun radar is connected (Early + Graduated lanes). Real alerts will appear in this chat.")
        log("test alert sent" if ok else "test alert FAILED: check TG_BOT_TOKEN and TG_CHAT_ID")
        sys.exit(0 if ok else 1)
    if args.summary:
        print(json.dumps(summarise(), indent=1)); sys.exit(0)
    started = time.time()
    scans = 0
    while True:
        t0 = time.time()
        scan(cfg, dry_run=args.dry_run)
        scans += 1
        if not args.loop:
            break
        if args.max_minutes and (time.time() - started) / 60 >= args.max_minutes:
            log(f"time budget reached after {scans} scans; exiting so state is saved")
            break
        time.sleep(max(5, args.loop - (time.time() - t0)))
