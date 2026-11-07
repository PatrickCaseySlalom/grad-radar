"""Offline checks for the graduation radar. Run: python -m tests.test_grad_radar"""
import contextlib
import copy
import io
import json
import time
from pathlib import Path

from pipeline import grad_radar as g

CFG = dict(g.DEFAULTS)
REPORT = json.loads((Path(__file__).parent / "fixture_worthless_rugcheck.json").read_text())


def pair(mc=60000, liq=15000, buys=400, sells=250, chg5=5, grad_min=20):
    return {"dexId": "pumpswap", "marketCap": mc, "liquidity": {"usd": liq},
            "txns": {"h1": {"buys": buys, "sells": sells}, "m5": {"buys": 40, "sells": 20}},
            "priceChange": {"m5": chg5, "h1": 120}, "volume": {"h1": 90000},
            "pairCreatedAt": (time.time() - grad_min * 60) * 1000,
            "quoteToken": {"symbol": "SOL"}, "baseToken": {"name": "Test", "symbol": "TST"}, "url": ""}


def test_clean_coin_alerts():
    res = g.evaluate("mint", [pair()], CFG, time.time(), REPORT)
    assert res["status"] == "alert", res
    assert any("cluster" in s for s in res["soft"])


def test_not_graduated():
    p = pair(); p["dexId"] = "pumpfun"
    assert g.evaluate("mint", [p], CFG, time.time(), REPORT)["status"] == "not_graduated"


def test_momentum_failures():
    for p in (pair(grad_min=90), pair(mc=400000), pair(liq=3000), pair(buys=100, sells=95), pair(chg5=-30)):
        assert g.evaluate("mint", [p], CFG, time.time(), REPORT)["status"] == "momentum_fail"


def test_safety_failures():
    bad = copy.deepcopy(REPORT); bad["token"]["freezeAuthority"] = "X"
    assert g.evaluate("mint", [pair()], CFG, time.time(), bad)["status"] == "safety_fail"
    bad = copy.deepcopy(REPORT); bad["graphInsidersDetected"] = 2
    assert g.evaluate("mint", [pair()], CFG, time.time(), bad)["status"] == "safety_fail"
    strict = dict(CFG, block_on_cluster=True)
    assert g.evaluate("mint", [pair()], strict, time.time(), REPORT)["status"] == "safety_fail"


def early(**kw):
    e = {"at": time.time(), "mc": 25000, "age_min": 40, "buyers_h1": 120, "sellers_h1": 60, "buyers_15m": 45,
         "buys_5m": 30, "sells_5m": 12, "chg_5m": 8, "chg_1h": 150, "vol_1h": 20000, "name": "T", "pool": "",
         "prev_buyers_h1": 90}
    e.update(kw); return e


def test_early_momentum():
    assert g.early_check(early(), CFG)[0]
    for bad in (early(mc=80000), early(age_min=300), early(buyers_h1=30), early(sellers_h1=110),
                early(buyers_15m=5), early(chg_5m=-40), early(prev_buyers_h1=130), early(vol_1h=1000)):
        assert not g.early_check(bad, CFG)[0], bad


def test_early_safety_dev_bag():
    r = copy.deepcopy(REPORT)
    r["creatorBalance"] = int(r["token"]["supply"] * 0.12)
    ok, hard, _, _ = g.rugcheck_gate("m", CFG, r, lane="early")
    assert not ok and any("dev still holds" in h for h in hard)
    ok, _, soft, _ = g.rugcheck_gate("m", CFG, r, lane="grad")
    assert any("dev holds" in s for s in soft)


def test_rate_limit_is_not_a_block():
    import urllib.error
    orig = g.http_json
    def limited(url, *a, **k):
        raise g.RateLimited(url)
    g.http_json = limited
    try:
        ok, hard, _, _ = g.rugcheck_gate("m", CFG)
        assert not ok and hard == ["RugCheck report unavailable"]
    finally:
        g.http_json = orig


def test_outcome_logged_once_after_window():
    cfg = dict(g.DEFAULTS)
    now = 100 * 3600.0
    state = {"alerted": {"G": {"at": 0, "mc": 50000, "peak_mc": 120000, "last_mc": 2000, "symbol": "GG"}},
             "alerted_early": {"E": {"at": now - 60, "mc": 10000, "peak_mc": 10000, "symbol": "EE"},
                               "B": {"at": 0, "mc": 9000, "blocked": True}}}
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        g.track_outcomes(state, {}, cfg, now)
    out = buf.getvalue()
    assert "OUTCOME grad G GG alert_mc=50000 peak_mc=120000 last_mc=2000 peak_x=2.40 graduated=True" in out
    assert "OUTCOME early" not in out  # still inside its window, and blocked entries are skipped
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        g.track_outcomes(state, {}, cfg, now + 60)
    assert "OUTCOME" not in buf.getvalue()


def test_holder_floor_waits_not_blocks():
    r = copy.deepcopy(REPORT); r["totalHolders"] = 120
    res = g.evaluate("mint", [pair()], CFG, time.time(), r)
    assert res["status"] == "holders_wait", res


def test_hold_test():
    cfg = dict(CFG, confirm_minutes=10, confirm_max_drop_pct=-20, confirm_expire_minutes=30)
    state, t = {}, 1000.0
    with contextlib.redirect_stdout(io.StringIO()):
        assert g.confirm(state, "grad", "M", 50000, t, cfg) == (False, None)            # first pass
        assert g.confirm(state, "grad", "M", 52000, t + 300, cfg)[0] is False            # too soon
        ready, first = g.confirm(state, "grad", "M", 48000, t + 660, cfg)                # held 11m, -4%
        assert ready and first["mc"] == 50000 and "grad:M" not in state["pending"]
        g.confirm(state, "grad", "D", 50000, t, cfg)
        assert g.confirm(state, "grad", "D", 35000, t + 660, cfg)[0] is False            # dumped 30%
        assert "grad:D" not in state["pending"]
        g.confirm(state, "grad", "X", 50000, t, cfg)
        assert g.confirm(state, "grad", "X", 50000, t + 3600, cfg)[0] is False           # stale: restarts
        assert state["pending"]["grad:X"]["at"] == t + 3600
    assert g.confirm({}, "grad", "Z", 1, t, dict(cfg, confirm_minutes=0)) == (True, None)


def test_early_needs_socials_holders_and_hold():
    cfg = dict(CFG, early_require_socials=True, early_min_holders=150, confirm_minutes=10, early_max_top10_pct=30)
    r = copy.deepcopy(REPORT); r["totalHolders"] = 400; r["creatorBalance"] = 0
    orig_gate, orig_send = g.rugcheck_gate, g.send_telegram
    g.rugcheck_gate = lambda mint, c, report=None, lane="grad": orig_gate(mint, c, r, lane)
    try:
        t = time.time()
        def run(now, pairs, holders=400):
            r["totalHolders"] = holders
            state["watch"]["M"]["early"] = early(at=now)
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                n = g.scan_early(state, {"M": pairs}, cfg, now, dry_run=True)
            return n, buf.getvalue()
        social = [{"dexId": "pumpfun", "info": {"socials": [{"type": "twitter"}]}}]
        state = {"watch": {"M": {"name": "T"}}, "alerted_early": {}, "blocked": {}}
        assert run(t, [])[0] == 0 and not state.get("pending")                  # no socials
        assert run(t, social, holders=90)[0] == 0 and not state.get("pending")   # too few holders
        assert run(t, social)[0] == 0 and "early:M" in state["pending"]          # first pass
        n, out = run(t + 660, social)
        assert n == 1 and "Held 11m" in out
        assert "ALERT early M" in out and "holders=400" in out and "socials=1" in out and "first_mc=25000" in out
    finally:
        g.rugcheck_gate, g.send_telegram = orig_gate, orig_send


def test_lookalike_names_blocked():
    assert g.lookalike_name("St\u0435ve")            # Cyrillic e
    assert not g.lookalike_name("Steve") and not g.lookalike_name("\ud76c\ub9dd\uc774")
    p = pair(); p["baseToken"]["name"] = "St\u0435ve"
    assert g.evaluate("mint", [p], CFG, time.time(), REPORT)["status"] == "momentum_fail"


def test_metadata_socials_ignore_tweet_links():
    orig = g.http_json
    try:
        g.http_json = lambda url, *a, **k: {"data": {"attributes": {"twitter_handle": "pipdotfun", "websites": ["https://x.fun"]}}}
        assert g.metadata_socials("m") == 2
        g.http_json = lambda url, *a, **k: {"data": {"attributes": {"twitter_handle": "someone/status/123", "websites": []}}}
        assert g.metadata_socials("m") == 0
    finally:
        g.http_json = orig


def test_grad_follow_up_only():
    cfg = dict(CFG, grad_follow_up_only=True, confirm_minutes=0, early_enabled=False)
    calls = []
    orig_eval, orig_disc, orig_dex, orig_load, orig_save = g.evaluate, g.discover, g.dexscreener_pairs, g.load_state, g.save_state
    state = {"watch": {"A": {"first_seen": 0}, "B": {"first_seen": 0}}, "alerted": {}, "blocked": {},
             "alerted_early": {"A": {"at": time.time(), "mc": 12000, "peak_mc": 12000, "symbol": "A"}},
             "last_scan": None, "last_error": None}
    def fake_eval(mint, pairs, c, now, report=None):
        calls.append(mint)
        p = pair(mc=60000); m = g.momentum_check(p, c, now)[2]
        return {"status": "alert", "metrics": m, "facts": {"holders": 900}, "soft": []}
    g.evaluate, g.discover, g.dexscreener_pairs = fake_eval, lambda c, s: None, lambda m: {}
    g.load_state, g.save_state = lambda: state, lambda s: None
    try:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            assert g.scan(cfg, dry_run=True) == 1
        assert calls == ["A"] and "FOLLOW-UP: Early alert was at $12,000 (5.0x since)" in buf.getvalue()
    finally:
        g.evaluate, g.discover, g.dexscreener_pairs, g.load_state, g.save_state = orig_eval, orig_disc, orig_dex, orig_load, orig_save


def test_exit_pings_once_and_first_exit():
    cfg = dict(CFG)
    now = time.time()
    state = {"alerted": {}, "alerted_early": {
        "W": {"at": now - 600, "mc": 10000, "peak_mc": 23000, "last_mc": 23000, "symbol": "WIN", "delivered": True},
        "L": {"at": now - 600, "mc": 10000, "peak_mc": 11000, "last_mc": 5500, "symbol": "LOSE", "delivered": True}}}
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        g.exit_pings(state, cfg, now, dry_run=True)
        g.exit_pings(state, cfg, now + 60, dry_run=True)
    out = buf.getvalue()
    assert out.count("EXIT tp early W") == 1 and "WIN hit 2.3x" in out
    assert out.count("EXIT stop early L") == 1 and "LOSE is down -45%" in out
    assert state["alerted_early"]["W"]["first_exit"] == "tp" and state["alerted_early"]["L"]["first_exit"] == "stop"
    state["alerted_early"]["W"]["last_mc"] = 4000                  # later stop does not change first_exit
    with contextlib.redirect_stdout(io.StringIO()):
        g.exit_pings(state, cfg, now + 120, dry_run=True)
    assert state["alerted_early"]["W"]["first_exit"] == "tp" and state["alerted_early"]["W"]["stop_sent"]


def test_buyer_concentration_and_grade():
    trades = [{"attributes": {"kind": "buy", "volume_in_usd": "500", "tx_from_address": "BIG"}}] * 4 + \
             [{"attributes": {"kind": "buy", "volume_in_usd": "2", "tx_from_address": f"w{i}"}} for i in range(20)] + \
             [{"attributes": {"kind": "sell", "volume_in_usd": "900", "tx_from_address": "S"}}]
    orig = g.http_json
    g.http_json = lambda url, *a, **k: {"data": trades}
    try:
        c = g.buyer_concentration("pool")
    finally:
        g.http_json = orig
    assert c["buy_wallets"] == 21 and c["top5_buy_pct"] > 98 and round(c["tiny_buy_pct"]) == 83
    facts = {"holders": 500, "top10": 15}
    assert g.grade("early", facts, [], 2, 60, {"top5_buy_pct": 20, "tiny_buy_pct": 5})[0] == "A"
    assert g.grade("early", facts, [], 2, 60, c)[0] == "B"            # bundled-looking buying drags it down
    assert g.grade("early", {"holders": 160, "top10": 24}, ["similar-size cluster: x"], 1, 10, c)[0] == "C"
    assert g.grade_ok("B", {"min_grade": "B"}) and not g.grade_ok("C", {"min_grade": "B"})


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn(); print("pass", name)
