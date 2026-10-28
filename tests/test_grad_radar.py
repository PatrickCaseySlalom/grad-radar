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


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn(); print("pass", name)
