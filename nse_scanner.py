# =============================================================================
# nse_screener.py  |  v5.3  |  All Bugs Fixed — Complete Production Script
# =============================================================================
# FIXES vs v5.2:
#  [1] CRITICAL — Price N/A for ALL stocks:
#      yfinance group_by="ticker" creates MultiIndex (field, ticker).
#      Old code used xs(level=1) which is the field level, not ticker level.
#      Fix: use raw[ticker] directly — yfinance top-level key IS the ticker.
#  [2] CRITICAL — Nifty return 0.0%:
#      nifty_raw["Close"] returns DataFrame in new yfinance, not Series.
#      Fix: use .squeeze() or iloc[:,0] to force a Series before float().
#  [3] CRITICAL — OCF = same ₹93 Cr for every stock:
#      info["operatingCashflow"] is stale/broken in newer yfinance.
#      Fix: extract OCF directly from stock.cashflow DataFrame rows.
#  [4] CRITICAL — CSV empty price fields + peg=0 + roe=14.6 for all:
#      calculate_scores was filling NaN with median in-place and saving that.
#      Fix: score on a temp copy, never modify original df. CSV keeps real NaN.
#  [5] Added scorecard verdict columns to CSV output:
#      rank, overall_verdict, sc_performance, sc_valuation, sc_growth,
#      sc_profitability, sc_entry_point, sc_red_flags, risk_note.
#  [6] FCF sign fix: yfinance capex is already negative (cash outflow),
#      so FCF = OCF + capex (not OCF - abs(capex)).
#  [7] peg_ratio=0 artifact removed: filter now ignores zero PEG (treat as None).
#  [8] Added robust OCF_PATTERNS / CAPEX_PATTERNS list for cashflow row matching.
# =============================================================================

import pandas as pd
import numpy as np
import yfinance as yf
import requests
import urllib3
import io
import os
import json
import time
import shutil
import tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============================================================================
# SECTION 1: CONFIG BLOCK
# =============================================================================

CONFIG = {

    # ── Universe ──────────────────────────────────────────────────────────────
    # "Nifty50"       →  50 stocks | Blue-chips only         | ~30 sec
    # "Nifty100"      → 100 stocks | Large-cap extended       | ~1 min
    # "Nifty500"      → 500 stocks | 92% of NSE market cap   | ~3 min (cached)
    # "Midcap150"     → 150 stocks | Companies ranked 101-250 | ~1 min
    # "Smallcap250"   → 250 stocks | Companies ranked 251-500 | ~2 min
    # "Top800_Custom" → ~750 stocks| Nifty500 + Microcap250   | ~5 min
    # TIP: Start with "Nifty500". Use "Top800_Custom" for widest scan.
    "universe": "Nifty500",

    # ── Archetype ─────────────────────────────────────────────────────────────
    # "Custom" | "Growth_Bargain" | "Cash_Machine" | "Low_Debt_Midcap"
    # "Quality_Compounder" | "Near_52W_Low"
    "archetype": "Custom",

    # ── Cache ─────────────────────────────────────────────────────────────────
    # Master ON/OFF. False = always re-fetch (~15 min per run).
    "use_cache": True,

    "cache_folder":            "cache/",
    "cache_tickers_subfolder": "cache/tickers/",

    # Days before fundamentals re-fetched. Try: 1 | 7 (default) | 14
    "cache_fundamental_days": 7,

    # Days before universe CSV re-downloaded.
    "cache_universe_days": 30,

    # True = wipe all cache. RESET TO FALSE after one run!
    "cache_force_refresh": False,

    # Print hit/miss stats after each run.
    "cache_show_stats": True,

    # ⚠️  CRITICAL: Must match script version exactly.
    # Mismatch = full 15-min re-fetch every single run.
    "cache_schema_version": "5.3",

    # ── Gate 1: Liquidity ─────────────────────────────────────────────────────
    # Try: 500 (lenient) | 1000 (default) | 5000 (large-cap only)
    "min_market_cap_cr": 1000,

    # Skipped when volume data is missing (never rejects on None).
    # Try: 100000 (lenient) | 300000 (default) | 1000000 (liquid only)
    "min_avg_daily_volume": 300000,

    # ── Gate 2: Red Flags ─────────────────────────────────────────────────────
    "reject_negative_ocf":        True,
    "reject_current_ratio_below": 1.0,
    "reject_shrinking_margins":   True,

    # ── Gate 3: Quality (Non-Financial) ───────────────────────────────────────
    # Try: 0.1 (debt-free) | 0.5 (default) | 1.0 (lenient)
    "max_debt_to_equity": 0.5,
    # Try: 10 (lenient) | 15 (default) | 20 (premium)
    "min_roe_pct": 15,

    # ── Gate 4: Growth ────────────────────────────────────────────────────────
    # Try: 5 (lenient) | 10 (default) | 15 (strict)
    "min_revenue_cagr_3y_pct": 10,
    # Try: 8 (lenient) | 12 (default) | 18 (strict)
    "min_netincome_cagr_3y_pct": 12,

    # ── Gate 5: Sector-Relative Valuation ─────────────────────────────────────
    # 1.2 = stock PE up to 20% above sector median.
    "max_pe_vs_sector_mult": 1.2,
    # 0.9 = stock ROE must be ≥ 90% of sector median.
    "min_roe_vs_sector_mult": 0.9,
    # ⚠️  PEG missing for most Indian stocks. 2.0 prevents over-filtering.
    "max_peg_ratio": 2.0,

    # ── Gate 6: Financials ────────────────────────────────────────────────────
    # Try: 1.5 (cheap banks) | 2.5 (default) | 3.5 (lenient)
    "max_pb_financials": 2.5,
    "financial_sectors": ["Bank", "NBFC", "Insurance",
                          "Financial Services", "Finance"],

    # ── Fetching ──────────────────────────────────────────────────────────────
    # Too high = Yahoo rate-limit errors. Try: 5 (safe) | 15 (default) | 20
    "fetch_max_workers": 15,

    # yfinance silently fails on 400+ tickers. Try: 50 | 100 (default) | 200
    "price_batch_size": 100,

    # ── Technical ─────────────────────────────────────────────────────────────
    # Price > SMA50 > SMA200 = confirmed uptrend. Try: 20 | 50 (default)
    "sma_short": 50,
    # Try: 100 | 200 (default — institutional benchmark)
    "sma_long": 200,
    # Try: 9 | 14 (default — Wilder) | 21
    "rsi_period": 14,

    # ── India 10Y Bond Yield ──────────────────────────────────────────────────
    # Update periodically. Check: https://www.rbi.org.in
    # As of March 2026: ~6.7% (RBI rate-cutting cycle since late 2025)
    "india_10y_bond_yield_pct": 6.7,

    # ── Output ────────────────────────────────────────────────────────────────
    # Try: 5 | 10 | 15 (default) | 25
    "top_n_results": 15,
    # Add date for archives: "Top_Stocks_2026_03_06.csv"
    "output_csv": "Top_Stocks_Report.csv",
}

NSE_URLS = {
    "Nifty50":     "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv",
    "Nifty100":    "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv",
    "Nifty500":    "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    "Midcap150":   "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv",
    "Smallcap250": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv",
    "Microcap250": "https://www.niftyindices.com/IndexConstituent/ind_niftymicrocap250_list.csv",
}

# Cashflow row patterns — covers all yfinance versions and label variations
OCF_PATTERNS = [
    "operating cash flow", "cash from operating", "net cash from operating",
    "total cash from operating", "cash flows from operating",
    "net cash provided by operating", "cash generated from operations",
    "operating activities",
]
CAPEX_PATTERNS = [
    "capital expenditure", "capital expenditures", "purchase of property",
    "purchase of plant", "purchase of ppe", "capex",
    "acquisition of fixed", "additions to property", "purchase of fixed",
    "payments for property", "acquisition of property",
]

# =============================================================================
# SECTION 1A: CONFIG VALIDATOR
# =============================================================================

_CONFIG_SCHEMA = {
    "universe":                  (str,   None, None, ["Nifty50","Nifty100","Nifty500",
                                                      "Midcap150","Smallcap250",
                                                      "Top800_Custom"],      "Nifty500"),
    "archetype":                 (str,   None, None, ["Custom","Growth_Bargain",
                                                      "Cash_Machine","Low_Debt_Midcap",
                                                      "Quality_Compounder",
                                                      "Near_52W_Low"],       "Custom"),
    "use_cache":                 (bool,  None, None, None,  True),
    "cache_folder":              (str,   None, None, None,  "cache/"),
    "cache_tickers_subfolder":   (str,   None, None, None,  "cache/tickers/"),
    "cache_fundamental_days":    (int,   1,    30,   None,  7),
    "cache_universe_days":       (int,   1,    90,   None,  30),
    "cache_force_refresh":       (bool,  None, None, None,  False),
    "cache_show_stats":          (bool,  None, None, None,  True),
    "cache_schema_version":      (str,   None, None, None,  "5.3"),
    "min_market_cap_cr":         (float, 0,    None, None,  1000),
    "min_avg_daily_volume":      (int,   0,    None, None,  300000),
    "reject_negative_ocf":       (bool,  None, None, None,  True),
    "reject_current_ratio_below":(float, 0,    5.0,  None,  1.0),
    "reject_shrinking_margins":  (bool,  None, None, None,  True),
    "max_debt_to_equity":        (float, 0,    10.0, None,  0.5),
    "min_roe_pct":               (float, -50,  100,  None,  15),
    "min_revenue_cagr_3y_pct":   (float, -50,  100,  None,  10),
    "min_netincome_cagr_3y_pct": (float, -50,  100,  None,  12),
    "max_pe_vs_sector_mult":     (float, 0.1,  5.0,  None,  1.2),
    "min_roe_vs_sector_mult":    (float, 0.1,  3.0,  None,  0.9),
    "max_peg_ratio":             (float, 0.1,  10.0, None,  2.0),
    "max_pb_financials":         (float, 0.1,  20.0, None,  2.5),
    "financial_sectors":         (list,  None, None, None,
                                  ["Bank","NBFC","Insurance",
                                   "Financial Services","Finance"]),
    "fetch_max_workers":         (int,   1,    30,   None,  15),
    "price_batch_size":          (int,   10,   500,  None,  100),
    "sma_short":                 (int,   5,    100,  None,  50),
    "sma_long":                  (int,   50,   500,  None,  200),
    "rsi_period":                (int,   2,    30,   None,  14),
    "india_10y_bond_yield_pct":  (float, 0,    25,   None,  6.7),
    "top_n_results":             (int,   1,    100,  None,  15),
    "output_csv":                (str,   None, None, None,  "Top_Stocks_Report.csv"),
}


def validate_and_sanitize_config(config: dict) -> dict:
    c      = config.copy()
    issues = []

    for key, (dtype, vmin, vmax, allowed, default) in _CONFIG_SCHEMA.items():
        if key not in c:
            c[key] = default
            issues.append(f"MISSING '{key}' → default: {default}")
            continue

        val = c[key]

        if dtype == bool:
            if not isinstance(val, bool):
                if str(val).lower() in ("1","true","yes"):   c[key] = True
                elif str(val).lower() in ("0","false","no"): c[key] = False
                else:
                    c[key] = default
                    issues.append(f"INVALID TYPE '{key}'={val!r} → {default}")
        elif dtype == list:
            if not isinstance(val, list):
                c[key] = default
                issues.append(f"INVALID TYPE '{key}' must be list → default")
        elif dtype in (int, float):
            try:   c[key] = dtype(val)
            except (ValueError, TypeError):
                c[key] = default
                issues.append(f"INVALID TYPE '{key}'={val!r} → {default}")
        elif dtype == str:
            c[key] = str(val).strip()

        val = c[key]

        if allowed and val not in allowed:
            c[key] = default
            issues.append(f"INVALID VALUE '{key}'={val!r} → '{default}'")
            val = c[key]

        if dtype in (int, float) and not isinstance(val, bool):
            if vmin is not None and val < vmin:
                c[key] = dtype(vmin)
                issues.append(f"OUT OF RANGE '{key}'={val} < {vmin} → clamped")
            if vmax is not None and val > vmax:
                c[key] = dtype(vmax)
                issues.append(f"OUT OF RANGE '{key}'={val} > {vmax} → clamped")

    if c["sma_short"] >= c["sma_long"]:
        c["sma_short"] = 50; c["sma_long"] = 200
        issues.append("CONFLICT: sma_short >= sma_long → reset 50/200")

    if not c["output_csv"].endswith(".csv"):
        c["output_csv"] += ".csv"
        issues.append("output_csv missing .csv → auto-added")

    if not c["cache_tickers_subfolder"].startswith(c["cache_folder"]):
        c["cache_tickers_subfolder"] = c["cache_folder"] + "tickers/"
        issues.append("cache_tickers_subfolder outside cache_folder → fixed")

    if issues:
        print(f"\n{'='*64}\n  ⚠️  CONFIG — {len(issues)} issue(s) auto-fixed\n{'='*64}")
        for msg in issues: print(f"  • {msg}")
        print(f"{'='*64}\n")
    else:
        print("[CONFIG] ✅ All settings validated successfully.")

    return c


# =============================================================================
# SECTION 2: ARCHETYPES & SECTOR MAPS
# =============================================================================

ARCHETYPES = {
    "Growth_Bargain":    {"min_netincome_cagr_3y_pct": 18, "max_pe_vs_sector_mult": 0.9,
                          "max_peg_ratio": 1.5, "min_roe_pct": 15, "max_debt_to_equity": 0.5},
    "Cash_Machine":      {"min_roe_pct": 20, "max_debt_to_equity": 0.1,
                          "min_revenue_cagr_3y_pct": 10, "min_netincome_cagr_3y_pct": 10},
    "Low_Debt_Midcap":   {"universe": "Midcap150", "max_debt_to_equity": 0.1,
                          "min_roe_pct": 15, "min_netincome_cagr_3y_pct": 12,
                          "max_pe_vs_sector_mult": 1.3},
    "Quality_Compounder":{"min_roe_pct": 20, "max_debt_to_equity": 0.3,
                          "min_revenue_cagr_3y_pct": 15, "min_netincome_cagr_3y_pct": 15,
                          "max_peg_ratio": 2.0},
    "Near_52W_Low":      {"min_pct_below_52w_high": 30, "min_roe_pct": 12,
                          "max_debt_to_equity": 0.8, "min_netincome_cagr_3y_pct": 10},
}

SECTOR_RISK_MAP = {
    "Bank":                   "RBI policy shifts, rising NPAs, or credit cycle deterioration.",
    "NBFC":                   "RBI liquidity tightening, borrowing cost spikes, or NPA risks.",
    "Insurance":              "IRDAI regulatory changes, claims inflation, or yield compression.",
    "Financial Services":     "RBI policy shifts, credit cycle downturns, or rising defaults.",
    "Finance":                "Rising interest rates, RBI changes, or NPA deterioration.",
    "Information Technology": "Global IT spending cuts, USD/INR volatility, client concentration.",
    "Technology":             "Global tech spending cuts, USD/INR volatility.",
    "Pharmaceuticals":        "USFDA observations, patent expirations, pricing controls.",
    "Healthcare":             "USFDA actions, drug pricing controls, or IP challenges.",
    "Automobile":             "EV disruption, fuel price volatility, material cost spikes.",
    "Consumer Goods":         "Input cost inflation, rural demand slowdown, GST changes.",
    "Infrastructure":         "Government capex cuts, land delays, rising interest rates.",
    "Energy":                 "Crude oil swings, government pricing intervention, transition risk.",
    "Metals":                 "Commodity cycle, import duty changes, global demand collapse.",
    "Realty":                 "Interest rate hikes, regulatory changes, demand cooling.",
    "default":                "Regulatory changes, global macro shocks, or black swan events.",
}


def apply_archetype(config: dict) -> dict:
    name = config.get("archetype", "Custom")
    if name == "Custom" or name not in ARCHETYPES:
        return config
    merged = config.copy()
    merged.update(ARCHETYPES[name])
    print(f"[INFO] Archetype '{name}' applied.")
    return validate_and_sanitize_config(merged)


# =============================================================================
# SECTION 3: CACHE ENGINE
# =============================================================================

_stats = {"fund_hits": 0, "fund_miss": 0, "master_hit": False, "master_secs": 0.0}


def _dirs():
    os.makedirs(CONFIG["cache_folder"], exist_ok=True)
    os.makedirs(CONFIG["cache_tickers_subfolder"], exist_ok=True)


def _tpath(ticker: str, dtype: str) -> str:
    safe = ticker.replace(".", "_").replace("/", "_").replace("\\", "_")
    return os.path.join(CONFIG["cache_tickers_subfolder"], f"{safe}_{dtype}.json")


def _json_safe(obj):
    """Recursively converts numpy scalars to Python natives for JSON."""
    if isinstance(obj, dict):          return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)): return [_json_safe(v) for v in obj]
    if isinstance(obj, np.integer):    return int(obj)
    if isinstance(obj, np.floating):   return None if np.isnan(obj) else float(obj)
    if isinstance(obj, np.bool_):      return bool(obj)
    if isinstance(obj, float) and np.isnan(obj): return None
    return obj


def save_ticker_cache(ticker: str, data: dict, dtype: str):
    if not CONFIG.get("use_cache"): return
    _dirs()
    try:
        payload = _json_safe(data)
        payload["fetched_at"]     = datetime.now().isoformat()
        payload["schema_version"] = CONFIG["cache_schema_version"]
        with open(_tpath(ticker, dtype), "w") as f:
            json.dump(payload, f, default=str)
    except Exception:
        pass


def load_ticker_cache(ticker: str, dtype: str) -> dict | None:
    if not CONFIG.get("use_cache"): return None
    path = _tpath(ticker, dtype)
    if not os.path.exists(path): return None
    try:
        with open(path) as f: c = json.load(f)
        if c.get("schema_version") != CONFIG["cache_schema_version"]: return None
        age = (datetime.now() - datetime.fromisoformat(c["fetched_at"])).days
        if age >= CONFIG["cache_fundamental_days"]: return None
        if all(c.get(k) is None for k in ["pe_ratio", "roe", "market_cap_cr"]): return None
        return c
    except Exception:
        return None


def save_master_cache(df: pd.DataFrame):
    if not CONFIG.get("use_cache"): return
    _dirs()
    pkl  = os.path.join(CONFIG["cache_folder"], "master_fundamentals.pkl")
    meta = os.path.join(CONFIG["cache_folder"], "cache_metadata.json")
    try:
        df.to_pickle(pkl)
        with open(meta, "w") as f:
            json.dump({
                "created_at":     datetime.now().isoformat(),
                "universe":       CONFIG["universe"],
                "archetype":      CONFIG.get("archetype", "Custom"),
                "schema_version": CONFIG["cache_schema_version"],
                "total_tickers":  len(df),
            }, f, indent=2)
        print(f"[CACHE] Saved → {pkl} ({len(df)} rows)")
    except Exception as e:
        print(f"[CACHE] Save failed: {e}")


def load_master_cache() -> pd.DataFrame | None:
    if not CONFIG.get("use_cache"): return None
    pkl  = os.path.join(CONFIG["cache_folder"], "master_fundamentals.pkl")
    meta = os.path.join(CONFIG["cache_folder"], "cache_metadata.json")
    if not os.path.exists(pkl) or not os.path.exists(meta): return None
    try:
        with open(meta) as f: m = json.load(f)
        if m.get("schema_version") != CONFIG["cache_schema_version"]: return None
        if m.get("universe")       != CONFIG["universe"]:              return None
        if m.get("archetype")      != CONFIG.get("archetype", "Custom"): return None
        age = (datetime.now() - datetime.fromisoformat(m["created_at"])).days
        if age >= CONFIG["cache_fundamental_days"]:
            print(f"[CACHE] Expired ({age}d). Re-fetching.")
            return None
        t0 = time.time()
        df = pd.read_pickle(pkl)
        el = round(time.time() - t0, 2)
        _stats["master_hit"]  = True
        _stats["master_secs"] = el
        print(f"[CACHE] ✅ HIT — {len(df)} rows in {el}s (age: {age}d)")
        return df
    except Exception as e:
        print(f"[CACHE] Load failed: {e}")
        return None


def wipe_cache():
    if os.path.exists(CONFIG["cache_folder"]):
        shutil.rmtree(CONFIG["cache_folder"])
        print("[CACHE] 🗑  Cache wiped.")
    _dirs()


def print_cache_stats(total: int):
    if not CONFIG.get("cache_show_stats"): return
    s    = _stats
    mins = round((s["fund_hits"] * 4.5) / 60, 1)
    print(f"\n{'='*64}")
    print(f"  CACHE STATS")
    print(f"  Master  : {'✅ HIT ('+str(s['master_secs'])+'s)' if s['master_hit'] else '❌ MISS (first run — fast next time)'}")
    print(f"  Tickers : {s['fund_hits']} hits / {s['fund_hits']+s['fund_miss']} total")
    print(f"  Saved   : ~{mins} mins")
    print(f"{'='*64}\n")


# =============================================================================
# SECTION 4: UNIVERSE LOADER
# =============================================================================

def fetch_csv(url: str) -> pd.DataFrame:
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
         "Accept": "text/html,*/*;q=0.8"}
    r = requests.get(url, headers=h, timeout=30, verify=False)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))


def _normalise_df(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    sym_col = next((c for c in df.columns if "symbol" in c.lower()), None)
    ind_col = next((c for c in df.columns
                    if any(x in c.lower() for x in ["industry", "sector"])), None)
    if not sym_col:
        raise ValueError(f"No Symbol column. Got: {df.columns.tolist()}")
    out = pd.DataFrame()
    out["Symbol"]   = df[sym_col].astype(str).str.strip().str.upper()
    out["Industry"] = df[ind_col].astype(str).str.strip() if ind_col else "Unknown"
    return out.dropna(subset=["Symbol"])


def get_universe_tickers(universe_name: str):
    print(f"\n[INFO] Loading universe: {universe_name}")
    _dirs()
    u_csv  = os.path.join(CONFIG["cache_folder"], f"universe_{universe_name}.csv")
    u_meta = os.path.join(CONFIG["cache_folder"], f"universe_{universe_name}_meta.json")
    df = None

    if CONFIG.get("use_cache") and os.path.exists(u_csv) and os.path.exists(u_meta):
        try:
            with open(u_meta) as f: um = json.load(f)
            age = (datetime.now() - datetime.fromisoformat(um["fetched_at"])).days
            if age < CONFIG["cache_universe_days"]:
                df = pd.read_csv(u_csv)
                print(f"[CACHE] Universe HIT — {len(df)} stocks (age: {age}d)")
        except Exception:
            df = None

    if df is None:
        if universe_name in NSE_URLS:
            try:
                raw = fetch_csv(NSE_URLS[universe_name])
                df  = _normalise_df(raw)
                df.to_csv(u_csv, index=False)
                with open(u_meta, "w") as f:
                    json.dump({"fetched_at": datetime.now().isoformat()}, f)
                print(f"[INFO] Fetched {len(df)} stocks from niftyindices.com")
            except Exception as e:
                print(f"[WARNING] Network fetch failed: {e}")

        elif universe_name == "Top800_Custom":
            # Nifty500 (ranks 1-500) + Microcap250 (501-750) — non-overlapping
            sub_indexes = {
                "Nifty500":    NSE_URLS["Nifty500"],
                "Microcap250": NSE_URLS["Microcap250"],
            }
            frames = []
            for name, url in sub_indexes.items():
                try:
                    raw    = fetch_csv(url)
                    normed = _normalise_df(raw)
                    frames.append(normed)
                    print(f"  [Top800] Fetched {len(normed)} from {name}")
                except Exception as e:
                    print(f"  [Top800] ⚠️  {name} failed: {e}")

            if not frames:
                raise RuntimeError("Top800_Custom: All sub-index fetches failed.")

            df = pd.concat(frames, ignore_index=True)
            df = df.drop_duplicates(subset=["Symbol"], keep="first")
            print(f"[INFO] Top800_Custom: {len(df)} unique stocks after dedup")
            df.to_csv(u_csv, index=False)
            with open(u_meta, "w") as f:
                json.dump({"fetched_at": datetime.now().isoformat()}, f)

    if df is None:
        for local in [f"{universe_name}.csv", universe_name]:
            if os.path.exists(local):
                df = _normalise_df(pd.read_csv(local))
                print(f"[INFO] Loaded local file: {local}")
                break

    if df is None:
        raise FileNotFoundError(
            f"Cannot load universe '{universe_name}'. "
            f"Download CSV from niftyindices.com and save as '{universe_name}.csv'."
        )

    df = df[df["Symbol"].astype(str).str.len() > 0]
    tickers    = [s + ".NS" for s in df["Symbol"].tolist()]
    sector_map = dict(zip([s + ".NS" for s in df["Symbol"]], df["Industry"]))

    seen = set()
    tickers = [t for t in tickers if not (t in seen or seen.add(t))]
    print(f"[INFO] Universe ready: {len(tickers)} unique tickers")
    return tickers, sector_map


# =============================================================================
# SECTION 5A: RSI CALCULATOR
# =============================================================================

def compute_rsi(close_series: pd.Series, period: int = 14) -> float | None:
    try:
        clean = close_series.dropna()
        if len(clean) < period + 1: return None
        delta = clean.diff()
        gain  = delta.clip(lower=0)
        loss  = (-delta).clip(lower=0)
        avg_g = gain.ewm(alpha=1/period, min_periods=period).mean()
        avg_l = loss.ewm(alpha=1/period, min_periods=period).mean()
        rs    = avg_g / avg_l.replace(0, np.nan)
        rsi   = 100 - (100 / (1 + rs))
        val   = rsi.dropna()
        return round(float(val.iloc[-1]), 2) if len(val) > 0 else None
    except Exception:
        return None


# =============================================================================
# SECTION 5B: PRICE DATA FETCH — BATCHED (v5.3 — MultiIndex level fix)
# =============================================================================

def _get_close_volume(raw, ticker: str, batch: list):
    """
    [v5.3 FIX] Extracts Close + Volume Series for one ticker from yfinance batch result.

    yfinance with group_by='ticker' produces MultiIndex columns: (ticker, field)
    So raw[ticker] gives a sub-DataFrame with columns: Close, Open, High, Low, Volume.
    This is the correct access pattern for ALL modern yfinance versions.

    Single-ticker downloads return a flat DataFrame directly — handled separately.
    """
    try:
        if len(batch) == 1:
            # Single ticker download → raw is already the flat OHLCV DataFrame
            df = raw
        else:
            # Multi-ticker download → raw[ticker] = sub-DataFrame for that ticker
            if ticker not in raw.columns.get_level_values(0):
                return None, None
            df = raw[ticker]

        if df is None or df.empty:
            return None, None

        # Flatten if somehow still MultiIndex after slicing
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(-1)

        close  = df["Close"].dropna()  if "Close"  in df.columns else None
        volume = df["Volume"].dropna() if "Volume" in df.columns else pd.Series(dtype=float)
        return close, volume
    except Exception:
        return None, None


def _process_single_price(raw, ticker: str, batch: list, nifty_ret: float) -> dict:
    """Computes all price-derived metrics for one ticker."""
    rec = {
        "ticker":             ticker,
        "current_price":      None,
        "52w_high":           None,
        "52w_low":            None,
        "avg_daily_volume":   None,
        "pct_below_52w_high": None,
        "sma_50":             None,
        "sma_200":            None,
        "rsi_14":             None,
        "technical_trend":    "N/A",
        "volume_surge":       False,
        "return_1y":          None,
        "rs_vs_nifty":        None,
    }
    try:
        cl, vol = _get_close_volume(raw, ticker, batch)

        if cl is None or len(cl) < 5:
            return rec

        ss = CONFIG["sma_short"]
        sl = CONFIG["sma_long"]
        rp = CONFIG["rsi_period"]

        cur    = round(float(cl.iloc[-1]), 2)
        hi52   = round(float(cl.max()), 2)
        lo52   = round(float(cl.min()), 2)
        v30avg = float(vol.tail(30).mean()) if vol is not None and len(vol) >= 5 else 0.0
        vcur   = float(vol.iloc[-1])        if vol is not None and len(vol) >= 1 else 0.0
        below  = round((hi52 - cur) / hi52 * 100, 2) if hi52 > 0 else None
        sma50  = round(float(cl.tail(ss).mean()), 2)  if len(cl) >= ss else None
        sma200 = round(float(cl.tail(sl).mean()), 2)  if len(cl) >= sl else None
        rsi14  = compute_rsi(cl, rp)
        vsurge = bool(vcur > v30avg * 1.5) if v30avg > 0 else False
        ret1y  = round((float(cl.iloc[-1]) / float(cl.iloc[0]) - 1) * 100, 2)
        rs_n   = round(ret1y - nifty_ret, 2)

        if sma50 and sma200:
            if   cur > sma50 > sma200: trend = "Uptrend ✅"
            elif cur < sma50 < sma200: trend = "Downtrend ❌"
            else:                      trend = "Sideways ➡️"
        else:
            trend = "N/A"

        rec.update({
            "current_price":      cur,
            "52w_high":           hi52,
            "52w_low":            lo52,
            "avg_daily_volume":   int(v30avg) if v30avg else 0,
            "pct_below_52w_high": below,
            "sma_50":             sma50,
            "sma_200":            sma200,
            "rsi_14":             rsi14,
            "technical_trend":    trend,
            "volume_surge":       vsurge,
            "return_1y":          ret1y,
            "rs_vs_nifty":        rs_n,
        })
    except Exception:
        pass
    return rec


def fetch_price_data(tickers: list) -> pd.DataFrame:
    """
    [v5.3] Price fetch — fully fixed:
    1. Nifty50 fetched separately as single ticker (never in batch)
    2. Close forced to Series via squeeze() before float() extraction
    3. Batches of price_batch_size (default 100) to avoid silent yfinance failures
    4. Extraction uses raw[ticker] (MultiIndex level 0 = ticker)
    """
    batch_size = CONFIG.get("price_batch_size", 100)
    batches    = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]

    print(f"\n[INFO] 🌐 Fetching live prices — "
          f"{len(tickers)} tickers in {len(batches)} batch(es) of {batch_size}...")

    # [v5.3 FIX] Fetch Nifty50 as single ticker, squeeze to Series before float()
    nifty_ret = 0.0
    try:
        nifty_raw = yf.download("^NSEI", period="1y",
                                auto_adjust=True, progress=False)
        if not nifty_raw.empty and "Close" in nifty_raw.columns:
            # squeeze() converts DataFrame to Series if single column
            nc = nifty_raw["Close"].squeeze().dropna()
            if len(nc) >= 2:
                nifty_ret = round((float(nc.iloc[-1]) / float(nc.iloc[0]) - 1) * 100, 2)
    except Exception as e:
        print(f"[WARNING] Nifty50 fetch failed: {e}. RS will be relative to 0%.")
    print(f"[INFO] Nifty 50 1Y return: {nifty_ret}%")

    all_records    = []
    failed_batches = 0

    for i, batch in enumerate(batches, 1):
        print(f"  [Price] Batch {i}/{len(batches)} ({len(batch)} tickers)...", end="\r")
        try:
            raw = yf.download(
                batch,
                period="1y",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if raw is None or raw.empty:
                raise ValueError("Empty response from yfinance")

            for ticker in batch:
                all_records.append(
                    _process_single_price(raw, ticker, batch, nifty_ret)
                )

        except Exception as e:
            failed_batches += 1
            print(f"\n  [Price] Batch {i} failed: {e}")
            for ticker in batch:
                all_records.append({"ticker": ticker})

    print(f"\n[INFO] Price fetch complete. Failed batches: {failed_batches}/{len(batches)}")

    if failed_batches == len(batches):
        print("[WARNING] ⚠️  ALL price batches failed! "
              "Volume filter will be skipped for all stocks.")

    return pd.DataFrame(all_records)


# =============================================================================
# SECTION 5C: FUNDAMENTALS — THREADED FETCH (v5.3 — OCF from cashflow fixed)
# =============================================================================

def compute_cagr(series: list) -> float | None:
    """
    Handles edge cases:
    - All zero → returns 0.0
    - Sign change (loss → profit) → returns None (CAGR would be meaningless)
    - Single value → returns None
    """
    try:
        clean = [float(v) for v in series
                 if v is not None and not np.isnan(float(v))]
        if len(clean) < 2:    return None
        if clean[0] <= 0 or clean[-1] <= 0: return None
        n = len(clean) - 1
        return round(((clean[-1] / clean[0]) ** (1/n) - 1) * 100, 2)
    except Exception:
        return None


def _match_row(index_labels, patterns: list) -> str | None:
    """Returns the first index label that matches any pattern string."""
    for label in index_labels:
        label_l = str(label).lower().strip()
        for pat in patterns:
            if pat in label_l:
                return label
    return None


def fetch_fundamentals(ticker: str) -> dict:
    """
    [v5.3 FIX] OCF extracted from stock.cashflow DataFrame rows directly.
    info["operatingCashflow"] is unreliable in newer yfinance versions —
    it sometimes returns a stale global value identical across all stocks.
    """
    cached = load_ticker_cache(ticker, "fundamental")
    if cached:
        _stats["fund_hits"] += 1
        return cached
    _stats["fund_miss"] += 1

    result = {
        "ticker": ticker,
        "pe_ratio": None, "pb_ratio": None, "peg_ratio": None,
        "roe": None, "debt_to_equity": None, "market_cap_cr": None,
        "current_ratio": None, "gross_margin_current": None, "gross_margin_prev": None,
        "operating_cash_flow": None, "capital_expenditure": None,
        "free_cash_flow": None, "net_income": None,
        "institutional_holding_pct": None,
        "revenue_cagr_3y": None, "netincome_cagr_3y": None,
        "revenue_cagr_5y": None, "netincome_cagr_5y": None,
        "operating_margin": None, "sector_yf": None,
        "data_years_available": 0,
        "consistent_profit": None, "high_earnings_quality": None,
    }

    def _try_fetch():
        for attempt in range(2):
            try:
                return yf.Ticker(ticker)
            except Exception:
                if attempt == 0: time.sleep(2)
        return None

    try:
        stock = _try_fetch()
        if stock is None:
            save_ticker_cache(ticker, result, "fundamental")
            return result

        info = stock.info or {}

        def sg(key, scale=1.0):
            v = info.get(key)
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                try: return round(float(v) * scale, 4)
                except Exception: pass
            return None

        # [v5.3] Deliberately NOT using info["operatingCashflow"] — broken.
        result.update({
            "pe_ratio":             sg("trailingPE"),
            "pb_ratio":             sg("priceToBook"),
            "peg_ratio":            sg("pegRatio"),
            "roe":                  sg("returnOnEquity", 100),
            "debt_to_equity":       sg("debtToEquity"),
            "market_cap_cr":        sg("marketCap", 1/1e7),
            "current_ratio":        sg("currentRatio"),
            "gross_margin_current": sg("grossMargins", 100),
            "operating_margin":     sg("operatingMargins", 100),
            "sector_yf":            info.get("sector"),
        })

        ih = info.get("heldPercentInstitutions")
        if ih is not None and not (isinstance(ih, float) and np.isnan(ih)):
            result["institutional_holding_pct"] = round(float(ih) * 100, 2)

        # ── Income Statement — Revenue + Net Income CAGRs ──────────────────
        try:
            fin = stock.financials
            if fin is not None and not fin.empty:
                rev_row = _match_row(fin.index, ["total revenue", "revenue"])
                ni_row  = _match_row(fin.index, ["net income"])
                gp_row  = _match_row(fin.index, ["gross profit"])
                n_cols  = fin.shape[1]
                result["data_years_available"] = n_cols

                if rev_row:
                    rv = []
                    for i in range(n_cols-1, -1, -1):
                        try:
                            v = float(fin.loc[rev_row].iloc[i])
                            if not np.isnan(v): rv.append(v)
                        except Exception: pass
                    if len(rv) >= 4: result["revenue_cagr_3y"] = compute_cagr(rv[-4:])
                    if len(rv) >= 6: result["revenue_cagr_5y"] = compute_cagr(rv[-6:])

                if ni_row:
                    ni = []
                    for i in range(n_cols-1, -1, -1):
                        try:
                            v = float(fin.loc[ni_row].iloc[i])
                            if not np.isnan(v): ni.append(v)
                        except Exception: pass
                    if ni:
                        result["net_income"] = float(fin.loc[ni_row].iloc[0])
                        if len(ni) >= 4: result["netincome_cagr_3y"] = compute_cagr(ni[-4:])
                        if len(ni) >= 6: result["netincome_cagr_5y"] = compute_cagr(ni[-6:])
                        chk = ni[-4:] if len(ni) >= 4 else ni
                        result["consistent_profit"] = bool(all(v > 0 for v in chk))

                # Previous year gross margin
                if gp_row and rev_row and n_cols >= 2:
                    try:
                        gp_p = float(fin.loc[gp_row].iloc[1])
                        rv_p = float(fin.loc[rev_row].iloc[1])
                        if rv_p > 0:
                            result["gross_margin_prev"] = round(gp_p / rv_p * 100, 4)
                    except Exception: pass
        except Exception:
            pass

        # ── Cash Flow — OCF and Capex extracted from cashflow rows ─────────
        try:
            cf = stock.cashflow
            if cf is not None and not cf.empty:

                # [v5.3 FIX] Find OCF row using pattern matching
                ocf_row = _match_row(cf.index, OCF_PATTERNS)
                if ocf_row:
                    try:
                        ocf_val = float(cf.loc[ocf_row].iloc[0])
                        if not np.isnan(ocf_val):
                            result["operating_cash_flow"] = ocf_val
                    except Exception: pass

                # Find Capex row
                cx_row = _match_row(cf.index, CAPEX_PATTERNS)
                if cx_row:
                    try:
                        cx_val = float(cf.loc[cx_row].iloc[0])
                        if not np.isnan(cx_val):
                            result["capital_expenditure"] = cx_val
                    except Exception: pass

                # [v5.3 FIX] FCF = OCF + capex (capex is already negative in yfinance)
                ocf = result["operating_cash_flow"]
                cx  = result["capital_expenditure"]
                if ocf is not None and cx is not None:
                    result["free_cash_flow"] = ocf + cx

                # Earnings quality: OCF > Net Income = real cash backing profits
                ocf = result["operating_cash_flow"]
                ni  = result["net_income"]
                if ocf is not None and ni is not None and ni > 0:
                    result["high_earnings_quality"] = bool(ocf > ni)
        except Exception:
            pass

    except Exception:
        pass

    save_ticker_cache(ticker, result, "fundamental")
    return result


def build_fundamental_dataframe(tickers: list, sector_map: dict) -> pd.DataFrame:
    print(f"\n[INFO] Fetching fundamentals "
          f"({len(tickers)} stocks, {CONFIG['fetch_max_workers']} threads)...")
    records   = []
    completed = 0
    total     = len(tickers)

    with ThreadPoolExecutor(max_workers=CONFIG["fetch_max_workers"]) as ex:
        futures = {ex.submit(fetch_fundamentals, t): t for t in tickers}
        for future in as_completed(futures):
            completed += 1
            print(f"  [{completed:>4}/{total}] {futures[future]:<24}", end="\r")
            try:    records.append(future.result())
            except: records.append({"ticker": futures[future]})

    print(f"\n[INFO] Fundamentals complete.")
    fund_df = pd.DataFrame(records)

    expected_cols = [
        "pe_ratio","pb_ratio","peg_ratio","roe","debt_to_equity","market_cap_cr",
        "current_ratio","gross_margin_current","gross_margin_prev",
        "operating_cash_flow","capital_expenditure","free_cash_flow",
        "net_income","institutional_holding_pct","revenue_cagr_3y","netincome_cagr_3y",
        "revenue_cagr_5y","netincome_cagr_5y","operating_margin",
        "consistent_profit","high_earnings_quality","data_years_available","sector_yf"
    ]
    for col in expected_cols:
        if col not in fund_df.columns:
            fund_df[col] = None

    # NSE sector takes priority; yfinance fills Unknown only
    fund_df["sector"] = fund_df["ticker"].map(sector_map).fillna("Unknown")
    mask = fund_df["sector"].isin(["Unknown","nan","None",""])
    fund_df.loc[mask, "sector"] = fund_df.loc[mask, "sector_yf"].fillna("Unknown")
    fund_df.drop(columns=["sector_yf"], inplace=True, errors="ignore")
    return fund_df


# =============================================================================
# SECTION 6: SECTOR CLASSIFICATION + MEDIANS
# =============================================================================

def classify_sector(s) -> str:
    if not s or pd.isna(s) or str(s).strip() in ["Unknown","nan","None",""]:
        return "non_financial"
    sl = str(s).lower()
    return "financial" if any(f.lower() in sl
                              for f in CONFIG.get("financial_sectors", [])) \
           else "non_financial"


def add_sector_medians(df: pd.DataFrame) -> pd.DataFrame:
    """3-tier fallback: sector median → market median → constant."""
    def safe_median(x):
        valid = x.dropna()
        return valid.median() if len(valid) >= 5 else np.nan

    overall_pe  = df["pe_ratio"].dropna().median() if df["pe_ratio"].notna().any()  else 20.0
    overall_pb  = df["pb_ratio"].dropna().median() if df["pb_ratio"].notna().any()  else 2.5
    overall_roe = df["roe"].dropna().median()       if df["roe"].notna().any()       else 15.0

    df["sector_median_pe"]  = df.groupby("sector")["pe_ratio"].transform(safe_median)\
                                .fillna(overall_pe)
    df["sector_median_pb"]  = df.groupby("sector")["pb_ratio"].transform(safe_median)\
                                .fillna(overall_pb)
    df["sector_median_roe"] = df.groupby("sector")["roe"].transform(safe_median)\
                                .fillna(overall_roe)
    return df


# =============================================================================
# SECTION 7: FILTER ENGINE (v5.3 — peg=0 treated as None)
# =============================================================================

def _col(df: pd.DataFrame, name: str) -> pd.Series:
    return df[name] if name in df.columns else pd.Series(np.nan, index=df.index)


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    print(f"\n{'='*64}")
    print(f"  FILTER ENGINE  |  {len(df)} stocks entering")
    print(f"{'='*64}")

    w = df.copy()
    w["sector_type"] = w["sector"].apply(classify_sector)
    w = add_sector_medians(w)

    # Gate 1: Liquidity
    b   = len(w)
    mc  = _col(w, "market_cap_cr")
    adv = _col(w, "avg_daily_volume")
    # None avg_daily_volume = price fetch failed → skip volume check
    vol_ok = adv.isna() | (adv >= CONFIG["min_avg_daily_volume"])
    w = w[mc.notna() & (mc >= CONFIG["min_market_cap_cr"]) & vol_ok]
    print(f"  Gate 1 [Liquidity]             : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 2: Red Flags
    b  = len(w); rf = pd.Series(False, index=w.index)
    if CONFIG.get("reject_negative_ocf", True):
        ocf = _col(w, "operating_cash_flow")
        rf |= ocf.notna() & (ocf < 0)
    cr_min = CONFIG.get("reject_current_ratio_below", 1.0)
    if cr_min and cr_min > 0:
        cr = _col(w, "current_ratio")
        rf |= cr.notna() & (cr < cr_min)
    if CONFIG.get("reject_shrinking_margins", True):
        gmc = _col(w, "gross_margin_current")
        gmp = _col(w, "gross_margin_prev")
        rf |= gmc.notna() & gmp.notna() & (gmc < gmp)
    w = w[~rf]
    print(f"  Gate 2 [Red Flags]             : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 3: Quality (Non-Financial only)
    b  = len(w); nf = w["sector_type"] == "non_financial"
    de  = _col(w, "debt_to_equity");  roe = _col(w, "roe")
    bad = nf & (
        (de.notna()  & (de  > CONFIG["max_debt_to_equity"])) |
        (roe.notna() & (roe < CONFIG["min_roe_pct"]))
    )
    w = w[~bad]
    print(f"  Gate 3 [Quality — Non-Fin]     : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 4: Growth
    b   = len(w)
    rv3 = _col(w, "revenue_cagr_3y"); ni3 = _col(w, "netincome_cagr_3y")
    bg  = (
        (rv3.notna() & (rv3 < CONFIG["min_revenue_cagr_3y_pct"])) |
        (ni3.notna() & (ni3 < CONFIG["min_netincome_cagr_3y_pct"]))
    )
    w = w[~bg]
    print(f"  Gate 4 [Growth — 3Y CAGR]      : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 5: Sector-Relative Valuation
    b   = len(w); nf = w["sector_type"] == "non_financial"
    pe  = _col(w, "pe_ratio");  spe  = _col(w, "sector_median_pe")
    roe = _col(w, "roe");       sroe = _col(w, "sector_median_roe")
    peg = _col(w, "peg_ratio")
    bv  = nf & pe.notna()  & spe.notna()  & (pe  > spe  * CONFIG["max_pe_vs_sector_mult"])
    br  = nf & roe.notna() & sroe.notna() & (roe < sroe * CONFIG["min_roe_vs_sector_mult"])
    # [v5.3 FIX] peg=0 treated as missing data — don't reject on zero PEG
    bp  = nf & peg.notna() & (peg > 0) & (peg > CONFIG["max_peg_ratio"])
    w   = w[~(bv | br | bp)]
    print(f"  Gate 5 [Sector-Rel Valuation]  : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 6: Financials P/B
    b   = len(w); fin = w["sector_type"] == "financial"
    pb  = _col(w, "pb_ratio")
    bf  = fin & pb.notna() & (pb > CONFIG["max_pb_financials"])
    w   = w[~bf]
    print(f"  Gate 6 [Financials — P/B]      : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 7: Near 52W Low (archetype only)
    if CONFIG.get("archetype") == "Near_52W_Low" and CONFIG.get("min_pct_below_52w_high"):
        b   = len(w); blw = _col(w, "pct_below_52w_high")
        w   = w[blw.notna() & (blw >= CONFIG["min_pct_below_52w_high"])]
        print(f"  Gate 7 [Near 52W Low]          : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    print(f"{'='*64}\n  ✅ Survivors: {len(w)}\n{'='*64}\n")

    if len(w) == 0:
        print("[WARNING] ⚠️  Zero stocks passed all filters!")
        print("[TIP] Try relaxing:")
        print("  • min_roe_pct              → try 10")
        print("  • min_netincome_cagr_3y_pct → try 8")
        print("  • max_debt_to_equity       → try 1.0")
        print("  • max_pe_vs_sector_mult    → try 1.5")
        print("  • reject_shrinking_margins → try False")
        print("  • min_avg_daily_volume     → try 100000")

    return w


# =============================================================================
# SECTION 8: 6-CATEGORY SCORECARD ENGINE
# =============================================================================

def _ok(v) -> bool:
    return v is not None and not (isinstance(v, float) and np.isnan(v))


def scorecard_performance(row: pd.Series) -> dict:
    rs = row.get("rs_vs_nifty"); r1 = row.get("return_1y")
    if not _ok(rs) or not _ok(r1):
        return {"tag":"N/A","subtext":"Insufficient price history.","emoji":"⚪"}
    if rs > 15:
        return {"tag":"High",
                "subtext":f"Return {round(r1,1)}% outperforms Nifty by {round(rs,1)}%.",
                "emoji":"🟢"}
    elif rs > -10 and r1 > 0:
        return {"tag":"Avg","subtext":"Price return is average, in line with market.","emoji":"🟡"}
    return {"tag":"Low",
            "subtext":f"Underperforming market by {abs(round(rs,1))}% over 1 year.",
            "emoji":"🔴"}


def scorecard_valuation(row: pd.Series) -> dict:
    pe = row.get("pe_ratio"); spe = row.get("sector_median_pe")
    if not _ok(pe) or not _ok(spe) or spe == 0:
        return {"tag":"N/A","subtext":"Valuation data unavailable.","emoji":"⚪"}
    if pe < spe * 0.8:
        return {"tag":"Attractive",
                "subtext":f"PE {round(pe,1)}x well below sector median {round(spe,1)}x.",
                "emoji":"🟢"}
    elif pe <= spe * 1.2:
        return {"tag":"Avg","subtext":"Moderately valued vs sector peers.","emoji":"🟡"}
    return {"tag":"Expensive",
            "subtext":f"Trading {round((pe/spe-1)*100,1)}% above sector PE median.",
            "emoji":"🔴"}


def scorecard_growth(row: pd.Series) -> dict:
    rv3 = row.get("revenue_cagr_3y"); ni3 = row.get("netincome_cagr_3y")
    if not _ok(rv3) and not _ok(ni3):
        return {"tag":"N/A","subtext":"Insufficient financial history.","emoji":"⚪"}
    rv  = rv3 if _ok(rv3) else 0
    ni  = ni3 if _ok(ni3) else 0
    avg = (rv + ni) / 2
    if avg > 15:
        return {"tag":"High",
                "subtext":f"Rev CAGR {round(rv,1)}% + Profit CAGR {round(ni,1)}% — outstanding.",
                "emoji":"🟢"}
    elif avg > 5:
        return {"tag":"Avg","subtext":"Financial growth has been moderate.","emoji":"🟡"}
    return {"tag":"Low","subtext":"Stagnant or declining growth trends.","emoji":"🔴"}


def scorecard_profitability(row: pd.Series) -> dict:
    roe = row.get("roe"); gmc = row.get("gross_margin_current")
    gmp = row.get("gross_margin_prev")
    if not _ok(roe):
        return {"tag":"N/A","subtext":"Profitability data unavailable.","emoji":"⚪"}
    margin_ok = True
    if _ok(gmc) and _ok(gmp) and gmp > 0:
        margin_ok = gmc >= gmp * 0.95
    if roe > 20 and margin_ok:
        return {"tag":"High",
                "subtext":f"Strong profitability — ROE {round(roe,1)}% with stable margins.",
                "emoji":"🟢"}
    elif roe > 15:
        return {"tag":"High","subtext":"Good profitability and capital efficiency.","emoji":"🟢"}
    elif roe > 8:
        return {"tag":"Avg","subtext":"Stable but room for improvement.","emoji":"🟡"}
    return {"tag":"Low","subtext":"Struggling to generate efficient returns.","emoji":"🔴"}


def scorecard_entry_point(row: pd.Series) -> dict:
    rsi  = row.get("rsi_14"); price = row.get("current_price")
    s200 = row.get("sma_200")
    if not _ok(rsi):
        return {"tag":"N/A","subtext":"Technical data unavailable.","emoji":"⚪"}
    rf = round(rsi, 1)
    if rsi < 40:
        return {"tag":"Good","subtext":f"RSI {rf} — not overbought, decent entry.","emoji":"🟢"}
    elif 40 <= rsi <= 55 and _ok(price) and _ok(s200) and price > s200:
        return {"tag":"Good",
                "subtext":f"RSI {rf} with price above 200 SMA — healthy momentum.",
                "emoji":"🟢"}
    elif 55 < rsi <= 70:
        return {"tag":"Average","subtext":f"RSI {rf} — neutral zone, proceed carefully.","emoji":"🟡"}
    return {"tag":"Risky","subtext":f"RSI {rf} — overbought, pullback risk.","emoji":"🔴"}


def scorecard_red_flags(row: pd.Series) -> dict:
    de  = row.get("debt_to_equity"); cr  = row.get("current_ratio")
    ocf = row.get("operating_cash_flow"); cp = row.get("consistent_profit")
    heq = row.get("high_earnings_quality"); gmc = row.get("gross_margin_current")
    gmp = row.get("gross_margin_prev"); st = row.get("sector_type","non_financial")
    flags = []
    # High D/E is expected for banks — only flag for non-financial
    if st == "non_financial" and _ok(de) and de > 1.5:
        flags.append(f"High Debt (D/E: {round(de,2)})")
    if _ok(cr)  and cr < 1.0:
        flags.append(f"Poor Liquidity (CR: {round(cr,2)})")
    if _ok(ocf) and ocf < 0:
        flags.append("Negative Operating Cash Flow")
    if cp is not None and not cp:
        flags.append("Loss in recent years")
    if heq is not None and not heq:
        flags.append("Earnings quality concern (OCF < Net Income)")
    if _ok(gmc) and _ok(gmp) and gmp > 0 and gmc < gmp * 0.9:
        flags.append("Gross margin declining sharply")
    if len(flags) == 0:
        return {"tag":"Low","emoji":"🟢","subtext":"No red flag found.","flags":[]}
    elif len(flags) == 1:
        return {"tag":"Moderate","emoji":"🟡",
                "subtext":f"1 flag: {flags[0]}.","flags":flags}
    return {"tag":"High","emoji":"🔴",
            "subtext":f"{len(flags)} flags: {'; '.join(flags[:3])}.","flags":flags}


def generate_scorecard(row: pd.Series) -> dict:
    return {
        "performance":   scorecard_performance(row),
        "valuation":     scorecard_valuation(row),
        "growth":        scorecard_growth(row),
        "profitability": scorecard_profitability(row),
        "entry_point":   scorecard_entry_point(row),
        "red_flags":     scorecard_red_flags(row),
    }


# =============================================================================
# SECTION 9: COMPOSITE SCORING (v5.3 — never mutates original df)
# =============================================================================

def mn(series: pd.Series) -> pd.Series:
    lo, hi = series.min(), series.max()
    if hi == lo: return pd.Series([0.5]*len(series), index=series.index)
    return (series - lo) / (hi - lo)


def calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    [v5.3 FIX] Scoring now works on a COPY for median-fill operations.
    The original df passed in keeps its real NaN values intact.
    Scores + grades are added to the original df without polluting raw fields.
    """
    if df.empty:
        print("[WARNING] Nothing to score.")
        return df

    orig = df.copy()   # preserve real NaN values
    s    = df.copy()   # temp copy for median-fill scoring only

    def safe_fill(col):
        if col in s.columns:
            med = s[col].median()
            s[col] = s[col].fillna(med if pd.notna(med) else 0)

    for col in ["roe","debt_to_equity","pe_ratio","peg_ratio",
                "operating_cash_flow","revenue_cagr_3y","netincome_cagr_3y","rs_vs_nifty"]:
        safe_fill(col)

    def safe_mn(col):
        return mn(s[col]) if col in s.columns else pd.Series(0.5, index=s.index)

    roe_n = safe_mn("roe")
    de_n  = 1 - safe_mn("debt_to_equity")
    pe_n  = 1 - safe_mn("pe_ratio")
    peg_n = 1 - safe_mn("peg_ratio")
    rs_n  = safe_mn("rs_vs_nifty")
    rv_n  = safe_mn("revenue_cagr_3y")

    # Write grades back to orig (not to s)
    orig["quality_grade"]    = ((roe_n + de_n) / 2 * 10).round(2)
    orig["valuation_grade"]  = ((pe_n  + peg_n) / 2 * 10).round(2)
    orig["momentum_grade"]   = ((rs_n * 0.6 + rv_n * 0.4) * 10).round(2)
    orig["final_rank_score"] = (
        orig["quality_grade"]   * 0.35 +
        orig["valuation_grade"] * 0.35 +
        orig["momentum_grade"]  * 0.30
    ).round(2)

    bond = CONFIG.get("india_10y_bond_yield_pct", 6.7)
    if "pe_ratio" in orig.columns:
        orig["earnings_yield_pct"] = orig["pe_ratio"].apply(
            lambda pe: round(100/pe, 2) if _ok(pe) and pe > 0 else None
        )
        orig["beats_bond_yield"] = orig["earnings_yield_pct"].apply(
            lambda ey: bool(_ok(ey) and ey > bond)
        )
    else:
        orig["earnings_yield_pct"] = None
        orig["beats_bond_yield"]   = False

    orig.sort_values("final_rank_score", ascending=False, inplace=True)
    n   = min(CONFIG["top_n_results"], len(orig))
    top = orig.head(n).reset_index(drop=True)
    print(f"[INFO] Scoring complete. Top {len(top)} stocks selected.")
    return top


# =============================================================================
# SECTION 10: CSV WRITER — with scorecard columns + real values
# =============================================================================

def build_csv_df(top_df: pd.DataFrame) -> pd.DataFrame:
    """
    [v5.3] Builds the final CSV DataFrame with all desired columns including:
    - rank, overall_verdict
    - sc_performance, sc_valuation, sc_growth, sc_profitability,
      sc_entry_point, sc_red_flags  (tag only for CSV readability)
    - risk_note
    All original raw fields preserved with their actual values (no median fills).
    """
    rows = []
    for rank, (_, row) in enumerate(top_df.iterrows(), 1):
        sc  = generate_scorecard(row)
        frs = row.get("final_rank_score", 0)
        verdict = "STRONG" if frs >= 7 else ("MODERATE" if frs >= 4 else "WEAK")

        # FCF Yield
        mkt = row.get("market_cap_cr"); fcf = row.get("free_cash_flow")
        fcf_yield = None
        if _ok(fcf) and _ok(mkt) and mkt > 0:
            fcf_yield = round((fcf / (mkt * 1e7)) * 100, 2)

        rows.append({
            # ── Identity ──────────────────────────────────────────────────
            "rank":                    rank,
            "ticker":                  row.get("ticker"),
            "sector":                  row.get("sector"),
            "overall_verdict":         verdict,
            "final_rank_score":        row.get("final_rank_score"),
            "quality_grade":           row.get("quality_grade"),
            "valuation_grade":         row.get("valuation_grade"),
            "momentum_grade":          row.get("momentum_grade"),

            # ── Scorecard Tags ────────────────────────────────────────────
            "sc_performance":          sc["performance"]["tag"],
            "sc_valuation":            sc["valuation"]["tag"],
            "sc_growth":               sc["growth"]["tag"],
            "sc_profitability":        sc["profitability"]["tag"],
            "sc_entry_point":          sc["entry_point"]["tag"],
            "sc_red_flags":            sc["red_flags"]["tag"],

            # ── Price & Technical ─────────────────────────────────────────
            "current_price":           row.get("current_price"),
            "52w_high":                row.get("52w_high"),
            "52w_low":                 row.get("52w_low"),
            "pct_below_52w_high":      row.get("pct_below_52w_high"),
            "sma_50":                  row.get("sma_50"),
            "sma_200":                 row.get("sma_200"),
            "rsi_14":                  row.get("rsi_14"),
            "technical_trend":         row.get("technical_trend"),
            "volume_surge":            row.get("volume_surge"),
            "avg_daily_volume":        row.get("avg_daily_volume"),
            "return_1y_pct":           row.get("return_1y"),
            "rs_vs_nifty_pct":         row.get("rs_vs_nifty"),

            # ── Valuation ─────────────────────────────────────────────────
            "pe_ratio":                row.get("pe_ratio"),
            "sector_median_pe":        row.get("sector_median_pe"),
            "pb_ratio":                row.get("pb_ratio"),
            "sector_median_pb":        row.get("sector_median_pb"),
            "peg_ratio":               row.get("peg_ratio"),
            "earnings_yield_pct":      row.get("earnings_yield_pct"),
            "beats_bond_yield":        row.get("beats_bond_yield"),

            # ── Profitability ─────────────────────────────────────────────
            "roe_pct":                 row.get("roe"),
            "sector_median_roe":       row.get("sector_median_roe"),
            "debt_to_equity":          row.get("debt_to_equity"),
            "current_ratio":           row.get("current_ratio"),
            "gross_margin_current_pct":row.get("gross_margin_current"),
            "gross_margin_prev_pct":   row.get("gross_margin_prev"),
            "operating_margin_pct":    row.get("operating_margin"),

            # ── Growth ────────────────────────────────────────────────────
            "revenue_cagr_3y_pct":     row.get("revenue_cagr_3y"),
            "netincome_cagr_3y_pct":   row.get("netincome_cagr_3y"),
            "revenue_cagr_5y_pct":     row.get("revenue_cagr_5y"),
            "netincome_cagr_5y_pct":   row.get("netincome_cagr_5y"),

            # ── Cash Flow ─────────────────────────────────────────────────
            "operating_cash_flow":     row.get("operating_cash_flow"),
            "capital_expenditure":     row.get("capital_expenditure"),
            "free_cash_flow":          row.get("free_cash_flow"),
            "fcf_yield_pct":           fcf_yield,

            # ── Size & Ownership ──────────────────────────────────────────
            "market_cap_cr":           row.get("market_cap_cr"),
            "net_income":              row.get("net_income"),
            "institutional_holding_pct":row.get("institutional_holding_pct"),
            "consistent_profit":       row.get("consistent_profit"),
            "high_earnings_quality":   row.get("high_earnings_quality"),
            "data_years_available":    row.get("data_years_available"),

            # ── Meta ──────────────────────────────────────────────────────
            "risk_note":               get_risk(row.get("sector")),
            "run_date":                datetime.now().strftime("%Y-%m-%d"),
        })

    return pd.DataFrame(rows)


def safe_write_csv(df: pd.DataFrame, path: str):
    """Atomic write via temp file + rename. Fallback to timestamped name."""
    try:
        dir_  = os.path.dirname(os.path.abspath(path)) or "."
        tmp   = tempfile.NamedTemporaryFile(mode="w", suffix=".csv",
                                            dir=dir_, delete=False)
        df.to_csv(tmp.name, index=False)
        tmp.close()
        if os.path.exists(path): os.remove(path)
        shutil.move(tmp.name, path)
        print(f"[INFO] Report saved → {path} ({len(df)} stocks, {len(df.columns)} columns)")
    except Exception as e:
        fallback = f"Top_Stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(fallback, index=False)
        print(f"[WARNING] Could not write to '{path}' ({e}). Saved to '{fallback}'.")


# =============================================================================
# SECTION 11: VERDICT — TERMINAL SCORECARD PRINTER
# =============================================================================

def get_risk(sector) -> str:
    if not sector or pd.isna(sector): return SECTOR_RISK_MAP["default"]
    for k in SECTOR_RISK_MAP:
        if k.lower() in str(sector).lower(): return SECTOR_RISK_MAP[k]
    return SECTOR_RISK_MAP["default"]


def fv(val, sfx="", d=1):
    """Safe formatter — returns 'N/A' for None/NaN."""
    if val is None or (isinstance(val, float) and np.isnan(val)): return "N/A"
    try: return f"{round(float(val), d)}{sfx}"
    except Exception: return str(val)


def print_scorecard_tier(category: str, tier: dict):
    print(f"  {tier.get('emoji','⚪')}  {category:<14} [ {tier.get('tag','N/A')} ]")
    print(f"     {tier.get('subtext','')}")


def generate_verdict(row: pd.Series, rank: int, master_df: pd.DataFrame = None):
    try:
        ticker = row.get("ticker","N/A");   sector = row.get("sector","Unknown")
        frs    = row.get("final_rank_score",0)
        price  = row.get("current_price");  pe    = row.get("pe_ratio")
        spe    = row.get("sector_median_pe"); pb   = row.get("pb_ratio")
        spb    = row.get("sector_median_pb"); roe  = row.get("roe")
        sroe   = row.get("sector_median_roe"); de  = row.get("debt_to_equity")
        rv3    = row.get("revenue_cagr_3y");  ni3  = row.get("netincome_cagr_3y")
        rv5    = row.get("revenue_cagr_5y");  ni5  = row.get("netincome_cagr_5y")
        ocf    = row.get("operating_cash_flow"); mkt = row.get("market_cap_cr")
        blw    = row.get("pct_below_52w_high"); hi52 = row.get("52w_high")
        lo52   = row.get("52w_low");  sma50 = row.get("sma_50")
        sma200 = row.get("sma_200");  trend = row.get("technical_trend","N/A")
        rsi    = row.get("rsi_14");   r1y   = row.get("return_1y")
        rs     = row.get("rs_vs_nifty"); ih = row.get("institutional_holding_pct")
        ey     = row.get("earnings_yield_pct"); bb = row.get("beats_bond_yield",False)
        bond   = CONFIG.get("india_10y_bond_yield_pct", 6.7)
        qg     = row.get("quality_grade",0);   vg = row.get("valuation_grade",0)
        mg     = row.get("momentum_grade",0)
        fcf_y  = "N/A"
        if _ok(row.get("free_cash_flow")) and _ok(mkt) and mkt > 0:
            fcf_y = f"{round((row['free_cash_flow']/(mkt*1e7))*100,1)}%"

        sc        = generate_scorecard(row)
        score_tag = "🟢 STRONG" if frs >= 7 else ("🟡 MODERATE" if frs >= 4 else "🔴 WEAK")

        print("=" * 70)
        print(f"  #{rank:<3} {ticker:<26}  {sector}")
        print(f"  {score_tag}  |  Score: {frs:.2f}")
        print(f"  Quality: {qg:.1f}/10  Valuation: {vg:.1f}/10  Momentum: {mg:.1f}/10")
        print(f"  MCap: ₹{fv(mkt,' Cr',d=0):<12} CMP: ₹{fv(price,d=2):<12} RSI: {fv(rsi,d=1)}")
        print(f"  52W: ₹{fv(lo52,d=2)} – ₹{fv(hi52,d=2)}  Below High: {fv(blw,'%')}")
        print(f"  Trend: {trend}  |  SMA50: ₹{fv(sma50,d=1)}  SMA200: ₹{fv(sma200,d=1)}")
        print(f"  1Y Return: {fv(r1y,'%')}  RS vs Nifty: {fv(rs,'%',d=1)}  Inst: {fv(ih,'%')}")
        print("-" * 70)
        print(f"  FUNDAMENTALS")
        print(f"  PE: {fv(pe):<8} vs Sector: {fv(spe):<8} "
              f"PB: {fv(pb):<8} vs Sector: {fv(spb)}")
        print(f"  ROE: {fv(roe,'%'):<8} vs Sector: {fv(sroe,'%'):<8} D/E: {fv(de)}")
        print(f"  Rev CAGR: 3Y={fv(rv3,'%'):<8} 5Y={fv(rv5,'%')}")
        print(f"  PAT CAGR: 3Y={fv(ni3,'%'):<8} 5Y={fv(ni5,'%')}")
        ocf_cr = fv(ocf/1e7 if _ok(ocf) else None, " Cr", d=0)
        print(f"  OCF: ₹{ocf_cr:<10}  FCF Yield: {fcf_y}  "
              f"EPS Yield: {fv(ey,'%')}  "
              f"{'✅ Beats Bond' if bb else '❌ Below Bond'} ({bond}%)")
        print("-" * 70)
        print(f"  📋  {ticker} STOCK SCORECARD")
        print("-" * 70)
        for cat, key in [
            ("Performance",  "performance"),
            ("Valuation",    "valuation"),
            ("Growth",       "growth"),
            ("Profitability","profitability"),
            ("Entry Point",  "entry_point"),
            ("Red Flags",    "red_flags"),
        ]:
            print_scorecard_tier(cat, sc.get(key,{"tag":"N/A","subtext":"","emoji":"⚪"}))
            print()

        print("-" * 70)

        # Peer comparison — crash-proof
        if master_df is not None and str(sector) not in ["Unknown","nan","None",""]:
            try:
                peers = master_df[
                    (master_df["sector"].astype(str) == str(sector)) &
                    (master_df["ticker"] != ticker)
                ].copy()
                peers = peers[peers["pe_ratio"].notna() | peers["roe"].notna()]
                if not peers.empty and _ok(mkt):
                    peers["_diff"] = (peers["market_cap_cr"].fillna(0) - mkt).abs()
                    peers = peers.nsmallest(min(3, len(peers)), "_diff")
                    print(f"  📊  SECTOR PEERS  ({sector})")
                    print(f"  {'Ticker':<22} {'PE':>6} {'PB':>6} {'ROE':>8} {'1Y Ret':>8}")
                    print(f"  {'-'*22} {'-'*6} {'-'*6} {'-'*8} {'-'*8}")
                    print(f"  {str(ticker):<22} {fv(pe):>6} {fv(pb):>6} "
                          f"{fv(roe,'%'):>8} {fv(r1y,'%'):>8}  ← THIS STOCK")
                    for _, p in peers.iterrows():
                        print(f"  {str(p.get('ticker','?')):<22} "
                              f"{fv(p.get('pe_ratio')):>6} "
                              f"{fv(p.get('pb_ratio')):>6} "
                              f"{fv(p.get('roe'),'%'):>8} "
                              f"{fv(p.get('return_1y'),'%'):>8}")
                    print("-" * 70)
            except Exception:
                pass

        print(f"  ⚠️   RISK: {get_risk(sector)}")
        print(f"  NOT FINANCIAL ADVICE. Do your own due diligence.")
        print("=" * 70 + "\n")

    except Exception as e:
        print(f"[WARNING] Verdict failed for rank #{rank}: {e}")
        print("=" * 70 + "\n")


# =============================================================================
# SECTION 12: MAIN EXECUTION
# =============================================================================

def main():
    global CONFIG

    CONFIG = validate_and_sanitize_config(CONFIG)
    CONFIG = apply_archetype(CONFIG)
    arch   = CONFIG.get("archetype","Custom")

    print("\n" + "=" * 70)
    print(f"  NSE RECOMMENDATION ENGINE v5.3  |  Universe: {CONFIG['universe']}")
    if arch != "Custom": print(f"  Archetype : {arch}")
    print(f"  Run Date  : {datetime.now().strftime('%d %B %Y | %I:%M %p')}")
    print(f"  Cache     : {'ON' if CONFIG['use_cache'] else 'OFF'}  "
          f"| Schema: {CONFIG['cache_schema_version']}")
    print("=" * 70)

    if CONFIG.get("cache_force_refresh"):
        wipe_cache()

    tickers, sector_map = get_universe_tickers(CONFIG["universe"])

    fund_df = load_master_cache()
    if fund_df is None:
        fund_df = build_fundamental_dataframe(tickers, sector_map)
        save_master_cache(fund_df)

    price_df  = fetch_price_data(tickers)
    master_df = pd.merge(fund_df, price_df, on="ticker", how="left",
                         suffixes=("","_price"))

    # Canonical SMA column aliases
    for src, dst in [(f"sma_{CONFIG['sma_short']}", "sma_50"),
                     (f"sma_{CONFIG['sma_long']}",  "sma_200")]:
        if src in master_df.columns and dst not in master_df.columns:
            master_df[dst] = master_df[src]

    print_cache_stats(len(tickers))

    try:
        master_df.to_csv("raw_data.csv", index=False)
        print(f"[INFO] Raw data saved → raw_data.csv ({len(master_df)} rows)")
    except Exception as e:
        print(f"[WARNING] raw_data.csv save failed: {e}")

    filtered = apply_filters(master_df)
    if filtered.empty:
        print("[INFO] No stocks passed filters. Check raw_data.csv.")
        return

    top_df  = calculate_scores(filtered)
    csv_df  = build_csv_df(top_df)
    safe_write_csv(csv_df, CONFIG["output_csv"])

    print("=" * 70)
    print(f"  TOP {len(top_df)} RECOMMENDATIONS  |  Archetype: {arch}")
    print("=" * 70 + "\n")

    for i, (_, row) in enumerate(top_df.iterrows(), 1):
        generate_verdict(row, rank=i, master_df=master_df)

    print("=" * 70)
    print(f"  ✅ Done.  {len(top_df)} stocks out of {len(tickers)} scanned.")
    print(f"  📁  Report   → {CONFIG['output_csv']}")
    print(f"  📁  Raw Data → raw_data.csv")
    print(f"  📦  Cache    → {CONFIG['cache_folder']}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted. Partial results may be in raw_data.csv.")
    except Exception as e:
        import traceback
        print(f"\n[CRITICAL ERROR] {e}")
        traceback.print_exc()
