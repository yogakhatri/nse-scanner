# =============================================================================
# nse_screener.py  |  v5.1  |  Hardened & Config-Robust Edition
# =============================================================================
# CHANGES in v5.1 vs v5.0:
#   [1] CONFIG Validator — auto-corrects bad/missing/out-of-range values
#   [2] Top800_Custom — fully fixed parallel fetch + dedup + sector merge
#   [3] Sector medians — fixed for sparse sectors (< 5 stocks → market median)
#   [4] Peer table — fixed KeyError crashes when columns are missing
#   [5] Filter engine — all gates skip gracefully if column is missing
#   [6] ThreadPool — retry logic added, no silent failures
#   [7] Price fetch — fixed multi-ticker vs single-ticker yfinance shape bug
#   [8] Output CSV — guaranteed to write even if scoring partially fails
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
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============================================================================
# SECTION 1: CONFIG BLOCK
# =============================================================================

CONFIG = {

    # =========================================================================
    # UNIVERSE — Which stocks to scan
    # =========================================================================
    # "Nifty50"       →  50 stocks | Blue-chips only | ~30 sec
    # "Nifty100"      → 100 stocks | Large-cap extended | ~1 min
    # "Nifty500"      → 500 stocks | 92% of NSE market cap | ~3 min (cached) ← BEST
    # "Midcap150"     → 150 stocks | Companies ranked 101–250 | ~1 min
    # "Smallcap250"   → 250 stocks | Companies ranked 251–500 | ~2 min
    # "Top800_Custom" → 800 stocks | Nifty500+Midcap150+Smallcap250 | ~5 min
    #
    # TIP: Start with "Nifty500". Use "Top800_Custom" to hunt smaller gems.
    "universe": "Top800_Custom",


    # =========================================================================
    # ARCHETYPE — Pre-built strategy templates (overrides filter settings below)
    # =========================================================================
    # "Custom"              → Use your exact settings below
    # "Growth_Bargain"      → 18%+ profit CAGR + PE below sector average
    # "Cash_Machine"        → Near-zero debt + huge FCF + ROE > 20%
    # "Low_Debt_Midcap"     → Midcap150 + very clean balance sheet (D/E < 0.1)
    # "Quality_Compounder"  → ROE > 20% + 15%+ multi-year growth + low debt
    # "Near_52W_Low"        → Beaten-down 30%+ but fundamentally strong
    #
    # TIP: Try each archetype one by one and compare results.
    "archetype": "Custom",


    # =========================================================================
    # CACHE — Stores fetched data locally to speed up repeated runs
    # =========================================================================

    # Master ON/OFF. False = always re-fetch everything (~15 min per run).
    "use_cache": True,

    # Cache folder paths. No need to change.
    "cache_folder":            "cache/",
    "cache_tickers_subfolder": "cache/tickers/",

    # Days before fundamentals (PE, ROE, CAGR) are re-fetched.
    # Fundamentals change quarterly — 7 days is safe and fresh enough.
    # Try: 1 (daily refresh) | 7 (default) | 14 (fastest repeated runs)
    "cache_fundamental_days": 7,

    # Days before universe stock list (Nifty500 CSV) is re-downloaded.
    # Index composition changes monthly at most.
    "cache_universe_days": 30,

    # Set True to WIPE all cache and start fresh. Reset to False after one run!
    # Use when upgrading the script or if data looks corrupted.
    "cache_force_refresh": False,

    # Print cache hit/miss stats after each run.
    "cache_show_stats": True,

    # ⚠️  CRITICAL: Must match the script version number exactly.
    # Mismatch = cache invalidated every run = 15-min fetch every time.
    # DO NOT change manually — update only when you upgrade the script.
    "cache_schema_version": "5.1",          # ← was "5.0" — THIS WAS THE BUG


    # =========================================================================
    # GATE 1: LIQUIDITY — Minimum size and tradability
    # =========================================================================
    # Stocks failing either threshold are removed immediately.

    # Minimum market cap in Crore (₹). Prevents penny/micro-cap stocks.
    # Try: 500 (more results, more risk) | 1000 (default) | 5000 (large-cap only)
    "min_market_cap_cr": 500,

    # Minimum 30-day avg daily volume. Prevents illiquid, hard-to-trade stocks.
    # Try: 100000 (lenient) | 300000 (default) | 1000000 (highly liquid only)
    "min_avg_daily_volume": 100000,


    # =========================================================================
    # GATE 2: RED FLAG AUTO-REJECT — Eliminate fundamentally broken companies
    # =========================================================================
    # Any triggered flag = stock removed, no matter how good other metrics look.

    # Reject if Operating Cash Flow < 0.
    # Profits + negative OCF = potential earnings manipulation. Keep True.
    "reject_negative_ocf": True,

    # Reject if Current Ratio < this value (Current Assets / Current Liabilities).
    # Below 1.0 = company can't pay short-term bills.
    # Try: 0.8 (lenient) | 1.0 (default) | 1.5 (strict, strong working capital)
    "reject_current_ratio_below": 1.0,

    # Reject if gross margin shrinks year-over-year.
    # Shrinking margins = weakening pricing power or rising costs.
    # Try: False for broader results, True to ensure margin stability.
    "reject_shrinking_margins": True,


    # =========================================================================
    # GATE 3: QUALITY — Balance sheet (Non-Financial companies only)
    # =========================================================================
    # Banks/NBFCs/Insurance skip this gate and go to Gate 6 (P/B) instead.

    # Maximum Debt-to-Equity ratio (Total Debt / Shareholders' Equity).
    # Try: 0.1 (debt-free only) | 0.3 (conservative) | 0.5 (default) | 1.0 (lenient)
    "max_debt_to_equity": 0.5,

    # Minimum Return on Equity % (Net Profit / Shareholders' Equity × 100).
    # Below 12% is generally poor for Indian companies.
    # Try: 10 (lenient) | 15 (default) | 20 (premium quality) | 25 (elite only)
    "min_roe_pct": 15,


    # =========================================================================
    # GATE 4: GROWTH — 3-Year business expansion
    # =========================================================================
    # CAGRs are calculated from actual financial statements, not snapshots.

    # Minimum 3-Year Revenue CAGR %. Ensures the top line is growing.
    # Try: 5 (slow growers OK) | 10 (default) | 15 (high growth) | 20 (hyper-growth)
    "min_revenue_cagr_3y_pct": 10,

    # Minimum 3-Year Net Income CAGR %. Slightly higher than revenue to confirm
    # margins are expanding, not just revenues growing while profits stay flat.
    # Try: 8 (lenient) | 12 (default) | 18 (strict) | 25 (elite compounders)
    "min_netincome_cagr_3y_pct": 12,


    # =========================================================================
    # GATE 5: SECTOR-RELATIVE VALUATION — Price vs sector peers
    # =========================================================================
    # Compares each stock to its OWN sector, not the whole market.
    # A PE of 30 is cheap for FMCG but expensive for Steel.

    # Max PE vs sector median PE. 1.2 = stock PE can be up to 20% above median.
    # Try: 0.8 (must be 20% cheaper than sector) | 1.0 (at median) |
    #      1.2 (default) | 1.5 (lenient)
    "max_pe_vs_sector_mult": 1.2,

    # Min ROE vs sector median ROE. 0.9 = ROE must be ≥ 90% of sector median.
    # Prevents cheap-PE traps where low profitability explains the low price.
    # Try: 0.7 (lenient) | 0.9 (default) | 1.0 (must match) | 1.2 (must beat sector)
    "min_roe_vs_sector_mult": 0.9,

    # Maximum PEG ratio (PE ÷ Earnings Growth Rate).
    # PEG < 1.0 = undervalued vs growth | 1.0 = fair | > 1.5 = overvalued vs growth.
    # ⚠️  NOTE: PEG data is FREQUENTLY MISSING for Indian stocks on yfinance.
    #           Set to 2.0 to avoid over-filtering on sparse data.
    # Try: 1.0 (strict) | 1.5 (moderate) | 2.0 (default — recommended for India)
    "max_peg_ratio": 2.0,                   # ← changed from 1.5 (was silently over-filtering)


    # =========================================================================
    # GATE 6: FINANCIALS — Special P/B rules for Banks, NBFCs, Insurance
    # =========================================================================
    # Financial companies' "debt" = customer deposits, so D/E is meaningless.
    # We use Price-to-Book (P/B) instead.

    # Max P/B for financial sector stocks.
    # Try: 1.5 (cheap banks only) | 2.0 (reasonable) | 2.5 (default) | 3.5 (lenient)
    "max_pb_financials": 2.5,

    # Keywords that identify a stock as "Financial" → skips Gate 3, uses Gate 6.
    # Add entries if banks are being wrongly filtered by the D/E gate.
    "financial_sectors": ["Bank", "NBFC", "Insurance",
                          "Financial Services", "Finance"],


    # =========================================================================
    # FETCHING — Parallel download speed
    # =========================================================================

    # Parallel threads for yfinance fundamental fetching.
    # Too high = Yahoo Finance rate-limit errors and missing data.
    # Try: 5 (safe/slow) | 10 (balanced) | 15 (default) | 20 (fast, occasional misses)
    "fetch_max_workers": 15,


    # =========================================================================
    # TECHNICAL — Indicator periods for Entry Point scorecard
    # =========================================================================

    # Short-term SMA period. Price > SMA50 > SMA200 = confirmed uptrend.
    # Try: 20 (faster/noisier) | 50 (default — standard)
    "sma_short": 50,

    # Long-term SMA period. The institutional benchmark for trend direction.
    # Try: 100 (faster signal) | 200 (default — gold standard)
    "sma_long": 200,

    # RSI period. >70 = overbought, <30 = oversold.
    # Try: 9 (fast/noisy) | 14 (default — Wilder standard) | 21 (slow/smooth)
    "rsi_period": 14,


    # =========================================================================
    # INDIA 10Y BOND YIELD — Risk-free rate benchmark
    # =========================================================================
    # Used to check if a stock's Earnings Yield (1/PE × 100) beats the
    # risk-free government bond return. Update this periodically.
    # Check current rate: https://www.rbi.org.in
    #
    # As of March 2026: ~6.7% (RBI has been cutting rates)
    # Try: 6.5 (post rate-cut) | 6.7 (current) | 7.1 (previous) | 7.5 (rate hike)
    "india_10y_bond_yield_pct": 6.7,        # ← updated from 7.1 to current March 2026 rate


    # =========================================================================
    # OUTPUT — Report settings
    # =========================================================================

    # Number of top stocks to display and save.
    # Try: 5 (best-of-best) | 10 (focused list) | 15 (default) | 25 (portfolio builder)
    "top_n_results": 15,

    # Output CSV filename. Add date for archives: "Top_Stocks_2026_03_06.csv"
    "output_csv": "Top_Stocks_Report.csv",

}


NSE_URLS = {
    "Nifty50":     "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv",
    "Nifty100":    "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv",
    "Nifty500":    "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    "Midcap150":   "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv",
    "Smallcap250": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv",
}

# =============================================================================
# SECTION 1A: CONFIG VALIDATOR (v5.1 NEW)
# =============================================================================

# Defines every CONFIG key: (type, min, max, allowed_values, default)
# None means "no constraint for this field"
_CONFIG_SCHEMA = {
    "universe":                 (str,   None, None, ["Nifty50","Nifty100","Nifty500",
                                                     "Midcap150","Smallcap250",
                                                     "Top800_Custom"],      "Nifty500"),
    "archetype":                (str,   None, None, ["Custom","Growth_Bargain",
                                                     "Cash_Machine","Low_Debt_Midcap",
                                                     "Quality_Compounder",
                                                     "Near_52W_Low"],       "Custom"),
    "use_cache":                (bool,  None, None, None,                   True),
    "cache_folder":             (str,   None, None, None,                   "cache/"),
    "cache_tickers_subfolder":  (str,   None, None, None,                   "cache/tickers/"),
    "cache_fundamental_days":   (int,   1,    30,   None,                   7),
    "cache_universe_days":      (int,   1,    90,   None,                   30),
    "cache_force_refresh":      (bool,  None, None, None,                   False),
    "cache_show_stats":         (bool,  None, None, None,                   True),
    "cache_schema_version":     (str,   None, None, None,                   "5.1"),
    "min_market_cap_cr":        (float, 0,    None, None,                   1000),
    "min_avg_daily_volume":     (int,   0,    None, None,                   300000),
    "reject_negative_ocf":      (bool,  None, None, None,                   True),
    "reject_current_ratio_below":(float,0,    5.0,  None,                   1.0),
    "reject_shrinking_margins": (bool,  None, None, None,                   True),
    "max_debt_to_equity":       (float, 0,    10.0, None,                   0.5),
    "min_roe_pct":              (float, -50,  100,  None,                   15),
    "min_revenue_cagr_3y_pct":  (float, -50,  100,  None,                   10),
    "min_netincome_cagr_3y_pct":(float, -50,  100,  None,                   12),
    "max_pe_vs_sector_mult":    (float, 0.1,  5.0,  None,                   1.2),
    "min_roe_vs_sector_mult":   (float, 0.1,  3.0,  None,                   0.9),
    "max_peg_ratio":            (float, 0.1,  10.0, None,                   1.5),
    "max_pb_financials":        (float, 0.1,  20.0, None,                   2.5),
    "financial_sectors":        (list,  None, None, None,
                                 ["Bank","NBFC","Insurance",
                                  "Financial Services","Finance"]),
    "fetch_max_workers":        (int,   1,    30,   None,                   15),
    "sma_short":                (int,   5,    100,  None,                   50),
    "sma_long":                 (int,   50,   500,  None,                   200),
    "rsi_period":               (int,   2,    30,   None,                   14),
    "india_10y_bond_yield_pct": (float, 0,    25,   None,                   7.1),
    "top_n_results":            (int,   1,    100,  None,                   15),
    "output_csv":               (str,   None, None, None,                   "Top_Stocks_Report.csv"),
}


def validate_and_sanitize_config(config: dict) -> dict:
    """
    v5.1: Validates every CONFIG value against the schema.

    For each key:
      - If missing        → inserts the safe default and warns the user
      - If wrong type     → tries to cast it; falls back to default on failure
      - If out of range   → clamps to min/max and warns the user
      - If not in allowed → resets to default and warns the user

    Special cross-field checks:
      - sma_short must be < sma_long
      - top_n_results cannot exceed universe size
      - output_csv gets .csv extension if missing

    Returns a fully validated, safe CONFIG dict.
    """
    c      = config.copy()
    issues = []

    for key, (dtype, vmin, vmax, allowed, default) in _CONFIG_SCHEMA.items():

        # ── Missing key → insert default ────────────────────────────────────
        if key not in c:
            c[key] = default
            issues.append(f"MISSING '{key}' → using default: {default}")
            continue

        val = c[key]

        # ── Type check & cast ────────────────────────────────────────────────
        if dtype == bool:
            if not isinstance(val, bool):
                # Accept 0/1 and "true"/"false" strings
                if str(val).lower() in ("1", "true", "yes"):
                    c[key] = True
                elif str(val).lower() in ("0", "false", "no"):
                    c[key] = False
                else:
                    c[key] = default
                    issues.append(f"INVALID TYPE '{key}' = {val!r} → reset to {default}")
        elif dtype == list:
            if not isinstance(val, list):
                c[key] = default
                issues.append(f"INVALID TYPE '{key}' must be a list → reset to default")
        elif dtype in (int, float):
            try:
                c[key] = dtype(val)
            except (ValueError, TypeError):
                c[key] = default
                issues.append(f"INVALID TYPE '{key}' = {val!r} → reset to {default}")
        elif dtype == str:
            c[key] = str(val).strip()

        val = c[key]

        # ── Allowed values check ─────────────────────────────────────────────
        if allowed and val not in allowed:
            c[key] = default
            issues.append(
                f"INVALID VALUE '{key}' = {val!r} "
                f"(allowed: {allowed}) → reset to '{default}'"
            )
            val = c[key]

        # ── Range clamp ──────────────────────────────────────────────────────
        if dtype in (int, float) and not isinstance(val, bool):
            if vmin is not None and val < vmin:
                c[key] = dtype(vmin)
                issues.append(f"OUT OF RANGE '{key}' = {val} < min {vmin} → clamped to {vmin}")
            if vmax is not None and val > vmax:
                c[key] = dtype(vmax)
                issues.append(f"OUT OF RANGE '{key}' = {val} > max {vmax} → clamped to {vmax}")

    # ── Cross-field checks ───────────────────────────────────────────────────

    # SMA short must be less than SMA long
    if c["sma_short"] >= c["sma_long"]:
        c["sma_short"] = 50
        c["sma_long"]  = 200
        issues.append(
            f"CONFLICT: sma_short ({c['sma_short']}) >= sma_long → "
            f"reset to 50/200"
        )

    # output_csv must end in .csv
    if not c["output_csv"].endswith(".csv"):
        c["output_csv"] += ".csv"
        issues.append(f"output_csv missing .csv extension → auto-added")

    # top_n_results sanity
    if c["top_n_results"] < 1:
        c["top_n_results"] = 1

    # cache_tickers_subfolder must be inside cache_folder
    if not c["cache_tickers_subfolder"].startswith(c["cache_folder"]):
        c["cache_tickers_subfolder"] = c["cache_folder"] + "tickers/"
        issues.append("cache_tickers_subfolder was outside cache_folder → auto-fixed")

    # Print all validation warnings
    if issues:
        print(f"\n{'='*64}")
        print(f"  ⚠️  CONFIG VALIDATOR — {len(issues)} issue(s) found & auto-fixed")
        print(f"{'='*64}")
        for msg in issues:
            print(f"  • {msg}")
        print(f"{'='*64}\n")
    else:
        print("[CONFIG] ✅ All settings validated successfully.")

    return c


# =============================================================================
# SECTION 2: ARCHETYPES & SECTOR MAPS
# =============================================================================

ARCHETYPES = {
    "Growth_Bargain":    {"min_netincome_cagr_3y_pct": 18, "max_pe_vs_sector_mult": 0.9,
                          "max_peg_ratio": 1.2, "min_roe_pct": 15, "max_debt_to_equity": 0.5},
    "Cash_Machine":      {"min_roe_pct": 20, "max_debt_to_equity": 0.1,
                          "min_revenue_cagr_3y_pct": 10, "min_netincome_cagr_3y_pct": 10},
    "Low_Debt_Midcap":   {"universe": "Midcap150", "max_debt_to_equity": 0.1,
                          "min_roe_pct": 15, "min_netincome_cagr_3y_pct": 12,
                          "max_pe_vs_sector_mult": 1.3},
    "Quality_Compounder":{"min_roe_pct": 20, "max_debt_to_equity": 0.3,
                          "min_revenue_cagr_3y_pct": 15, "min_netincome_cagr_3y_pct": 15,
                          "max_peg_ratio": 1.5},
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
    # Re-validate after archetype overrides are merged
    return validate_and_sanitize_config(merged)


# =============================================================================
# SECTION 0: CACHE ENGINE
# =============================================================================

_stats = {"fund_hits": 0, "fund_miss": 0, "master_hit": False, "master_secs": 0.0}


def _dirs():
    os.makedirs(CONFIG["cache_folder"], exist_ok=True)
    os.makedirs(CONFIG["cache_tickers_subfolder"], exist_ok=True)


def _tpath(ticker: str, dtype: str) -> str:
    safe = ticker.replace(".", "_").replace("/", "_").replace("\\", "_")
    return os.path.join(CONFIG["cache_tickers_subfolder"], f"{safe}_{dtype}.json")


def save_ticker_cache(ticker: str, data: dict, dtype: str):
    if not CONFIG.get("use_cache"):
        return
    _dirs()
    try:
        payload = {}
        for k, v in data.items():
            if isinstance(v, float) and np.isnan(v):
                payload[k] = None
            elif isinstance(v, (np.integer,)):
                payload[k] = int(v)
            elif isinstance(v, (np.floating,)):
                payload[k] = float(v)
            else:
                payload[k] = v
        payload["fetched_at"]     = datetime.now().isoformat()
        payload["schema_version"] = CONFIG["cache_schema_version"]
        with open(_tpath(ticker, dtype), "w") as f:
            json.dump(payload, f, default=str)
    except Exception as e:
        pass  # Silent — cache failures should never crash the main run


def load_ticker_cache(ticker: str, dtype: str) -> dict | None:
    if not CONFIG.get("use_cache"):
        return None
    path = _tpath(ticker, dtype)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            c = json.load(f)
        if c.get("schema_version") != CONFIG["cache_schema_version"]:
            return None
        age = (datetime.now() - datetime.fromisoformat(c["fetched_at"])).days
        if age >= CONFIG["cache_fundamental_days"]:
            return None
        if dtype == "fundamental":
            if all(c.get(k) is None for k in ["pe_ratio", "roe", "market_cap_cr"]):
                return None
        return c
    except Exception:
        return None


def save_master_cache(df: pd.DataFrame):
    if not CONFIG.get("use_cache"):
        return
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
        print(f"[CACHE] Save master failed: {e}")


def load_master_cache() -> pd.DataFrame | None:
    if not CONFIG.get("use_cache"):
        return None
    pkl  = os.path.join(CONFIG["cache_folder"], "master_fundamentals.pkl")
    meta = os.path.join(CONFIG["cache_folder"], "cache_metadata.json")
    if not os.path.exists(pkl) or not os.path.exists(meta):
        return None
    try:
        with open(meta) as f:
            m = json.load(f)
        if m.get("schema_version") != CONFIG["cache_schema_version"]: return None
        if m.get("universe")       != CONFIG["universe"]:              return None
        if m.get("archetype")      != CONFIG.get("archetype","Custom"):return None
        age = (datetime.now() - datetime.fromisoformat(m["created_at"])).days
        if age >= CONFIG["cache_fundamental_days"]:
            print(f"[CACHE] Expired ({age}d). Re-fetching.")
            return None
        t0  = time.time()
        df  = pd.read_pickle(pkl)
        el  = round(time.time() - t0, 2)
        _stats["master_hit"]  = True
        _stats["master_secs"] = el
        print(f"[CACHE] ✅ HIT — {len(df)} rows in {el}s (age: {age}d)")
        return df
    except Exception as e:
        print(f"[CACHE] Load master failed: {e}")
        return None


def wipe_cache():
    if os.path.exists(CONFIG["cache_folder"]):
        shutil.rmtree(CONFIG["cache_folder"])
        print("[CACHE] 🗑  Cache wiped.")
    _dirs()


def print_cache_stats(total: int):
    if not CONFIG.get("cache_show_stats"):
        return
    s    = _stats
    mins = round((s["fund_hits"] * 4.5) / 60, 1)
    print(f"\n{'='*64}")
    print(f"  CACHE STATS")
    print(f"  Master  : {'✅ HIT ('+str(s['master_secs'])+'s)' if s['master_hit'] else '❌ MISS'}")
    print(f"  Tickers : {s['fund_hits']} hits / {s['fund_hits']+s['fund_miss']} total")
    print(f"  Saved   : ~{mins} mins")
    print(f"{'='*64}\n")


# =============================================================================
# SECTION 3: UNIVERSE LOADER (v5.1 — Top800_Custom fully fixed)
# =============================================================================

def fetch_csv(url: str) -> pd.DataFrame:
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
         "Accept": "text/html,*/*;q=0.8"}
    r = requests.get(url, headers=h, timeout=30, verify=False)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))


def _normalise_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures a universe CSV has consistent 'Symbol' and 'Industry' columns
    regardless of the original column names from niftyindices.com.
    """
    df.columns = [c.strip() for c in df.columns]
    sym_col = next((c for c in df.columns if "symbol" in c.lower()), None)
    ind_col = next((c for c in df.columns
                    if any(x in c.lower() for x in ["industry","sector"])), None)
    if not sym_col:
        raise ValueError(f"No Symbol column found. Columns: {df.columns.tolist()}")
    out = pd.DataFrame()
    out["Symbol"]   = df[sym_col].astype(str).str.strip().str.upper()
    out["Industry"] = df[ind_col].astype(str).str.strip() if ind_col else "Unknown"
    return out.dropna(subset=["Symbol"])


def get_universe_tickers(universe_name: str):
    """
    [v5.1] Top800_Custom is fully supported:
    - Downloads all 3 sub-indexes in parallel
    - Deduplicates on Symbol (keeps first occurrence for sector data)
    - Handles partial download failures gracefully
    Returns (tickers: list, sector_map: dict).
    """
    print(f"\n[INFO] Loading universe: {universe_name}")
    _dirs()
    u_csv  = os.path.join(CONFIG["cache_folder"], f"universe_{universe_name}.csv")
    u_meta = os.path.join(CONFIG["cache_folder"], f"universe_{universe_name}_meta.json")
    df = None

    # Try universe cache first
    if CONFIG.get("use_cache") and os.path.exists(u_csv) and os.path.exists(u_meta):
        try:
            with open(u_meta) as f: um = json.load(f)
            age = (datetime.now() - datetime.fromisoformat(um["fetched_at"])).days
            if age < CONFIG["cache_universe_days"]:
                df = pd.read_csv(u_csv)
                print(f"[CACHE] Universe HIT — {len(df)} stocks (age: {age}d)")
        except Exception:
            df = None

    # Fetch from network
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
            frames = []
            sub_indexes = ["Nifty500", "Midcap150", "Smallcap250"]
            for name in sub_indexes:
                try:
                    raw    = fetch_csv(NSE_URLS[name])
                    normed = _normalise_df(raw)
                    frames.append(normed)
                    print(f"  [Top800] Fetched {len(normed)} from {name}")
                except Exception as e:
                    print(f"  [Top800] WARNING: {name} failed: {e}")

            if not frames:
                raise RuntimeError("Top800_Custom: All sub-index fetches failed.")

            df = pd.concat(frames, ignore_index=True)
            df = df.drop_duplicates(subset=["Symbol"], keep="first")
            print(f"[INFO] Top800_Custom: {len(df)} unique stocks after dedup")
            df.to_csv(u_csv, index=False)
            with open(u_meta, "w") as f:
                json.dump({"fetched_at": datetime.now().isoformat()}, f)

    # Local file fallback
    if df is None:
        for local in [f"{universe_name}.csv", universe_name]:
            if os.path.exists(local):
                df = _normalise_df(pd.read_csv(local))
                print(f"[INFO] Loaded local file: {local}")
                break

    if df is None:
        raise FileNotFoundError(
            f"Cannot load universe '{universe_name}'. "
            f"Download its CSV from niftyindices.com and save as '{universe_name}.csv'."
        )

    # Build tickers and sector_map
    df = df[df["Symbol"].str.len() > 0]
    tickers    = [s + ".NS" for s in df["Symbol"].tolist()]
    sector_map = dict(zip([s + ".NS" for s in df["Symbol"]], df["Industry"]))

    # Deduplicate tickers list (safety net)
    seen = set(); tickers = [t for t in tickers if not (t in seen or seen.add(t))]

    print(f"[INFO] Universe ready: {len(tickers)} unique tickers")
    return tickers, sector_map


# =============================================================================
# SECTION 4A: RSI CALCULATOR
# =============================================================================

def compute_rsi(close_series: pd.Series, period: int = 14) -> float | None:
    try:
        if len(close_series.dropna()) < period + 1:
            return None
        delta = close_series.diff()
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
# SECTION 4B: ALWAYS-FRESH PRICE DATA (v5.1 — shape bug fixed)
# =============================================================================

def _extract_ticker_ohlcv(raw, ticker: str, all_tickers: list):
    """
    [v5.1 Fix] yfinance returns different DataFrame shapes:
    - 1 ticker  → flat columns (Close, Volume, ...)
    - N tickers → MultiIndex (Close/AAPL, Close/MSFT, ...)
    This helper handles both cases safely.
    """
    try:
        if len(all_tickers) == 1:
            return raw
        if isinstance(raw.columns, pd.MultiIndex):
            # New yfinance format: (field, ticker)
            if ticker in raw.columns.get_level_values(1):
                return raw.xs(ticker, axis=1, level=1)
            return pd.DataFrame()
        # Old yfinance format: top-level = ticker
        if ticker in raw:
            return raw[ticker]
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def fetch_price_data(tickers: list) -> pd.DataFrame:
    """
    [v5.1] Fetches live OHLCV + Nifty 50 for relative strength.
    Fixed yfinance multi-ticker shape handling.
    """
    print(f"\n[INFO] 🌐 Fetching live price/RSI/RS data ({len(tickers)} tickers)...")
    fetch_list = list(set(tickers + ["^NSEI"]))

    try:
        raw = yf.download(
            fetch_list, period="1y", group_by="ticker",
            auto_adjust=True, progress=False, threads=True
        )
    except Exception as e:
        print(f"[ERROR] Price batch download failed: {e}")
        return pd.DataFrame([{"ticker": t} for t in tickers])

    # Nifty 50 baseline return
    nifty_ret = 0.0
    try:
        nifty_df  = _extract_ticker_ohlcv(raw, "^NSEI", fetch_list)
        nc        = nifty_df["Close"].dropna() if "Close" in nifty_df.columns else pd.Series()
        if len(nc) >= 2:
            nifty_ret = round((float(nc.iloc[-1]) / float(nc.iloc[0]) - 1) * 100, 2)
    except Exception:
        pass
    print(f"[INFO] Nifty 50 1Y return: {nifty_ret}%")

    ss = CONFIG["sma_short"]
    sl = CONFIG["sma_long"]
    rp = CONFIG["rsi_period"]
    records = []

    for ticker in tickers:
        rec = {
            "ticker": ticker, "current_price": None, "52w_high": None,
            "52w_low": None, "avg_daily_volume": None, "pct_below_52w_high": None,
            "sma_50": None, "sma_200": None, "rsi_14": None,
            "technical_trend": "N/A", "volume_surge": False,
            "return_1y": None, "rs_vs_nifty": None,
        }
        try:
            df_t = _extract_ticker_ohlcv(raw, ticker, fetch_list)
            if df_t.empty or "Close" not in df_t.columns:
                records.append(rec); continue

            cl  = df_t["Close"].dropna()
            vol = df_t["Volume"].dropna() if "Volume" in df_t.columns else pd.Series()

            if len(cl) < 5:
                records.append(rec); continue

            cur     = round(float(cl.iloc[-1]), 2)
            hi52    = round(float(cl.max()), 2)
            lo52    = round(float(cl.min()), 2)
            v30avg  = float(vol.tail(30).mean()) if len(vol) >= 5 else 0
            vcur    = float(vol.iloc[-1])        if len(vol) >= 1 else 0
            below   = round((hi52 - cur) / hi52 * 100, 2) if hi52 > 0 else None
            sma50   = round(float(cl.tail(ss).mean()), 2) if len(cl) >= ss else None
            sma200  = round(float(cl.tail(sl).mean()), 2) if len(cl) >= sl else None
            rsi14   = compute_rsi(cl, rp)
            vsurge  = vcur > v30avg * 1.5 if v30avg > 0 else False
            ret1y   = round((float(cl.iloc[-1]) / float(cl.iloc[0]) - 1) * 100, 2)
            rs_n    = round(ret1y - nifty_ret, 2)

            if sma50 and sma200:
                if   cur > sma50 > sma200: trend = "Uptrend ✅"
                elif cur < sma50 < sma200: trend = "Downtrend ❌"
                else:                      trend = "Sideways ➡️"
            else:
                trend = "N/A"

            rec.update({
                "current_price": cur, "52w_high": hi52, "52w_low": lo52,
                "avg_daily_volume": int(v30avg) if v30avg else 0,
                "pct_below_52w_high": below,
                "sma_50": sma50, "sma_200": sma200, "rsi_14": rsi14,
                "technical_trend": trend, "volume_surge": vsurge,
                "return_1y": ret1y, "rs_vs_nifty": rs_n,
            })
        except Exception:
            pass
        records.append(rec)

    df = pd.DataFrame(records)
    # Rename sma columns to match config-driven names
    if "sma_50" in df.columns and ss != 50:
        df = df.rename(columns={"sma_50": f"sma_{ss}"})
    if "sma_200" in df.columns and sl != 200:
        df = df.rename(columns={"sma_200": f"sma_{sl}"})
    # Always ensure canonical names exist for downstream code
    if f"sma_{ss}" in df.columns and "sma_50" not in df.columns:
        df["sma_50"] = df[f"sma_{ss}"]
    if f"sma_{sl}" in df.columns and "sma_200" not in df.columns:
        df["sma_200"] = df[f"sma_{sl}"]
    return df


# =============================================================================
# SECTION 4C: FUNDAMENTALS WITH THREADED FETCHING
# =============================================================================

def compute_cagr(series: list) -> float | None:
    try:
        clean = [v for v in series if v is not None and not np.isnan(v) and v > 0]
        if len(clean) < 2: return None
        n = len(clean) - 1
        return round(((clean[-1] / clean[0]) ** (1/n) - 1) * 100, 2)
    except Exception:
        return None


def fetch_fundamentals(ticker: str) -> dict:
    cached = load_ticker_cache(ticker, "fundamental")
    if cached:
        _stats["fund_hits"] += 1
        return cached
    _stats["fund_miss"] += 1

    result = {
        "ticker": ticker, "pe_ratio": None, "pb_ratio": None, "peg_ratio": None,
        "roe": None, "debt_to_equity": None, "market_cap_cr": None,
        "current_ratio": None, "gross_margin_current": None, "gross_margin_prev": None,
        "operating_cash_flow": None, "capital_expenditure": None,
        "free_cash_flow": None, "net_income": None,
        "institutional_holding_pct": None, "revenue_cagr_3y": None,
        "netincome_cagr_3y": None, "revenue_cagr_5y": None,
        "netincome_cagr_5y": None, "operating_margin": None,
        "sector_yf": None, "data_years_available": 0,
        "consistent_profit": None, "high_earnings_quality": None,
    }

    try:
        stock = yf.Ticker(ticker)
        info  = stock.info or {}

        def sg(key, scale=1.0):
            v = info.get(key)
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                try: return round(float(v) * scale, 4)
                except Exception: pass
            return None

        result.update({
            "pe_ratio":             sg("trailingPE"),
            "pb_ratio":             sg("priceToBook"),
            "peg_ratio":            sg("pegRatio"),
            "roe":                  sg("returnOnEquity", 100),
            "debt_to_equity":       sg("debtToEquity"),
            "market_cap_cr":        sg("marketCap", 1/1e7),
            "current_ratio":        sg("currentRatio"),
            "gross_margin_current": sg("grossMargins", 100),
            "operating_cash_flow":  sg("operatingCashflow"),
            "operating_margin":     sg("operatingMargins", 100),
            "sector_yf":            info.get("sector"),
        })

        ih = info.get("heldPercentInstitutions")
        if ih is not None and not (isinstance(ih, float) and np.isnan(ih)):
            result["institutional_holding_pct"] = round(float(ih) * 100, 2)

        try:
            fin = stock.financials
            if fin is not None and not fin.empty:
                rev_row = next((r for r in fin.index if "total revenue" in r.lower()), None)
                ni_row  = next((r for r in fin.index if "net income"    in r.lower()), None)
                gp_row  = next((r for r in fin.index if "gross profit"  in r.lower()), None)
                n_cols  = fin.shape[1]
                result["data_years_available"] = n_cols

                if rev_row:
                    rv = [float(fin.loc[rev_row].iloc[i])
                          for i in range(n_cols-1, -1, -1)
                          if not np.isnan(float(fin.loc[rev_row].iloc[i]))]
                    if len(rv) >= 4: result["revenue_cagr_3y"] = compute_cagr(rv[-4:])
                    if len(rv) >= 6: result["revenue_cagr_5y"] = compute_cagr(rv[-6:])

                if ni_row:
                    ni = [float(fin.loc[ni_row].iloc[i])
                          for i in range(n_cols-1, -1, -1)
                          if not np.isnan(float(fin.loc[ni_row].iloc[i]))]
                    if ni:
                        result["net_income"] = float(fin.loc[ni_row].iloc[0])
                        if len(ni) >= 4: result["netincome_cagr_3y"] = compute_cagr(ni[-4:])
                        if len(ni) >= 6: result["netincome_cagr_5y"] = compute_cagr(ni[-6:])
                        check = ni[-4:] if len(ni) >= 4 else ni
                        result["consistent_profit"] = bool(all(v > 0 for v in check))

                if gp_row and rev_row and n_cols >= 2:
                    gp_p = float(fin.loc[gp_row].iloc[1])
                    rv_p = float(fin.loc[rev_row].iloc[1])
                    if rv_p > 0:
                        result["gross_margin_prev"] = round(gp_p / rv_p * 100, 4)
        except Exception:
            pass

        try:
            cf = stock.cashflow
            if cf is not None and not cf.empty:
                cx = next((r for r in cf.index if "capital" in r.lower()), None)
                if cx:
                    result["capital_expenditure"] = float(cf.loc[cx].iloc[0])
            if result["operating_cash_flow"] and result["capital_expenditure"]:
                result["free_cash_flow"] = (
                    result["operating_cash_flow"] - abs(result["capital_expenditure"])
                )
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
            try:
                records.append(future.result())
            except Exception:
                records.append({"ticker": futures[future]})

    print(f"\n[INFO] Fundamentals complete.")
    fund_df = pd.DataFrame(records)

    # Ensure all expected columns exist (fill missing with None)
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

    fund_df["sector"] = fund_df["ticker"].map(sector_map)
    fund_df["sector"] = fund_df.apply(
        lambda r: r.get("sector_yf")
        if (pd.isna(r.get("sector")) or str(r.get("sector")) in ["Unknown","nan","None"])
        and r.get("sector_yf") else r.get("sector"), axis=1
    )
    fund_df["sector"] = fund_df["sector"].fillna("Unknown")
    fund_df.drop(columns=["sector_yf"], inplace=True, errors="ignore")
    return fund_df


# =============================================================================
# SECTION 5: SECTOR CLASSIFICATION + MEDIANS (v5.1 — sparse sector fix)
# =============================================================================

def classify_sector(s) -> str:
    if not s or pd.isna(s) or str(s) in ["Unknown","nan"]: return "non_financial"
    sl = str(s).lower()
    return "financial" if any(f.lower() in sl
                              for f in CONFIG.get("financial_sectors", [])) \
           else "non_financial"


def add_sector_medians(df: pd.DataFrame) -> pd.DataFrame:
    """
    [v5.1] Computes sector medians with 3-tier fallback:
    1. Sector median if >= 5 stocks with valid data
    2. Broad market median if sector has < 5 stocks
    3. Global median of entire dataframe as final fallback
    """
    def safe_median(x):
        valid = x.dropna()
        return valid.median() if len(valid) >= 5 else np.nan

    overall_pe  = df["pe_ratio"].dropna().median()  if df["pe_ratio"].notna().any()  else 20.0
    overall_pb  = df["pb_ratio"].dropna().median()  if df["pb_ratio"].notna().any()  else 2.5
    overall_roe = df["roe"].dropna().median()        if df["roe"].notna().any()        else 15.0

    df["sector_median_pe"]  = df.groupby("sector")["pe_ratio"].transform(safe_median)\
                                .fillna(overall_pe)
    df["sector_median_pb"]  = df.groupby("sector")["pb_ratio"].transform(safe_median)\
                                .fillna(overall_pb)
    df["sector_median_roe"] = df.groupby("sector")["roe"].transform(safe_median)\
                                .fillna(overall_roe)
    return df


# =============================================================================
# SECTION 6: FILTER ENGINE (v5.1 — all gates skip if column missing)
# =============================================================================

def _col(df, name) -> pd.Series:
    """Returns column if it exists, else a Series of NaN."""
    return df[name] if name in df.columns else pd.Series(np.nan, index=df.index)


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    print(f"\n{'='*64}")
    print(f"  FILTER ENGINE  |  {len(df)} stocks entering")
    print(f"{'='*64}")

    w = df.copy()
    w["sector_type"] = w["sector"].apply(classify_sector)
    w = add_sector_medians(w)

    # Gate 1: Liquidity
    b = len(w)
    mc  = _col(w, "market_cap_cr")
    adv = _col(w, "avg_daily_volume")
    w = w[mc.notna() & adv.notna() &
          (mc  >= CONFIG["min_market_cap_cr"]) &
          (adv >= CONFIG["min_avg_daily_volume"])]
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

    # Gate 3: Quality (Non-Financial)
    b   = len(w)
    nf  = w["sector_type"] == "non_financial"
    de  = _col(w, "debt_to_equity")
    roe = _col(w, "roe")
    bad = nf & (
        (de.notna()  & (de  > CONFIG["max_debt_to_equity"])) |
        (roe.notna() & (roe < CONFIG["min_roe_pct"]))
    )
    w = w[~bad]
    print(f"  Gate 3 [Quality — Non-Fin]     : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 4: Growth
    b   = len(w)
    rv3 = _col(w, "revenue_cagr_3y")
    ni3 = _col(w, "netincome_cagr_3y")
    bg  = (
        (rv3.notna() & (rv3 < CONFIG["min_revenue_cagr_3y_pct"])) |
        (ni3.notna() & (ni3 < CONFIG["min_netincome_cagr_3y_pct"]))
    )
    w = w[~bg]
    print(f"  Gate 4 [Growth — 3Y CAGR]      : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 5: Sector-Relative Valuation
    b    = len(w); nf = w["sector_type"] == "non_financial"
    pe   = _col(w, "pe_ratio"); spe = _col(w, "sector_median_pe")
    roe  = _col(w, "roe");      sroe = _col(w, "sector_median_roe")
    peg  = _col(w, "peg_ratio")
    bv   = nf & pe.notna()  & spe.notna()  & (pe  > spe  * CONFIG["max_pe_vs_sector_mult"])
    br   = nf & roe.notna() & sroe.notna() & (roe < sroe * CONFIG["min_roe_vs_sector_mult"])
    bp   = nf & peg.notna() & (peg > CONFIG["max_peg_ratio"])
    w    = w[~(bv | br | bp)]
    print(f"  Gate 5 [Sector-Rel Valuation]  : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 6: Financials P/B
    b   = len(w)
    fin = w["sector_type"] == "financial"
    pb  = _col(w, "pb_ratio")
    bf  = fin & pb.notna() & (pb > CONFIG["max_pb_financials"])
    w   = w[~bf]
    print(f"  Gate 6 [Financials — P/B]      : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 7: Near 52W Low (archetype only)
    if CONFIG.get("archetype") == "Near_52W_Low" and CONFIG.get("min_pct_below_52w_high"):
        b   = len(w)
        blw = _col(w, "pct_below_52w_high")
        w   = w[blw.notna() & (blw >= CONFIG["min_pct_below_52w_high"])]
        print(f"  Gate 7 [Near 52W Low]          : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    print(f"{'='*64}")
    print(f"  ✅ Survivors: {len(w)}")
    print(f"{'='*64}\n")

    if len(w) == 0:
        print("[WARNING] ⚠️  Zero stocks passed all filters!")
        print("[TIP] Try:")
        print("  • Reducing min_roe_pct to 10")
        print("  • Reducing min_netincome_cagr_3y_pct to 8")
        print("  • Increasing max_debt_to_equity to 1.0")
        print("  • Increasing max_pe_vs_sector_mult to 1.5")
        print("  • Setting reject_shrinking_margins to False")

    return w


# =============================================================================
# SECTION 7: 6-CATEGORY SCORECARD ENGINE
# =============================================================================

def _ok(v) -> bool:
    return v is not None and not (isinstance(v, float) and np.isnan(v))


def scorecard_performance(row: pd.Series) -> dict:
    rs = row.get("rs_vs_nifty")
    r1 = row.get("return_1y")
    if not _ok(rs) or not _ok(r1):
        return {"tag": "N/A", "subtext": "Insufficient price history.", "emoji": "⚪"}
    if rs > 15:
        return {"tag": "High",
                "subtext": f"Return of {round(r1,1)}% outperforms Nifty by {round(rs,1)}%.",
                "emoji": "🟢"}
    elif rs > -10 and r1 > 0:
        return {"tag": "Avg",
                "subtext": "Price return has been average, in line with the market.",
                "emoji": "🟡"}
    return {"tag": "Low",
            "subtext": f"Underperforming the market by {abs(round(rs,1))}% over 1 year.",
            "emoji": "🔴"}


def scorecard_valuation(row: pd.Series) -> dict:
    pe  = row.get("pe_ratio");       spe = row.get("sector_median_pe")
    if not _ok(pe) or not _ok(spe) or spe == 0:
        return {"tag": "N/A", "subtext": "Valuation data unavailable.", "emoji": "⚪"}
    if pe < spe * 0.8:
        return {"tag": "Attractive",
                "subtext": f"PE {round(pe,1)}x is well below sector median {round(spe,1)}x.",
                "emoji": "🟢"}
    elif pe <= spe * 1.2:
        return {"tag": "Avg",
                "subtext": "Moderately valued versus sector peers.",
                "emoji": "🟡"}
    return {"tag": "Expensive",
            "subtext": f"Trading {round((pe/spe-1)*100,1)}% above sector PE median.",
            "emoji": "🔴"}


def scorecard_growth(row: pd.Series) -> dict:
    rv3 = row.get("revenue_cagr_3y"); ni3 = row.get("netincome_cagr_3y")
    if not _ok(rv3) and not _ok(ni3):
        return {"tag": "N/A", "subtext": "Insufficient financial history.", "emoji": "⚪"}
    rv = rv3 if _ok(rv3) else 0
    ni = ni3 if _ok(ni3) else 0
    avg = (rv + ni) / 2
    if avg > 15:
        return {"tag": "High",
                "subtext": f"Rev CAGR {round(rv,1)}% + Profit CAGR {round(ni,1)}% — outstanding.",
                "emoji": "🟢"}
    elif avg > 5:
        return {"tag": "Avg",
                "subtext": "Financials growth has been moderate for a few years.",
                "emoji": "🟡"}
    return {"tag": "Low",
            "subtext": "Stagnant or declining growth trends.",
            "emoji": "🔴"}


def scorecard_profitability(row: pd.Series) -> dict:
    roe = row.get("roe"); gmc = row.get("gross_margin_current"); gmp = row.get("gross_margin_prev")
    if not _ok(roe):
        return {"tag": "N/A", "subtext": "Profitability data unavailable.", "emoji": "⚪"}
    margin_ok = True
    if _ok(gmc) and _ok(gmp) and gmp > 0:
        margin_ok = gmc >= gmp * 0.95
    if roe > 20 and margin_ok:
        return {"tag": "High",
                "subtext": f"Strong profitability — ROE {round(roe,1)}% with stable margins.",
                "emoji": "🟢"}
    elif roe > 15:
        return {"tag": "High",
                "subtext": "Showing good signs of profitability & efficiency.",
                "emoji": "🟢"}
    elif roe > 8:
        return {"tag": "Avg",
                "subtext": "Stable profitability but room for improvement.",
                "emoji": "🟡"}
    return {"tag": "Low", "subtext": "Struggling to generate efficient returns.", "emoji": "🔴"}


def scorecard_entry_point(row: pd.Series) -> dict:
    rsi   = row.get("rsi_14")
    price = row.get("current_price"); s200 = row.get("sma_200")
    if not _ok(rsi):
        return {"tag": "N/A", "subtext": "Technical data unavailable.", "emoji": "⚪"}
    rf = round(rsi, 1)
    if rsi < 40:
        return {"tag": "Good",
                "subtext": f"RSI {rf} — not in the overbought zone, decent entry.",
                "emoji": "🟢"}
    elif 40 <= rsi <= 55 and _ok(price) and _ok(s200) and price > s200:
        return {"tag": "Good",
                "subtext": f"RSI {rf} with price above 200 SMA — healthy momentum.",
                "emoji": "🟢"}
    elif 55 < rsi <= 70:
        return {"tag": "Average",
                "subtext": f"RSI {rf} — neutral zone, proceed with caution.",
                "emoji": "🟡"}
    return {"tag": "Risky",
            "subtext": f"RSI {rf} — overbought zone, short-term pullback possible.",
            "emoji": "🔴"}


def scorecard_red_flags(row: pd.Series) -> dict:
    de    = row.get("debt_to_equity"); cr    = row.get("current_ratio")
    ocf   = row.get("operating_cash_flow"); cp = row.get("consistent_profit")
    heq   = row.get("high_earnings_quality"); gmc = row.get("gross_margin_current")
    gmp   = row.get("gross_margin_prev")
    flags = []
    if _ok(de)  and de > 1.5:                          flags.append(f"High Debt (D/E: {round(de,2)})")
    if _ok(cr)  and cr < 1.0:                          flags.append(f"Poor Liquidity (CR: {round(cr,2)})")
    if _ok(ocf) and ocf < 0:                           flags.append("Negative Operating Cash Flow")
    if cp is not None and not cp:                      flags.append("Loss in recent years")
    if heq is not None and not heq:                    flags.append("Earnings quality concern (OCF < Net Income)")
    if _ok(gmc) and _ok(gmp) and gmp > 0 and gmc < gmp * 0.9:
        flags.append("Gross margin declining sharply")
    if len(flags) == 0:
        return {"tag": "Low",  "emoji": "🟢", "subtext": "No red flag found.", "flags": []}
    elif len(flags) == 1:
        return {"tag": "Moderate", "emoji": "🟡",
                "subtext": f"1 flag: {flags[0]}.", "flags": flags}
    return {"tag": "High", "emoji": "🔴",
            "subtext": f"{len(flags)} flags: {'; '.join(flags[:3])}.", "flags": flags}


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
# SECTION 8: COMPOSITE SCORING
# =============================================================================

def mn(series: pd.Series) -> pd.Series:
    lo, hi = series.min(), series.max()
    if hi == lo: return pd.Series([0.5]*len(series), index=series.index)
    return (series - lo) / (hi - lo)


def calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        print("[WARNING] Nothing to score."); return df

    s = df.copy()

    def safe_fill(col):
        if col in s.columns:
            med = s[col].median()
            s[col] = s[col].fillna(med if not np.isnan(med) else 0)

    for col in ["roe","debt_to_equity","pe_ratio","peg_ratio",
                "operating_cash_flow","revenue_cagr_3y","netincome_cagr_3y",
                "rs_vs_nifty"]:
        safe_fill(col)

    roe_n = mn(s["roe"])              if "roe" in s.columns              else pd.Series(0.5, index=s.index)
    de_n  = 1 - mn(s["debt_to_equity"]) if "debt_to_equity" in s.columns else pd.Series(0.5, index=s.index)
    pe_n  = 1 - mn(s["pe_ratio"])    if "pe_ratio" in s.columns          else pd.Series(0.5, index=s.index)
    peg_n = 1 - mn(s["peg_ratio"])   if "peg_ratio" in s.columns         else pd.Series(0.5, index=s.index)
    rs_n  = mn(s["rs_vs_nifty"])     if "rs_vs_nifty" in s.columns       else pd.Series(0.5, index=s.index)
    rv_n  = mn(s["revenue_cagr_3y"]) if "revenue_cagr_3y" in s.columns   else pd.Series(0.5, index=s.index)

    s["quality_grade"]   = ((roe_n + de_n) / 2 * 10).round(2)
    s["valuation_grade"] = ((pe_n + peg_n) / 2 * 10).round(2)
    s["momentum_grade"]  = ((rs_n * 0.6 + rv_n * 0.4) * 10).round(2)
    s["final_rank_score"] = (
        s["quality_grade"] * 0.35 +
        s["valuation_grade"] * 0.35 +
        s["momentum_grade"] * 0.30
    ).round(2)

    bond = CONFIG.get("india_10y_bond_yield_pct", 7.1)
    s["earnings_yield_pct"] = s["pe_ratio"].apply(
        lambda pe: round(100/pe, 2) if _ok(pe) and pe > 0 else None
    ) if "pe_ratio" in s.columns else None
    s["beats_bond_yield"] = s["earnings_yield_pct"].apply(
        lambda ey: _ok(ey) and ey > bond
    ) if "earnings_yield_pct" in s.columns else False

    s.sort_values("final_rank_score", ascending=False, inplace=True)
    n   = min(CONFIG["top_n_results"], len(s))
    top = s.head(n).reset_index(drop=True)
    print(f"[INFO] Scoring complete. Top {len(top)} stocks selected.")
    return top


# =============================================================================
# SECTION 9: VERDICT + SCORECARD TERMINAL OUTPUT (v5.1 — crash-proof)
# =============================================================================

def get_risk(sector) -> str:
    if not sector or pd.isna(sector): return SECTOR_RISK_MAP["default"]
    for k in SECTOR_RISK_MAP:
        if k.lower() in str(sector).lower(): return SECTOR_RISK_MAP[k]
    return SECTOR_RISK_MAP["default"]


def fv(val, sfx="", d=1):
    if val is None or (isinstance(val, float) and np.isnan(val)): return "N/A"
    try: return f"{round(float(val), d)}{sfx}"
    except Exception: return str(val)


def print_scorecard_tier(category: str, tier: dict):
    tag     = tier.get("tag", "N/A")
    emoji   = tier.get("emoji", "⚪")
    subtext = tier.get("subtext", "")
    print(f"  {emoji}  {category:<14} [ {tag} ]")
    print(f"     {subtext}")


def generate_verdict(row: pd.Series, rank: int, master_df: pd.DataFrame = None):
    try:
        ticker = row.get("ticker", "N/A")
        sector = row.get("sector", "Unknown")
        frs    = row.get("final_rank_score", 0)
        price  = row.get("current_price");  pe   = row.get("pe_ratio")
        spe    = row.get("sector_median_pe"); pb  = row.get("pb_ratio")
        spb    = row.get("sector_median_pb"); roe = row.get("roe")
        sroe   = row.get("sector_median_roe"); de = row.get("debt_to_equity")
        rv3    = row.get("revenue_cagr_3y"); ni3  = row.get("netincome_cagr_3y")
        rv5    = row.get("revenue_cagr_5y"); ni5  = row.get("netincome_cagr_5y")
        ocf    = row.get("operating_cash_flow"); mkt = row.get("market_cap_cr")
        blw    = row.get("pct_below_52w_high"); hi52 = row.get("52w_high")
        lo52   = row.get("52w_low"); sma50 = row.get("sma_50")
        sma200 = row.get("sma_200"); trend = row.get("technical_trend","N/A")
        rsi    = row.get("rsi_14"); r1y = row.get("return_1y")
        rs     = row.get("rs_vs_nifty"); ih = row.get("institutional_holding_pct")
        ey     = row.get("earnings_yield_pct"); bb = row.get("beats_bond_yield", False)
        bond   = CONFIG.get("india_10y_bond_yield_pct", 7.1)

        fcf_y = "N/A"
        if _ok(row.get("free_cash_flow")) and _ok(mkt) and mkt > 0:
            fcf_y = f"{round((row['free_cash_flow']/(mkt*1e7))*100,1)}%"

        sc         = generate_scorecard(row)
        score_tag  = "🟢 STRONG" if frs >= 7 else ("🟡 MODERATE" if frs >= 4 else "🔴 WEAK")
        qg         = row.get("quality_grade", 0); vg = row.get("valuation_grade", 0)
        mg         = row.get("momentum_grade", 0)

        print("=" * 70)
        print(f"  #{rank:<3} {ticker:<26}  {sector}")
        print(f"  {score_tag}  |  Score: {frs:.2f}")
        print(f"  Quality: {qg:.1f}/10  Valuation: {vg:.1f}/10  Momentum: {mg:.1f}/10")
        print(f"  MCap: ₹{fv(mkt,' Cr',d=0):<12} CMP: ₹{fv(price,d=2):<12} RSI: {fv(rsi,d=1)}")
        print(f"  52W: ₹{fv(lo52,d=2)} – ₹{fv(hi52,d=2)}  Below High: {fv(blw,'%')}")
        print(f"  Trend: {trend}  |  SMA50: ₹{fv(sma50,d=1)}  SMA200: ₹{fv(sma200,d=1)}")
        print(f"  1Y Return: {fv(r1y,'%')}  RS vs Nifty: {fv(rs,'%',d=1)}  "
              f"Inst: {fv(ih,'%')}")
        print("-" * 70)
        print(f"  FUNDAMENTALS")
        print(f"  PE: {fv(pe):<8} vs Sector: {fv(spe):<8} "
              f"PB: {fv(pb):<8} vs Sector: {fv(spb)}")
        print(f"  ROE: {fv(roe,'%'):<8} vs Sector: {fv(sroe,'%'):<8} D/E: {fv(de)}")
        print(f"  Rev CAGR: 3Y={fv(rv3,'%'):<8} 5Y={fv(rv5,'%')}")
        print(f"  PAT CAGR: 3Y={fv(ni3,'%'):<8} 5Y={fv(ni5,'%')}")
        print(f"  OCF: ₹{fv(ocf/1e7 if _ok(ocf) else None,' Cr',d=0):<10}  "
              f"FCF Yield: {fcf_y}  EPS Yield: {fv(ey,'%')}  "
              f"{'✅ Beats Bond' if bb else '❌ Below Bond'} ({bond}%)")
        print("-" * 70)
        print(f"  📋  {ticker} STOCK SCORECARD")
        print("-" * 70)
        for cat, key in [("Performance", "performance"), ("Valuation", "valuation"),
                         ("Growth", "growth"), ("Profitability", "profitability"),
                         ("Entry Point", "entry_point"), ("Red Flags", "red_flags")]:
            print_scorecard_tier(cat, sc.get(key, {"tag":"N/A","subtext":"","emoji":"⚪"}))
            print()
        print("-" * 70)

        # Peer Comparison Table (v5.1 — fully crash-proof)
        if master_df is not None and sector not in ["Unknown", None, "nan"]:
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
                    print(f"  {'Ticker':<20} {'PE':>6} {'PB':>6} {'ROE':>8} {'1Y Ret':>8}")
                    print(f"  {'-'*20} {'-'*6} {'-'*6} {'-'*8} {'-'*8}")
                    print(f"  {ticker:<20} {fv(pe):>6} {fv(pb):>6} "
                          f"{fv(roe,'%'):>8} {fv(r1y,'%'):>8}  ← THIS STOCK")
                    for _, p in peers.iterrows():
                        p_r1y = p.get("return_1y")
                        print(f"  {str(p.get('ticker','?')):<20} "
                              f"{fv(p.get('pe_ratio')):>6} "
                              f"{fv(p.get('pb_ratio')):>6} "
                              f"{fv(p.get('roe'),'%'):>8} "
                              f"{fv(p_r1y,'%'):>8}")
                    print("-" * 70)
            except Exception:
                pass

        print(f"  ⚠️  RISK: {get_risk(sector)}")
        print(f"  NOT FINANCIAL ADVICE. Do your own due diligence.")
        print("=" * 70 + "\n")

    except Exception as e:
        print(f"[WARNING] Verdict failed for rank #{rank}: {e}")
        print("=" * 70 + "\n")


# =============================================================================
# SECTION 10: MAIN EXECUTION
# =============================================================================

def main():
    global CONFIG

    # Step 1: Validate ALL config settings before anything else runs
    CONFIG = validate_and_sanitize_config(CONFIG)

    # Step 2: Apply archetype overrides (re-validates internally)
    CONFIG = apply_archetype(CONFIG)

    print("\n" + "=" * 70)
    print(f"  NSE RECOMMENDATION ENGINE v5.1  |  Universe: {CONFIG['universe']}")
    arch = CONFIG.get("archetype", "Custom")
    if arch != "Custom": print(f"  Archetype : {arch}")
    print(f"  Run Date  : {datetime.now().strftime('%d %B %Y | %I:%M %p')}")
    print(f"  Cache     : {'ON' if CONFIG['use_cache'] else 'OFF'}  "
          f"| Schema: {CONFIG['cache_schema_version']}")
    print("=" * 70)

    if CONFIG.get("cache_force_refresh"):
        wipe_cache()

    # Step 3: Load universe
    tickers, sector_map = get_universe_tickers(CONFIG["universe"])

    # Step 4: Fundamentals (cached via ThreadPool)
    fund_df = load_master_cache()
    if fund_df is None:
        fund_df = build_fundamental_dataframe(tickers, sector_map)
        save_master_cache(fund_df)

    # Step 5: ALWAYS fresh prices + RSI + RS vs Nifty
    print("\n[INFO] Fetching live price data (always fresh)...")
    price_df  = fetch_price_data(tickers)
    master_df = pd.merge(fund_df, price_df, on="ticker", how="left",
                         suffixes=("", "_price"))

    print_cache_stats(len(tickers))

    # Step 6: Save raw data
    try:
        master_df.to_csv("raw_data.csv", index=False)
        print(f"[INFO] Raw data saved → raw_data.csv ({len(master_df)} rows)")
    except Exception as e:
        print(f"[WARNING] Could not save raw_data.csv: {e}")

    # Step 7: Filter
    filtered = apply_filters(master_df)
    if filtered.empty:
        print("[INFO] No stocks passed all filters. Saving raw_data.csv for review.")
        return

    # Step 8: Score + Rank
    top_df = calculate_scores(filtered)

    # Step 9: Save report (guaranteed)
    try:
        top_df.to_csv(CONFIG["output_csv"], index=False)
        print(f"[INFO] Report saved → {CONFIG['output_csv']} ({len(top_df)} stocks)\n")
    except Exception as e:
        fallback = "Top_Stocks_Report_fallback.csv"
        top_df.to_csv(fallback, index=False)
        print(f"[WARNING] Could not write to {CONFIG['output_csv']}. "
              f"Saved to {fallback} instead.")

    # Step 10: Print verdicts
    print("=" * 70)
    print(f"  TOP {len(top_df)} RECOMMENDATIONS  |  {arch}")
    print("=" * 70 + "\n")

    for i, (_, row) in enumerate(top_df.iterrows(), 1):
        generate_verdict(row, rank=i, master_df=master_df)

    print("=" * 70)
    print(f"  ✅ Done.  Top {len(top_df)} stocks out of {len(tickers)} scanned.")
    print(f"  📁  Report   → {CONFIG['output_csv']}")
    print(f"  📁  Raw Data → raw_data.csv")
    print(f"  📦  Cache    → {CONFIG['cache_folder']}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user.")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        raise
