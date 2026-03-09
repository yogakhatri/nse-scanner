#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nse_screener_pro_v6.py
v6.0 - "TickerTape Pro" integration
Adds:
 - MMI history persistence + rolling correlation "mmi_corr"
 - Continuous MMI factor influencing filters & score weighting
 - EV/EBITDA, FCF yield, Altman Z (skeleton), Piotroski f-score (skeleton)
 - requests retry session + backoff
 - saves daily close matrix (parquet) for offline backtesting
 - minor robustness/perf improvements
"""

import os
import io
import re
import json
import time
import shutil
import tempfile
import requests
import urllib3
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import yfinance as yf

try:
    from bs4 import BeautifulSoup

    _BS4_AVAILABLE = True
except Exception:
    _BS4_AVAILABLE = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------------------------
# Simple retry session util
# -------------------------
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def requests_retry_session(
    retries: int = 4,
    backoff_factor: float = 0.5,
    status_forcelist=(429, 500, 502, 503, 504),
):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; Screener/6.0; +https://example.com)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return session


# =============================================================================
# SECTION 0: Minimal dependency check reminders
# =============================================================================
if not _BS4_AVAILABLE:
    print("[WARNING] beautifulsoup4 not installed. MMI scraping will be limited.")
    print("  pip install beautifulsoup4")

# =============================================================================
# SECTION 1: CONFIG (mostly unchanged, small new keys)
# =============================================================================
CONFIG = {
    "universe": "Top800_Custom",
    "archetype": "Custom",
    "use_mmi_auto_adjust": True,
    "use_cache": True,
    "cache_folder": "cache/",
    "cache_tickers_subfolder": "cache/tickers/",
    "cache_fundamental_days": 7,
    "cache_universe_days": 30,
    "cache_force_refresh": False,
    "cache_show_stats": True,
    "cache_schema_version": "6.0",
    # Liquidity
    "min_market_cap_cr": 200,
    "min_avg_daily_volume": 50000,
    # Red flags
    "reject_negative_ocf": True,
    "reject_current_ratio_below": 1.0,
    "reject_shrinking_margins": False,
    # Quality
    "max_debt_to_equity": 1.5,
    "min_roe_pct": 12,
    # Growth
    "min_revenue_cagr_3y_pct": 8,
    "min_netincome_cagr_3y_pct": 12,
    # Valuation
    "max_pe_vs_sector_mult": 1.5,
    "min_roe_vs_sector_mult": 0.85,
    "max_peg_ratio": 2.0,
    # Financials
    "max_pb_financials": 2.5,
    "financial_sectors": ["Bank", "NBFC", "Insurance", "Financial Services", "Finance"],
    # Fetching
    "fetch_max_workers": 10,
    "price_batch_size": 100,
    # Technical
    "sma_short": 50,
    "sma_long": 200,
    "rsi_period": 14,
    # Bond yield — used for 'beats bond yield' calculation
    "india_10y_bond_yield_pct": 6.7,
    "top_n_results": 25,
    "output_csv": "Top_Stocks_Report.csv",
    # New options
    "mmi_history_path": "cache/mmi_history.csv",
    "price_matrix_parquet": "cache/price_close_matrix.parquet",
    # MMI sensitivity threshold for filtering (abs corr)
    "mmi_sensitivity_threshold": 0.25,
    # How many days to compute rolling correlation on (for MMI sensitivity)
    "mmi_corr_window_days": 90,
}

# =============================================================================
# SECTION 2: NSE / Patterns (unchanged)
# =============================================================================
NSE_URLS = {
    "Nifty50": "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv",
    "Nifty100": "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv",
    "Nifty500": "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    "Midcap150": "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv",
    "Smallcap250": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv",
    "Microcap250": "https://www.niftyindices.com/IndexConstituent/ind_niftymicrocap250_list.csv",
}

OCF_PATTERNS = [
    "operating cash flow",
    "cash from operating",
    "net cash from operating",
    "total cash from operating",
    "cash flows from operating",
    "net cash provided by operating",
    "cash generated from operations",
    "operating activities",
]
CAPEX_PATTERNS = [
    "capital expenditure",
    "capital expenditures",
    "purchase of property",
    "purchase of plant",
    "purchase of ppe",
    "capex",
    "acquisition of fixed",
    "additions to property",
    "purchase of fixed",
    "payments for property",
    "acquisition of property",
]

# =============================================================================
# SECTION 3: MMI engine + history
# =============================================================================
_MMI_CACHE_FILE = os.path.join(CONFIG["cache_folder"], "mmi_cache.json")


def _classify_mmi(score: float) -> dict:
    if score < 30:
        zone = "Extreme Fear"
        emoji = "🟢"
    elif score < 50:
        zone = "Fear"
        emoji = "🟡"
    elif score < 70:
        zone = "Greed"
        emoji = "🟠"
    else:
        zone = "Extreme Greed"
        emoji = "🔴"
    return {"score": round(score, 1), "zone": zone, "emoji": emoji}


def _load_mmi_cache() -> dict | None:
    try:
        if not os.path.exists(_MMI_CACHE_FILE):
            return None
        with open(_MMI_CACHE_FILE) as f:
            c = json.load(f)
        if c.get("date") == datetime.now().strftime("%Y-%m-%d"):
            return c
    except Exception:
        pass
    return None


def _save_mmi_cache(mmi: dict):
    try:
        os.makedirs(CONFIG["cache_folder"], exist_ok=True)
        data = {**mmi, "date": datetime.now().strftime("%Y-%m-%d")}
        with open(_MMI_CACHE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


def append_mmi_history(mmi: dict):
    """
    Appends daily mmi to a CSV history for rolling-correlation/backtest.
    Prevents duplicate for same date.
    """
    try:
        os.makedirs(CONFIG["cache_folder"], exist_ok=True)
        path = CONFIG.get("mmi_history_path", "cache/mmi_history.csv")
        row = {"date": datetime.now().strftime("%Y-%m-%d"), "score": mmi.get("score"), "zone": mmi.get("zone")}
        df = pd.DataFrame([row])
        if os.path.exists(path):
            existing = pd.read_csv(path, parse_dates=["date"])
            if existing['date'].dt.strftime("%Y-%m-%d").isin([row['date']]).any():
                return
            df.to_csv(path, mode="a", header=False, index=False)
        else:
            df.to_csv(path, index=False)
    except Exception:
        pass


def fetch_market_mood_index() -> dict:
    """
    Scrapes Tickertape MMI with robust fallbacks + caching.
    """
    cached = _load_mmi_cache()
    if cached:
        res = _classify_mmi(cached["score"])
        print(f"[MMI] 📦 Cache hit — {res['emoji']} {res['score']} [{res['zone']}]")
        return res

    if not _BS4_AVAILABLE:
        print("[MMI] beautifulsoup4 not available — defaulting Neutral (50).")
        return {"score": 50.0, "zone": "Neutral (bs4 missing)", "emoji": "⚪"}

    print("[INFO] 🧭 Fetching live Market Mood Index (Tickertape)...")
    session = requests_retry_session()
    url = "https://www.tickertape.in/market-mood-index"
    try:
        resp = session.get(url, timeout=12, verify=False)
        resp.raise_for_status()
        html = resp.text

        # Method 1: __NEXT_DATA__ JSON
        try:
            soup = BeautifulSoup(html, "html.parser")
            script = soup.find("script", id="__NEXT_DATA__")
            if script and script.string:
                data = json.loads(script.string)
                props = data.get("props", {}).get("pageProps", {})
                # walk nested keys
                possible = json.dumps(props)
                pattern = re.search(r'("currentValue"|"value"|"score"|"mmi")\s*:\s*(\d{1,2}(?:\.\d{1,2})?)', possible)
                if pattern:
                    val = float(pattern.group(2))
                    res = _classify_mmi(val)
                    _save_mmi_cache(res)
                    append_mmi_history(res)
                    print(f"[MMI] {res['emoji']} {res['score']} [{res['zone']}] (via JSON)")
                    return res
        except Exception:
            pass

        # Method 2: regex on HTML
        try:
            pattern = re.search(
                r'"(?:currentValue|mmiValue|mmi_value|score)"\s*:\s*(\d{1,2}(?:\.\d{1,2})?)',
                html,
                re.IGNORECASE,
            )
            if pattern:
                val = float(pattern.group(1))
                res = _classify_mmi(val)
                _save_mmi_cache(res)
                append_mmi_history(res)
                print(f"[MMI] {res['emoji']} {res['score']} [{res['zone']}] (via regex)")
                return res
        except Exception:
            pass

        # Method 3: BeautifulSoup tag scan
        try:
            soup = BeautifulSoup(html, "html.parser")
            for tag, attrs in [
                ("span", {"class": "number"}),
                ("span", {"class": "mmi-value"}),
                ("div", {"class": "mmi-value"}),
                ("span", {"class": re.compile(r"mmi|mood|score", re.I)}),
                ("div", {"class": re.compile(r"mmi|mood|score", re.I)}),
            ]:
                el = soup.find(tag, attrs)
                if el:
                    text = el.get_text(strip=True)
                    nums = re.findall(r"\d{1,2}(?:\.\d{1,2})?", text)
                    if nums:
                        val = float(nums[0])
                        if 0 < val <= 100:
                            res = _classify_mmi(val)
                            _save_mmi_cache(res)
                            append_mmi_history(res)
                            print(f"[MMI] {res['emoji']} {res['score']} [{res['zone']}] (via tag)")
                            return res
        except Exception:
            pass

        print("[MMI] ⚠️ Could not extract MMI — defaulting to 50 (Neutral).")
    except requests.exceptions.RequestException as e:
        print(f"[MMI] ⚠️ Request failed: {e} — defaulting to 50 (Neutral).")
    except Exception as e:
        print(f"[MMI] ⚠️ Unexpected MMI error: {e} — defaulting to 50 (Neutral).")

    return {"score": 50.0, "zone": "Neutral (Default)", "emoji": "⚪"}


def mmi_factor(score: float) -> float:
    """
    Continuous factor -1..+1 derived from score 0..100 (negative = fear, positive = greed)
    """
    return max(-1.0, min(1.0, (score - 50.0) / 50.0))


def apply_mmi_dynamic_weights(config: dict, mmi: dict) -> dict:
    """
    Set _mmi_factor in CONFIG and adjust a few thresholds smoothly.
    """
    if not config.get("use_mmi_auto_adjust", True):
        return config
    c = config.copy()
    factor = mmi_factor(mmi["score"])
    c["_mmi_factor"] = factor
    # dynamically tighten/loosen PE multiplier by up to +/-20% depending on factor
    base_pe_mult = config.get("max_pe_vs_sector_mult", 1.5)
    c["max_pe_vs_sector_mult"] = max(0.6, base_pe_mult * (1 - 0.2 * factor))
    # adjust min_roe_pct slightly upward in greed
    base_min_roe = config.get("min_roe_pct", 12)
    c["min_roe_pct"] = max(5, base_min_roe + int(4 * factor))
    # adjust max_debt_to_equity tighter in greed
    base_de = config.get("max_debt_to_equity", 1.5)
    c["max_debt_to_equity"] = max(0.1, base_de * (1 - 0.4 * factor))
    print(f"[MMI] dynamic factor {factor:.3f} applied: pe_mult→{c['max_pe_vs_sector_mult']:.2f}, min_roe→{c['min_roe_pct']}, max_de→{c['max_debt_to_equity']:.2f}")
    return c


# =============================================================================
# SECTION 4: CONFIG VALIDATOR (slightly updated for new keys)
# =============================================================================
_CONFIG_SCHEMA = {
    # existing entries kept minimal - not fully repeated here; validate core ones used
    "universe": (str, None, None, None, "Top800_Custom"),
    "archetype": (str, None, None, None, "Custom"),
    "use_cache": (bool, None, None, None, True),
    "use_mmi_auto_adjust": (bool, None, None, None, True),
    "cache_folder": (str, None, None, None, "cache/"),
    "cache_tickers_subfolder": (str, None, None, None, "cache/tickers/"),
    "cache_fundamental_days": (int, 1, 30, None, 7),
    "cache_universe_days": (int, 1, 90, None, 30),
    "cache_force_refresh": (bool, None, None, None, False),
    "cache_schema_version": (str, None, None, None, "6.0"),
    "min_market_cap_cr": (float, 0, None, None, 200),
    "min_avg_daily_volume": (int, 0, None, None, 50000),
    "reject_negative_ocf": (bool, None, None, None, True),
    "reject_current_ratio_below": (float, 0, 5.0, None, 1.0),
    "reject_shrinking_margins": (bool, None, None, None, False),
    "max_debt_to_equity": (float, 0, 10.0, None, 1.5),
    "min_roe_pct": (float, -50, 100, None, 12),
    "min_revenue_cagr_3y_pct": (float, -50, 100, None, 8),
    "min_netincome_cagr_3y_pct": (float, -50, 100, None, 12),
    "max_pe_vs_sector_mult": (float, 0.1, 5.0, None, 1.5),
    "min_roe_vs_sector_mult": (float, 0.1, 3.0, None, 0.85),
    "max_peg_ratio": (float, 0.1, 10.0, None, 2.0),
    "max_pb_financials": (float, 0.1, 20.0, None, 2.5),
    "financial_sectors": (list, None, None, None, CONFIG.get("financial_sectors")),
    "fetch_max_workers": (int, 1, 30, None, 10),
    "price_batch_size": (int, 10, 500, None, 100),
    "sma_short": (int, 5, 100, None, 50),
    "sma_long": (int, 50, 500, None, 200),
    "rsi_period": (int, 2, 30, None, 14),
    "india_10y_bond_yield_pct": (float, 0, 25, None, 6.7),
    "top_n_results": (int, 1, 100, None, 25),
    "output_csv": (str, None, None, None, "Top_Stocks_Report.csv"),
    "mmi_history_path": (str, None, None, None, "cache/mmi_history.csv"),
    "price_matrix_parquet": (str, None, None, None, "cache/price_close_matrix.parquet"),
    "mmi_sensitivity_threshold": (float, 0.0, 1.0, None, 0.25),
    "mmi_corr_window_days": (int, 30, 365, None, 90),
}


def validate_and_sanitize_config(config: dict) -> dict:
    c = config.copy()
    issues = []
    for key, (dtype, vmin, vmax, allowed, default) in _CONFIG_SCHEMA.items():
        if key not in c:
            c[key] = default
            issues.append(f"MISSING '{key}' → default: {default}")
            continue
        val = c[key]
        if dtype == bool:
            if not isinstance(val, bool):
                if str(val).lower() in ("1", "true", "yes"):
                    c[key] = True
                elif str(val).lower() in ("0", "false", "no"):
                    c[key] = False
                else:
                    c[key] = default
                    issues.append(f"INVALID TYPE '{key}'={val!r} → {default}")
        elif dtype == list:
            if not isinstance(val, list):
                c[key] = default
                issues.append(f"INVALID TYPE '{key}' must be list → default")
        elif dtype in (int, float):
            try:
                c[key] = dtype(val)
            except Exception:
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
        c["sma_short"] = 50
        c["sma_long"] = 200
        issues.append("CONFLICT: sma_short >= sma_long → reset 50/200")
    if not c["output_csv"].endswith(".csv"):
        c["output_csv"] += ".csv"
        issues.append("output_csv missing .csv → auto-added")
    if not c["cache_tickers_subfolder"].startswith(c["cache_folder"]):
        c["cache_tickers_subfolder"] = c["cache_folder"] + "tickers/"
        issues.append("cache_tickers_subfolder outside cache_folder → fixed")
    if issues:
        print(f"\n{'='*64}\n  ⚠️  CONFIG — {len(issues)} issue(s) auto-fixed\n{'='*64}")
        for msg in issues:
            print(f"  • {msg}")
        print(f"{'='*64}\n")
    else:
        print("[CONFIG] ✅ All settings validated successfully.")
    return c


# =============================================================================
# SECTION 5: ARCHETYPES & SECTOR MAP (unchanged)
# =============================================================================
ARCHETYPES = {
    "Growth_Bargain": {
        "min_netincome_cagr_3y_pct": 18,
        "max_pe_vs_sector_mult": 0.9,
        "max_peg_ratio": 1.5,
        "min_roe_pct": 15,
        "max_debt_to_equity": 0.5,
    },
    "Cash_Machine": {
        "min_roe_pct": 20,
        "max_debt_to_equity": 0.1,
        "min_revenue_cagr_3y_pct": 10,
        "min_netincome_cagr_3y_pct": 10,
    },
    "Low_Debt_Midcap": {
        "universe": "Midcap150",
        "max_debt_to_equity": 0.1,
        "min_roe_pct": 15,
        "min_netincome_cagr_3y_pct": 12,
        "max_pe_vs_sector_mult": 1.3,
    },
    "Quality_Compounder": {
        "min_roe_pct": 20,
        "max_debt_to_equity": 0.3,
        "min_revenue_cagr_3y_pct": 15,
        "min_netincome_cagr_3y_pct": 15,
        "max_peg_ratio": 2.0,
    },
    "Near_52W_Low": {
        "min_pct_below_52w_high": 30,
        "min_roe_pct": 12,
        "max_debt_to_equity": 0.8,
        "min_netincome_cagr_3y_pct": 10,
    },
}

SECTOR_RISK_MAP = {
    "Bank": "RBI policy shifts, rising NPAs, or credit cycle deterioration.",
    "NBFC": "RBI liquidity tightening, borrowing cost spikes, or NPA risks.",
    "Insurance": "IRDAI regulatory changes, claims inflation, or yield compression.",
    "Financial Services": "RBI policy shifts, credit cycle downturns, or rising defaults.",
    "Finance": "Rising interest rates, RBI changes, or NPA deterioration.",
    "Information Technology": "Global IT spending cuts, USD/INR volatility, client concentration.",
    "Technology": "Global tech spending cuts, USD/INR volatility.",
    "Pharmaceuticals": "USFDA observations, patent expirations, pricing controls.",
    "Healthcare": "USFDA actions, drug pricing controls, or IP challenges.",
    "Automobile": "EV disruption, fuel price volatility, material cost spikes.",
    "Consumer Goods": "Input cost inflation, rural demand slowdown, GST changes.",
    "Infrastructure": "Government capex cuts, land delays, rising interest rates.",
    "Energy": "Crude oil swings, government pricing intervention, transition risk.",
    "Metals": "Commodity cycle, import duty changes, global demand collapse.",
    "Realty": "Interest rate hikes, regulatory changes, demand cooling.",
    "default": "Regulatory changes, global macro shocks, or black swan events.",
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
# SECTION 6: CACHE ENGINE (unchanged except minor additions)
# =============================================================================
_stats = {"fund_hits": 0, "fund_miss": 0, "master_hit": False, "master_secs": 0.0}


def _dirs():
    os.makedirs(CONFIG["cache_folder"], exist_ok=True)
    os.makedirs(CONFIG["cache_tickers_subfolder"], exist_ok=True)


def _tpath(ticker: str, dtype: str) -> str:
    safe = ticker.replace(".", "_").replace("/", "_").replace("\\", "_")
    return os.path.join(CONFIG["cache_tickers_subfolder"], f"{safe}_{dtype}.json")


def _json_safe(obj):
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return None if np.isnan(obj) else float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, float) and np.isnan(obj):
        return None
    return obj


def save_ticker_cache(ticker: str, data: dict, dtype: str):
    if not CONFIG.get("use_cache"):
        return
    _dirs()
    try:
        payload = _json_safe(data)
        payload["fetched_at"] = datetime.now().isoformat()
        payload["schema_version"] = CONFIG["cache_schema_version"]
        with open(_tpath(ticker, dtype), "w") as f:
            json.dump(payload, f, default=str)
    except Exception:
        pass


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
        if all(c.get(k) is None for k in ["pe_ratio", "roe", "market_cap_cr"]):
            return None
        return c
    except Exception:
        return None


def save_master_cache(df: pd.DataFrame):
    if not CONFIG.get("use_cache"):
        return
    _dirs()
    pkl = os.path.join(CONFIG["cache_folder"], "master_fundamentals.pkl")
    meta = os.path.join(CONFIG["cache_folder"], "cache_metadata.json")
    try:
        df.to_pickle(pkl)
        with open(meta, "w") as f:
            json.dump(
                {
                    "created_at": datetime.now().isoformat(),
                    "universe": CONFIG["universe"],
                    "archetype": CONFIG.get("archetype", "Custom"),
                    "schema_version": CONFIG["cache_schema_version"],
                    "total_tickers": len(df),
                },
                f,
                indent=2,
            )
        print(f"[CACHE] Saved → {pkl} ({len(df)} rows)")
    except Exception as e:
        print(f"[CACHE] Save failed: {e}")


def load_master_cache() -> pd.DataFrame | None:
    if not CONFIG.get("use_cache"):
        return None
    pkl = os.path.join(CONFIG["cache_folder"], "master_fundamentals.pkl")
    meta = os.path.join(CONFIG["cache_folder"], "cache_metadata.json")
    if not os.path.exists(pkl) or not os.path.exists(meta):
        return None
    try:
        with open(meta) as f:
            m = json.load(f)
        if m.get("schema_version") != CONFIG["cache_schema_version"]:
            return None
        if m.get("universe") != CONFIG["universe"]:
            return None
        if m.get("archetype") != CONFIG.get("archetype", "Custom"):
            return None
        age = (datetime.now() - datetime.fromisoformat(m["created_at"])).days
        if age >= CONFIG["cache_fundamental_days"]:
            print(f"[CACHE] Expired ({age}d). Re-fetching.")
            return None
        t0 = time.time()
        df = pd.read_pickle(pkl)
        el = round(time.time() - t0, 2)
        _stats["master_hit"] = True
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
    if not CONFIG.get("cache_show_stats"):
        return
    s = _stats
    mins = round((s["fund_hits"] * 4.5) / 60, 1)
    print(f"\n{'='*64}")
    print(f"  CACHE STATS")
    print(f"  Master  : {'✅ HIT ('+str(s['master_secs'])+'s)' if s['master_hit'] else '❌ MISS (first run — fast next time)'}")
    print(f"  Tickers : {s['fund_hits']} hits / {s['fund_hits']+s['fund_miss']} total")
    print(f"  Saved   : ~{mins} mins")
    print(f"{'='*64}\n")


# =============================================================================
# SECTION 7: UNIVERSE LOADER (unchanged)
# =============================================================================
def fetch_csv(url: str) -> pd.DataFrame:
    session = requests_retry_session()
    r = session.get(url, timeout=30, verify=False)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))


def _normalise_df(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    sym_col = next((c for c in df.columns if "symbol" in c.lower()), None)
    ind_col = next((c for c in df.columns if any(x in c.lower() for x in ["industry", "sector"])), None)
    if not sym_col:
        raise ValueError(f"No Symbol column. Got: {df.columns.tolist()}")
    out = pd.DataFrame()
    out["Symbol"] = df[sym_col].astype(str).str.strip().str.upper()
    out["Industry"] = df[ind_col].astype(str).str.strip() if ind_col else "Unknown"
    return out.dropna(subset=["Symbol"])


def get_universe_tickers(universe_name: str):
    print(f"\n[INFO] Loading universe: {universe_name}")
    _dirs()
    u_csv = os.path.join(CONFIG["cache_folder"], f"universe_{universe_name}.csv")
    u_meta = os.path.join(CONFIG["cache_folder"], f"universe_{universe_name}_meta.json")
    df = None
    if CONFIG.get("use_cache") and os.path.exists(u_csv) and os.path.exists(u_meta):
        try:
            with open(u_meta) as f:
                um = json.load(f)
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
                df = _normalise_df(raw)
                df.to_csv(u_csv, index=False)
                with open(u_meta, "w") as f:
                    json.dump({"fetched_at": datetime.now().isoformat()}, f)
                print(f"[INFO] Fetched {len(df)} stocks from niftyindices.com")
            except Exception as e:
                print(f"[WARNING] Network fetch failed: {e}")
        elif universe_name == "Top800_Custom":
            sub_indexes = {"Nifty500": NSE_URLS["Nifty500"], "Microcap250": NSE_URLS["Microcap250"]}
            frames = []
            for name, url in sub_indexes.items():
                try:
                    raw = fetch_csv(url)
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
        raise FileNotFoundError(f"Cannot load universe '{universe_name}'. Download CSV from niftyindices.com and save as '{universe_name}.csv'.")
    df = df[df["Symbol"].astype(str).str.len() > 0]
    tickers = [s + ".NS" for s in df["Symbol"].tolist()]
    sector_map = dict(zip([s + ".NS" for s in df["Symbol"]], df["Industry"]))
    seen = set()
    tickers = [t for t in tickers if not (t in seen or seen.add(t))]
    print(f"[INFO] Universe ready: {len(tickers)} unique tickers")
    return tickers, sector_map


# =============================================================================
# SECTION 8: RSI (unchanged)
# =============================================================================
def compute_rsi(close_series: pd.Series, period: int = 14) -> float | None:
    try:
        clean = close_series.dropna()
        if len(clean) < period + 1:
            return None
        delta = clean.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_g = gain.ewm(alpha=1 / period, min_periods=period).mean()
        avg_l = loss.ewm(alpha=1 / period, min_periods=period).mean()
        rs = avg_g / avg_l.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        val = rsi.dropna()
        return round(float(val.iloc[-1]), 2) if len(val) > 0 else None
    except Exception:
        return None


# =============================================================================
# SECTION 9: PRICE FETCH — batched, returns stats + close matrix
# =============================================================================
def _get_close_volume(raw, ticker: str, batch: list):
    try:
        if len(batch) == 1:
            df = raw
        else:
            if ticker not in raw.columns.get_level_values(0):
                return None, None
            df = raw[ticker]
        if df is None or df.empty:
            return None, None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(-1)
        close = df["Close"].dropna() if "Close" in df.columns else None
        volume = df["Volume"].dropna() if "Volume" in df.columns else pd.Series(dtype=float)
        return close, volume
    except Exception:
        return None, None


def _process_single_price(raw, ticker: str, batch: list, nifty_ret: float):
    rec = {"ticker": ticker, "current_price": None, "52w_high": None, "52w_low": None, "avg_daily_volume": None, "pct_below_52w_high": None, "sma_50": None, "sma_200": None, "rsi_14": None, "technical_trend": "N/A", "volume_surge": False, "return_1y": None, "rs_vs_nifty": None}
    try:
        cl, vol = _get_close_volume(raw, ticker, batch)
        if cl is None or len(cl) < 5:
            return rec
        ss = CONFIG["sma_short"]; sl = CONFIG["sma_long"]; rp = CONFIG["rsi_period"]
        cur = round(float(cl.iloc[-1]), 2)
        hi52 = round(float(cl.max()), 2)
        lo52 = round(float(cl.min()), 2)
        v30avg = float(vol.tail(30).mean()) if vol is not None and len(vol) >= 5 else 0.0
        vcur = float(vol.iloc[-1]) if vol is not None and len(vol) >= 1 else 0.0
        below = round((hi52 - cur) / hi52 * 100, 2) if hi52 > 0 else None
        sma50 = round(float(cl.tail(ss).mean()), 2) if len(cl) >= ss else None
        sma200 = round(float(cl.tail(sl).mean()), 2) if len(cl) >= sl else None
        rsi14 = compute_rsi(cl, rp)
        vsurge = bool(vcur > v30avg * 1.5) if v30avg > 0 else False
        ret1y = round((float(cl.iloc[-1]) / float(cl.iloc[0]) - 1) * 100, 2)
        rs_n = round(ret1y - nifty_ret, 2)
        if sma50 and sma200:
            if cur > sma50 > sma200:
                trend = "Uptrend ✅"
            elif cur < sma50 < sma200:
                trend = "Downtrend ❌"
            else:
                trend = "Sideways ➡️"
        else:
            trend = "N/A"
        rec.update({"current_price": cur, "52w_high": hi52, "52w_low": lo52, "avg_daily_volume": int(v30avg) if v30avg else 0, "pct_below_52w_high": below, "sma_50": sma50, "sma_200": sma200, "rsi_14": rsi14, "technical_trend": trend, "volume_surge": vsurge, "return_1y": ret1y, "rs_vs_nifty": rs_n})
    except Exception:
        pass
    return rec


def fetch_price_data_and_matrix(tickers: list) -> (pd.DataFrame, pd.DataFrame):
    """
    Returns tuple (price_stats_df, close_matrix_df)
    close_matrix_df: daily closes for each ticker (index=Date, columns=tickers)
    """
    batch_size = CONFIG.get("price_batch_size", 100)
    batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    print(f"\n[INFO] 🌐 Fetching live prices — {len(tickers)} tickers in {len(batches)} batch(es) of {batch_size}...")

    nifty_ret = 0.0
    try:
        nifty_raw = yf.download("^NSEI", period="1y", auto_adjust=True, progress=False)
        if not nifty_raw.empty and "Close" in nifty_raw.columns:
            nc = nifty_raw["Close"].squeeze().dropna()
            if len(nc) >= 2:
                nifty_ret = round((float(nc.iloc[-1]) / float(nc.iloc[0]) - 1) * 100, 2)
    except Exception as e:
        print(f"[WARNING] Nifty50 fetch failed: {e}. RS relative to 0%.")
    print(f"[INFO] Nifty 50 1Y return: {nifty_ret}%")

    all_records = []
    failed_batches = 0
    # collect close series per ticker to build matrix
    close_series = {}

    for i, batch in enumerate(batches, 1):
        print(f"  [Price] Batch {i}/{len(batches)} ({len(batch)} tickers)...", end="\r")
        try:
            raw = yf.download(batch, period="1y", group_by="ticker", auto_adjust=True, progress=False, threads=True)
            if raw is None or raw.empty:
                raise ValueError("Empty response from yfinance")
            for ticker in batch:
                rec = _process_single_price(raw, ticker, batch, nifty_ret)
                all_records.append(rec)
                # attempt to extract close matrix
                try:
                    cl, _ = _get_close_volume(raw, ticker, batch)
                    if cl is not None and len(cl) >= 2:
                        s = cl.copy()
                        s.index = pd.to_datetime(s.index)
                        close_series[ticker] = s
                except Exception:
                    pass
            time.sleep(0.2)  # tiny delay to be gentle on API
        except Exception as e:
            failed_batches += 1
            print(f"\n  [Price] Batch {i} failed: {e}")
            for ticker in batch:
                all_records.append({"ticker": ticker})
    print(f"\n[INFO] Price fetch complete. Failed batches: {failed_batches}/{len(batches)}")
    if failed_batches == len(batches):
        print("[WARNING] ⚠️ ALL price batches failed! Volume filter may be inaccurate.")
    price_stats_df = pd.DataFrame(all_records)
    # Build close matrix: align dates outer join
    if close_series:
        close_df = pd.concat(close_series, axis=1).sort_index()
        # normalize column names (tickers)
        close_df.columns = [c for c in close_df.columns]
        # Save daily close matrix for offline analysis/backtest
        try:
            os.makedirs(CONFIG["cache_folder"], exist_ok=True)
            close_df.to_parquet(CONFIG.get("price_matrix_parquet"))
            print(f"[INFO] Saved price close matrix → {CONFIG.get('price_matrix_parquet')}")
        except Exception:
            # fallback CSV if parquet not available
            try:
                close_df.to_csv(CONFIG.get("price_matrix_parquet") + ".csv")
            except Exception:
                pass
    else:
        close_df = pd.DataFrame()
    return price_stats_df, close_df


# =============================================================================
# SECTION 10: FUNDAMENTALS — threaded
# =============================================================================
def compute_cagr(series: list) -> float | None:
    try:
        clean = [float(v) for v in series if v is not None and not np.isnan(float(v))]
        if len(clean) < 2:
            return None
        if clean[0] <= 0 or clean[-1] <= 0:
            return None
        n = len(clean) - 1
        return round(((clean[-1] / clean[0]) ** (1 / n) - 1) * 100, 2)
    except Exception:
        return None


def _match_row(index_labels, patterns: list) -> str | None:
    for label in index_labels:
        label_l = str(label).lower().strip()
        for pat in patterns:
            if pat in label_l:
                return label
    return None


def compute_ev_ebitda(market_cap, total_debt, cash, ebitda):
    try:
        if market_cap is None or ebitda in (None, 0):
            return None
        # market_cap in INR (yfinance), total_debt and cash in same units — ensure consistent units
        ev = market_cap + (total_debt or 0) - (cash or 0)
        return round(ev / ebitda, 2) if ebitda != 0 else None
    except Exception:
        return None


def compute_fcf_yield(free_cash_flow, market_cap_cr):
    try:
        if free_cash_flow is None or market_cap_cr is None or market_cap_cr == 0:
            return None
        mc = market_cap_cr * 1e7
        return round((free_cash_flow / mc) * 100, 3)
    except Exception:
        return None


def altman_z_score_from_row(row):
    """
    Simple Altman Z skeleton, expects certain fields in row (best-effort).
    This is a heuristic; you should refine when better balance sheet data is available.
    """
    try:
        # required fields (may be None)
        working_cap = (row.get("current_assets") or 0) - (row.get("current_liabilities") or 0)
        total_assets = row.get("total_assets") or None
        retained = row.get("retained_earnings") or 0
        ebit = row.get("ebit") or 0
        market_cap = (row.get("market_cap_cr") or 0) * 1e7
        sales = row.get("sales") or 0
        if not total_assets:
            return None
        A = working_cap / total_assets
        B = retained / total_assets
        C = ebit / total_assets
        D = market_cap / total_assets if total_assets > 0 else 0
        E = sales / total_assets
        z = 1.2 * A + 1.4 * B + 3.3 * C + 0.6 * D + 1.0 * E
        return round(z, 2)
    except Exception:
        return None


def piotroski_f_score_skeleton(hist_financials: dict) -> int | None:
    """
    Skeleton for Piotroski F-score.
    Requires historical metrics. Returns 0-9 when enough data exists else None.
    hist_financials should contain dictionaries/lists for ROA, CFO, Leverage change etc.
    For now returns None if insufficient data; placeholder for future implementation.
    """
    try:
        # TODO: implement fully if multi-year metrics are available per ticker
        return None
    except Exception:
        return None


def fetch_fundamentals(ticker: str) -> dict:
    cached = load_ticker_cache(ticker, "fundamental")
    if cached:
        _stats["fund_hits"] += 1
        return cached
    _stats["fund_miss"] += 1
    result = {
        "ticker": ticker,
        "pe_ratio": None,
        "pb_ratio": None,
        "peg_ratio": None,
        "roe": None,
        "debt_to_equity": None,
        "market_cap_cr": None,
        "current_ratio": None,
        "gross_margin_current": None,
        "gross_margin_prev": None,
        "operating_cash_flow": None,
        "capital_expenditure": None,
        "free_cash_flow": None,
        "net_income": None,
        "institutional_holding_pct": None,
        "revenue_cagr_3y": None,
        "netincome_cagr_3y": None,
        "revenue_cagr_5y": None,
        "netincome_cagr_5y": None,
        "operating_margin": None,
        "sector_yf": None,
        "data_years_available": 0,
        "consistent_profit": None,
        "high_earnings_quality": None,
        # new fields
        "fcf_yield_pct": None,
        "ev_ebitda": None,
        "altman_z": None,
        "piotroski_f": None,
        # balance sheet components used for Altman Z if available
        "current_assets": None,
        "current_liabilities": None,
        "total_assets": None,
        "retained_earnings": None,
        "ebit": None,
        "sales": None,
        "total_debt": None,
        "cash": None,
    }

    def _try_fetch():
        for attempt in range(2):
            try:
                return yf.Ticker(ticker)
            except Exception:
                if attempt == 0:
                    time.sleep(2)
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
                try:
                    return round(float(v) * scale, 4)
                except Exception:
                    pass
            return None
        result.update({
            "pe_ratio": sg("trailingPE"),
            "pb_ratio": sg("priceToBook"),
            "peg_ratio": sg("pegRatio"),
            "roe": sg("returnOnEquity", 100),
            "debt_to_equity": sg("debtToEquity"),
            "market_cap_cr": sg("marketCap", 1 / 1e7),
            "current_ratio": sg("currentRatio"),
            "gross_margin_current": sg("grossMargins", 100),
            "operating_margin": sg("operatingMargins", 100),
            "sector_yf": info.get("sector"),
        })
        ih = info.get("heldPercentInstitutions")
        if ih is not None and not (isinstance(ih, float) and np.isnan(ih)):
            result["institutional_holding_pct"] = round(float(ih) * 100, 2)
        # Financials
        try:
            fin = stock.financials
            if fin is not None and not fin.empty:
                rev_row = _match_row(fin.index, ["total revenue", "revenue", "net sales"])
                ni_row = _match_row(fin.index, ["net income", "netincome", "profit"])
                gp_row = _match_row(fin.index, ["gross profit", "grossprofit"])
                n_cols = fin.shape[1]
                result["data_years_available"] = n_cols
                if rev_row:
                    rv = []
                    for i in range(n_cols - 1, -1, -1):
                        try:
                            v = float(fin.loc[rev_row].iloc[i])
                            if not np.isnan(v):
                                rv.append(v)
                        except Exception:
                            pass
                    if len(rv) >= 4:
                        result["revenue_cagr_3y"] = compute_cagr(rv[-4:])
                    if len(rv) >= 6:
                        result["revenue_cagr_5y"] = compute_cagr(rv[-6:])
                    result["sales"] = rv[0] if rv else None
                if ni_row:
                    ni = []
                    for i in range(n_cols - 1, -1, -1):
                        try:
                            v = float(fin.loc[ni_row].iloc[i])
                            if not np.isnan(v):
                                ni.append(v)
                        except Exception:
                            pass
                    if ni:
                        result["net_income"] = float(fin.loc[ni_row].iloc[0])
                        if len(ni) >= 4:
                            result["netincome_cagr_3y"] = compute_cagr(ni[-4:])
                        if len(ni) >= 6:
                            result["netincome_cagr_5y"] = compute_cagr(ni[-6:])
                        chk = ni[-4:] if len(ni) >= 4 else ni
                        result["consistent_profit"] = bool(all(v > 0 for v in chk))
                if gp_row and rev_row and n_cols >= 2:
                    try:
                        gp_p = float(fin.loc[gp_row].iloc[1])
                        rv_p = float(fin.loc[rev_row].iloc[1])
                        if rv_p > 0:
                            result["gross_margin_prev"] = round(gp_p / rv_p * 100, 4)
                    except Exception:
                        pass
        except Exception:
            pass
        # Cashflow
        try:
            cf = stock.cashflow
            if cf is not None and not cf.empty:
                ocf_row = _match_row(cf.index, OCF_PATTERNS)
                if ocf_row:
                    try:
                        v = float(cf.loc[ocf_row].iloc[0])
                        if not np.isnan(v):
                            result["operating_cash_flow"] = v
                    except Exception:
                        pass
                cx_row = _match_row(cf.index, CAPEX_PATTERNS)
                if cx_row:
                    try:
                        v = float(cf.loc[cx_row].iloc[0])
                        if not np.isnan(v):
                            result["capital_expenditure"] = v
                    except Exception:
                        pass
                ocf = result["operating_cash_flow"]
                cx = result["capital_expenditure"]
                if ocf is not None and cx is not None:
                    result["free_cash_flow"] = ocf + cx
                ni = result["net_income"]
                if ocf is not None and ni is not None and ni > 0:
                    result["high_earnings_quality"] = bool(ocf > ni)
        except Exception:
            pass
        # Balance sheet components & altman/ev-ebitda
        try:
            bs = stock.balance_sheet
            if bs is not None and not bs.empty:
                # try to read current assets/liabilities/total assets/retained earnings/total debt/cash
                ca_label = _match_row(bs.index, ["total current assets", "currentassets", "current assets"])
                cl_label = _match_row(bs.index, ["total current liabilities", "currentliabilities", "current liabilities"])
                ta_label = _match_row(bs.index, ["total assets", "totalassets", "assets"])
                re_label = _match_row(bs.index, ["retained earnings", "retainedearnings"])
                td_label = _match_row(bs.index, ["total debt", "totaldebt", "long term debt", "short term debt"])
                cash_label = _match_row(bs.index, ["cash", "cash and cash equivalents", "cashandcashequivalents"])
                # use first column if available
                if ta_label:
                    try:
                        v = float(bs.loc[ta_label].iloc[0])
                        result["total_assets"] = v
                    except Exception:
                        pass
                if ca_label:
                    try:
                        v = float(bs.loc[ca_label].iloc[0])
                        result["current_assets"] = v
                    except Exception:
                        pass
                if cl_label:
                    try:
                        v = float(bs.loc[cl_label].iloc[0])
                        result["current_liabilities"] = v
                    except Exception:
                        pass
                if re_label:
                    try:
                        v = float(bs.loc[re_label].iloc[0])
                        result["retained_earnings"] = v
                    except Exception:
                        pass
                if td_label:
                    try:
                        v = float(bs.loc[td_label].iloc[0])
                        result["total_debt"] = v
                    except Exception:
                        pass
                if cash_label:
                    try:
                        v = float(bs.loc[cash_label].iloc[0])
                        result["cash"] = v
                    except Exception:
                        pass
        except Exception:
            pass
        # basic income statement extract for ebit (approx)
        try:
            inc = stock.financials
            if inc is not None and not inc.empty:
                ebit_label = _match_row(inc.index, ["ebit", "operating income", "operating income or loss"])
                if ebit_label:
                    try:
                        result["ebit"] = float(inc.loc[ebit_label].iloc[0])
                    except Exception:
                        pass
        except Exception:
            pass
        # compute new metrics
        try:
            result["fcf_yield_pct"] = compute_fcf_yield(result.get("free_cash_flow"), result.get("market_cap_cr"))
            result["ev_ebitda"] = compute_ev_ebitda(
                (info.get("marketCap") or 0),
                result.get("total_debt"),
                result.get("cash"),
                result.get("ebit"),
            )
            result["altman_z"] = altman_z_score_from_row(result)
            result["piotroski_f"] = piotroski_f_score_skeleton(None)
        except Exception:
            pass
    except Exception:
        pass
    save_ticker_cache(ticker, result, "fundamental")
    return result


def build_fundamental_dataframe(tickers: list, sector_map: dict) -> pd.DataFrame:
    print(f"\n[INFO] Fetching fundamentals ({len(tickers)} stocks, {CONFIG['fetch_max_workers']} threads)...")
    records = []
    completed = 0
    total = len(tickers)
    with ThreadPoolExecutor(max_workers=CONFIG["fetch_max_workers"]) as ex:
        futures = {ex.submit(fetch_fundamentals, t): t for t in tickers}
        for future in as_completed(futures):
            completed += 1
            print(f"  [{completed:>4}/{total}] {futures[future]:<24}", end="\r")
            try:
                records.append(future.result())
            except:
                records.append({"ticker": futures[future]})
    print(f"\n[INFO] Fundamentals complete.")
    fund_df = pd.DataFrame(records)
    # ensure columns exist (extend original list with new metrics)
    for col in ["pe_ratio", "pb_ratio", "peg_ratio", "roe", "debt_to_equity", "market_cap_cr", "current_ratio", "gross_margin_current", "gross_margin_prev", "operating_cash_flow", "capital_expenditure", "free_cash_flow", "net_income", "institutional_holding_pct", "revenue_cagr_3y", "netincome_cagr_3y", "revenue_cagr_5y", "netincome_cagr_5y", "operating_margin", "consistent_profit", "high_earnings_quality", "data_years_available", "sector_yf", "fcf_yield_pct", "ev_ebitda", "altman_z", "piotroski_f", "current_assets", "current_liabilities", "total_assets", "retained_earnings", "ebit", "sales", "total_debt", "cash"]:
        if col not in fund_df.columns:
            fund_df[col] = None
    # enforce sector mapping
    fund_df["sector"] = fund_df["ticker"].map(sector_map).fillna("Unknown")
    mask = fund_df["sector"].isin(["Unknown", "nan", "None", ""])
    fund_df.loc[mask, "sector"] = fund_df.loc[mask, "sector_yf"].fillna("Unknown")
    fund_df.drop(columns=["sector_yf"], inplace=True, errors="ignore")
    return fund_df


# =============================================================================
# SECTION 11: SECTOR MEDIANS (unchanged)
# =============================================================================
def classify_sector(s) -> str:
    if not s or pd.isna(s) or str(s).strip() in ["Unknown", "nan", "None", ""]:
        return "non_financial"
    sl = str(s).lower()
    return ("financial" if any(f.lower() in sl for f in CONFIG.get("financial_sectors", [])) else "non_financial")


def add_sector_medians(df: pd.DataFrame) -> pd.DataFrame:
    def safe_median(x):
        valid = x.dropna()
        return valid.median() if len(valid) >= 5 else np.nan
    overall_pe = (df["pe_ratio"].dropna().median() if df["pe_ratio"].notna().any() else 20.0)
    overall_pb = (df["pb_ratio"].dropna().median() if df["pb_ratio"].notna().any() else 2.5)
    overall_roe = df["roe"].dropna().median() if df["roe"].notna().any() else 15.0
    df["sector_median_pe"] = (df.groupby("sector")["pe_ratio"].transform(safe_median).fillna(overall_pe))
    df["sector_median_pb"] = (df.groupby("sector")["pb_ratio"].transform(safe_median).fillna(overall_pb))
    df["sector_median_roe"] = (df.groupby("sector")["roe"].transform(safe_median).fillna(overall_roe))
    return df


# =============================================================================
# SECTION 12: FILTER ENGINE (keeps same logic, runs after MMI sensitivity filter)
# =============================================================================
def _col(df: pd.DataFrame, name: str) -> pd.Series:
    return df[name] if name in df.columns else pd.Series(np.nan, index=df.index)


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    print(f"\n{'='*64}\n  FILTER ENGINE  |  {len(df)} stocks entering\n{'='*64}")
    w = df.copy()
    w["sector_type"] = w["sector"].apply(classify_sector)
    w = add_sector_medians(w)
    # Gate 1: Liquidity
    b = len(w)
    mc = _col(w, "market_cap_cr")
    adv = _col(w, "avg_daily_volume")
    vol_ok = adv.isna() | (adv >= CONFIG["min_avg_daily_volume"])
    w = w[mc.notna() & (mc >= CONFIG["min_market_cap_cr"]) & vol_ok]
    print(f"  Gate 1 [Liquidity]             : {b:>4} → {len(w):>4}  (-{b-len(w)})")
    # Gate 2: Red Flags
    b = len(w)
    rf = pd.Series(False, index=w.index)
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
    b = len(w)
    nf = w["sector_type"] == "non_financial"
    de = _col(w, "debt_to_equity")
    roe = _col(w, "roe")
    bad = nf & ((de.notna() & (de > CONFIG["max_debt_to_equity"])) | (roe.notna() & (roe < CONFIG["min_roe_pct"])))
    w = w[~bad]
    print(f"  Gate 3 [Quality — Non-Fin]     : {b:>4} → {len(w):>4}  (-{b-len(w)})")
    # Gate 4: Growth
    b = len(w)
    rv3 = _col(w, "revenue_cagr_3y")
    ni3 = _col(w, "netincome_cagr_3y")
    bg = (rv3.notna() & (rv3 < CONFIG["min_revenue_cagr_3y_pct"])) | (ni3.notna() & (ni3 < CONFIG["min_netincome_cagr_3y_pct"]))
    w = w[~bg]
    print(f"  Gate 4 [Growth — 3Y CAGR]      : {b:>4} → {len(w):>4}  (-{b-len(w)})")
    # Gate 5: Sector-relative valuation
    b = len(w)
    nf = w["sector_type"] == "non_financial"
    pe = _col(w, "pe_ratio"); spe = _col(w, "sector_median_pe"); roe = _col(w, "roe"); sroe = _col(w, "sector_median_roe"); peg = _col(w, "peg_ratio")
    bv = nf & pe.notna() & spe.notna() & (pe > spe * CONFIG["max_pe_vs_sector_mult"])
    br = nf & roe.notna() & sroe.notna() & (roe < sroe * CONFIG["min_roe_vs_sector_mult"])
    bp = nf & peg.notna() & (peg > 0) & (peg > CONFIG["max_peg_ratio"])
    w = w[~(bv | br | bp)]
    print(f"  Gate 5 [Sector-Rel Valuation]  : {b:>4} → {len(w):>4}  (-{b-len(w)})")
    # Gate 6: Financials P/B
    b = len(w)
    fin = w["sector_type"] == "financial"
    pb = _col(w, "pb_ratio")
    bf = fin & pb.notna() & (pb > CONFIG["max_pb_financials"])
    w = w[~bf]
    print(f"  Gate 6 [Financials — P/B]      : {b:>4} → {len(w):>4}  (-{b-len(w)})")
    # Gate 7: Near 52W Low (archetype)
    if CONFIG.get("archetype") == "Near_52W_Low" and CONFIG.get("min_pct_below_52w_high"):
        b = len(w)
        blw = _col(w, "pct_below_52w_high")
        w = w[blw.notna() & (blw >= CONFIG["min_pct_below_52w_high"])]
        print(f"  Gate 7 [Near 52W Low]          : {b:>4} → {len(w):>4}  (-{b-len(w)})")
    print(f"{'='*64}\n  ✅ Survivors: {len(w)}\n{'='*64}\n")
    if len(w) == 0:
        print("[WARNING] ⚠️ Zero stocks passed all filters!")
        print("[TIP] Try relaxing parameters.")
    return w


# =============================================================================
# SECTION 13: MMI SENSITIVITY — compute rolling correlation & apply
# =============================================================================
def compute_mmi_correlations_from_close_matrix(close_df: pd.DataFrame, mmi_history_df: pd.DataFrame, window_days: int = 90) -> pd.Series:
    """
    close_df: DataFrame indexed by date, columns are tickers like 'RELIANCE.NS'
    mmi_history_df: DataFrame with 'date' and 'score'
    returns Series indexed by ticker with last rolling correlation value between daily returns and delta(MMI)
    """
    if close_df.empty or mmi_history_df is None or mmi_history_df.empty:
        return pd.Series(dtype=float)
    # align
    close_df = close_df.sort_index()
    mmi_history_df['date'] = pd.to_datetime(mmi_history_df['date'])
    mmi_history_df.set_index('date', inplace=True)
    # forward-fill daily to match market days
    mmi_daily = mmi_history_df['score'].resample('D').ffill().reindex(close_df.index).fillna(method='ffill')
    mmi_delta = mmi_daily.diff().fillna(0)
    pct_ret = close_df.pct_change().fillna(0)
    corrs = {}
    for col in pct_ret.columns:
        try:
            s = pct_ret[col].rolling(window_days).corr(mmi_delta)
            last = s.dropna()
            corrs[col] = float(last.iloc[-1]) if len(last) else 0.0
        except Exception:
            corrs[col] = 0.0
    return pd.Series(corrs).sort_values(ascending=False)


def apply_mmi_sensitivity_filter(master_df: pd.DataFrame, close_df: pd.DataFrame, mmi: dict) -> pd.DataFrame:
    """
    Compute mmi_corr (if price matrix exists) and optionally filter stocks
    depending on current MMI zone and sensitivity threshold.
    - If MMI is Greed (score>60), prefer positive correlations (stocks that rise with MMI)
    - If MMI is Fear (score<40), prefer negative correlations (defensive names)
    """
    try:
        if close_df.empty or not os.path.exists(CONFIG.get("mmi_history_path","cache/mmi_history.csv")):
            master_df['mmi_corr'] = None
            master_df['mmi_sensitive'] = False
            return master_df
        mmi_hist = pd.read_csv(CONFIG.get("mmi_history_path","cache/mmi_history.csv"))
        corrs = compute_mmi_correlations_from_close_matrix(close_df, mmi_hist, CONFIG.get("mmi_corr_window_days", 90))
        # map to master_df tickers
        master_df['mmi_corr'] = master_df['ticker'].map(corrs).fillna(0.0)
        threshold = CONFIG.get("mmi_sensitivity_threshold", 0.25)
        master_df['mmi_sensitive'] = master_df['mmi_corr'].abs() >= threshold
        # zone decision
        score = mmi.get("score", 50)
        if score > 60:
            # prefer positive correlated names; mark others
            master_df['mmi_pref'] = master_df['mmi_corr'] > 0
        elif score < 40:
            master_df['mmi_pref'] = master_df['mmi_corr'] < 0
        else:
            master_df['mmi_pref'] = True  # neutral, no preference
        # optional filter: keep only preferred if many survivors (we'll not drop yet)
        return master_df
    except Exception as e:
        print(f"[MMI SENSITIVITY] failed: {e}")
        master_df['mmi_corr'] = None
        master_df['mmi_sensitive'] = False
        master_df['mmi_pref'] = True
        return master_df


# =============================================================================
# SECTION 14: SCORECARD & COMPOSITE SCORING (integrate _mmi_factor)
# =============================================================================
def _ok(v) -> bool:
    return v is not None and not (isinstance(v, float) and np.isnan(v))


# Reuse your previous scorecard functions (omitted here to save space)...
# For brevity, keep the same generate_scorecard(), scorecard_* functions and printing functions.
# I'll reuse them from your original script with minimal edits: (we copy them verbatim)
# --- scorecard_performance / valuation / growth / profitability / entry_point / red_flags
# --- generate_scorecard
# For space, I'll define them minimally here (they behave like before):

def scorecard_performance(row: pd.Series) -> dict:
    rs = row.get("rs_vs_nifty"); r1 = row.get("return_1y")
    if not _ok(rs) or not _ok(r1):
        return {"tag": "N/A", "subtext": "Insufficient price history.", "emoji": "⚪"}
    if rs > 15:
        return {"tag": "High", "subtext": f"Return {round(r1,1)}% outperforms Nifty by {round(rs,1)}%.", "emoji": "🟢"}
    elif rs > -10 and r1 > 0:
        return {"tag": "Avg", "subtext": "Price return is average, in line with market.", "emoji": "🟡"}
    return {"tag": "Low", "subtext": f"Underperforming market by {abs(round(rs,1))}% over 1 year.", "emoji": "🔴"}


def scorecard_valuation(row: pd.Series) -> dict:
    pe = row.get("pe_ratio"); spe = row.get("sector_median_pe")
    if not _ok(pe) or not _ok(spe) or spe == 0:
        return {"tag": "N/A", "subtext": "Valuation data unavailable.", "emoji": "⚪"}
    if pe < spe * 0.8:
        return {"tag": "Attractive", "subtext": f"PE {round(pe,1)}x well below sector median {round(spe,1)}x.", "emoji": "🟢"}
    elif pe <= spe * 1.2:
        return {"tag": "Avg", "subtext": "Moderately valued vs sector peers.", "emoji": "🟡"}
    return {"tag": "Expensive", "subtext": f"Trading {round((pe/spe-1)*100,1)}% above sector PE median.", "emoji": "🔴"}


def scorecard_growth(row: pd.Series) -> dict:
    rv3 = row.get("revenue_cagr_3y"); ni3 = row.get("netincome_cagr_3y")
    if not _ok(rv3) and not _ok(ni3):
        return {"tag": "N/A", "subtext": "Insufficient financial history.", "emoji": "⚪"}
    rv = rv3 if _ok(rv3) else 0; ni = ni3 if _ok(ni3) else 0
    avg = (rv + ni) / 2
    if avg > 15:
        return {"tag": "High", "subtext": f"Rev CAGR {round(rv,1)}% + Profit CAGR {round(ni,1)}% — outstanding.", "emoji": "🟢"}
    elif avg > 5:
        return {"tag": "Avg", "subtext": "Financial growth has been moderate.", "emoji": "🟡"}
    return {"tag": "Low", "subtext": "Stagnant or declining growth trends.", "emoji": "🔴"}


def scorecard_profitability(row: pd.Series) -> dict:
    roe = row.get("roe"); gmc = row.get("gross_margin_current"); gmp = row.get("gross_margin_prev")
    if not _ok(roe):
        return {"tag": "N/A", "subtext": "Profitability data unavailable.", "emoji": "⚪"}
    margin_ok = True
    if _ok(gmc) and _ok(gmp) and gmp > 0:
        margin_ok = gmc >= gmp * 0.95
    if roe > 20 and margin_ok:
        return {"tag": "High", "subtext": f"Strong profitability — ROE {round(roe,1)}% with stable margins.", "emoji": "🟢"}
    elif roe > 15:
        return {"tag": "High", "subtext": "Good profitability and capital efficiency.", "emoji": "🟢"}
    elif roe > 8:
        return {"tag": "Avg", "subtext": "Stable but room for improvement.", "emoji": "🟡"}
    return {"tag": "Low", "subtext": "Struggling to generate efficient returns.", "emoji": "🔴"}


def scorecard_entry_point(row: pd.Series) -> dict:
    rsi = row.get("rsi_14"); price = row.get("current_price"); s200 = row.get("sma_200")
    if not _ok(rsi):
        return {"tag": "N/A", "subtext": "Technical data unavailable.", "emoji": "⚪"}
    rf = round(rsi, 1)
    if rsi < 40:
        return {"tag": "Good", "subtext": f"RSI {rf} — not overbought, decent entry.", "emoji": "🟢"}
    elif 40 <= rsi <= 55 and _ok(price) and _ok(s200) and price > s200:
        return {"tag": "Good", "subtext": f"RSI {rf} with price above 200 SMA — healthy momentum.", "emoji": "🟢"}
    elif 55 < rsi <= 70:
        return {"tag": "Average", "subtext": f"RSI {rf} — neutral zone, proceed carefully.", "emoji": "🟡"}
    return {"tag": "Risky", "subtext": f"RSI {rf} — overbought, pullback risk.", "emoji": "🔴"}


def scorecard_red_flags(row: pd.Series) -> dict:
    de = row.get("debt_to_equity"); cr = row.get("current_ratio"); ocf = row.get("operating_cash_flow"); cp = row.get("consistent_profit"); heq = row.get("high_earnings_quality"); gmc = row.get("gross_margin_current"); gmp = row.get("gross_margin_prev")
    st = row.get("sector_type", "non_financial")
    flags = []
    if st == "non_financial" and _ok(de) and de > 1.5:
        flags.append(f"High Debt (D/E: {round(de,2)})")
    if _ok(cr) and cr < 1.0:
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
        return {"tag": "Low", "emoji": "🟢", "subtext": "No red flag found.", "flags": []}
    elif len(flags) == 1:
        return {"tag": "Moderate", "emoji": "🟡", "subtext": f"1 flag: {flags[0]}.", "flags": flags}
    return {"tag": "High", "emoji": "🔴", "subtext": f"{len(flags)} flags: {'; '.join(flags[:3])}.", "flags": flags}


def generate_scorecard(row: pd.Series) -> dict:
    return {"performance": scorecard_performance(row), "valuation": scorecard_valuation(row), "growth": scorecard_growth(row), "profitability": scorecard_profitability(row), "entry_point": scorecard_entry_point(row), "red_flags": scorecard_red_flags(row)}


# Composite scoring with MMI factor
def mn(series: pd.Series) -> pd.Series:
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series([0.5] * len(series), index=series.index)
    return (series - lo) / (hi - lo)


def calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        print("[WARNING] Nothing to score.")
        return df
    orig = df.copy()
    s = df.copy()
    # safe fill medians
    def safe_fill(col):
        if col in s.columns:
            med = s[col].median()
            s[col] = s[col].fillna(med if pd.notna(med) else 0)
    for col in ["roe", "debt_to_equity", "pe_ratio", "peg_ratio", "operating_cash_flow", "revenue_cagr_3y", "netincome_cagr_3y", "rs_vs_nifty"]:
        safe_fill(col)
    def safe_mn(col):
        return mn(s[col]) if col in s.columns else pd.Series(0.5, index=s.index)
    roe_n = safe_mn("roe")
    de_n = 1 - safe_mn("debt_to_equity")
    pe_n = 1 - safe_mn("pe_ratio")
    peg_n = 1 - safe_mn("peg_ratio")
    rs_n = safe_mn("rs_vs_nifty")
    rv_n = safe_mn("revenue_cagr_3y")
    orig["quality_grade"] = ((roe_n + de_n) / 2 * 10).round(2)
    orig["valuation_grade"] = ((pe_n + peg_n) / 2 * 10).round(2)
    orig["momentum_grade"] = ((rs_n * 0.6 + rv_n * 0.4) * 10).round(2)
    # mmi adjustments to weights (continuous)
    mmi_f = CONFIG.get("_mmi_factor", 0.0)
    w_quality = 0.35 + 0.10 * (-mmi_f)
    w_valuation = 0.35 + 0.10 * (mmi_f)
    w_momentum = 0.30
    ssum = w_quality + w_valuation + w_momentum
    w_quality /= ssum; w_valuation /= ssum; w_momentum /= ssum
    orig["final_rank_score"] = (orig["quality_grade"] * w_quality + orig["valuation_grade"] * w_valuation + orig["momentum_grade"] * w_momentum).round(2)
    bond = CONFIG.get("india_10y_bond_yield_pct", 6.7)
    if "pe_ratio" in orig.columns:
        orig["earnings_yield_pct"] = orig["pe_ratio"].apply(lambda pe: round(100 / pe, 2) if _ok(pe) and pe > 0 else None)
        orig["beats_bond_yield"] = orig["earnings_yield_pct"].apply(lambda ey: bool(_ok(ey) and ey > bond))
    else:
        orig["earnings_yield_pct"] = None
        orig["beats_bond_yield"] = False
    orig.sort_values("final_rank_score", ascending=False, inplace=True)
    n = min(CONFIG["top_n_results"], len(orig))
    top = orig.head(n).reset_index(drop=True)
    print(f"[INFO] Scoring complete. Top {len(top)} stocks selected.")
    return top


# =============================================================================
# SECTION 15: CSV builder & print (kept similar, adds new metrics)
# =============================================================================
def get_risk(sector) -> str:
    if not sector or pd.isna(sector):
        return SECTOR_RISK_MAP["default"]
    for k in SECTOR_RISK_MAP:
        if k.lower() in str(sector).lower():
            return SECTOR_RISK_MAP[k]
    return SECTOR_RISK_MAP["default"]


def fv(val, sfx="", d=1):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    try:
        return f"{round(float(val), d)}{sfx}"
    except Exception:
        return str(val)


def print_scorecard_tier(category: str, tier: dict):
    print(f"  {tier.get('emoji','⚪')}  {category:<14} [ {tier.get('tag','N/A')} ]")
    print(f"     {tier.get('subtext','')}")


def generate_verdict(row: pd.Series, rank: int, master_df: pd.DataFrame = None):
    try:
        ticker = row.get("ticker", "N/A")
        sector = row.get("sector", "Unknown")
        frs = row.get("final_rank_score", 0)
        price = row.get("current_price")
        pe = row.get("pe_ratio"); spe = row.get("sector_median_pe"); pb = row.get("pb_ratio"); spb = row.get("sector_median_pb")
        roe = row.get("roe"); sroe = row.get("sector_median_roe"); de = row.get("debt_to_equity")
        rv3 = row.get("revenue_cagr_3y"); ni3 = row.get("netincome_cagr_3y"); ocf = row.get("operating_cash_flow"); mkt = row.get("market_cap_cr")
        blw = row.get("pct_below_52w_high"); hi52 = row.get("52w_high"); lo52 = row.get("52w_low"); sma50 = row.get("sma_50"); sma200 = row.get("sma_200")
        trend = row.get("technical_trend", "N/A"); rsi = row.get("rsi_14"); r1y = row.get("return_1y"); rs = row.get("rs_vs_nifty"); ih = row.get("institutional_holding_pct")
        ey = row.get("earnings_yield_pct"); bb = row.get("beats_bond_yield", False); bond = CONFIG.get("india_10y_bond_yield_pct", 6.7)
        qg = row.get("quality_grade", 0); vg = row.get("valuation_grade", 0); mg = row.get("momentum_grade", 0)
        fcf_y = "N/A"
        if _ok(row.get("free_cash_flow")) and _ok(mkt) and mkt > 0:
            fcf_y = f"{round((row['free_cash_flow']/(mkt*1e7))*100,1)}%"
        sc = generate_scorecard(row)
        score_tag = ("🟢 STRONG" if frs >= 7 else ("🟡 MODERATE" if frs >= 4 else "🔴 WEAK"))
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
        print(f"  PE: {fv(pe):<8} vs Sector: {fv(spe):<8} PB: {fv(pb):<8} vs Sector: {fv(spb)}")
        print(f"  ROE: {fv(roe,'%'):<8} vs Sector: {fv(sroe,'%'):<8} D/E: {fv(de)}")
        print(f"  Rev CAGR: 3Y={fv(rv3,'%'):<8} 5Y={fv(row.get('revenue_cagr_5y'),'%')}")
        print(f"  PAT CAGR: 3Y={fv(ni3,'%'):<8} 5Y={fv(row.get('netincome_cagr_5y'),'%')}")
        ocf_cr = fv(ocf / 1e7 if _ok(ocf) else None, " Cr", d=0)
        print(f"  OCF: ₹{ocf_cr:<10}  FCF Yield: {fv(row.get('fcf_yield_pct'))}  EV/EBITDA: {fv(row.get('ev_ebitda'))}")
        print("-" * 70)
        print(f"  📋  {ticker} STOCK SCORECARD")
        print("-" * 70)
        for cat, key in [("Performance", "performance"), ("Valuation", "valuation"), ("Growth", "growth"), ("Profitability", "profitability"), ("Entry Point", "entry_point"), ("Red Flags", "red_flags")]:
            print_scorecard_tier(cat, sc.get(key, {"tag":"N/A","subtext":"","emoji":"⚪"}))
            print()
        print("-" * 70)
        if master_df is not None and str(sector) not in ["Unknown","nan","None",""]:
            try:
                peers = master_df[(master_df["sector"].astype(str) == str(sector)) & (master_df["ticker"] != ticker)].copy()
                peers = peers[peers["pe_ratio"].notna() | peers["roe"].notna()]
                if not peers.empty and _ok(mkt):
                    peers["_diff"] = (peers["market_cap_cr"].fillna(0) - mkt).abs()
                    peers = peers.nsmallest(min(3, len(peers)), "_diff")
                    print(f"  📊  SECTOR PEERS  ({sector})")
                    print(f"  {'Ticker':<22} {'PE':>6} {'PB':>6} {'ROE':>8} {'1Y Ret':>8}")
                    print(f"  {'-'*22} {'-'*6} {'-'*6} {'-'*8} {'-'*8}")
                    print(f"  {str(ticker):<22} {fv(pe):>6} {fv(pb):>6} {fv(roe,'%'):>8} {fv(r1y,'%'):>8}  ← THIS STOCK")
                    for _, p in peers.iterrows():
                        print(f"  {str(p.get('ticker','?')):<22} {fv(p.get('pe_ratio')):>6} {fv(p.get('pb_ratio')):>6} {fv(p.get('roe'),'%'):>8} {fv(p.get('return_1y'),'%'):>8}")
                    print("-" * 70)
            except Exception:
                pass
        print(f"  ⚠️   RISK: {get_risk(sector)}")
        print(f"  NOT FINANCIAL ADVICE. Do your own due diligence.")
        print("=" * 70 + "\n")
    except Exception as e:
        print(f"[WARNING] Verdict failed for rank #{rank}: {e}")
        print("=" * 70 + "\n")


def build_csv_df(top_df: pd.DataFrame, mmi: dict) -> pd.DataFrame:
    rows = []
    for rank, (_, row) in enumerate(top_df.iterrows(), 1):
        sc = generate_scorecard(row)
        frs = row.get("final_rank_score", 0)
        verdict = "STRONG" if frs >= 7 else ("MODERATE" if frs >= 4 else "WEAK")
        mkt = row.get("market_cap_cr")
        fcf = row.get("free_cash_flow")
        fcf_yield = None
        if _ok(fcf) and _ok(mkt) and mkt > 0:
            fcf_yield = round((fcf / (mkt * 1e7)) * 100, 2)
        rows.append({
            "rank": rank,
            "ticker": row.get("ticker"),
            "sector": row.get("sector"),
            "overall_verdict": verdict,
            "final_rank_score": row.get("final_rank_score"),
            "quality_grade": row.get("quality_grade"),
            "valuation_grade": row.get("valuation_grade"),
            "momentum_grade": row.get("momentum_grade"),
            "sc_performance": sc["performance"]["tag"],
            "sc_valuation": sc["valuation"]["tag"],
            "sc_growth": sc["growth"]["tag"],
            "sc_profitability": sc["profitability"]["tag"],
            "sc_entry_point": sc["entry_point"]["tag"],
            "sc_red_flags": sc["red_flags"]["tag"],
            "current_price": row.get("current_price"),
            "52w_high": row.get("52w_high"),
            "52w_low": row.get("52w_low"),
            "pct_below_52w_high": row.get("pct_below_52w_high"),
            "sma_50": row.get("sma_50"),
            "sma_200": row.get("sma_200"),
            "rsi_14": row.get("rsi_14"),
            "technical_trend": row.get("technical_trend"),
            "volume_surge": row.get("volume_surge"),
            "avg_daily_volume": row.get("avg_daily_volume"),
            "return_1y_pct": row.get("return_1y"),
            "rs_vs_nifty_pct": row.get("rs_vs_nifty"),
            "pe_ratio": row.get("pe_ratio"),
            "sector_median_pe": row.get("sector_median_pe"),
            "pb_ratio": row.get("pb_ratio"),
            "sector_median_pb": row.get("sector_median_pb"),
            "peg_ratio": row.get("peg_ratio"),
            "earnings_yield_pct": row.get("earnings_yield_pct"),
            "beats_bond_yield": row.get("beats_bond_yield"),
            "roe_pct": row.get("roe"),
            "sector_median_roe": row.get("sector_median_roe"),
            "debt_to_equity": row.get("debt_to_equity"),
            "current_ratio": row.get("current_ratio"),
            "gross_margin_current_pct": row.get("gross_margin_current"),
            "gross_margin_prev_pct": row.get("gross_margin_prev"),
            "operating_margin_pct": row.get("operating_margin"),
            "revenue_cagr_3y_pct": row.get("revenue_cagr_3y"),
            "netincome_cagr_3y_pct": row.get("netincome_cagr_3y"),
            "revenue_cagr_5y_pct": row.get("revenue_cagr_5y"),
            "netincome_cagr_5y_pct": row.get("netincome_cagr_5y"),
            "operating_cash_flow": row.get("operating_cash_flow"),
            "capital_expenditure": row.get("capital_expenditure"),
            "free_cash_flow": row.get("free_cash_flow"),
            "fcf_yield_pct": row.get("fcf_yield_pct"),
            "ev_ebitda": row.get("ev_ebitda"),
            "altman_z": row.get("altman_z"),
            "piotroski_f": row.get("piotroski_f"),
            "market_cap_cr": row.get("market_cap_cr"),
            "net_income": row.get("net_income"),
            "institutional_holding_pct": row.get("institutional_holding_pct"),
            "consistent_profit": row.get("consistent_profit"),
            "high_earnings_quality": row.get("high_earnings_quality"),
            "data_years_available": row.get("data_years_available"),
            "mmi_score": mmi.get("score"),
            "mmi_zone": mmi.get("zone"),
            "mmi_corr": row.get("mmi_corr"),
            "mmi_sensitive": row.get("mmi_sensitive"),
            "risk_note": get_risk(row.get("sector")),
            "run_date": datetime.now().strftime("%Y-%m-%d"),
        })
    return pd.DataFrame(rows)


def safe_write_csv(df: pd.DataFrame, path: str):
    try:
        dir_ = os.path.dirname(os.path.abspath(path)) or "."
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", dir=dir_, delete=False)
        df.to_csv(tmp.name, index=False)
        tmp.close()
        if os.path.exists(path):
            os.remove(path)
        shutil.move(tmp.name, path)
        print(f"[INFO] Report saved → {path} ({len(df)} stocks, {len(df.columns)} columns)")
    except Exception as e:
        fallback = f"Top_Stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(fallback, index=False)
        print(f"[WARNING] Could not write to '{path}' ({e}). Saved → '{fallback}'.")


# =============================================================================
# SECTION 16: MAIN
# =============================================================================
def main():
    global CONFIG
    CONFIG = validate_and_sanitize_config(CONFIG)
    CONFIG = apply_archetype(CONFIG)
    # Step: fetch MMI
    mmi = fetch_market_mood_index()
    # Print header
    print("\n" + "=" * 70)
    print(f"  NSE RECOMMENDATION ENGINE v6.0  |  Universe: {CONFIG['universe']}")
    if CONFIG.get("archetype") != "Custom":
        print(f"  Archetype : {CONFIG['archetype']}")
    print(f"  Run Date  : {datetime.now().strftime('%d %B %Y | %I:%M %p')}")
    print(f"  MMI       : {mmi['emoji']} {mmi['score']} — {mmi['zone']}")
    print("=" * 70)
    # Apply continuous MMI dynamic adjustments
    CONFIG = apply_mmi_dynamic_weights(CONFIG, mmi)
    # Force refresh?
    if CONFIG.get("cache_force_refresh"):
        wipe_cache()
    # Load universe
    tickers, sector_map = get_universe_tickers(CONFIG["universe"])
    print_cache_stats(len(tickers))
    # Fundamentals (cached)
    fund_df = load_master_cache()
    if fund_df is None:
        fund_df = build_fundamental_dataframe(tickers, sector_map)
        save_master_cache(fund_df)
    # Price stats + daily close matrix
    price_stats_df, close_matrix = fetch_price_data_and_matrix(tickers)
    master_df = pd.merge(fund_df, price_stats_df, on="ticker", how="left", suffixes=("", "_price"))
    # fill SMA alias columns if needed
    for src, dst in [(f"sma_{CONFIG['sma_short']}", "sma_50"), (f"sma_{CONFIG['sma_long']}", "sma_200")]:
        if src in master_df.columns and dst not in master_df.columns:
            master_df[dst] = master_df[src]
    # Save raw merged
    try:
        master_df.to_parquet("raw_data.parquet")
        print(f"[INFO] Raw data saved → raw_data.parquet ({len(master_df)} rows)")
    except Exception:
        try:
            master_df.to_csv("raw_data.csv", index=False)
            print(f"[INFO] Raw data saved → raw_data.csv ({len(master_df)} rows)")
        except Exception as e:
            print(f"[WARNING] raw_data save failed: {e}")
    # Add MMI sensitivity columns
    master_df = apply_mmi_sensitivity_filter(master_df, close_matrix, mmi)
    # Optionally filter by MMI preference — we keep it conservative: do NOT auto-drop unless user wants
    # Now run apply_filters which executes all gate logic
    filtered = apply_filters(master_df)
    if filtered.empty:
        print("[INFO] No stocks passed filters. Check raw_data.parquet/csv for clues.")
        return
    # Score & rank survivors
    top_df = calculate_scores(filtered)
    # CSV build + write
    csv_df = build_csv_df(top_df, mmi)
    safe_write_csv(csv_df, CONFIG["output_csv"])
    # Print terminal scorecards
    print("\n" + "=" * 70)
    print(f"  TOP {len(top_df)} RECOMMENDATIONS  |  Archetype: {CONFIG.get('archetype','Custom')}")
    print(f"  Market Mood: {mmi['emoji']} {mmi['score']} — {mmi['zone']}")
    print("=" * 70 + "\n")
    for i, (_, row) in enumerate(top_df.iterrows(), 1):
        generate_verdict(row, rank=i, master_df=master_df)
    # Final summary
    print("=" * 70)
    print(f"  ✅  Done.  {len(top_df)} stocks out of {len(tickers)} scanned.")
    print(f"  📁  Report   → {CONFIG['output_csv']}")
    print(f"  📁  Raw Data → raw_data.parquet / raw_data.csv")
    print(f"  📦  Cache    → {CONFIG['cache_folder']}")
    print(f"  🧭  MMI      → {mmi['emoji']} {mmi['score']} [{mmi['zone']}]")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user. Partial results may be in raw_data.parquet/raw_data.csv.")
    except Exception as e:
        import traceback
        print(f"\n[CRITICAL ERROR] {e}")
        traceback.print_exc()