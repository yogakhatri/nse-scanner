# =============================================================================
# nse_screener.py  |  v4.0  |  Tickertape-Level NSE Recommendation Engine
# =============================================================================
# UPGRADES in v4.0:
#   [1] Fixed Cache: Fundamentals cached, Prices always re-fetched daily
#   [2] Computed 3Y/5Y CAGR from financial statements (not yfinance snapshots)
#   [3] True Sector-Relative Valuation (PE, PB, ROE vs Sector Median)
#   [4] Dedicated scoring model for Financials (Bank/NBFC)
#   [5] Institutional Smart Money signal
#   [6] Multi-Axis Grading: Quality(0-10) + Valuation(0-10) + Momentum(0-10)
#   [7] Dynamic "Why Now?" Bull Case explainability output
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
import pickle
import shutil
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============================================================================
# SECTION 1: CONFIG BLOCK
# =============================================================================

CONFIG = {
    # ── Universe ──────────────────────────────────────────────────────────────
    # Options: "Nifty50", "Nifty100", "Nifty500", "Midcap150",
    #          "Smallcap250", "Top800_Custom"
    "universe":  "Nifty500",

    # ── Archetype ─────────────────────────────────────────────────────────────
    # Options: "Custom", "Growth_Bargain", "Cash_Machine",
    #          "Low_Debt_Midcap", "Quality_Compounder", "Near_52W_Low"
    "archetype": "Custom",

    # ── Cache ─────────────────────────────────────────────────────────────────
    "use_cache":                 True,
    "cache_folder":              "cache/",
    "cache_tickers_subfolder":   "cache/tickers/",
    "cache_fundamental_days":    7,    # Re-fetch fundamentals after 7 days
    "cache_universe_days":       30,   # Re-fetch universe CSV after 30 days
    "cache_force_refresh":       False,
    "cache_show_stats":          True,
    "cache_schema_version":      "4.0",  # Invalidates old cache on upgrade

    # ── Gate 1: Liquidity ─────────────────────────────────────────────────────
    "min_market_cap_cr":         1000,
    "min_avg_daily_volume":      300000,

    # ── Gate 2: Red Flag Auto-Reject ──────────────────────────────────────────
    "reject_negative_ocf":       True,
    "reject_current_ratio_below": 1.0,
    "reject_shrinking_margins":   True,

    # ── Gate 3: Quality (Non-Financials) ─────────────────────────────────────
    "max_debt_to_equity":        0.5,
    "min_roe_pct":               15,

    # ── Gate 4: Growth ────────────────────────────────────────────────────────
    "min_revenue_cagr_3y_pct":   10,
    "min_netincome_cagr_3y_pct": 12,

    # ── Gate 5: Sector-Relative Valuation ─────────────────────────────────────
    "max_pe_vs_sector_mult":     1.2,
    "min_roe_vs_sector_mult":    0.9,
    "max_peg_ratio":             1.5,

    # ── Gate 6: Financials ────────────────────────────────────────────────────
    "max_pb_financials":         2.5,
    "financial_sectors":         ["Bank", "NBFC", "Insurance",
                                  "Financial Services", "Finance"],

    # ── Scoring Weights (Multi-Axis) ──────────────────────────────────────────
    "quality_weight":            10,   # max 10 points
    "valuation_weight":          10,   # max 10 points
    "momentum_weight":           10,   # max 10 points

    # ── Technical ─────────────────────────────────────────────────────────────
    "sma_short":                 50,
    "sma_long":                  200,

    # ── India 10Y Bond Yield ──────────────────────────────────────────────────
    "india_10y_bond_yield_pct":  7.1,

    # ── Output ────────────────────────────────────────────────────────────────
    "top_n_results":             15,
    "output_csv":                "Top_Stocks_Report.csv",
}

NSE_URLS = {
    "Nifty50":     "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv",
    "Nifty100":    "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv",
    "Nifty500":    "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    "Midcap150":   "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv",
    "Smallcap250": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv",
}

# =============================================================================
# SECTION 2: ARCHETYPES
# =============================================================================

ARCHETYPES = {
    "Growth_Bargain": {
        "min_netincome_cagr_3y_pct":  18,
        "max_pe_vs_sector_mult":       0.9,
        "max_peg_ratio":               1.2,
        "min_roe_pct":                 15,
        "max_debt_to_equity":          0.5,
    },
    "Cash_Machine": {
        "min_roe_pct":                 20,
        "max_debt_to_equity":          0.1,
        "min_revenue_cagr_3y_pct":     10,
        "min_netincome_cagr_3y_pct":   10,
        "reject_negative_ocf":         True,
    },
    "Low_Debt_Midcap": {
        "universe":                    "Midcap150",
        "max_debt_to_equity":          0.1,
        "min_roe_pct":                 15,
        "min_netincome_cagr_3y_pct":   12,
        "max_pe_vs_sector_mult":       1.3,
    },
    "Quality_Compounder": {
        "min_roe_pct":                 20,
        "max_debt_to_equity":          0.3,
        "min_revenue_cagr_3y_pct":     15,
        "min_netincome_cagr_3y_pct":   15,
        "max_peg_ratio":               1.5,
    },
    "Near_52W_Low": {
        "min_pct_below_52w_high":      30,
        "min_roe_pct":                 12,
        "max_debt_to_equity":          0.8,
        "min_netincome_cagr_3y_pct":   10,
    },
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
    """Merges archetype overrides into the CONFIG if a named archetype is selected."""
    name = config.get("archetype", "Custom")
    if name == "Custom" or name not in ARCHETYPES:
        return config
    merged = config.copy()
    merged.update(ARCHETYPES[name])
    print(f"[INFO] Archetype '{name}' applied.")
    return merged


# =============================================================================
# SECTION 0: CACHE ENGINE
# =============================================================================

_stats = {
    "fund_hits": 0, "fund_miss": 0,
    "price_always_fresh": 0,
    "master_hit": False, "master_secs": 0.0,
}


def _dirs():
    os.makedirs(CONFIG["cache_folder"], exist_ok=True)
    os.makedirs(CONFIG["cache_tickers_subfolder"], exist_ok=True)


def _tpath(ticker: str, dtype: str) -> str:
    return os.path.join(
        CONFIG["cache_tickers_subfolder"],
        f"{ticker.replace('.', '_')}_{dtype}.json"
    )


def save_ticker_cache(ticker: str, data: dict, dtype: str):
    """Saves a ticker's data to a JSON file with fetch timestamp and schema version."""
    if not CONFIG.get("use_cache"):
        return
    _dirs()
    payload = {k: (v if not isinstance(v, float) or not np.isnan(v) else None)
               for k, v in data.items()}
    payload["fetched_at"]      = datetime.now().isoformat()
    payload["schema_version"]  = CONFIG["cache_schema_version"]
    try:
        with open(_tpath(ticker, dtype), "w") as f:
            json.dump(payload, f, default=str)
    except Exception as e:
        print(f"[CACHE] Save failed for {ticker}: {e}")


def load_ticker_cache(ticker: str, dtype: str) -> dict | None:
    """
    Loads cached data for a ticker if:
    - File exists
    - Schema version matches current config
    - Data is within freshness window
    - Critical fields are not all None (guards against silent fetch failures)
    Returns None on any failure (triggers live re-fetch).
    """
    if not CONFIG.get("use_cache"):
        return None
    path = _tpath(ticker, dtype)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            c = json.load(f)

        # Schema version guard
        if c.get("schema_version") != CONFIG["cache_schema_version"]:
            return None

        age = (datetime.now() - datetime.fromisoformat(c["fetched_at"])).days
        if age >= CONFIG["cache_fundamental_days"]:
            return None

        # Data quality guard: reject if all critical fields are None
        if dtype == "fundamental":
            if all(c.get(k) is None for k in ["pe_ratio", "roe", "market_cap_cr"]):
                return None

        return c
    except Exception:
        return None


def save_master_cache(df: pd.DataFrame):
    """Saves fundamental DataFrame as pickle with metadata for freshness validation."""
    if not CONFIG.get("use_cache"):
        return
    _dirs()
    pkl  = os.path.join(CONFIG["cache_folder"], "master_fundamentals.pkl")
    meta = os.path.join(CONFIG["cache_folder"], "cache_metadata.json")
    try:
        df.to_pickle(pkl)
        with open(meta, "w") as f:
            json.dump({
                "created_at":        datetime.now().isoformat(),
                "universe":          CONFIG["universe"],
                "archetype":         CONFIG.get("archetype", "Custom"),
                "schema_version":    CONFIG["cache_schema_version"],
                "total_tickers":     len(df),
            }, f, indent=2)
        print(f"[CACHE] Fundamentals saved → {pkl} ({len(df)} rows)")
    except Exception as e:
        print(f"[CACHE] Save master failed: {e}")


def load_master_cache() -> pd.DataFrame | None:
    """
    Loads the fundamental DataFrame from pickle if:
    - Files exist, schema matches, universe matches, archetype matches
    - Cache age is within cache_fundamental_days
    NOTE: Price data is NEVER loaded from cache. Always re-fetched.
    """
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
            print(f"[CACHE] Fundamentals expired ({age}d). Re-fetching.")
            return None
        t0 = time.time()
        df = pd.read_pickle(pkl)
        el = round(time.time() - t0, 2)
        _stats["master_hit"]  = True
        _stats["master_secs"] = el
        print(f"[CACHE] ✅ Fundamentals HIT — {len(df)} rows in {el}s (age: {age}d)")
        return df
    except Exception as e:
        print(f"[CACHE] Load master failed: {e}")
        return None


def wipe_cache():
    """Deletes entire cache folder."""
    if os.path.exists(CONFIG["cache_folder"]):
        shutil.rmtree(CONFIG["cache_folder"])
        print("[CACHE] 🗑  Cache wiped.")
    _dirs()


def print_cache_stats(total: int):
    """Prints formatted cache hit/miss statistics."""
    if not CONFIG.get("cache_show_stats"):
        return
    s = _stats
    saved_min = round(((s["fund_hits"]) * 4.5) / 60, 1)
    print(f"\n{'='*64}")
    print(f"  CACHE STATS")
    print(f"  Fundamentals cache : ✅ {'HIT ('+str(s['master_secs'])+'s)' if s['master_hit'] else 'MISS'}")
    print(f"  Ticker fund hits   : {s['fund_hits']} / {s['fund_hits']+s['fund_miss']}")
    print(f"  Price data         : Always re-fetched (v4.0 design)")
    print(f"  Estimated time saved : ~{saved_min} minutes")
    print(f"{'='*64}\n")


# =============================================================================
# SECTION 3: UNIVERSE LOADER
# =============================================================================

def fetch_csv(url: str) -> pd.DataFrame:
    """Fetches a CSV with browser headers and SSL bypass."""
    h = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,*/*;q=0.8",
    }
    r = requests.get(url, headers=h, timeout=20, verify=False)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))


def get_universe_tickers(universe_name: str):
    """
    Loads NSE ticker list and sector map.
    Caches the universe CSV for 30 days to avoid repeated downloads.
    Returns (tickers: list, sector_map: dict).
    """
    print(f"\n[INFO] Loading universe: {universe_name}")
    _dirs()
    u_csv  = os.path.join(CONFIG["cache_folder"], f"universe_{universe_name}.csv")
    u_meta = os.path.join(CONFIG["cache_folder"], f"universe_{universe_name}_meta.json")
    df = None

    # Try universe cache
    if CONFIG.get("use_cache") and os.path.exists(u_csv) and os.path.exists(u_meta):
        try:
            with open(u_meta) as f:
                um = json.load(f)
            age = (datetime.now() - datetime.fromisoformat(um["fetched_at"])).days
            if age < CONFIG["cache_universe_days"]:
                df = pd.read_csv(u_csv)
                print(f"[CACHE] Universe cache HIT — {len(df)} stocks (age: {age}d)")
        except Exception:
            df = None

    # Fetch from network
    if df is None:
        if universe_name in NSE_URLS:
            try:
                df = fetch_csv(NSE_URLS[universe_name])
                df.to_csv(u_csv, index=False)
                with open(u_meta, "w") as f:
                    json.dump({"fetched_at": datetime.now().isoformat()}, f)
                print(f"[INFO] Fetched {len(df)} stocks from niftyindices.com")
            except Exception as e:
                print(f"[WARNING] Network fetch failed: {e}")
        elif universe_name == "Top800_Custom":
            try:
                frames = [fetch_csv(NSE_URLS[k]) for k in ["Nifty500","Midcap150","Smallcap250"]]
                df = pd.concat(frames).drop_duplicates(subset=["Symbol"]).head(800)
            except Exception as e:
                print(f"[WARNING] Top800 failed: {e}")

    # Local fallback
    if df is None:
        local = f"{universe_name}.csv"
        if os.path.exists(local):
            df = pd.read_csv(local)
        else:
            raise FileNotFoundError(
                f"Cannot load universe '{universe_name}'. "
                f"Download CSV from niftyindices.com and save as '{local}'."
            )

    df.columns = [c.strip() for c in df.columns]
    sym = next((c for c in df.columns if "symbol"   in c.lower()), None)
    ind = next((c for c in df.columns if any(x in c.lower()
               for x in ["industry","sector"])), None)
    if not sym:
        raise ValueError(f"Symbol column not found. Columns: {df.columns.tolist()}")

    tickers    = [str(s).strip().upper() + ".NS" for s in df[sym].dropna()]
    sector_map = {}
    if ind:
        for _, row in df.iterrows():
            t = str(row[sym]).strip().upper() + ".NS"
            sector_map[t] = str(row[ind]).strip() if pd.notna(row[ind]) else "Unknown"
    else:
        sector_map = {t: "Unknown" for t in tickers}

    print(f"[INFO] Tickers loaded: {len(tickers)}")
    return tickers, sector_map


# =============================================================================
# SECTION 4A: ALWAYS-FRESH PRICE + TECHNICAL DATA
# =============================================================================

def fetch_price_data(tickers: list) -> pd.DataFrame:
    """
    [v4.0] Price data is ALWAYS fetched fresh. Never cached.
    Downloads 1Y daily OHLCV in one batch call.
    Computes: current_price, 52w_high, 52w_low, avg_daily_volume,
              pct_below_52w_high, sma_50, sma_200, technical_trend,
              volume_surge (current vol > 30d avg by 50%).
    """
    print(f"\n[INFO] 🌐 Fetching LIVE price/technical data ({len(tickers)} tickers)...")
    try:
        raw = yf.download(
            tickers, period="1y", group_by="ticker",
            auto_adjust=True, progress=False, threads=True
        )
    except Exception as e:
        print(f"[ERROR] Batch download failed: {e}")
        return pd.DataFrame()

    ss, sl = CONFIG["sma_short"], CONFIG["sma_long"]
    records = []

    for ticker in tickers:
        try:
            df_t = raw[ticker] if len(tickers) > 1 else raw
            if df_t.empty or "Close" not in df_t.columns:
                raise ValueError("No data")

            cl  = df_t["Close"].dropna()
            vol = df_t["Volume"].dropna()

            cur    = round(float(cl.iloc[-1]), 2)
            hi52   = round(float(cl.max()), 2)
            lo52   = round(float(cl.min()), 2)
            v30avg = float(vol.tail(30).mean()) if len(vol) >= 5 else 0
            vcur   = float(vol.iloc[-1]) if len(vol) >= 1 else 0
            below  = round((hi52 - cur) / hi52 * 100, 2) if hi52 > 0 else None
            sma50  = round(float(cl.tail(ss).mean()), 2) if len(cl) >= ss else None
            sma200 = round(float(cl.tail(sl).mean()), 2) if len(cl) >= sl else None
            vsurge = (vcur > v30avg * 1.5) if v30avg > 0 else False

            if sma50 and sma200:
                if   cur > sma50 > sma200: trend = "Uptrend ✅"
                elif cur < sma50 < sma200: trend = "Downtrend ❌"
                else:                      trend = "Sideways ➡️"
            else:
                trend = "N/A"

            records.append({
                "ticker":             ticker,
                "current_price":      cur,
                "52w_high":           hi52,
                "52w_low":            lo52,
                "avg_daily_volume":   int(v30avg),
                "pct_below_52w_high": below,
                "sma_50":             sma50,
                "sma_200":            sma200,
                "technical_trend":    trend,
                "volume_surge":       vsurge,
            })
        except Exception:
            records.append({
                "ticker": ticker, "current_price": None, "52w_high": None,
                "52w_low": None, "avg_daily_volume": None,
                "pct_below_52w_high": None, "sma_50": None,
                "sma_200": None, "technical_trend": "N/A",
                "volume_surge": False,
            })

    return pd.DataFrame(records)


# =============================================================================
# SECTION 4B: FUNDAMENTAL DATA (WITH COMPUTED CAGR)
# =============================================================================

def compute_cagr(series: list) -> float | None:
    """
    Computes CAGR given a list of values [oldest, ..., newest].
    Returns percentage CAGR. Returns None if data is invalid.
    """
    try:
        clean = [v for v in series if v is not None and not np.isnan(v) and v > 0]
        if len(clean) < 2:
            return None
        n      = len(clean) - 1
        result = ((clean[-1] / clean[0]) ** (1 / n) - 1) * 100
        return round(result, 2)
    except Exception:
        return None


def fetch_fundamentals(ticker: str) -> dict:
    """
    Fetches fundamentals for one ticker with 3-tier data extraction:
    1. Check per-ticker JSON cache first (respects TTL + schema version).
    2. If cache miss, fetch from yfinance (.info + .financials + .cashflow).
    3. Compute multi-year CAGR from historical statements.
    4. Save result to cache.

    Fields computed:
        pe_ratio, pb_ratio, peg_ratio, roe, debt_to_equity, market_cap_cr,
        current_ratio, gross_margin_current, gross_margin_prev,
        operating_cash_flow, capital_expenditure, free_cash_flow,
        net_income, institutional_holding_pct,
        revenue_cagr_3y, netincome_cagr_3y,
        revenue_cagr_5y, netincome_cagr_5y,
        sector_yf, data_years_available
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
        "current_ratio": None, "gross_margin_current": None,
        "gross_margin_prev": None, "operating_cash_flow": None,
        "capital_expenditure": None, "free_cash_flow": None,
        "net_income": None, "institutional_holding_pct": None,
        "revenue_cagr_3y": None, "netincome_cagr_3y": None,
        "revenue_cagr_5y": None, "netincome_cagr_5y": None,
        "sector_yf": None, "data_years_available": 0,
    }

    try:
        stock = yf.Ticker(ticker)
        info  = stock.info or {}

        def sg(key, scale=1.0):
            v = info.get(key)
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                return round(float(v) * scale, 4)
            return None

        result["pe_ratio"]             = sg("trailingPE")
        result["pb_ratio"]             = sg("priceToBook")
        result["peg_ratio"]            = sg("pegRatio")
        result["roe"]                  = sg("returnOnEquity", 100)
        result["debt_to_equity"]       = sg("debtToEquity")
        result["market_cap_cr"]        = sg("marketCap", 1/1e7)
        result["current_ratio"]        = sg("currentRatio")
        result["gross_margin_current"] = sg("grossMargins", 100)
        result["operating_cash_flow"]  = sg("operatingCashflow")
        result["sector_yf"]            = info.get("sector")

        # Institutional holding
        ih = info.get("heldPercentInstitutions")
        if ih is not None and not (isinstance(ih, float) and np.isnan(ih)):
            result["institutional_holding_pct"] = round(float(ih) * 100, 2)

        # ── Financial Statements: Multi-year CAGR ────────────────────────────
        try:
            fin = stock.financials  # Columns = years desc
            if fin is not None and not fin.empty:
                rev_row = next((r for r in fin.index
                                if "total revenue"  in r.lower()), None)
                ni_row  = next((r for r in fin.index
                                if "net income"     in r.lower()), None)
                gp_row  = next((r for r in fin.index
                                if "gross profit"   in r.lower()), None)

                n_cols = fin.shape[1]
                result["data_years_available"] = n_cols

                # Revenue and Net Income series (oldest→newest)
                if rev_row:
                    rev_vals = [float(fin.loc[rev_row].iloc[i])
                                for i in range(n_cols - 1, -1, -1)
                                if not np.isnan(fin.loc[rev_row].iloc[i])]
                    if len(rev_vals) >= 4:
                        result["revenue_cagr_3y"] = compute_cagr(rev_vals[-4:])
                    if len(rev_vals) >= 6:
                        result["revenue_cagr_5y"] = compute_cagr(rev_vals[-6:])

                if ni_row:
                    ni_vals = [float(fin.loc[ni_row].iloc[i])
                               for i in range(n_cols - 1, -1, -1)
                               if not np.isnan(fin.loc[ni_row].iloc[i])]
                    result["net_income"] = float(fin.loc[ni_row].iloc[0])
                    if len(ni_vals) >= 4:
                        result["netincome_cagr_3y"] = compute_cagr(ni_vals[-4:])
                    if len(ni_vals) >= 6:
                        result["netincome_cagr_5y"] = compute_cagr(ni_vals[-6:])

                # Previous year gross margin
                if gp_row and rev_row and n_cols >= 2:
                    gp_p  = float(fin.loc[gp_row].iloc[1])
                    rv_p  = float(fin.loc[rev_row].iloc[1])
                    if rv_p > 0:
                        result["gross_margin_prev"] = round(gp_p / rv_p * 100, 4)

        except Exception:
            pass

        # ── Cash Flow: CapEx and FCF ─────────────────────────────────────────
        try:
            cf = stock.cashflow
            if cf is not None and not cf.empty:
                cx_row = next((r for r in cf.index if "capital" in r.lower()), None)
                if cx_row:
                    result["capital_expenditure"] = float(cf.loc[cx_row].iloc[0])
            if result["operating_cash_flow"] and result["capital_expenditure"]:
                result["free_cash_flow"] = (
                    result["operating_cash_flow"] - abs(result["capital_expenditure"])
                )
        except Exception:
            pass

    except Exception:
        pass

    save_ticker_cache(ticker, result, "fundamental")
    return result


# =============================================================================
# SECTION 4C: MASTER DATAFRAME BUILDER
# =============================================================================

def build_fundamental_dataframe(tickers: list, sector_map: dict) -> pd.DataFrame:
    """
    Fetches fundamentals for each ticker (with cache) and builds master DataFrame.
    NOTE: Does NOT fetch price data here. Price is always fetched separately.
    """
    print(f"\n[INFO] Fetching fundamentals ({len(tickers)} stocks)...")
    records = []
    total   = len(tickers)

    for i, ticker in enumerate(tickers, 1):
        in_cache = os.path.exists(_tpath(ticker, "fundamental"))
        label    = "📦" if in_cache else "🌐"
        print(f"  [{i:>4}/{total}] {ticker:<22} {label}", end="\r")
        try:
            records.append(fetch_fundamentals(ticker))
        except Exception:
            records.append({"ticker": ticker})
        if not in_cache:
            time.sleep(0.5)

    print(f"\n[INFO] Fundamentals done.")
    fund_df = pd.DataFrame(records)
    fund_df["sector"] = fund_df["ticker"].map(sector_map)
    fund_df["sector"] = fund_df.apply(
        lambda r: r.get("sector_yf")
        if (pd.isna(r.get("sector")) or r.get("sector") == "Unknown")
        and r.get("sector_yf") else r.get("sector"), axis=1
    )
    fund_df.drop(columns=["sector_yf"], inplace=True, errors="ignore")
    return fund_df


def build_master_dataframe(tickers: list, sector_map: dict) -> pd.DataFrame:
    """
    [v4.0] Merges always-fresh price data with fundamental data.
    If fundamental cache hits, only price is fetched (very fast).
    """
    fund_df  = build_fundamental_dataframe(tickers, sector_map)
    price_df = fetch_price_data(tickers)
    master   = pd.merge(fund_df, price_df, on="ticker", how="left")
    print(f"[INFO] Master DataFrame: {len(master)} rows.")
    return master


# =============================================================================
# SECTION 5: SECTOR CLASSIFICATION + SECTOR MEDIANS
# =============================================================================

def classify_sector(s) -> str:
    """Returns 'financial' or 'non_financial'."""
    if not s or pd.isna(s):
        return "non_financial"
    sl = str(s).lower()
    return "financial" if any(f.lower() in sl for f in CONFIG["financial_sectors"]) \
           else "non_financial"


def add_sector_medians(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds sector-level median columns used for relative valuation and scoring.
    Requires at least 5 stocks per sector for a stable median.
    Falls back to overall median if fewer stocks exist in a sector.
    """
    overall_pe  = df["pe_ratio"].median()
    overall_pb  = df["pb_ratio"].median()
    overall_roe = df["roe"].median()

    sect_pe  = df.groupby("sector")["pe_ratio"].transform(
        lambda x: x.median() if len(x.dropna()) >= 5 else overall_pe)
    sect_pb  = df.groupby("sector")["pb_ratio"].transform(
        lambda x: x.median() if len(x.dropna()) >= 5 else overall_pb)
    sect_roe = df.groupby("sector")["roe"].transform(
        lambda x: x.median() if len(x.dropna()) >= 5 else overall_roe)

    df["sector_median_pe"]  = sect_pe
    df["sector_median_pb"]  = sect_pb
    df["sector_median_roe"] = sect_roe
    return df


# =============================================================================
# SECTION 6: FILTER ENGINE (6 GATES)
# =============================================================================

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    6-gate sequential filter engine.
    Gate 1: Liquidity & Size
    Gate 2: Red Flag Auto-Reject
    Gate 3: Quality (Non-Financials)
    Gate 4: Growth (Computed 3Y CAGR)
    Gate 5: Sector-Relative Valuation
    Gate 6: Financial Sector (P/B Rule)
    """
    print(f"\n{'='*64}")
    print(f"  FILTER ENGINE  |  {len(df)} stocks entering")
    print(f"{'='*64}")

    w = df.copy()
    w["sector_type"] = w["sector"].apply(classify_sector)
    w = add_sector_medians(w)

    # Gate 1: Liquidity
    b = len(w)
    w = w[w["market_cap_cr"].notna() & w["avg_daily_volume"].notna()]
    w = w[(w["market_cap_cr"]    >= CONFIG["min_market_cap_cr"]) &
          (w["avg_daily_volume"] >= CONFIG["min_avg_daily_volume"])]
    print(f"  Gate 1 [Liquidity]             : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 2: Red Flags
    b   = len(w)
    rf  = pd.Series(False, index=w.index)
    if CONFIG["reject_negative_ocf"]:
        rf |= w["operating_cash_flow"].notna() & (w["operating_cash_flow"] < 0)
    if CONFIG["reject_current_ratio_below"]:
        rf |= w["current_ratio"].notna() & (w["current_ratio"] < CONFIG["reject_current_ratio_below"])
    if CONFIG["reject_shrinking_margins"]:
        rf |= (w["gross_margin_current"].notna() & w["gross_margin_prev"].notna() &
               (w["gross_margin_current"] < w["gross_margin_prev"]))
    w = w[~rf]
    print(f"  Gate 2 [Red Flags]             : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 3: Quality (Non-Financials)
    b   = len(w)
    nf  = w["sector_type"] == "non_financial"
    bad = nf & (
        (w["debt_to_equity"].notna() & (w["debt_to_equity"] > CONFIG["max_debt_to_equity"])) |
        (w["roe"].notna()            & (w["roe"]            < CONFIG["min_roe_pct"]))
    )
    w = w[~bad]
    print(f"  Gate 3 [Quality — Non-Fin]     : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 4: Growth (Computed 3Y CAGR)
    b  = len(w)
    bg = (
        (w["revenue_cagr_3y"].notna()    & (w["revenue_cagr_3y"]    < CONFIG["min_revenue_cagr_3y_pct"])) |
        (w["netincome_cagr_3y"].notna()  & (w["netincome_cagr_3y"]  < CONFIG["min_netincome_cagr_3y_pct"]))
    )
    w = w[~bg]
    print(f"  Gate 4 [Growth — 3Y CAGR]      : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 5: Sector-Relative Valuation (Non-Financials)
    b   = len(w)
    nf  = w["sector_type"] == "non_financial"
    bv  = nf & (
        w["pe_ratio"].notna() & w["sector_median_pe"].notna() &
        (w["pe_ratio"] > w["sector_median_pe"] * CONFIG["max_pe_vs_sector_mult"])
    )
    br  = nf & (
        w["roe"].notna() & w["sector_median_roe"].notna() &
        (w["roe"] < w["sector_median_roe"] * CONFIG["min_roe_vs_sector_mult"])
    )
    bp  = nf & (w["peg_ratio"].notna() & (w["peg_ratio"] > CONFIG["max_peg_ratio"]))
    w   = w[~(bv | br | bp)]
    print(f"  Gate 5 [Sector-Rel Valuation]  : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Gate 6: Financial P/B
    b   = len(w)
    fin = w["sector_type"] == "financial"
    bf  = fin & (w["pb_ratio"].notna() & (w["pb_ratio"] > CONFIG["max_pb_financials"]))
    w   = w[~bf]
    print(f"  Gate 6 [Financials — P/B]      : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    # Optional Near_52W_Low gate
    if CONFIG.get("archetype") == "Near_52W_Low" and CONFIG.get("min_pct_below_52w_high"):
        b = len(w)
        w = w[w["pct_below_52w_high"].notna() &
              (w["pct_below_52w_high"] >= CONFIG["min_pct_below_52w_high"])]
        print(f"  Gate 7 [Near 52W Low]          : {b:>4} → {len(w):>4}  (-{b-len(w)})")

    print(f"{'='*64}")
    print(f"  ✅ Survivors: {len(w)}")
    print(f"{'='*64}\n")
    return w


# =============================================================================
# SECTION 7: MULTI-AXIS SCORING ENGINE
# =============================================================================

def mn(series: pd.Series) -> pd.Series:
    """Min-max normalize to [0, 1]."""
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series([0.5] * len(series), index=series.index)
    return (series - lo) / (hi - lo)


def score_quality(row: pd.Series) -> float:
    """
    Quality Grade (0-10).
    NON-FINANCIALS: ROE vs sector, 3Y Net Income CAGR, D/E, Current Ratio, OCF quality.
    FINANCIALS:     ROE vs sector, 3Y CAGR, P/B vs sector, D/E skipped.
    """
    pts  = 0.0
    st   = row.get("sector_type", "non_financial")
    roe  = row.get("roe")
    sroe = row.get("sector_median_roe")
    ni3  = row.get("netincome_cagr_3y")
    de   = row.get("debt_to_equity")
    cr   = row.get("current_ratio")
    ocf  = row.get("operating_cash_flow")
    ni   = row.get("net_income")
    pb   = row.get("pb_ratio")
    spb  = row.get("sector_median_pb")

    def ok(v): return v is not None and not (isinstance(v, float) and np.isnan(v))

    # ROE vs sector (2pts)
    if ok(roe) and ok(sroe):
        if roe > sroe * 1.2:      pts += 2.0
        elif roe > sroe * 0.9:    pts += 1.0

    # 3Y Net Income CAGR (2pts)
    if ok(ni3):
        if ni3 > 20:              pts += 2.0
        elif ni3 > 12:            pts += 1.0

    # Debt (2pts) — skip for financials
    if st == "non_financial":
        if ok(de):
            if de < 0.2:          pts += 2.0
            elif de < 0.5:        pts += 1.0
    else:
        # For financials: PB vs sector
        if ok(pb) and ok(spb):
            if pb < spb * 0.8:    pts += 2.0
            elif pb < spb:        pts += 1.0

    # Current Ratio (2pts) — non-financial only
    if st == "non_financial" and ok(cr):
        if cr > 2.0:              pts += 2.0
        elif cr > 1.2:            pts += 1.0

    # OCF > Net Income (2pts) — cash earnings quality
    if ok(ocf) and ok(ni) and ni > 0 and ocf > ni:
        pts += 2.0

    return min(round(pts, 2), 10.0)


def score_valuation(row: pd.Series) -> float:
    """
    Valuation Grade (0-10).
    NON-FINANCIALS: PE vs sector, PB vs sector, PEG, Earnings Yield vs Bond Yield, FCF Yield.
    FINANCIALS:     PB vs sector only.
    """
    pts   = 0.0
    st    = row.get("sector_type", "non_financial")
    pe    = row.get("pe_ratio")
    spe   = row.get("sector_median_pe")
    pb    = row.get("pb_ratio")
    spb   = row.get("sector_median_pb")
    peg   = row.get("peg_ratio")
    mkt   = row.get("market_cap_cr")
    fcf   = row.get("free_cash_flow")
    bond  = CONFIG["india_10y_bond_yield_pct"]

    def ok(v): return v is not None and not (isinstance(v, float) and np.isnan(v))

    if st == "non_financial":
        # PE vs sector (3pts)
        if ok(pe) and ok(spe):
            if pe < spe * 0.7:    pts += 3.0
            elif pe < spe * 0.9:  pts += 2.0
            elif pe < spe * 1.1:  pts += 1.0

        # PB vs sector (2pts)
        if ok(pb) and ok(spb):
            if pb < spb * 0.8:    pts += 2.0
            elif pb < spb:        pts += 1.0

        # PEG (2pts)
        if ok(peg):
            if peg < 0.8:         pts += 2.0
            elif peg < 1.2:       pts += 1.0

        # Earnings Yield > Bond Yield (2pts)
        if ok(pe) and pe > 0:
            ey = 100 / pe
            if ey > bond * 1.5:   pts += 2.0
            elif ey > bond:       pts += 1.0

        # FCF Yield > 4% (1pt)
        if ok(fcf) and ok(mkt) and mkt > 0:
            fy = (fcf / (mkt * 1e7)) * 100
            if fy > 4:            pts += 1.0

    else:
        # Financial: PB vs sector (5pts)
        if ok(pb) and ok(spb):
            if pb < spb * 0.6:    pts += 5.0
            elif pb < spb * 0.8:  pts += 3.0
            elif pb < spb:        pts += 1.0

        # Financial: PE (3pts)
        if ok(pe) and ok(spe):
            if pe < spe * 0.8:    pts += 3.0
            elif pe < spe:        pts += 1.0

        # Earnings Yield (2pts)
        if ok(pe) and pe > 0:
            ey = 100 / pe
            if ey > bond * 1.5:   pts += 2.0
            elif ey > bond:       pts += 1.0

    return min(round(pts, 2), 10.0)


def score_momentum(row: pd.Series) -> float:
    """
    Momentum/Confidence Grade (0-10).
    Covers: Price vs SMA, Volume Surge, Revenue CAGR, Institutional Holding.
    """
    pts   = 0.0
    price = row.get("current_price")
    s50   = row.get("sma_50")
    s200  = row.get("sma_200")
    vsrg  = row.get("volume_surge", False)
    rv3   = row.get("revenue_cagr_3y")
    ih    = row.get("institutional_holding_pct")
    blw   = row.get("pct_below_52w_high")

    def ok(v): return v is not None and not (isinstance(v, float) and np.isnan(v))

    # Price vs SMA (3pts)
    if ok(price) and ok(s50) and ok(s200):
        if price > s50 > s200:    pts += 3.0   # Full uptrend
        elif price > s50:         pts += 1.5   # Partial uptrend
        elif price > s200:        pts += 0.5   # Weak recovery

    # Volume Surge (1pt)
    if vsrg:                      pts += 1.0

    # Revenue CAGR (3pts)
    if ok(rv3):
        if rv3 > 25:              pts += 3.0
        elif rv3 > 15:            pts += 2.0
        elif rv3 > 10:            pts += 1.0

    # Institutional Holding (2pts)
    if ok(ih):
        if ih > 30:               pts += 2.0
        elif ih > 15:             pts += 1.0

    # Not too far below 52W High (1pt bonus for near-high stocks)
    if ok(blw) and blw < 10:      pts += 1.0

    return min(round(pts, 2), 10.0)


def calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes Multi-Axis grades for each surviving stock:
    - quality_grade (0-10)
    - valuation_grade (0-10)
    - momentum_grade (0-10)
    - final_rank_score (0-30) = sum of all three
    Sorts by final_rank_score descending. Returns top N.
    """
    if df.empty:
        print("[WARNING] Nothing to score.")
        return df

    s = df.copy()
    for col in ["roe", "debt_to_equity", "pe_ratio", "peg_ratio",
                "operating_cash_flow", "revenue_cagr_3y", "netincome_cagr_3y"]:
        if col in s.columns:
            s[col] = s[col].fillna(s[col].median())

    s["quality_grade"]   = s.apply(score_quality,   axis=1)
    s["valuation_grade"] = s.apply(score_valuation,  axis=1)
    s["momentum_grade"]  = s.apply(score_momentum,   axis=1)
    s["final_rank_score"] = (
        s["quality_grade"] + s["valuation_grade"] + s["momentum_grade"]
    ).round(2)

    # Earnings Yield for display
    bond = CONFIG["india_10y_bond_yield_pct"]
    s["earnings_yield_pct"] = s["pe_ratio"].apply(
        lambda pe: round(100/pe, 2) if pe and pe > 0 else None
    )
    s["beats_bond_yield"] = s["earnings_yield_pct"].apply(
        lambda ey: ey is not None and ey > bond
    )

    s.sort_values("final_rank_score", ascending=False, inplace=True)
    top = s.head(CONFIG["top_n_results"]).reset_index(drop=True)
    print(f"[INFO] Scoring complete. Top {len(top)} stocks selected.")
    return top


# =============================================================================
# SECTION 8: WHY-NOW BULL CASE ENGINE
# =============================================================================

def build_bull_case(row: pd.Series) -> str:
    """
    Generates a dynamic, data-driven 'Why Now?' sentence by checking
    multiple conditions. Returns the highest-priority matching case.
    Returns a neutral statement if no strong case is found.
    """
    price   = row.get("current_price")
    blw     = row.get("pct_below_52w_high")
    ni3     = row.get("netincome_cagr_3y")
    rv3     = row.get("revenue_cagr_3y")
    pe      = row.get("pe_ratio")
    spe     = row.get("sector_median_pe")
    roe     = row.get("roe")
    de      = row.get("debt_to_equity")
    ih      = row.get("institutional_holding_pct")
    trend   = row.get("technical_trend", "")
    s50     = row.get("sma_50")
    s200    = row.get("sma_200")
    vsrg    = row.get("volume_surge", False)
    ni5     = row.get("netincome_cagr_5y")
    bb      = row.get("beats_bond_yield", False)
    fcf     = row.get("free_cash_flow")
    mkt     = row.get("market_cap_cr")
    sroe    = row.get("sector_median_roe")

    def ok(v): return v is not None and not (isinstance(v, float) and np.isnan(v))
    def fmt(v, d=1): return round(v, d) if ok(v) else "N/A"

    cases = []

    # Condition A: Near 52W High + Strong Earnings Growth
    if ok(blw) and ok(ni3) and blw < 5 and ni3 > 20:
        cases.append((3, f"Trading near 52-week highs with {fmt(ni3)}% 3Y net income CAGR — "
                         f"indicates strong business momentum and market confidence."))

    # Condition B: Deeply Undervalued vs Sector with Superior ROE
    if ok(pe) and ok(spe) and ok(roe) and spe > 0:
        pe_disc = round((spe - pe) / spe * 100, 1)
        if pe_disc > 20 and roe > 20:
            cases.append((3, f"Trades at {pe_disc}% discount to sector PE "
                             f"despite exceptional ROE of {fmt(roe)}% — classic value-quality gap."))

    # Condition C: Institutional Smart Money + Clean Balance Sheet
    if ok(ih) and ok(de) and ih > 20 and de < 0.1:
        cases.append((2, f"Strong institutional backing ({fmt(ih)}% holding) combined with "
                         f"near-zero debt (D/E: {fmt(de)}) — high-safety compounding setup."))

    # Condition D: Technical Breakout with Volume Surge
    if "Uptrend" in str(trend) and vsrg and ok(ni3) and ni3 > 15:
        cases.append((2, f"Technical uptrend confirmed with a volume surge — "
                         f"price breaking out while earnings grow at {fmt(ni3)}% CAGR."))

    # Condition E: Long-term Consistent Compounder
    if ok(ni5) and ok(ni3) and ni5 > 15 and ni3 > 15 and ok(de) and de < 0.3:
        cases.append((2, f"Consistent compounder: {fmt(ni5)}% 5Y and {fmt(ni3)}% 3Y "
                         f"net income CAGR with low debt — ideal for long-horizon holding."))

    # Condition F: Earnings Yield Beats Bond Yield Comfortably
    if bb and ok(pe) and pe > 0:
        ey = round(100 / pe, 1)
        bond = CONFIG["india_10y_bond_yield_pct"]
        if ey > bond * 1.5:
            cases.append((1, f"Earnings yield ({ey}%) is {round(ey/bond, 1)}x the 10Y bond "
                             f"yield ({bond}%) — equity risk well-compensated."))

    # Condition G: Revenue + Profit both growing fast
    if ok(rv3) and ok(ni3) and rv3 > 15 and ni3 > rv3:
        cases.append((2, f"Revenue growing at {fmt(rv3)}% CAGR with even faster profit "
                         f"growth at {fmt(ni3)}% — expanding margins signal operating leverage."))

    # Condition H: FCF Yield (Hidden value)
    if ok(fcf) and ok(mkt) and mkt > 0:
        fy = round((fcf / (mkt * 1e7)) * 100, 1)
        if fy > 6:
            cases.append((1, f"Free cash flow yield of {fy}% is very attractive — "
                             f"business generates real cash well above its market valuation."))

    if cases:
        # Return highest-priority case
        best = sorted(cases, key=lambda x: -x[0])[0][1]
        return best

    return ("Balanced fundamentals with reasonable growth and valuation across "
            "quality, valuation, and technical dimensions.")


# =============================================================================
# SECTION 9: VERDICT GENERATOR
# =============================================================================

def get_risk(sector) -> str:
    """Returns sector-specific risk text."""
    if not sector or pd.isna(sector):
        return SECTOR_RISK_MAP["default"]
    for k in SECTOR_RISK_MAP:
        if k.lower() in str(sector).lower():
            return SECTOR_RISK_MAP[k]
    return SECTOR_RISK_MAP["default"]


def f(val, sfx="", pfx="", d=1):
    """Safe number formatter."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    return f"{pfx}{round(val, d)}{sfx}"


def grade_bar(score: float, max_score: float = 10) -> str:
    """Converts a score to a visual bar: ████░░░░░░"""
    filled = int(round((score / max_score) * 10))
    return "█" * filled + "░" * (10 - filled)


def generate_verdict(row: pd.Series, rank: int):
    """
    Prints a complete, formatted CLI recommendation box for one stock.
    Includes: rank, scores, grades, price, technicals, fundamentals,
              dynamic bull case, and sector-specific risk disclaimer.
    """
    ticker = row.get("ticker", "N/A")
    sector = row.get("sector", "Unknown")
    frs    = row.get("final_rank_score", 0)
    qg     = row.get("quality_grade", 0)
    vg     = row.get("valuation_grade", 0)
    mg     = row.get("momentum_grade", 0)
    price  = row.get("current_price")
    pe     = row.get("pe_ratio");     spe  = row.get("sector_median_pe")
    pb     = row.get("pb_ratio");     spb  = row.get("sector_median_pb")
    peg    = row.get("peg_ratio");    roe  = row.get("roe")
    sroe   = row.get("sector_median_roe")
    de     = row.get("debt_to_equity");cr  = row.get("current_ratio")
    rv3    = row.get("revenue_cagr_3y"); ni3 = row.get("netincome_cagr_3y")
    rv5    = row.get("revenue_cagr_5y"); ni5 = row.get("netincome_cagr_5y")
    ocf    = row.get("operating_cash_flow"); fcf = row.get("free_cash_flow")
    mkt    = row.get("market_cap_cr")
    blw    = row.get("pct_below_52w_high")
    hi52   = row.get("52w_high");     lo52  = row.get("52w_low")
    sma50  = row.get("sma_50");       sma200 = row.get("sma_200")
    trend  = row.get("technical_trend", "N/A")
    vsrg   = row.get("volume_surge", False)
    ih     = row.get("institutional_holding_pct")
    ey     = row.get("earnings_yield_pct")
    bb     = row.get("beats_bond_yield", False)
    dyr    = row.get("data_years_available", 0)
    bull   = build_bull_case(row)
    bond   = CONFIG["india_10y_bond_yield_pct"]

    fcf_y  = f"{round((fcf/(mkt*1e7))*100, 1)}%" \
             if fcf and mkt and mkt > 0 else "N/A"

    total_badge = "🟢 STRONG" if frs >= 22 else ("🟡 MODERATE" if frs >= 15 else "🔴 WEAK")

    print("=" * 70)
    print(f"  #{rank:<3} {ticker:<24}  {sector}")
    print(f"  {total_badge}  |  Final Score: {frs:.1f}/30")
    print(f"  Quality: {qg}/10 [{grade_bar(qg)}]")
    print(f"  Valuation: {vg}/10 [{grade_bar(vg)}]")
    print(f"  Momentum: {mg}/10 [{grade_bar(mg)}]")
    print("-" * 70)
    print(f"  PRICE & TECHNICALS")
    print(f"    CMP     : ₹{f(price, d=2):<12} Mkt Cap : ₹{f(mkt, ' Cr', d=0)}")
    print(f"    52W     : ₹{f(lo52, d=2)} — ₹{f(hi52, d=2)}   Below High: {f(blw, '%')}")
    print(f"    Trend   : {trend:<20} Vol Surge: {'✅ Yes' if vsrg else '❌ No'}")
    print(f"    SMA50   : ₹{f(sma50, d=1):<12} SMA200  : ₹{f(sma200, d=1)}")
    print("-" * 70)
    print(f"  VALUATION (Stock vs Sector)")
    print(f"    P/E     : {f(pe):<10}   Sector P/E : {f(spe)}")
    print(f"    P/B     : {f(pb):<10}   Sector P/B : {f(spb)}")
    print(f"    PEG     : {f(peg):<10}   EPS Yield  : {f(ey, '%')}"
          f"  {'✅ Beats Bond' if bb else '❌ Below Bond'} ({bond}%)")
    print(f"    FCF Yield: {fcf_y}")
    print("-" * 70)
    print(f"  FUNDAMENTALS")
    print(f"    ROE     : {f(roe, '%'):<10}   Sector ROE : {f(sroe, '%')}")
    print(f"    D/E     : {f(de):<10}   Curr Ratio : {f(cr)}")
    print(f"    Rev CAGR: 3Y={f(rv3, '%'):<8}  5Y={f(rv5, '%')}")
    print(f"    PAT CAGR: 3Y={f(ni3, '%'):<8}  5Y={f(ni5, '%')}")
    print(f"    Op. CF  : ₹{f(ocf/1e7 if ocf else None, ' Cr', d=0):<10}"
          f"  FCF    : ₹{f(fcf/1e7 if fcf else None, ' Cr', d=0)}")
    print(f"    Inst. Hold: {f(ih, '%')}   Data: {dyr} years")
    print("-" * 70)
    print(f"  💡 THE BULL CASE")
    # Word-wrap bull case at 64 chars
    words = bull.split()
    line  = "    "
    for w_ in words:
        if len(line) + len(w_) + 1 > 68:
            print(line)
            line = "    " + w_ + " "
        else:
            line += w_ + " "
    if line.strip():
        print(line)
    print("-" * 70)
    print(f"  ⚠️  DISCLAIMER")
    print(f"    Sector risk ({sector}):")
    print(f"    {get_risk(sector)}")
    print(f"    THIS IS NOT FINANCIAL ADVICE. Always do your own due diligence.")
    print("=" * 70 + "\n")


# =============================================================================
# SECTION 10: MAIN EXECUTION
# =============================================================================

def main():
    """
    v4.0 Pipeline:
    1. Apply archetype overrides
    2. Wipe cache if force_refresh
    3. Load fundamental cache (or fetch fresh)
    4. ALWAYS re-fetch price/technical data (v4.0 rule)
    5. Filter → Multi-Axis Score → Sort → Report
    """
    global CONFIG
    CONFIG = apply_archetype(CONFIG)

    print("\n" + "=" * 70)
    print(f"  NSE RECOMMENDATION ENGINE v4.0  |  Universe: {CONFIG['universe']}")
    arch = CONFIG.get("archetype", "Custom")
    if arch != "Custom":
        print(f"  Archetype : {arch}")
    print(f"  Run Date  : {datetime.now().strftime('%d %B %Y | %I:%M %p')}")
    print(f"  Cache     : {'ON' if CONFIG['use_cache'] else 'OFF'}  "
          f"| Schema: {CONFIG['cache_schema_version']}")
    print("=" * 70)

    # Force refresh
    if CONFIG.get("cache_force_refresh"):
        wipe_cache()

    # Load universe
    tickers, sector_map = get_universe_tickers(CONFIG["universe"])

    # Fundamental data (cached)
    fund_df = load_master_cache()
    if fund_df is None:
        fund_df = build_fundamental_dataframe(tickers, sector_map)
        save_master_cache(fund_df)

    # ALWAYS fetch fresh price data (v4.0 rule)
    print("\n[INFO] v4.0: Refreshing price/technical data regardless of cache...")
    price_df  = fetch_price_data(tickers)
    master_df = pd.merge(fund_df, price_df, on="ticker", how="left", suffixes=("", "_price"))

    # Cache stats
    print_cache_stats(len(tickers))

    # Save raw
    master_df.to_csv("raw_data.csv", index=False)
    print(f"[INFO] Raw data → raw_data.csv ({len(master_df)} rows)")

    # Filter
    filtered = apply_filters(master_df)
    if filtered.empty:
        print("[WARNING] No stocks survived filters. Relax CONFIG thresholds.")
        return

    # Score
    top_df = calculate_scores(filtered)
    top_df.to_csv(CONFIG["output_csv"], index=False)
    print(f"[INFO] Report → {CONFIG['output_csv']} ({len(top_df)} stocks)\n")

    # Print verdicts
    print("=" * 70)
    print(f"  TOP {len(top_df)} RECOMMENDATIONS  |  {arch}")
    print("=" * 70 + "\n")

    for i, (_, row) in enumerate(top_df.iterrows(), 1):
        try:
            generate_verdict(row, rank=i)
        except Exception as e:
            print(f"[WARNING] Verdict error for {row.get('ticker','?')}: {e}")

    print("=" * 70)
    print(f"  ✅ Complete.")
    print(f"  📁  Report      → {CONFIG['output_csv']}")
    print(f"  📁  Raw Data    → raw_data.csv")
    print(f"  📦  Cache       → {CONFIG['cache_folder']}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted.")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        raise
