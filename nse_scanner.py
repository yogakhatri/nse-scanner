# =============================================================================
# nse_screener.py  |  v3.0  |  NSE Stock Screener + Smart Cache Engine
# =============================================================================
# WHAT'S NEW in v3.0:
#   - Layer 1: Per-ticker JSON cache (fundamentals + price)
#   - Layer 2: Master DataFrame pickle cache
#   - Smart freshness validation (detects corrupt/incomplete cached data)
#   - Force refresh mode (wipes all cache)
#   - Universe/archetype change detection (auto-invalidates cache)
#   - Cache statistics output (hits, misses, time saved)
#   - Partial resume support (mid-run crash recovery)
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
from datetime import datetime, timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============================================================================
# SECTION 1: CONFIG BLOCK
# =============================================================================

CONFIG = {
    # ── Universe ──────────────────────────────────────────────────────────────
    # Options: "Nifty50", "Nifty100", "Nifty500",
    #          "Midcap150", "Smallcap250", "Top800_Custom"
    "universe": "Nifty500",

    # ── Screen Archetype ──────────────────────────────────────────────────────
    # Options: "Custom", "Growth_Bargain", "Cash_Machine",
    #          "Low_Debt_Midcap", "Quality_Compounder", "Near_52W_Low"
    "archetype": "Custom",

    # ── Cache Settings ────────────────────────────────────────────────────────
    "use_cache":                 True,
    "cache_folder":              "cache/",
    "cache_tickers_subfolder":   "cache/tickers/",
    "cache_fundamental_days":    7,
    "cache_price_days":          1,
    "cache_universe_days":       30,
    "cache_force_refresh":       False,
    "cache_show_hit_miss_stats": True,

    # ── Gate 1: Liquidity & Size ──────────────────────────────────────────────
    "min_market_cap_cr":    1000,
    "min_avg_daily_volume": 300000,

    # ── Gate 2: Red Flag Auto-Reject ──────────────────────────────────────────
    "reject_negative_operating_cashflow": True,
    "reject_current_ratio_below":         1.0,
    "reject_shrinking_gross_margins":      True,

    # ── Gate 3: Quality (Non-Financials) ─────────────────────────────────────
    "max_debt_to_equity": 0.5,
    "min_roe_pct":        15,
    "min_roce_pct":       15,

    # ── Gate 4: Growth ────────────────────────────────────────────────────────
    "min_revenue_growth_yoy_pct":  12,
    "min_earnings_growth_yoy_pct": 15,

    # ── Gate 5: Sector-Relative Valuation ─────────────────────────────────────
    "max_pe_vs_sector_median_multiplier": 1.2,
    "max_peg_ratio":                      1.5,

    # ── Gate 6: Financial Sector ──────────────────────────────────────────────
    "max_pb_ratio_financials": 2.5,
    "financial_sectors":       ["Bank", "NBFC", "Insurance",
                                 "Financial Services", "Finance"],

    # ── Scoring Weights ───────────────────────────────────────────────────────
    "weight_growth":        0.30,
    "weight_quality":       0.30,
    "weight_valuation":     0.20,
    "weight_balance_sheet": 0.20,

    # ── Technical Momentum ───────────────────────────────────────────────────
    "use_technical_momentum": True,
    "sma_short":  50,
    "sma_long":   200,

    # ── India 10Y Bond Yield (update manually or via RBI feed) ───────────────
    "india_10y_bond_yield_pct": 7.1,

    # ── Output ────────────────────────────────────────────────────────────────
    "top_n_results": 15,
    "output_csv":    "Top_Stocks_Report.csv",
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
        "min_earnings_growth_yoy_pct":       20,
        "max_pe_vs_sector_median_multiplier": 0.9,
        "max_peg_ratio":                      1.2,
        "min_roe_pct":                        15,
        "max_debt_to_equity":                 0.5,
    },
    "Cash_Machine": {
        "min_roe_pct":                        20,
        "max_debt_to_equity":                 0.1,
        "min_revenue_growth_yoy_pct":         10,
        "min_earnings_growth_yoy_pct":        10,
        "reject_negative_operating_cashflow": True,
    },
    "Low_Debt_Midcap": {
        "universe":                           "Midcap150",
        "max_debt_to_equity":                 0.1,
        "min_roe_pct":                        15,
        "min_earnings_growth_yoy_pct":        12,
        "max_pe_vs_sector_median_multiplier": 1.3,
    },
    "Quality_Compounder": {
        "min_roe_pct":                        20,
        "min_roce_pct":                       20,
        "max_debt_to_equity":                 0.3,
        "min_revenue_growth_yoy_pct":         15,
        "min_earnings_growth_yoy_pct":        15,
        "max_peg_ratio":                      1.5,
    },
    "Near_52W_Low": {
        "min_pct_below_52w_high":             30,
        "min_roe_pct":                        12,
        "max_debt_to_equity":                 0.8,
        "min_earnings_growth_yoy_pct":        10,
    },
}

SECTOR_RISK_MAP = {
    "Bank":                   "RBI policy shifts, rising NPAs, or liquidity tightening.",
    "NBFC":                   "RBI liquidity tightening, rising borrowing costs, or NPA spikes.",
    "Insurance":              "IRDAI regulatory changes, claims inflation, or yield compression.",
    "Financial Services":     "RBI policy shifts, credit cycle downturns, or rising NPAs.",
    "Finance":                "Rising interest rates, RBI changes, or NPA deterioration.",
    "Information Technology": "Global IT spending cuts, USD/INR volatility, or client concentration.",
    "Technology":             "Global tech spending cuts, USD/INR volatility.",
    "Pharmaceuticals":        "USFDA observations, patent expirations, or pricing controls.",
    "Healthcare":             "USFDA regulatory actions, pricing controls, or IP challenges.",
    "Automobile":             "EV disruption, fuel price volatility, or material cost spikes.",
    "Consumer Goods":         "Input cost inflation, rural demand slowdown, or GST changes.",
    "Infrastructure":         "Government capex cuts, land delays, or rising interest rates.",
    "Energy":                 "Crude oil swings, government pricing intervention, or transition risk.",
    "Metals":                 "Global commodity cycle, import duty changes, or demand collapse.",
    "Realty":                 "Interest rate hikes, regulatory changes, or demand cooling.",
    "default":                "Sudden regulatory changes, global macro shocks, or black swan events.",
}


def apply_archetype(config: dict) -> dict:
    """Merges archetype overrides into CONFIG if a non-Custom archetype is selected."""
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

# Internal counters for cache statistics
_cache_stats = {
    "fundamental_hits":  0,
    "fundamental_miss":  0,
    "price_hits":        0,
    "price_miss":        0,
    "master_hit":        False,
    "master_load_secs":  0.0,
}


def _ensure_cache_dirs():
    """Creates cache folders if they do not exist."""
    os.makedirs(CONFIG["cache_folder"], exist_ok=True)
    os.makedirs(CONFIG["cache_tickers_subfolder"], exist_ok=True)


def _ticker_cache_path(ticker: str, data_type: str) -> str:
    """Returns the file path for a ticker's cache JSON file."""
    safe = ticker.replace(".", "_")
    return os.path.join(CONFIG["cache_tickers_subfolder"], f"{safe}_{data_type}.json")


def save_ticker_cache(ticker: str, data: dict, data_type: str):
    """
    Saves a ticker's fetched data dict as a JSON file with a fetched_at timestamp.
    data_type: 'fundamental' or 'price'.
    """
    if not CONFIG.get("use_cache"):
        return
    _ensure_cache_dirs()
    payload = dict(data)
    payload["fetched_at"] = datetime.now().isoformat()
    try:
        with open(_ticker_cache_path(ticker, data_type), "w") as f:
            json.dump(payload, f, default=str)
    except Exception as e:
        print(f"[CACHE] Could not save {ticker} {data_type} cache: {e}")


def load_ticker_cache(ticker: str, data_type: str) -> dict | None:
    """
    Loads a ticker's cached data if it exists and is within the freshness window.
    Returns None (CACHE MISS) if stale, corrupt, or missing.

    Freshness:
      - 'fundamental' → CONFIG['cache_fundamental_days']
      - 'price'       → CONFIG['cache_price_days']
    """
    if not CONFIG.get("use_cache"):
        return None

    path = _ticker_cache_path(ticker, data_type)
    if not os.path.exists(path):
        return None

    try:
        with open(path, "r") as f:
            cached = json.load(f)

        fetched_at = datetime.fromisoformat(cached.get("fetched_at", "2000-01-01"))
        age_days   = (datetime.now() - fetched_at).days
        expiry     = CONFIG["cache_fundamental_days"] if data_type == "fundamental" \
                     else CONFIG["cache_price_days"]

        if age_days >= expiry:
            return None  # Stale

        # ── Freshness Validation ────────────────────────────────────────────
        # Reject if all critical fundamental fields are None simultaneously
        if data_type == "fundamental":
            critical = ["pe_ratio", "roe", "market_cap_cr"]
            if all(cached.get(k) is None for k in critical):
                return None  # Previously failed silent fetch

        # Reject if price data has zero or missing price
        if data_type == "price":
            if not cached.get("current_price") or cached.get("current_price", 0) <= 0:
                return None
            if cached.get("52w_high") is None:
                return None

        return cached  # ✅ CACHE HIT

    except Exception:
        return None  # Corrupt file


def save_master_cache(df: pd.DataFrame):
    """
    Saves the master DataFrame as a pickle file along with metadata JSON.
    Metadata includes universe, archetype, creation timestamp, and config params.
    """
    if not CONFIG.get("use_cache"):
        return
    _ensure_cache_dirs()
    pkl_path  = os.path.join(CONFIG["cache_folder"], "master_cache.pkl")
    meta_path = os.path.join(CONFIG["cache_folder"], "cache_metadata.json")
    try:
        df.to_pickle(pkl_path)
        meta = {
            "created_at":              datetime.now().isoformat(),
            "universe":                CONFIG["universe"],
            "archetype":               CONFIG.get("archetype", "Custom"),
            "total_tickers":           len(df),
            "cache_fundamental_days":  CONFIG["cache_fundamental_days"],
            "cache_price_days":        CONFIG["cache_price_days"],
        }
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
        print(f"[CACHE] Master cache saved → {pkl_path} ({len(df)} rows)")
    except Exception as e:
        print(f"[CACHE] Could not save master cache: {e}")


def load_master_cache() -> pd.DataFrame | None:
    """
    Loads the master DataFrame from pickle if:
      1. Both .pkl and metadata.json exist.
      2. Universe matches CONFIG['universe'].
      3. Archetype matches CONFIG['archetype'].
      4. Cache age is within cache_fundamental_days.
    Returns None (CACHE MISS) if any check fails.
    """
    if not CONFIG.get("use_cache"):
        return None

    pkl_path  = os.path.join(CONFIG["cache_folder"], "master_cache.pkl")
    meta_path = os.path.join(CONFIG["cache_folder"], "cache_metadata.json")

    if not os.path.exists(pkl_path) or not os.path.exists(meta_path):
        return None

    try:
        with open(meta_path, "r") as f:
            meta = json.load(f)

        # Check universe match
        if meta.get("universe") != CONFIG["universe"]:
            print(f"[CACHE] Universe changed ({meta.get('universe')} → {CONFIG['universe']}). Cache invalidated.")
            return None

        # Check archetype match
        if meta.get("archetype") != CONFIG.get("archetype", "Custom"):
            print(f"[CACHE] Archetype changed. Cache invalidated.")
            return None

        # Check age
        created_at = datetime.fromisoformat(meta.get("created_at", "2000-01-01"))
        age_days   = (datetime.now() - created_at).days
        if age_days >= CONFIG["cache_fundamental_days"]:
            print(f"[CACHE] Master cache is {age_days} days old (limit: {CONFIG['cache_fundamental_days']}). Refreshing.")
            return None

        # Load pickle
        t0 = time.time()
        df = pd.read_pickle(pkl_path)
        elapsed = round(time.time() - t0, 2)

        _cache_stats["master_hit"]       = True
        _cache_stats["master_load_secs"] = elapsed

        print(f"[CACHE] ✅ Master cache HIT — loaded {len(df)} rows in {elapsed}s (age: {age_days} day(s))")
        return df

    except Exception as e:
        print(f"[CACHE] Master cache load failed: {e}")
        return None


def force_wipe_cache():
    """Deletes all cache files when CONFIG['cache_force_refresh'] is True."""
    folder = CONFIG["cache_folder"]
    if os.path.exists(folder):
        shutil.rmtree(folder)
        print(f"[CACHE] 🗑  All cache files wiped from '{folder}'.")
    _ensure_cache_dirs()


def print_cache_stats(total_tickers: int):
    """Prints a formatted cache statistics summary after data loading."""
    if not CONFIG.get("cache_show_hit_miss_stats"):
        return

    s = _cache_stats
    fund_total  = s["fundamental_hits"] + s["fundamental_miss"]
    price_total = s["price_hits"] + s["price_miss"]

    # Rough time estimate: each yfinance call ≈ 4 seconds with sleep
    missed   = s["fundamental_miss"]
    saved_s  = (total_tickers - missed) * 4
    saved_m  = round(saved_s / 60, 1)

    print(f"\n{'='*62}")
    print(f"  CACHE STATISTICS")
    if s["master_hit"]:
        print(f"  Master Cache       : ✅ HIT  (loaded in {s['master_load_secs']}s)")
    else:
        print(f"  Master Cache       : ❌ MISS (fresh fetch performed)")
    if fund_total > 0:
        print(f"  Fundamental Cache  : {s['fundamental_hits']} hits / {fund_total} total")
        print(f"  Price Cache        : {s['price_hits']} hits / {max(price_total,1)} total")
        print(f"  Time Saved         : ~{saved_m} minutes estimated")
    print(f"{'='*62}\n")


# =============================================================================
# SECTION 3: UNIVERSE LOADER
# =============================================================================

def fetch_csv_from_url(url: str) -> pd.DataFrame:
    """Fetches a CSV with browser-like headers and SSL verification disabled."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    }
    r = requests.get(url, headers=headers, timeout=20, verify=False)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))


def get_universe_tickers(universe_name: str):
    """
    Loads the NSE ticker list and sector map for the requested universe.
    Checks a local universe cache first (30-day expiry), then fetches
    from niftyindices.com, then falls back to a local CSV.

    Returns:
        tickers (list): ['TCS.NS', 'RELIANCE.NS', ...]
        sector_map (dict): {'TCS.NS': 'Information Technology', ...}
    """
    print(f"\n[INFO] Loading universe: {universe_name}")
    _ensure_cache_dirs()

    # Universe-level cache
    uni_cache_path = os.path.join(CONFIG["cache_folder"], f"universe_{universe_name}.csv")
    uni_meta_path  = os.path.join(CONFIG["cache_folder"], f"universe_{universe_name}_meta.json")
    df = None

    if CONFIG.get("use_cache") and os.path.exists(uni_cache_path) and os.path.exists(uni_meta_path):
        try:
            with open(uni_meta_path) as f:
                uni_meta = json.load(f)
            age = (datetime.now() - datetime.fromisoformat(uni_meta["fetched_at"])).days
            if age < CONFIG["cache_universe_days"]:
                df = pd.read_csv(uni_cache_path)
                print(f"[CACHE] Universe cache HIT — {len(df)} stocks (age: {age} day(s))")
        except Exception:
            df = None

    if df is None:
        if universe_name in NSE_URLS:
            try:
                df = fetch_csv_from_url(NSE_URLS[universe_name])
                print(f"[INFO] Fetched {len(df)} stocks from niftyindices.com")
                # Save universe cache
                df.to_csv(uni_cache_path, index=False)
                with open(uni_meta_path, "w") as f:
                    json.dump({"fetched_at": datetime.now().isoformat()}, f)
            except Exception as e:
                print(f"[WARNING] Network fetch failed: {e}. Trying local fallback...")

        elif universe_name == "Top800_Custom":
            try:
                frames = []
                for key in ["Nifty500", "Midcap150", "Smallcap250"]:
                    frames.append(fetch_csv_from_url(NSE_URLS[key]))
                df = pd.concat(frames).drop_duplicates(subset=["Symbol"]).head(800)
                print(f"[INFO] Top800_Custom: {len(df)} stocks")
            except Exception as e:
                print(f"[WARNING] Top800_Custom fetch failed: {e}")

    if df is None:
        local = f"{universe_name}.csv"
        if os.path.exists(local):
            df = pd.read_csv(local)
            print(f"[INFO] Loaded from local fallback: {local}")
        else:
            raise FileNotFoundError(
                f"[ERROR] No data for '{universe_name}'. "
                f"Download CSV from niftyindices.com and save as '{universe_name}.csv'."
            )

    df.columns = [c.strip() for c in df.columns]
    sym_col = next((c for c in df.columns if "symbol"   in c.lower()), None)
    ind_col = next((c for c in df.columns if any(x in c.lower()
                    for x in ["industry", "sector"])), None)

    if not sym_col:
        raise ValueError(f"[ERROR] Symbol column missing. Found: {df.columns.tolist()}")

    tickers    = [str(s).strip().upper() + ".NS" for s in df[sym_col].dropna()]
    sector_map = {}
    if ind_col:
        for _, row in df.iterrows():
            t = str(row[sym_col]).strip().upper() + ".NS"
            sector_map[t] = str(row[ind_col]).strip() if pd.notna(row[ind_col]) else "Unknown"
    else:
        sector_map = {t: "Unknown" for t in tickers}

    print(f"[INFO] Universe loaded: {len(tickers)} tickers")
    return tickers, sector_map


# =============================================================================
# SECTION 4: DATA FETCHING ENGINE (WITH CACHE)
# =============================================================================

def fetch_price_and_technical_data(tickers: list) -> pd.DataFrame:
    """
    Downloads 1-year daily OHLCV for all tickers in one batch call.
    Per-ticker price records are loaded from cache where fresh.
    Computes: current_price, 52w_high, 52w_low, avg_daily_volume,
              pct_below_52w_high, sma_50, sma_200, technical_trend.
    """
    print(f"\n[INFO] Fetching price/technical data for {len(tickers)} tickers...")

    # Identify which tickers need fresh price data
    to_fetch = []
    cached_price_records = {}

    for ticker in tickers:
        cached = load_ticker_cache(ticker, "price")
        if cached:
            _cache_stats["price_hits"] += 1
            cached_price_records[ticker] = cached
        else:
            _cache_stats["price_miss"] += 1
            to_fetch.append(ticker)

    # Batch fetch only the tickers not in cache
    batch_data = {}
    if to_fetch:
        try:
            raw = yf.download(
                to_fetch,
                period="1y",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            batch_data = raw
        except Exception as e:
            print(f"[ERROR] Batch price download failed: {e}")

    sma_s = CONFIG.get("sma_short",  50)
    sma_l = CONFIG.get("sma_long",  200)
    records = []

    for ticker in tickers:
        # Use cached if available
        if ticker in cached_price_records:
            r = cached_price_records[ticker]
            records.append({k: v for k, v in r.items() if k != "fetched_at"})
            continue

        # Parse from fresh batch download
        try:
            df_t = batch_data[ticker] if len(to_fetch) > 1 else batch_data
            if df_t.empty or "Close" not in df_t.columns:
                raise ValueError("No data")

            close  = df_t["Close"].dropna()
            volume = df_t["Volume"].dropna()

            cur    = round(float(close.iloc[-1]), 2)
            hi52   = round(float(close.max()), 2)
            lo52   = round(float(close.min()), 2)
            vol30  = int(volume.tail(30).mean()) if len(volume) >= 5 else 0
            below  = round((hi52 - cur) / hi52 * 100, 2) if hi52 > 0 else None
            sma50  = round(float(close.tail(sma_s).mean()), 2) if len(close) >= sma_s else None
            sma200 = round(float(close.tail(sma_l).mean()), 2) if len(close) >= sma_l else None

            if sma50 and sma200:
                if cur > sma50 > sma200:     trend = "Uptrend ✅"
                elif cur < sma50 < sma200:   trend = "Downtrend ❌"
                else:                        trend = "Sideways ➡️"
            else:
                trend = "N/A"

            rec = {
                "ticker":             ticker,
                "current_price":      cur,
                "52w_high":           hi52,
                "52w_low":            lo52,
                "avg_daily_volume":   vol30,
                "pct_below_52w_high": below,
                "sma_50":             sma50,
                "sma_200":            sma200,
                "technical_trend":    trend,
            }
            save_ticker_cache(ticker, rec, "price")
            records.append(rec)

        except Exception:
            records.append({
                "ticker": ticker, "current_price": None, "52w_high": None,
                "52w_low": None, "avg_daily_volume": None,
                "pct_below_52w_high": None, "sma_50": None,
                "sma_200": None, "technical_trend": "N/A",
            })

    return pd.DataFrame(records)


def fetch_fundamentals(ticker: str) -> dict:
    """
    Fetches fundamental data for a single ticker from yfinance.
    Checks the per-ticker JSON cache first. If fresh, returns cached data.
    If stale or missing, fetches from yfinance and saves to cache.

    Fields: pe_ratio, pb_ratio, peg_ratio, roe, debt_to_equity, market_cap_cr,
            revenue_growth_yoy, earnings_growth_yoy, operating_cash_flow,
            capital_expenditure, free_cash_flow, current_ratio,
            gross_margin_current, gross_margin_prev, net_income, sector_yf
    """
    # ── Layer 1 Cache Check ──────────────────────────────────────────────────
    cached = load_ticker_cache(ticker, "fundamental")
    if cached:
        _cache_stats["fundamental_hits"] += 1
        return cached
    _cache_stats["fundamental_miss"] += 1

    # ── Live Fetch ───────────────────────────────────────────────────────────
    result = {
        "ticker": ticker, "pe_ratio": None, "pb_ratio": None, "peg_ratio": None,
        "roe": None, "debt_to_equity": None, "market_cap_cr": None,
        "revenue_growth_yoy": None, "earnings_growth_yoy": None,
        "operating_cash_flow": None, "capital_expenditure": None,
        "free_cash_flow": None, "current_ratio": None,
        "gross_margin_current": None, "gross_margin_prev": None,
        "net_income": None, "sector_yf": None,
    }

    try:
        stock = yf.Ticker(ticker)
        info  = stock.info or {}

        def sg(key, scale=1.0):
            v = info.get(key)
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                return round(float(v) * scale, 4)
            return None

        result["pe_ratio"]            = sg("trailingPE")
        result["pb_ratio"]            = sg("priceToBook")
        result["peg_ratio"]           = sg("pegRatio")
        result["roe"]                 = sg("returnOnEquity", 100)
        result["debt_to_equity"]      = sg("debtToEquity")
        result["market_cap_cr"]       = sg("marketCap", 1 / 1e7)
        result["revenue_growth_yoy"]  = sg("revenueGrowth", 100)
        result["earnings_growth_yoy"] = sg("earningsGrowth", 100)
        result["operating_cash_flow"] = sg("operatingCashflow")
        result["current_ratio"]       = sg("currentRatio")
        result["gross_margin_current"] = sg("grossMargins", 100)
        result["sector_yf"]           = info.get("sector")

        try:
            cf = stock.cashflow
            if cf is not None and not cf.empty:
                capex_row = next((r for r in cf.index if "capital" in r.lower()), None)
                if capex_row:
                    result["capital_expenditure"] = float(cf.loc[capex_row].iloc[0])
        except Exception:
            pass

        try:
            fin = stock.financials
            if fin is not None and not fin.empty:
                ni_row  = next((r for r in fin.index if "net income"    in r.lower()), None)
                gp_row  = next((r for r in fin.index if "gross profit"  in r.lower()), None)
                rev_row = next((r for r in fin.index if "total revenue" in r.lower()), None)

                if ni_row:
                    result["net_income"] = float(fin.loc[ni_row].iloc[0])

                if gp_row and rev_row and fin.shape[1] >= 2:
                    gp_p  = float(fin.loc[gp_row].iloc[1])
                    rev_p = float(fin.loc[rev_row].iloc[1])
                    if rev_p != 0:
                        result["gross_margin_prev"] = round((gp_p / rev_p) * 100, 4)
        except Exception:
            pass

        if result["operating_cash_flow"] and result["capital_expenditure"]:
            result["free_cash_flow"] = (
                result["operating_cash_flow"] - abs(result["capital_expenditure"])
            )

    except Exception:
        pass

    # ── Save to Cache ────────────────────────────────────────────────────────
    save_ticker_cache(ticker, result, "fundamental")
    return result


def build_master_dataframe(tickers: list, sector_map: dict) -> pd.DataFrame:
    """
    Combines batch price/technical data with individually fetched fundamentals.
    Uses per-ticker cache to skip already-fetched tickers on repeat runs.
    Shows a real-time progress counter with cache hit/miss indicators.
    """
    price_df = fetch_price_and_technical_data(tickers)

    print(f"\n[INFO] Fetching fundamentals ({len(tickers)} stocks, 0.5s delay each)...")
    fund_records = []
    total = len(tickers)

    for i, ticker in enumerate(tickers, start=1):
        cached_check = os.path.exists(_ticker_cache_path(ticker, "fundamental"))
        label = "📦 cache" if cached_check else "🌐 fetch"
        print(f"  [{i:>4}/{total}] {ticker:<20} {label}    ", end="\r")
        try:
            fund_records.append(fetch_fundamentals(ticker))
        except Exception:
            fund_records.append({"ticker": ticker})
        if not cached_check:
            time.sleep(0.5)  # Only delay on live fetches

    print(f"\n[INFO] Fundamentals complete.")

    fund_df   = pd.DataFrame(fund_records)
    master_df = pd.merge(price_df, fund_df, on="ticker", how="left")

    master_df["sector"] = master_df["ticker"].map(sector_map)
    master_df["sector"] = master_df.apply(
        lambda r: r.get("sector_yf")
        if (pd.isna(r.get("sector")) or r.get("sector") == "Unknown")
        and r.get("sector_yf") else r.get("sector"),
        axis=1,
    )
    master_df.drop(columns=["sector_yf"], inplace=True, errors="ignore")

    print(f"[INFO] Master DataFrame ready: {len(master_df)} rows.")
    return master_df


# =============================================================================
# SECTION 5: SECTOR CLASSIFICATION
# =============================================================================

def classify_sector(sector_name) -> str:
    """Returns 'financial' or 'non_financial' based on sector name."""
    if not sector_name or pd.isna(sector_name):
        return "non_financial"
    s = str(sector_name).lower()
    for fs in CONFIG["financial_sectors"]:
        if fs.lower() in s:
            return "financial"
    return "non_financial"


def add_sector_medians(df: pd.DataFrame) -> pd.DataFrame:
    """Adds sector_median_pe and sector_median_roe columns for relative valuation."""
    df["sector_median_pe"]  = df.groupby("sector")["pe_ratio"].transform("median")
    df["sector_median_roe"] = df.groupby("sector")["roe"].transform("median")
    return df


# =============================================================================
# SECTION 6: FILTERING ENGINE (6 GATES)
# =============================================================================

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies 6 sequential filtering gates.
    Prints survival counts after each gate.
    """
    print(f"\n{'='*62}")
    print(f"  FILTER ENGINE  |  Starting with {len(df)} stocks")
    print(f"{'='*62}")

    w = df.copy()
    w["sector_type"] = w["sector"].apply(classify_sector)
    w = add_sector_medians(w)

    # Gate 1: Liquidity & Size
    before = len(w)
    w = w[w["market_cap_cr"].notna() & w["avg_daily_volume"].notna()]
    w = w[
        (w["market_cap_cr"]    >= CONFIG["min_market_cap_cr"]) &
        (w["avg_daily_volume"] >= CONFIG["min_avg_daily_volume"])
    ]
    print(f"  Gate 1 [Liquidity & Size]         : {before:>4} → {len(w):>4}  (-{before-len(w)})")

    # Gate 2: Red Flag Auto-Reject
    before = len(w)
    rf = pd.Series(False, index=w.index)
    if CONFIG.get("reject_negative_operating_cashflow"):
        rf |= w["operating_cash_flow"].notna() & (w["operating_cash_flow"] < 0)
    if CONFIG.get("reject_current_ratio_below"):
        rf |= w["current_ratio"].notna() & (w["current_ratio"] < CONFIG["reject_current_ratio_below"])
    if CONFIG.get("reject_shrinking_gross_margins"):
        rf |= (
            w["gross_margin_current"].notna() &
            w["gross_margin_prev"].notna() &
            (w["gross_margin_current"] < w["gross_margin_prev"])
        )
    w = w[~rf]
    print(f"  Gate 2 [Red Flag Auto-Reject]     : {before:>4} → {len(w):>4}  (-{before-len(w)})")

    # Gate 3: Quality (Non-Financials)
    before = len(w)
    nf  = w["sector_type"] == "non_financial"
    bad = nf & (
        (w["debt_to_equity"].notna() & (w["debt_to_equity"] > CONFIG["max_debt_to_equity"])) |
        (w["roe"].notna()            & (w["roe"]            < CONFIG["min_roe_pct"]))
    )
    w = w[~bad]
    print(f"  Gate 3 [Quality — Non-Financials] : {before:>4} → {len(w):>4}  (-{before-len(w)})")

    # Gate 4: Growth
    before = len(w)
    bg = (
        (w["revenue_growth_yoy"].notna()  & (w["revenue_growth_yoy"]  < CONFIG["min_revenue_growth_yoy_pct"])) |
        (w["earnings_growth_yoy"].notna() & (w["earnings_growth_yoy"] < CONFIG["min_earnings_growth_yoy_pct"]))
    )
    w = w[~bg]
    print(f"  Gate 4 [Growth]                   : {before:>4} → {len(w):>4}  (-{before-len(w)})")

    # Gate 5: Sector-Relative Valuation (Non-Financials)
    before = len(w)
    nf  = w["sector_type"] == "non_financial"
    mul = CONFIG["max_pe_vs_sector_median_multiplier"]
    bv  = nf & (
        w["pe_ratio"].notna() & w["sector_median_pe"].notna() &
        (w["pe_ratio"] > w["sector_median_pe"] * mul)
    )
    bp = nf & (w["peg_ratio"].notna() & (w["peg_ratio"] > CONFIG["max_peg_ratio"]))
    w  = w[~(bv | bp)]
    print(f"  Gate 5 [Sector-Relative Valuation]: {before:>4} → {len(w):>4}  (-{before-len(w)})")

    # Gate 6: Financial Sector P/B Rule
    before = len(w)
    fin = w["sector_type"] == "financial"
    bf  = fin & (w["pb_ratio"].notna() & (w["pb_ratio"] > CONFIG["max_pb_ratio_financials"]))
    w   = w[~bf]
    print(f"  Gate 6 [Financials — P/B Rule]    : {before:>4} → {len(w):>4}  (-{before-len(w)})")

    # Optional Near_52W_Low gate
    if CONFIG.get("archetype") == "Near_52W_Low" and CONFIG.get("min_pct_below_52w_high"):
        before = len(w)
        w = w[w["pct_below_52w_high"].notna() & (w["pct_below_52w_high"] >= CONFIG["min_pct_below_52w_high"])]
        print(f"  Gate 7 [Near 52W Low]             : {before:>4} → {len(w):>4}  (-{before-len(w)})")

    print(f"{'='*62}")
    print(f"  ✅ Stocks surviving all filters: {len(w)}")
    print(f"{'='*62}\n")
    return w


# =============================================================================
# SECTION 7: 1-10 FUNDAMENTAL SCORE
# =============================================================================

def calculate_fundamental_score(row: pd.Series, sector_median_roe: float) -> int:
    """
    Awards 1 point per quality test passed (max 10, base 1).
    Tests: ROE vs sector, earnings growth, revenue growth, OCF quality,
           debt safety, current ratio, PEG, FCF yield.
    """
    score = 1

    def ok(v):
        return v is not None and not (isinstance(v, float) and np.isnan(v))

    roe   = row.get("roe");           earn_g = row.get("earnings_growth_yoy")
    rev_g = row.get("revenue_growth_yoy")
    ocf   = row.get("operating_cash_flow"); ni = row.get("net_income")
    de    = row.get("debt_to_equity"); pb  = row.get("pb_ratio")
    cr    = row.get("current_ratio"); peg = row.get("peg_ratio")
    fcf   = row.get("free_cash_flow"); mkt = row.get("market_cap_cr")
    stype = row.get("sector_type", "non_financial")

    if ok(roe) and ok(sector_median_roe) and roe > sector_median_roe: score += 1
    if ok(earn_g) and earn_g > 10:                                     score += 1
    if ok(rev_g)  and rev_g  > 10:                                     score += 1
    if ok(ocf) and ok(ni) and ni > 0 and ocf > ni:                    score += 1

    if stype == "non_financial":
        if ok(de) and de < 0.5:                                        score += 1
    else:
        if ok(pb) and pb < 2.5:                                        score += 1

    if ok(cr)  and cr  > 1.2:                                         score += 1
    if ok(peg) and peg < 1.2:                                         score += 1

    if ok(fcf) and ok(mkt) and mkt > 0:
        if (fcf / (mkt * 1e7)) * 100 > 4:                             score += 1

    return min(score, 10)


# =============================================================================
# SECTION 8: SCORING & RANKING
# =============================================================================

def mnorm(series: pd.Series) -> pd.Series:
    """Min-max normalization to [0, 1]."""
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series([0.5] * len(series), index=series.index)
    return (series - lo) / (hi - lo)


def calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes two scores per stock:
      - fundamental_score (1-10): Tickertape Pro-style binary tests.
      - composite_score (0-100): Weighted sub-score formula.
    Sorts descending by composite_score and returns top N.
    """
    if df.empty:
        print("[WARNING] Nothing to score. Relax CONFIG thresholds.")
        return df

    s = df.copy()

    for col in ["earnings_growth_yoy", "roe", "debt_to_equity",
                "pe_ratio", "peg_ratio", "operating_cash_flow"]:
        if col in s.columns:
            s[col] = s[col].fillna(s[col].median())

    s["fundamental_score"] = s.apply(
        lambda r: calculate_fundamental_score(r, r.get("sector_median_roe", 15)), axis=1
    )

    bond = CONFIG.get("india_10y_bond_yield_pct", 7.1)
    s["earnings_yield_pct"] = s["pe_ratio"].apply(
        lambda pe: round(100 / pe, 2) if pe and pe > 0 else None
    )
    s["beats_bond_yield"] = s["earnings_yield_pct"].apply(
        lambda ey: ey is not None and ey > bond
    )

    s["growth_score"]        = mnorm(s["earnings_growth_yoy"])
    s["quality_score"]       = (mnorm(s["roe"]) + (1 - mnorm(s["debt_to_equity"]))) / 2
    s["valuation_score"]     = ((1 - mnorm(s["pe_ratio"])) + (1 - mnorm(s["peg_ratio"]))) / 2
    s["balance_sheet_score"] = mnorm(s["operating_cash_flow"])

    s["composite_score"] = round(
        (
            s["growth_score"]        * CONFIG["weight_growth"]        +
            s["quality_score"]       * CONFIG["weight_quality"]       +
            s["valuation_score"]     * CONFIG["weight_valuation"]     +
            s["balance_sheet_score"] * CONFIG["weight_balance_sheet"]
        ) * 100, 2
    )

    s.sort_values("composite_score", ascending=False, inplace=True)
    top = s.head(CONFIG["top_n_results"]).reset_index(drop=True)
    print(f"[INFO] Scoring complete. Top {len(top)} stocks selected.\n")
    return top


# =============================================================================
# SECTION 9: VERDICT GENERATOR
# =============================================================================

def get_sector_risk(sector) -> str:
    """Returns the risk text for a given sector from SECTOR_RISK_MAP."""
    if not sector or pd.isna(sector):
        return SECTOR_RISK_MAP["default"]
    for key in SECTOR_RISK_MAP:
        if key.lower() in str(sector).lower():
            return SECTOR_RISK_MAP[key]
    return SECTOR_RISK_MAP["default"]


def fv(val, suffix="", prefix="", d=1):
    """Safe formatter for display output."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    return f"{prefix}{round(val, d)}{suffix}"


def generate_verdict(row: pd.Series, rank: int):
    """
    Prints a fully formatted CLI verdict box for one stock, including:
    price, 52W range, trend, fundamentals, scores, earnings yield,
    FCF yield, plain-English verdict, and sector risk disclaimer.
    """
    ticker     = row.get("ticker", "N/A")
    sector     = row.get("sector", "Unknown")
    comp       = row.get("composite_score", 0)
    fscore     = row.get("fundamental_score", 0)
    price      = row.get("current_price")
    pe         = row.get("pe_ratio");    pb  = row.get("pb_ratio")
    peg        = row.get("peg_ratio");   roe = row.get("roe")
    de         = row.get("debt_to_equity")
    rev_g      = row.get("revenue_growth_yoy")
    earn_g     = row.get("earnings_growth_yoy")
    ocf        = row.get("operating_cash_flow")
    fcf        = row.get("free_cash_flow")
    mkt        = row.get("market_cap_cr")
    below      = row.get("pct_below_52w_high")
    hi52       = row.get("52w_high");    lo52 = row.get("52w_low")
    sma50      = row.get("sma_50");      sma200 = row.get("sma_200")
    trend      = row.get("technical_trend", "N/A")
    ey         = row.get("earnings_yield_pct")
    beats_bond = row.get("beats_bond_yield", False)
    sect_pe    = row.get("sector_median_pe")
    bond       = CONFIG.get("india_10y_bond_yield_pct", 7.1)

    fcf_yield_str = "N/A"
    if fcf and mkt and mkt > 0:
        fcf_yield_str = f"{round((fcf / (mkt * 1e7)) * 100, 1)}%"

    parts = []
    if earn_g and earn_g > 0:   parts.append(f"earnings growth of {fv(earn_g, '%')}")
    if roe and roe > 0:          parts.append(f"ROE of {fv(roe, '%')}")
    if de is not None and de < 0.5: parts.append(f"low leverage (D/E: {fv(de)})")
    if peg and peg < 1.2:        parts.append(f"very attractive valuation (PEG: {fv(peg)})")
    if beats_bond:               parts.append(f"earnings yield ({fv(ey, '%')}) > bond ({bond}%)")
    verdict_str = ", ".join(parts) if parts else "balanced fundamentals"

    badge = "🟢 STRONG" if fscore >= 8 else ("🟡 MODERATE" if fscore >= 6 else "🔴 WEAK")

    print("=" * 68)
    print(f"  #{rank:<3} {ticker:<22}  |  {sector}")
    print(f"  Composite: {comp:.2f}/100   Fundamental: {fscore}/10  {badge}")
    print(f"  Price: ₹{fv(price, d=2)}   52W: ₹{fv(lo52, d=2)} – ₹{fv(hi52, d=2)}   Below High: {fv(below, '%')}")
    print(f"  Trend: {trend}   SMA50: ₹{fv(sma50, d=1)}   SMA200: ₹{fv(sma200, d=1)}")
    print("-" * 68)
    print(f"  VALUATION")
    print(f"    P/E: {fv(pe):<10}  Sector Median P/E: {fv(sect_pe)}")
    print(f"    P/B: {fv(pb):<10}  PEG: {fv(peg)}")
    print(f"    Earnings Yield: {fv(ey, '%'):<8}  Bond Yield: {bond}%  "
          f"→ {'✅ Beats Bond' if beats_bond else '❌ Below Bond'}")
    print(f"    FCF Yield: {fcf_yield_str}")
    print("-" * 68)
    print(f"  FUNDAMENTALS")
    print(f"    ROE: {fv(roe, '%'):<12}  D/E: {fv(de)}")
    print(f"    Revenue Growth: {fv(rev_g, '%'):<8}  Earnings Growth: {fv(earn_g, '%')}")
    print(f"    Operating Cash Flow: ₹{fv(ocf/1e7 if ocf else None, ' Cr', d=0)}")
    print("-" * 68)
    print(f"  ✅  VERDICT")
    print(f"    {ticker} shows {verdict_str}.")
    print(f"    Fundamental score {fscore}/10. Strong potential to compound")
    print(f"    wealth over the medium to long term under normal conditions.")
    print("-" * 68)
    print(f"  ⚠️   DISCLAIMER")
    print(f"    Sector risk ({sector}): {get_sector_risk(sector)}")
    print(f"    THIS IS NOT FINANCIAL ADVICE. Do your own due diligence.")
    print("=" * 68 + "\n")


# =============================================================================
# SECTION 10: MAIN EXECUTION
# =============================================================================

def main():
    """
    Full pipeline:
    Cache Check → Universe → Fetch → Filter → Score → Report → Save
    """
    global CONFIG
    CONFIG = apply_archetype(CONFIG)

    print("\n" + "=" * 68)
    print(f"  NSE STOCK SCREENER v3.0  |  Universe: {CONFIG['universe']}")
    arch = CONFIG.get("archetype", "Custom")
    if arch != "Custom":
        print(f"  Archetype : {arch}")
    print(f"  Run Date  : {datetime.now().strftime('%d %B %Y | %I:%M %p')}")
    print(f"  Cache     : {'ENABLED' if CONFIG['use_cache'] else 'DISABLED'}")
    print("=" * 68)

    # ── Force Refresh ────────────────────────────────────────────────────────
    if CONFIG.get("cache_force_refresh"):
        force_wipe_cache()

    # ── Layer 2 Cache: Try loading master pickle ──────────────────────────────
    master_df = load_master_cache()

    if master_df is None:
        # Full fetch pipeline
        tickers, sector_map = get_universe_tickers(CONFIG["universe"])
        master_df = build_master_dataframe(tickers, sector_map)
        master_df.to_csv("raw_data.csv", index=False)
        save_master_cache(master_df)
        print(f"[INFO] Raw data saved → raw_data.csv ({len(master_df)} rows)")
    else:
        print(f"[INFO] Skipped full fetch. Using cached master DataFrame.")

    # ── Cache Statistics ──────────────────────────────────────────────────────
    total = len(master_df)
    print_cache_stats(total)

    # ── Filter → Score → Report ───────────────────────────────────────────────
    filtered_df = apply_filters(master_df)

    if filtered_df.empty:
        print("[WARNING] No stocks passed all filters. Relax thresholds in CONFIG.")
        return

    top_df = calculate_scores(filtered_df)
    top_df.to_csv(CONFIG["output_csv"], index=False)

    print("\n" + "=" * 68)
    print(f"  TOP {len(top_df)} RECOMMENDATIONS  |  Archetype: {arch}")
    print("=" * 68 + "\n")

    for i, (_, row) in enumerate(top_df.iterrows(), start=1):
        try:
            generate_verdict(row, rank=i)
        except Exception as e:
            print(f"[WARNING] Verdict error for {row.get('ticker', '?')}: {e}")

    print("=" * 68)
    print(f"  ✅  Complete.")
    print(f"  📁  Report   → {CONFIG['output_csv']}")
    print(f"  📁  Raw Data → raw_data.csv")
    print(f"  📦  Cache    → {CONFIG['cache_folder']}")
    print("=" * 68 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user.")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        raise
