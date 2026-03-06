# =============================================================================
# nse_screener.py  |  v2.0  |  Tickertape Pro-Level NSE Stock Screener
# =============================================================================
# UPGRADES over v1.0:
#   - Sector-relative valuation (PE vs Sector Median, not fixed cutoff)
#   - 1-10 Fundamental Score (9 binary quality tests)
#   - Red Flag Auto-Reject Gate (negative cash flow, current ratio, margins)
#   - Technical Momentum (50/200 SMA trend detection)
#   - Screen Archetypes (Growth_Bargain, Cash_Machine, Low_Debt_Midcap, etc.)
#   - Free Cash Flow Yield & Earnings Yield vs Bond Yield
#   - SSL fix for niftyindices.com fetching
# =============================================================================

import pandas as pd
import numpy as np
import yfinance as yf
import requests
import urllib3
import io
import time
import os
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============================================================================
# SECTION 1: CONFIG BLOCK
# =============================================================================

CONFIG = {
    # -------------------------------------------------------------------------
    # Universe: "Nifty50", "Nifty100", "Nifty500", "Midcap150",
    #           "Smallcap250", "Top800_Custom"
    # -------------------------------------------------------------------------
    "universe": "Nifty500",

    # -------------------------------------------------------------------------
    # Screen Archetype: Overrides individual gate thresholds for convenience.
    # Options: "Custom", "Growth_Bargain", "Cash_Machine",
    #          "Low_Debt_Midcap", "Quality_Compounder", "Near_52W_Low"
    # Set to "Custom" to use individual thresholds below.
    # -------------------------------------------------------------------------
    "archetype": "Custom",

    # -------------------------------------------------------------------------
    # Gate 1: Liquidity & Size
    # -------------------------------------------------------------------------
    "min_market_cap_cr":    1000,
    "min_avg_daily_volume": 300000,

    # -------------------------------------------------------------------------
    # Gate 2: Red Flag Auto-Reject (applied BEFORE quality scoring)
    # -------------------------------------------------------------------------
    "reject_negative_operating_cashflow": True,
    "reject_current_ratio_below":         1.0,
    "reject_shrinking_gross_margins":      True,

    # -------------------------------------------------------------------------
    # Gate 3: Quality (Non-Financials)
    # -------------------------------------------------------------------------
    "max_debt_to_equity": 0.5,
    "min_roe_pct":        15,
    "min_roce_pct":       15,

    # -------------------------------------------------------------------------
    # Gate 4: Growth
    # -------------------------------------------------------------------------
    "min_revenue_growth_yoy_pct":  12,
    "min_earnings_growth_yoy_pct": 15,

    # -------------------------------------------------------------------------
    # Gate 5: Sector-Relative Valuation
    # Stock PE must be below (sector_median_pe * this multiplier)
    # -------------------------------------------------------------------------
    "max_pe_vs_sector_median_multiplier": 1.2,
    "max_peg_ratio":                      1.5,

    # -------------------------------------------------------------------------
    # Gate 6: Financial Sector Specific
    # -------------------------------------------------------------------------
    "max_pb_ratio_financials":   2.5,
    "financial_sectors":         ["Bank", "NBFC", "Insurance",
                                  "Financial Services", "Finance"],

    # -------------------------------------------------------------------------
    # Scoring
    # -------------------------------------------------------------------------
    "weight_growth":        0.30,
    "weight_quality":       0.30,
    "weight_valuation":     0.20,
    "weight_balance_sheet": 0.20,

    # -------------------------------------------------------------------------
    # Technical Momentum
    # -------------------------------------------------------------------------
    "use_technical_momentum": True,
    "sma_short":  50,
    "sma_long":   200,

    # -------------------------------------------------------------------------
    # India 10Y Govt Bond Yield (for Earnings Yield comparison)
    # Update this manually or automate via RBI data feed
    # -------------------------------------------------------------------------
    "india_10y_bond_yield_pct": 7.1,

    # -------------------------------------------------------------------------
    # Output
    # -------------------------------------------------------------------------
    "top_n_results": 15,
    "output_csv":    "Top_Stocks_Report.csv",
}

# NSE URLs (niftyindices.com — works without SSL block)
NSE_URLS = {
    "Nifty50":     "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv",
    "Nifty100":    "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv",
    "Nifty500":    "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    "Midcap150":   "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv",
    "Smallcap250": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv",
}

# =============================================================================
# SECTION 2: SCREEN ARCHETYPES
# =============================================================================

ARCHETYPES = {
    "Growth_Bargain": {
        "min_earnings_growth_yoy_pct":        20,
        "max_pe_vs_sector_median_multiplier":  0.9,
        "max_peg_ratio":                       1.2,
        "min_roe_pct":                         15,
        "max_debt_to_equity":                  0.5,
    },
    "Cash_Machine": {
        "min_roe_pct":                         20,
        "max_debt_to_equity":                  0.1,
        "min_revenue_growth_yoy_pct":          10,
        "min_earnings_growth_yoy_pct":         10,
        "reject_negative_operating_cashflow":  True,
    },
    "Low_Debt_Midcap": {
        "universe":                            "Midcap150",
        "max_debt_to_equity":                  0.1,
        "min_roe_pct":                         15,
        "min_earnings_growth_yoy_pct":         12,
        "max_pe_vs_sector_median_multiplier":  1.3,
    },
    "Quality_Compounder": {
        "min_roe_pct":                         20,
        "min_roce_pct":                        20,
        "max_debt_to_equity":                  0.3,
        "min_revenue_growth_yoy_pct":          15,
        "min_earnings_growth_yoy_pct":         15,
        "max_peg_ratio":                       1.5,
    },
    "Near_52W_Low": {
        "min_pct_below_52w_high":              30,
        "min_roe_pct":                         12,
        "max_debt_to_equity":                  0.8,
        "min_earnings_growth_yoy_pct":         10,
    },
}

def apply_archetype(config: dict) -> dict:
    """
    Merges archetype overrides into CONFIG if a non-Custom archetype is selected.
    Returns the modified config dictionary.
    """
    archetype_name = config.get("archetype", "Custom")
    if archetype_name == "Custom" or archetype_name not in ARCHETYPES:
        return config
    overrides = ARCHETYPES[archetype_name]
    merged = config.copy()
    merged.update(overrides)
    print(f"[INFO] Archetype '{archetype_name}' applied. CONFIG overrides: {overrides}")
    return merged

# =============================================================================
# SECTION 3: SECTOR RISK MAP
# =============================================================================

SECTOR_RISK_MAP = {
    "Bank":                    "RBI policy shifts, rising NPAs, or liquidity tightening.",
    "NBFC":                    "RBI liquidity norm tightening, rising borrowing costs, or NPA spikes.",
    "Insurance":               "IRDAI regulatory changes, claims inflation, or yield compression.",
    "Financial Services":      "RBI policy shifts, credit cycle downturns, or rising NPAs.",
    "Finance":                 "Rising interest rates, RBI changes, or NPA deterioration.",
    "Information Technology":  "Global IT spending cuts, USD/INR volatility, or client concentration.",
    "Technology":              "Global tech spending cuts, USD/INR volatility.",
    "Pharmaceuticals":         "USFDA observations, patent expirations, or domestic pricing controls.",
    "Healthcare":              "USFDA regulatory actions, drug pricing controls, or IP challenges.",
    "Automobile":              "EV disruption, fuel price volatility, or material cost spikes.",
    "Consumer Goods":          "Input cost inflation, rural demand slowdown, or GST changes.",
    "Infrastructure":          "Government capex cuts, land acquisition delays, or interest rate rises.",
    "Energy":                  "Crude oil price swings, government pricing intervention, or transition risk.",
    "Metals":                  "Global commodity cycle downturns, import duty changes, or demand collapse.",
    "Realty":                  "Interest rate hikes, regulatory changes, or demand cooling.",
    "default":                 "Sudden regulatory changes, global macro shocks, or black swan events.",
}

# =============================================================================
# SECTION 4: DYNAMIC UNIVERSE LOADER
# =============================================================================

def fetch_csv_from_url(url: str) -> pd.DataFrame:
    """
    Fetches a CSV file from a URL using browser-like headers and SSL verification
    disabled to bypass NSE/niftyindices server restrictions.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    response = requests.get(url, headers=headers, timeout=20, verify=False)
    response.raise_for_status()
    return pd.read_csv(io.StringIO(response.text))


def get_universe_tickers(universe_name: str):
    """
    Loads NSE stock tickers and their sector map for the requested universe.
    Tries niftyindices.com first; falls back to a local CSV if the fetch fails.

    Returns:
        tickers (list): List of '<SYMBOL>.NS' strings.
        sector_map (dict): Dict mapping ticker -> sector/industry name.
    """
    print(f"\n[INFO] Loading universe: {universe_name}")
    df = None

    if universe_name in NSE_URLS:
        try:
            df = fetch_csv_from_url(NSE_URLS[universe_name])
            print(f"[INFO] Fetched {len(df)} stocks from niftyindices.com for '{universe_name}'.")
        except Exception as e:
            print(f"[WARNING] Network fetch failed: {e}. Trying local fallback...")

    elif universe_name == "Top800_Custom":
        try:
            frames = []
            for key in ["Nifty500", "Midcap150", "Smallcap250"]:
                frames.append(fetch_csv_from_url(NSE_URLS[key]))
            df = pd.concat(frames).drop_duplicates(subset=["Symbol"]).head(800)
            print(f"[INFO] Top800_Custom built: {len(df)} unique stocks.")
        except Exception as e:
            print(f"[WARNING] Top800_Custom network fetch failed: {e}. Trying local fallback...")

    if df is None:
        local_file = f"{universe_name}.csv"
        if os.path.exists(local_file):
            print(f"[INFO] Loading from local file: {local_file}")
            df = pd.read_csv(local_file)
        else:
            raise FileNotFoundError(
                f"[ERROR] No data for universe '{universe_name}'. "
                f"Download the CSV from niftyindices.com and save as '{local_file}'."
            )

    df.columns = [c.strip() for c in df.columns]
    sym_col = next((c for c in df.columns if "symbol" in c.lower()), None)
    ind_col = next((c for c in df.columns if any(
        x in c.lower() for x in ["industry", "sector"]
    )), None)

    if sym_col is None:
        raise ValueError(f"[ERROR] Symbol column not found. Columns: {df.columns.tolist()}")

    tickers = [str(s).strip().upper() + ".NS" for s in df[sym_col].dropna()]
    sector_map = {}
    if ind_col:
        for _, row in df.iterrows():
            t = str(row[sym_col]).strip().upper() + ".NS"
            sector_map[t] = str(row[ind_col]).strip() if pd.notna(row[ind_col]) else "Unknown"
    else:
        sector_map = {t: "Unknown" for t in tickers}

    print(f"[INFO] Total tickers loaded: {len(tickers)}")
    return tickers, sector_map

# =============================================================================
# SECTION 5: DATA FETCHING ENGINE
# =============================================================================

def fetch_price_and_technical_data(tickers: list) -> pd.DataFrame:
    """
    Batch-downloads 1 year of daily OHLCV for all tickers.
    Computes: current_price, 52w_high, 52w_low, avg_daily_volume,
    pct_below_52w_high, sma_50, sma_200, and technical_trend.
    """
    print(f"\n[INFO] Batch-fetching price + technical data for {len(tickers)} tickers...")
    try:
        raw = yf.download(
            tickers,
            period="1y",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as e:
        print(f"[ERROR] yfinance batch download failed: {e}")
        return pd.DataFrame()

    records = []
    sma_s = CONFIG.get("sma_short", 50)
    sma_l = CONFIG.get("sma_long", 200)

    for ticker in tickers:
        try:
            df_t = raw[ticker] if len(tickers) > 1 else raw
            if df_t.empty or "Close" not in df_t.columns:
                raise ValueError("No data")

            close  = df_t["Close"].dropna()
            volume = df_t["Volume"].dropna()

            cur_price      = round(float(close.iloc[-1]), 2)
            high_52w       = round(float(close.max()), 2)
            low_52w        = round(float(close.min()), 2)
            avg_vol_30d    = int(volume.tail(30).mean()) if len(volume) >= 5 else 0
            pct_below      = round((high_52w - cur_price) / high_52w * 100, 2) if high_52w > 0 else None
            sma50          = round(float(close.tail(sma_s).mean()), 2) if len(close) >= sma_s else None
            sma200         = round(float(close.tail(sma_l).mean()), 2) if len(close) >= sma_l else None

            # Technical Trend: Uptrend only if Price > SMA50 > SMA200
            if sma50 and sma200:
                if cur_price > sma50 > sma200:
                    tech_trend = "Uptrend ✅"
                elif cur_price < sma50 < sma200:
                    tech_trend = "Downtrend ❌"
                else:
                    tech_trend = "Sideways ➡️"
            else:
                tech_trend = "N/A"

            records.append({
                "ticker":             ticker,
                "current_price":      cur_price,
                "52w_high":           high_52w,
                "52w_low":            low_52w,
                "avg_daily_volume":   avg_vol_30d,
                "pct_below_52w_high": pct_below,
                "sma_50":             sma50,
                "sma_200":            sma200,
                "technical_trend":    tech_trend,
            })
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
    Fetches comprehensive fundamental data for a single ticker via yfinance.
    Covers: PE, PB, PEG, ROE, D/E, market cap, revenue/earnings growth,
    operating cash flow, capex, gross margins (current + previous), and current ratio.
    Returns None for any unavailable field.
    """
    base = {
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

        base["pe_ratio"]            = sg("trailingPE")
        base["pb_ratio"]            = sg("priceToBook")
        base["peg_ratio"]           = sg("pegRatio")
        base["roe"]                 = sg("returnOnEquity", 100)
        base["debt_to_equity"]      = sg("debtToEquity")
        base["market_cap_cr"]       = sg("marketCap", 1 / 1e7)
        base["revenue_growth_yoy"]  = sg("revenueGrowth", 100)
        base["earnings_growth_yoy"] = sg("earningsGrowth", 100)
        base["operating_cash_flow"] = sg("operatingCashflow")
        base["current_ratio"]       = sg("currentRatio")
        base["sector_yf"]           = info.get("sector")

        # Gross margin current from info
        base["gross_margin_current"] = sg("grossMargins", 100)

        # Net income & capex from financials/cashflow
        try:
            cf = stock.cashflow
            if cf is not None and not cf.empty:
                capex_row = next((r for r in cf.index if "capital" in r.lower()), None)
                if capex_row:
                    base["capital_expenditure"] = float(cf.loc[capex_row].iloc[0])
        except Exception:
            pass

        try:
            fin = stock.financials
            if fin is not None and not fin.empty:
                ni_row = next((r for r in fin.index if "net income" in r.lower()), None)
                gp_row = next((r for r in fin.index if "gross profit" in r.lower()), None)
                rev_row = next((r for r in fin.index if "total revenue" in r.lower()), None)

                if ni_row:
                    base["net_income"] = float(fin.loc[ni_row].iloc[0])

                # Gross margin previous year
                if gp_row and rev_row and fin.shape[1] >= 2:
                    gp_prev  = float(fin.loc[gp_row].iloc[1])
                    rev_prev = float(fin.loc[rev_row].iloc[1])
                    if rev_prev != 0:
                        base["gross_margin_prev"] = round((gp_prev / rev_prev) * 100, 4)
        except Exception:
            pass

        # Free Cash Flow
        if base["operating_cash_flow"] and base["capital_expenditure"]:
            base["free_cash_flow"] = base["operating_cash_flow"] - abs(base["capital_expenditure"])

    except Exception:
        pass

    return base


def build_master_dataframe(tickers: list, sector_map: dict) -> pd.DataFrame:
    """
    Combines batch price/technical data with individually fetched fundamental
    data into a single master Pandas DataFrame.
    """
    price_df = fetch_price_and_technical_data(tickers)

    print(f"\n[INFO] Fetching fundamentals for {len(tickers)} stocks (0.5s delay each)...")
    fund_records = []
    total = len(tickers)

    for i, ticker in enumerate(tickers, start=1):
        print(f"  [{i}/{total}] {ticker}...          ", end="\r")
        try:
            fund_records.append(fetch_fundamentals(ticker))
        except Exception:
            fund_records.append({"ticker": ticker})
        time.sleep(0.5)

    fund_df   = pd.DataFrame(fund_records)
    master_df = pd.merge(price_df, fund_df, on="ticker", how="left")

    # Attach sector from NSE map; fallback to yfinance sector
    master_df["sector"] = master_df["ticker"].map(sector_map)
    master_df["sector"] = master_df.apply(
        lambda r: r.get("sector_yf") if (
            pd.isna(r.get("sector")) or r.get("sector") == "Unknown"
        ) and r.get("sector_yf") else r.get("sector"),
        axis=1
    )
    master_df.drop(columns=["sector_yf"], inplace=True, errors="ignore")

    print(f"\n[INFO] Master DataFrame ready: {len(master_df)} rows.")
    return master_df

# =============================================================================
# SECTION 6: SECTOR CLASSIFICATION
# =============================================================================

def classify_sector(sector_name) -> str:
    """Returns 'financial' if sector matches financial keywords, else 'non_financial'."""
    if not sector_name or pd.isna(sector_name):
        return "non_financial"
    s = str(sector_name).lower()
    for fs in CONFIG["financial_sectors"]:
        if fs.lower() in s:
            return "financial"
    return "non_financial"


def add_sector_medians(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds sector_median_pe and sector_median_roe columns to the DataFrame.
    These are used for sector-relative valuation comparisons.
    """
    df["sector_median_pe"]  = df.groupby("sector")["pe_ratio"].transform("median")
    df["sector_median_roe"] = df.groupby("sector")["roe"].transform("median")
    return df

# =============================================================================
# SECTION 7: FILTERING ENGINE (6 GATES)
# =============================================================================

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies 6 sequential gates to filter stocks. Prints survival counts after
    each gate so you can see exactly where stocks are being dropped.
    """
    print(f"\n{'='*62}")
    print(f"  FILTER ENGINE  |  Starting with {len(df)} stocks")
    print(f"{'='*62}")

    w = df.copy()
    w["sector_type"] = w["sector"].apply(classify_sector)
    w = add_sector_medians(w)

    # ── GATE 1: Liquidity & Size ──────────────────────────────────────────────
    before = len(w)
    w = w[w["market_cap_cr"].notna() & w["avg_daily_volume"].notna()]
    w = w[
        (w["market_cap_cr"]    >= CONFIG["min_market_cap_cr"]) &
        (w["avg_daily_volume"] >= CONFIG["min_avg_daily_volume"])
    ]
    print(f"  Gate 1 [Liquidity & Size]         : {before:>4} → {len(w):>4}  ({before-len(w)} dropped)")

    # ── GATE 2: Red Flag Auto-Reject ──────────────────────────────────────────
    before = len(w)
    red_flag = pd.Series([False] * len(w), index=w.index)

    if CONFIG.get("reject_negative_operating_cashflow"):
        red_flag |= w["operating_cash_flow"].notna() & (w["operating_cash_flow"] < 0)

    if CONFIG.get("reject_current_ratio_below"):
        red_flag |= w["current_ratio"].notna() & (
            w["current_ratio"] < CONFIG["reject_current_ratio_below"]
        )

    if CONFIG.get("reject_shrinking_gross_margins"):
        red_flag |= (
            w["gross_margin_current"].notna() &
            w["gross_margin_prev"].notna() &
            (w["gross_margin_current"] < w["gross_margin_prev"])
        )

    w = w[~red_flag]
    print(f"  Gate 2 [Red Flag Auto-Reject]     : {before:>4} → {len(w):>4}  ({before-len(w)} dropped)")

    # ── GATE 3: Quality (Non-Financials) ─────────────────────────────────────
    before = len(w)
    nf  = w["sector_type"] == "non_financial"
    bad = nf & (
        (w["debt_to_equity"].notna() & (w["debt_to_equity"] > CONFIG["max_debt_to_equity"])) |
        (w["roe"].notna() & (w["roe"] < CONFIG["min_roe_pct"]))
    )
    w = w[~bad]
    print(f"  Gate 3 [Quality — Non-Financials] : {before:>4} → {len(w):>4}  ({before-len(w)} dropped)")

    # ── GATE 4: Growth ────────────────────────────────────────────────────────
    before = len(w)
    bad_growth = (
        (w["revenue_growth_yoy"].notna()  & (w["revenue_growth_yoy"]  < CONFIG["min_revenue_growth_yoy_pct"])) |
        (w["earnings_growth_yoy"].notna() & (w["earnings_growth_yoy"] < CONFIG["min_earnings_growth_yoy_pct"]))
    )
    w = w[~bad_growth]
    print(f"  Gate 4 [Growth]                   : {before:>4} → {len(w):>4}  ({before-len(w)} dropped)")

    # ── GATE 5: Sector-Relative Valuation (Non-Financials) ───────────────────
    before = len(w)
    nf = w["sector_type"] == "non_financial"
    max_mult = CONFIG["max_pe_vs_sector_median_multiplier"]
    bad_val = nf & (
        w["pe_ratio"].notna() &
        w["sector_median_pe"].notna() &
        (w["pe_ratio"] > w["sector_median_pe"] * max_mult)
    )
    w = w[~bad_val]
    # Also reject very high PEG
    bad_peg = nf & (w["peg_ratio"].notna() & (w["peg_ratio"] > CONFIG["max_peg_ratio"]))
    w = w[~bad_peg]
    print(f"  Gate 5 [Sector-Relative Valuation]: {before:>4} → {len(w):>4}  ({before-len(w)} dropped)")

    # ── GATE 6: Financials (P/B Rule) ─────────────────────────────────────────
    before = len(w)
    fin = w["sector_type"] == "financial"
    bad_fin = fin & (w["pb_ratio"].notna() & (w["pb_ratio"] > CONFIG["max_pb_ratio_financials"]))
    w = w[~bad_fin]
    print(f"  Gate 6 [Financials — P/B Rule]    : {before:>4} → {len(w):>4}  ({before-len(w)} dropped)")

    # Optional: Near 52W Low archetype gate
    if CONFIG.get("archetype") == "Near_52W_Low" and CONFIG.get("min_pct_below_52w_high"):
        before = len(w)
        min_drop = CONFIG["min_pct_below_52w_high"]
        w = w[w["pct_below_52w_high"].notna() & (w["pct_below_52w_high"] >= min_drop)]
        print(f"  Gate 7 [Near 52W Low]             : {before:>4} → {len(w):>4}  ({before-len(w)} dropped)")

    print(f"{'='*62}")
    print(f"  ✅ Stocks surviving all filters: {len(w)}")
    print(f"{'='*62}\n")
    return w

# =============================================================================
# SECTION 8: 1-10 FUNDAMENTAL SCORE (Tickertape Pro-Style)
# =============================================================================

def calculate_fundamental_score(row: pd.Series, sector_median_roe: float) -> int:
    """
    Calculates a 1-10 Fundamental Score by awarding 1 point per test passed.
    Modeled on Tickertape Pro's proprietary scoring framework.

    Tests:
      1. ROE > sector median ROE
      2. ROCE > 15%
      3. Revenue Growth YoY > 10%
      4. Earnings Growth YoY > 10%
      5. Operating Cash Flow > Net Income (Earnings Quality)
      6. Debt/Equity < 0.5 (or PB < 2.5 for financials)
      7. Current Ratio > 1.2
      8. PEG Ratio < 1.2
      9. Free Cash Flow Yield > 4% (FCF / Market Cap)
    """
    score = 1  # base score

    def safe(val):
        return val is not None and not (isinstance(val, float) and np.isnan(val))

    roe           = row.get("roe")
    earnings_g    = row.get("earnings_growth_yoy")
    revenue_g     = row.get("revenue_growth_yoy")
    ocf           = row.get("operating_cash_flow")
    net_income    = row.get("net_income")
    de            = row.get("debt_to_equity")
    pb            = row.get("pb_ratio")
    cr            = row.get("current_ratio")
    peg           = row.get("peg_ratio")
    fcf           = row.get("free_cash_flow")
    mktcap        = row.get("market_cap_cr")
    sector_type   = row.get("sector_type", "non_financial")

    # Test 1: ROE > sector median
    if safe(roe) and safe(sector_median_roe) and roe > sector_median_roe:
        score += 1

    # Test 2: Earnings Growth > 10%
    if safe(earnings_g) and earnings_g > 10:
        score += 1

    # Test 3: Revenue Growth > 10%
    if safe(revenue_g) and revenue_g > 10:
        score += 1

    # Test 4: Operating Cash Flow > Net Income (quality check)
    if safe(ocf) and safe(net_income) and net_income > 0 and ocf > net_income:
        score += 1

    # Test 5: Debt check
    if sector_type == "non_financial":
        if safe(de) and de < 0.5:
            score += 1
    else:
        if safe(pb) and pb < 2.5:
            score += 1

    # Test 6: Current Ratio > 1.2
    if safe(cr) and cr > 1.2:
        score += 1

    # Test 7: PEG < 1.2
    if safe(peg) and peg < 1.2:
        score += 1

    # Test 8: Free Cash Flow Yield > 4%
    if safe(fcf) and safe(mktcap) and mktcap > 0:
        fcf_yield = (fcf / (mktcap * 1e7)) * 100
        if fcf_yield > 4:
            score += 1

    return min(score, 10)

# =============================================================================
# SECTION 9: COMPOSITE SCORING & RANKING
# =============================================================================

def min_max_norm(series: pd.Series) -> pd.Series:
    """Min-max normalization to 0-1 range."""
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series([0.5] * len(series), index=series.index)
    return (series - lo) / (hi - lo)


def calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates two scores for every surviving stock:
      1. fundamental_score (1–10) — Tickertape Pro-style binary test score.
      2. composite_score (0–100)  — Weighted combination of sub-scores.
    Sorts descending by composite_score, returns top N.
    """
    if df.empty:
        print("[WARNING] No stocks to score. Try relaxing thresholds in CONFIG.")
        return df

    scored = df.copy()

    # Fill NaN with column medians for scoring columns only
    for col in ["earnings_growth_yoy", "roe", "debt_to_equity",
                "pe_ratio", "peg_ratio", "operating_cash_flow"]:
        if col in scored.columns:
            scored[col] = scored[col].fillna(scored[col].median())

    # Fundamental Score (1-10)
    scored["fundamental_score"] = scored.apply(
        lambda r: calculate_fundamental_score(r, r.get("sector_median_roe", 15)),
        axis=1
    )

    # Earnings Yield vs Bond Yield (Tickertape-style value signal)
    bond_yield = CONFIG.get("india_10y_bond_yield_pct", 7.1)
    scored["earnings_yield_pct"] = scored["pe_ratio"].apply(
        lambda pe: round(100 / pe, 2) if pe and pe > 0 else None
    )
    scored["beats_bond_yield"] = scored["earnings_yield_pct"].apply(
        lambda ey: True if ey and ey > bond_yield else False
    )

    # Sub-score 1: Growth
    scored["growth_score"]        = min_max_norm(scored["earnings_growth_yoy"])

    # Sub-score 2: Quality (ROE high + D/E low)
    roe_norm = min_max_norm(scored["roe"])
    de_norm  = 1 - min_max_norm(scored["debt_to_equity"])
    scored["quality_score"]       = (roe_norm + de_norm) / 2

    # Sub-score 3: Valuation (PE low + PEG low)
    pe_norm  = 1 - min_max_norm(scored["pe_ratio"])
    peg_norm = 1 - min_max_norm(scored["peg_ratio"])
    scored["valuation_score"]     = (pe_norm + peg_norm) / 2

    # Sub-score 4: Balance Sheet (OCF high)
    scored["balance_sheet_score"] = min_max_norm(scored["operating_cash_flow"])

    # Composite Score (0-100)
    scored["composite_score"] = round(
        (
            scored["growth_score"]        * CONFIG["weight_growth"] +
            scored["quality_score"]       * CONFIG["weight_quality"] +
            scored["valuation_score"]     * CONFIG["weight_valuation"] +
            scored["balance_sheet_score"] * CONFIG["weight_balance_sheet"]
        ) * 100, 2
    )

    scored.sort_values("composite_score", ascending=False, inplace=True)
    top = scored.head(CONFIG["top_n_results"]).reset_index(drop=True)
    print(f"[INFO] Scoring complete. Top {len(top)} stocks selected.\n")
    return top

# =============================================================================
# SECTION 10: VERDICT GENERATOR
# =============================================================================

def get_sector_risk(sector) -> str:
    """Returns the risk description for a sector from SECTOR_RISK_MAP."""
    if not sector or pd.isna(sector):
        return SECTOR_RISK_MAP["default"]
    for key in SECTOR_RISK_MAP:
        if key.lower() in str(sector).lower():
            return SECTOR_RISK_MAP[key]
    return SECTOR_RISK_MAP["default"]


def fmt(val, suffix="", prefix="", decimals=1):
    """Safely formats a numeric value for display."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    return f"{prefix}{round(val, decimals)}{suffix}"


def generate_verdict(row: pd.Series, rank: int):
    """
    Prints a formatted CLI verdict box for a single stock including:
    - Price, 52W range, technical trend
    - Key fundamentals (PE, PB, PEG, ROE, D/E)
    - 1-10 Fundamental Score and Composite Score
    - Earnings Yield vs Bond Yield comparison
    - Free Cash Flow Yield
    - Plain-English verdict with growth + quality summary
    - Sector-specific risk disclaimer
    """
    ticker      = row.get("ticker", "N/A")
    sector      = row.get("sector", "Unknown")
    comp_score  = row.get("composite_score", 0)
    fund_score  = row.get("fundamental_score", 0)
    price       = row.get("current_price")
    pe          = row.get("pe_ratio")
    pb          = row.get("pb_ratio")
    peg         = row.get("peg_ratio")
    roe         = row.get("roe")
    de          = row.get("debt_to_equity")
    rev_g       = row.get("revenue_growth_yoy")
    earn_g      = row.get("earnings_growth_yoy")
    ocf         = row.get("operating_cash_flow")
    fcf         = row.get("free_cash_flow")
    mktcap      = row.get("market_cap_cr")
    below_52w   = row.get("pct_below_52w_high")
    high_52w    = row.get("52w_high")
    low_52w     = row.get("52w_low")
    sma50       = row.get("sma_50")
    sma200      = row.get("sma_200")
    trend       = row.get("technical_trend", "N/A")
    ey          = row.get("earnings_yield_pct")
    beats_bond  = row.get("beats_bond_yield", False)
    sector_pe   = row.get("sector_median_pe")
    bond_yield  = CONFIG.get("india_10y_bond_yield_pct", 7.1)

    fcf_yield_str = "N/A"
    if fcf and mktcap and mktcap > 0:
        fcf_yield_val = (fcf / (mktcap * 1e7)) * 100
        fcf_yield_str = f"{round(fcf_yield_val, 1)}%"

    # Build plain-English verdict
    parts = []
    if earn_g and earn_g > 0:
        parts.append(f"earnings growth of {fmt(earn_g, '%')}")
    if roe and roe > 0:
        parts.append(f"ROE of {fmt(roe, '%')}")
    if de is not None and de < 0.5:
        parts.append(f"low leverage (D/E: {fmt(de)})")
    if peg and peg < 1.2:
        parts.append(f"very attractive valuation (PEG: {fmt(peg)})")
    if beats_bond:
        parts.append(f"earnings yield ({fmt(ey, '%')}) above bond yield ({bond_yield}%)")
    verdict_str = ", ".join(parts) if parts else "balanced fundamentals"

    # Score badge
    if fund_score >= 8:
        badge = "🟢 STRONG"
    elif fund_score >= 6:
        badge = "🟡 MODERATE"
    else:
        badge = "🔴 WEAK"

    print("=" * 68)
    print(f"  #{rank:<3} {ticker:<22}  |  {sector}")
    print(f"  Composite Score : {comp_score:.2f}/100  |  Fundamental Score: {fund_score}/10  {badge}")
    print(f"  Current Price   : ₹{fmt(price, decimals=2)}")
    print(f"  52W High/Low    : ₹{fmt(high_52w, decimals=2)} / ₹{fmt(low_52w, decimals=2)}  |  Below High: {fmt(below_52w, '%')}")
    print(f"  Technical Trend : {trend}  |  SMA50: ₹{fmt(sma50, decimals=1)}  SMA200: ₹{fmt(sma200, decimals=1)}")
    print("-" * 68)
    print(f"  VALUATION")
    print(f"    P/E   : {fmt(pe):<12} Sector Median P/E: {fmt(sector_pe)}")
    print(f"    P/B   : {fmt(pb):<12} PEG: {fmt(peg)}")
    print(f"    EPS Yield: {fmt(ey, '%'):<10} vs Bond Yield: {bond_yield}%  → {'✅ Beats Bond' if beats_bond else '❌ Below Bond'}")
    print(f"    FCF Yield : {fcf_yield_str}")
    print("-" * 68)
    print(f"  FUNDAMENTALS")
    print(f"    ROE  : {fmt(roe, '%'):<12} D/E    : {fmt(de)}")
    print(f"    Rev Growth (YoY): {fmt(rev_g, '%'):<8}  Earnings Growth: {fmt(earn_g, '%')}")
    print(f"    Op. Cash Flow   : ₹{fmt(ocf/1e7 if ocf else None, ' Cr', decimals=0)}")
    print("-" * 68)
    print(f"  ✅  VERDICT")
    print(f"    {ticker} shows {verdict_str}.")
    print(f"    Fundamental Score {fund_score}/10. This stock has good potential")
    print(f"    to compound wealth under normal market conditions.")
    print("-" * 68)
    print(f"  ⚠️   DISCLAIMER")
    print(f"    Sector risk ({sector}): {get_sector_risk(sector)}")
    print(f"    THIS IS NOT FINANCIAL ADVICE. Do your own due diligence.")
    print("=" * 68 + "\n")

# =============================================================================
# SECTION 11: MAIN EXECUTION
# =============================================================================

def main():
    """
    Full pipeline:
    Config → Universe Load → Fetch Data → Filters → Score → Report
    """
    global CONFIG
    CONFIG = apply_archetype(CONFIG)

    print("\n" + "=" * 68)
    print(f"  NSE STOCK SCREENER v2.0  |  Universe: {CONFIG['universe']}")
    arch = CONFIG.get("archetype", "Custom")
    if arch != "Custom":
        print(f"  Archetype: {arch}")
    print(f"  Run Date : {datetime.now().strftime('%d %B %Y | %I:%M %p')}")
    print("=" * 68)

    # Step 1: Universe
    tickers, sector_map = get_universe_tickers(CONFIG["universe"])

    # Step 2: Fetch all data
    master_df = build_master_dataframe(tickers, sector_map)
    master_df.to_csv("raw_data.csv", index=False)
    print(f"[INFO] Raw data saved → raw_data.csv ({len(master_df)} rows)")

    # Step 3: Filter
    filtered_df = apply_filters(master_df)
    if filtered_df.empty:
        print("[WARNING] No stocks passed all filters. Relax thresholds in CONFIG.")
        return

    # Step 4: Score & rank
    top_df = calculate_scores(filtered_df)
    top_df.to_csv(CONFIG["output_csv"], index=False)
    print(f"[INFO] Report saved → {CONFIG['output_csv']} ({len(top_df)} stocks)")

    # Step 5: Print verdicts
    print("\n" + "=" * 68)
    print(f"  TOP {len(top_df)} STOCK RECOMMENDATIONS  |  Archetype: {arch}")
    print("=" * 68 + "\n")

    for i, (_, row) in enumerate(top_df.iterrows(), start=1):
        try:
            generate_verdict(row, rank=i)
        except Exception as e:
            print(f"[WARNING] Verdict error for {row.get('ticker', '?')}: {e}")

    print("=" * 68)
    print(f"  ✅  Screening Complete.")
    print(f"  📁  Report   : {CONFIG['output_csv']}")
    print(f"  📁  Raw Data : raw_data.csv")
    print("=" * 68 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user.")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        raise
