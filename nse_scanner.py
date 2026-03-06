# =============================================================================
# nse_screener.py
# NSE Stock Screener | Fundamental + Growth + Valuation
# Author: Generated via Master Instruction Document (SSL Fix Applied)
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

# Suppress InsecureRequestWarning when using verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============================================================================
# SECTION 1: CONFIG BLOCK
# =============================================================================

CONFIG = {
    # Universe Options: "Nifty50", "Nifty100", "Nifty500", "Midcap150", "Smallcap250", "Top800_Custom"
    "universe": "Nifty500",

    # Gate 1: Liquidity & Size
    "min_market_cap_cr": 1000,
    "min_avg_daily_volume": 300000,

    # Gate 2: Quality (Non-Financials)
    "max_debt_to_equity": 0.5,
    "min_roe_pct": 15,
    "min_roce_pct": 15,

    # Gate 3: Growth
    "min_revenue_cagr_3yr_pct": 12,
    "min_profit_cagr_3yr_pct": 15,

    # Gate 4: Valuation (Non-Financials)
    "max_peg_ratio": 1.5,

    # Gate 5: Financial Sector specific
    "max_pb_ratio_financials": 2.5,
    "financial_sectors": ["Bank", "NBFC", "Insurance", "Financial Services", "Finance"],

    # Scoring Weights
    "weight_growth": 0.30,
    "weight_quality": 0.30,
    "weight_valuation": 0.20,
    "weight_balance_sheet": 0.20,

    # Output Settings
    "top_n_results": 15,
    "output_csv": "Top_Stocks_Report.csv",
}

# FIXED URLs: Pointing to niftyindices.com instead of www1.nseindia.com
NSE_URLS = {
    "Nifty50":     "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv",
    "Nifty100":    "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv",
    "Nifty500":    "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    "Midcap150":   "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv",
    "Smallcap250": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv",
}

# =============================================================================
# SECTION 2: SECTOR RISK MAP
# =============================================================================

SECTOR_RISK_MAP = {
    "Bank":                 "RBI policy shifts, rising NPAs, or liquidity tightening.",
    "NBFC":                 "RBI tightening liquidity norms, rising borrowing costs, or NPA spikes.",
    "Insurance":            "IRDAI regulatory changes, claims inflation, or investment yield compression.",
    "Financial Services":   "RBI policy shifts, credit cycle downturns, or rising NPAs.",
    "Finance":              "Rising interest rates, RBI policy changes, or NPA deterioration.",
    "Information Technology": "Global IT spending cuts, USD/INR volatility, or client concentration risks.",
    "Technology":           "Global tech spending cuts, USD/INR volatility, or client concentration risks.",
    "Pharmaceuticals":      "USFDA observations, drug patent expirations, or domestic pricing controls.",
    "Healthcare":           "USFDA regulatory actions, drug pricing controls, or IP challenges.",
    "Automobile":           "EV transition disruption, fuel price volatility, or material cost spikes.",
    "Consumer Goods":       "Input cost inflation, rural demand slowdown, or GST rate changes.",
    "Infrastructure":       "Government capex cuts, land acquisition delays, or rising interest rates.",
    "Energy":               "Crude oil price volatility, government pricing intervention, or transition risk.",
    "Metals":               "Global commodity cycle downturns, import duty changes, or demand collapse.",
    "Realty":               "Interest rate hikes, regulatory changes, or demand cooling.",
    "default":              "Sudden regulatory changes, global macroeconomic shocks, or black swan events.",
}

# =============================================================================
# SECTION 3: DYNAMIC UNIVERSE LOADER
# =============================================================================

def fetch_csv_from_url(url: str) -> pd.DataFrame:
    """Helper function to fetch CSV data safely with SSL verification disabled."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
    }
    # verify=False fixes the strict SSL internal error from NSE servers
    response = requests.get(url, headers=headers, timeout=15, verify=False)
    response.raise_for_status()
    return pd.read_csv(io.StringIO(response.text))


def get_universe_tickers(universe_name: str):
    print(f"\n[INFO] Loading universe: {universe_name}")
    df = None

    if universe_name in NSE_URLS:
        try:
            df = fetch_csv_from_url(NSE_URLS[universe_name])
            print(f"[INFO] Successfully fetched {len(df)} stocks from NSE for {universe_name}.")
        except Exception as e:
            print(f"[WARNING] Network fetch failed: {e}. Trying local fallback...")

    elif universe_name == "Top800_Custom":
        try:
            frames = []
            for key in ["Nifty500", "Midcap150", "Smallcap250"]:
                frames.append(fetch_csv_from_url(NSE_URLS[key]))
            df = pd.concat(frames).drop_duplicates(subset=["Symbol"])
            df = df.head(800)
            print(f"[INFO] Top800_Custom: fetched {len(df)} unique stocks.")
        except Exception as e:
            print(f"[WARNING] Top800_Custom fetch failed: {e}. Trying local fallback...")

    # Fallback to local file if network fails completely
    if df is None:
        local_file = f"{universe_name}.csv"
        if os.path.exists(local_file):
            print(f"[INFO] Loading from local file: {local_file}")
            df = pd.read_csv(local_file)
        else:
            raise FileNotFoundError(
                f"[ERROR] Could not download data from network, and local file '{local_file}' is missing. "
                "Please download it manually from niftyindices.com and save it in this folder."
            )

    df.columns = [c.strip() for c in df.columns]

    sym_col = next((c for c in df.columns if "symbol" in c.lower()), None)
    ind_col = next((c for c in df.columns if any(x in c.lower() for x in ["industry", "sector"])), None)

    if sym_col is None:
        raise ValueError(f"[ERROR] Could not find Symbol column. Columns: {df.columns.tolist()}")

    tickers = [str(s).strip().upper() + ".NS" for s in df[sym_col].dropna()]
    sector_map = {}
    if ind_col:
        for _, row in df.iterrows():
            ticker = str(row[sym_col]).strip().upper() + ".NS"
            sector_map[ticker] = str(row[ind_col]).strip() if pd.notna(row[ind_col]) else "Unknown"
    else:
        sector_map = {t: "Unknown" for t in tickers}

    print(f"[INFO] Total tickers loaded: {len(tickers)}")
    return tickers, sector_map


# =============================================================================
# SECTION 4A: PRICE & VOLUME DATA
# =============================================================================

def fetch_price_data(tickers: list) -> pd.DataFrame:
    print(f"\n[INFO] Fetching price data for {len(tickers)} tickers (batch)...")
    try:
        raw = yf.download(
            tickers,
            period="1y",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True
        )
    except Exception as e:
        print(f"[ERROR] yfinance batch download failed: {e}")
        return pd.DataFrame()

    records = []
    for ticker in tickers:
        try:
            if len(tickers) == 1:
                df_ticker = raw
            else:
                df_ticker = raw[ticker] if ticker in raw.columns.get_level_values(0) else pd.DataFrame()

            if df_ticker.empty or "Close" not in df_ticker.columns:
                raise ValueError("Empty Data")

            close = df_ticker["Close"].dropna()
            volume = df_ticker["Volume"].dropna()

            current_price   = round(float(close.iloc[-1]), 2)
            high_52w        = round(float(close.max()), 2)
            low_52w         = round(float(close.min()), 2)
            avg_vol_30d     = int(volume.tail(30).mean()) if len(volume) >= 5 else 0
            pct_below_high  = round((high_52w - current_price) / high_52w * 100, 2) if high_52w > 0 else None

            records.append({
                "ticker":              ticker,
                "current_price":       current_price,
                "52w_high":            high_52w,
                "52w_low":             low_52w,
                "avg_daily_volume":    avg_vol_30d,
                "pct_below_52w_high":  pct_below_high,
            })
        except Exception:
            records.append({
                "ticker": ticker, "current_price": None, "52w_high": None, 
                "52w_low": None, "avg_daily_volume": None, "pct_below_52w_high": None,
            })

    return pd.DataFrame(records)


# =============================================================================
# SECTION 4B: FUNDAMENTALS
# =============================================================================

def fetch_fundamentals(ticker: str) -> dict:
    result = {
        "ticker": ticker, "pe_ratio": None, "pb_ratio": None, "peg_ratio": None,
        "roe": None, "debt_to_equity": None, "market_cap_cr": None,
        "revenue_growth_yoy": None, "earnings_growth_yoy": None, "operating_cash_flow": None, "sector_yf": None,
    }

    try:
        stock = yf.Ticker(ticker)
        info  = stock.info or {}

        def safe_get(key, scale=1.0):
            val = info.get(key)
            if val is not None and not (isinstance(val, float) and np.isnan(val)):
                return round(float(val) * scale, 4)
            return None

        result["pe_ratio"]            = safe_get("trailingPE")
        result["pb_ratio"]            = safe_get("priceToBook")
        result["peg_ratio"]           = safe_get("pegRatio")
        result["roe"]                 = safe_get("returnOnEquity", scale=100)
        result["debt_to_equity"]      = safe_get("debtToEquity")
        result["market_cap_cr"]       = safe_get("marketCap", scale=1 / 1e7)
        result["revenue_growth_yoy"]  = safe_get("revenueGrowth", scale=100)
        result["earnings_growth_yoy"] = safe_get("earningsGrowth", scale=100)
        result["operating_cash_flow"] = safe_get("operatingCashflow")
        result["sector_yf"]           = info.get("sector", None)
    except Exception:
        pass 

    return result


# =============================================================================
# SECTION 4C: MASTER DATAFRAME BUILDER
# =============================================================================

def build_master_dataframe(tickers: list, sector_map: dict) -> pd.DataFrame:
    price_df = fetch_price_data(tickers)

    print(f"\n[INFO] Fetching fundamentals one by one (this takes time)...")
    fund_records = []
    total = len(tickers)
    failed_tickers = []

    for i, ticker in enumerate(tickers, start=1):
        print(f"  [{i}/{total}] Fetching {ticker}...", end="\r")
        try:
            fund = fetch_fundamentals(ticker)
            fund_records.append(fund)
        except Exception:
            failed_tickers.append(ticker)
            fund_records.append({"ticker": ticker})
        time.sleep(0.5)

    fund_df = pd.DataFrame(fund_records)
    master_df = pd.merge(price_df, fund_df, on="ticker", how="left")

    master_df["sector"] = master_df["ticker"].map(sector_map)
    master_df["sector"] = master_df.apply(
        lambda row: row["sector_yf"] if (pd.isna(row["sector"]) or row["sector"] == "Unknown")
                    and row.get("sector_yf") else row["sector"],
        axis=1
    )
    master_df.drop(columns=["sector_yf"], inplace=True, errors="ignore")

    print(f"\n[INFO] Master DataFrame built: {len(master_df)} rows.")
    return master_df


# =============================================================================
# SECTION 5: SECTOR CLASSIFICATION & FILTERING ENGINE
# =============================================================================

def classify_sector(sector_name: str) -> str:
    if not sector_name or pd.isna(sector_name):
        return "non_financial"
    sector_lower = str(sector_name).lower()
    for fs in CONFIG["financial_sectors"]:
        if fs.lower() in sector_lower:
            return "financial"
    return "non_financial"


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    print(f"\n{'='*60}")
    print(f"  APPLYING FILTERS | Starting with {len(df)} stocks")
    print(f"{'='*60}")

    working_df = df.copy()
    working_df["sector_type"] = working_df["sector"].apply(classify_sector)

    # Gate 1: Liquidity & Size 
    before = len(working_df)
    working_df = working_df[
        (working_df["market_cap_cr"].isna() | (working_df["market_cap_cr"] >= CONFIG["min_market_cap_cr"])) &
        (working_df["avg_daily_volume"].isna() | (working_df["avg_daily_volume"] >= CONFIG["min_avg_daily_volume"]))
    ]
    working_df = working_df[working_df["market_cap_cr"].notna() & working_df["avg_daily_volume"].notna()]
    print(f"  Gate 1 [Liquidity & Size]    : {before} → {len(working_df)} survived")

    # Gate 2: Quality (Non-Financials) 
    before = len(working_df)
    non_fin_mask = working_df["sector_type"] == "non_financial"
    fail_non_fin = non_fin_mask & (
        (working_df["debt_to_equity"] > CONFIG["max_debt_to_equity"]) |
        (working_df["roe"] < CONFIG["min_roe_pct"])
    )
    working_df = working_df[~fail_non_fin]
    print(f"  Gate 2 [Quality - Non-Fin]   : {before} → {len(working_df)} survived")

    # Gate 3: Growth 
    before = len(working_df)
    fail_growth = (
        (working_df["revenue_growth_yoy"].notna() & (working_df["revenue_growth_yoy"] < CONFIG["min_revenue_cagr_3yr_pct"])) |
        (working_df["earnings_growth_yoy"].notna() & (working_df["earnings_growth_yoy"] < CONFIG["min_profit_cagr_3yr_pct"]))
    )
    working_df = working_df[~fail_growth]
    print(f"  Gate 3 [Growth]              : {before} → {len(working_df)} survived")

    # Gate 4: Valuation (Non-Financials) 
    before = len(working_df)
    fail_val = non_fin_mask & (working_df["peg_ratio"].notna() & (working_df["peg_ratio"] > CONFIG["max_peg_ratio"]))
    working_df = working_df[~fail_val]
    print(f"  Gate 4 [Valuation - Non-Fin] : {before} → {len(working_df)} survived")

    # Gate 5: Financial Sector 
    before = len(working_df)
    fin_mask = working_df["sector_type"] == "financial"
    fail_fin = fin_mask & (working_df["pb_ratio"].notna() & (working_df["pb_ratio"] > CONFIG["max_pb_ratio_financials"]))
    working_df = working_df[~fail_fin]
    print(f"  Gate 5 [Financials PB]       : {before} → {len(working_df)} survived")

    print(f"{'='*60}")
    print(f"  ✅ Total stocks after all filters: {len(working_df)}")
    print(f"{'='*60}\n")

    return working_df


# =============================================================================
# SECTION 6: SCORING & RANKING
# =============================================================================

def min_max_normalize(series: pd.Series) -> pd.Series:
    min_val = series.min()
    max_val = series.max()
    if max_val == min_val:
        return pd.Series([0.5] * len(series), index=series.index)
    return (series - min_val) / (max_val - min_val)


def calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    scored = df.copy()

    for col in ["earnings_growth_yoy", "roe", "debt_to_equity", "pe_ratio", "peg_ratio", "operating_cash_flow"]:
        if col in scored.columns:
            scored[col] = scored[col].fillna(scored[col].median())

    scored["growth_score"] = min_max_normalize(scored["earnings_growth_yoy"])
    
    roe_norm  = min_max_normalize(scored["roe"])
    de_norm   = 1 - min_max_normalize(scored["debt_to_equity"]) 
    scored["quality_score"] = (roe_norm + de_norm) / 2

    pe_norm   = 1 - min_max_normalize(scored["pe_ratio"])
    peg_norm  = 1 - min_max_normalize(scored["peg_ratio"])
    scored["valuation_score"] = (pe_norm + peg_norm) / 2

    scored["balance_sheet_score"] = min_max_normalize(scored["operating_cash_flow"])

    scored["composite_score"] = round(
        (
            scored["growth_score"]       * CONFIG["weight_growth"] +
            scored["quality_score"]      * CONFIG["weight_quality"] +
            scored["valuation_score"]    * CONFIG["weight_valuation"] +
            scored["balance_sheet_score"] * CONFIG["weight_balance_sheet"]
        ) * 100, 2
    )

    scored.sort_values("composite_score", ascending=False, inplace=True)
    return scored.head(CONFIG["top_n_results"]).reset_index(drop=True)


# =============================================================================
# SECTION 7: VERDICT GENERATOR
# =============================================================================

def get_sector_risk(sector: str) -> str:
    if not sector or pd.isna(sector):
        return SECTOR_RISK_MAP["default"]
    for key in SECTOR_RISK_MAP:
        if key.lower() in str(sector).lower():
            return SECTOR_RISK_MAP[key]
    return SECTOR_RISK_MAP["default"]


def generate_verdict(row: pd.Series):
    ticker          = row.get("ticker", "N/A")
    sector          = row.get("sector", "Unknown")
    score           = row.get("composite_score", 0)
    price           = row.get("current_price", None)
    pe              = row.get("pe_ratio", None)
    pb              = row.get("pb_ratio", None)
    peg             = row.get("peg_ratio", None)
    roe             = row.get("roe", None)
    de              = row.get("debt_to_equity", None)
    rev_growth      = row.get("revenue_growth_yoy", None)
    earn_growth     = row.get("earnings_growth_yoy", None)
    ocf             = row.get("operating_cash_flow", None)
    below_52w       = row.get("pct_below_52w_high", None)
    high_52w        = row.get("52w_high", None)
    low_52w         = row.get("52w_low", None)

    def fmt(val, suffix="", prefix="", decimals=1):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return "N/A"
        return f"{prefix}{round(val, decimals)}{suffix}"

    verdict_parts = []
    if earn_growth and earn_growth > 0: verdict_parts.append(f"strong earnings growth of {fmt(earn_growth, '%')}")
    if roe and roe > 0: verdict_parts.append(f"healthy ROE of {fmt(roe, '%')}")
    if de is not None and de < 0.5: verdict_parts.append(f"low leverage (D/E: {fmt(de)})")
    if peg and peg < 1.5: verdict_parts.append(f"attractive valuation (PEG: {fmt(peg)})")

    verdict_str = ", ".join(verdict_parts) if verdict_parts else "balanced fundamentals"
    risk_str    = get_sector_risk(sector)

    print("=" * 66)
    print(f"  🏢  {ticker:<20}  |  Sector: {sector}")
    print(f"  💯  Composite Score : {score:.2f} / 100")
    print(f"  💰  Current Price   : ₹{fmt(price, decimals=2)}")
    print(f"  📊  52W High/Low    : ₹{fmt(high_52w, decimals=2)} / ₹{fmt(low_52w, decimals=2)}")
    if below_52w: print(f"  📉  Below 52W High  : {fmt(below_52w, '%')}")
    print("-" * 66)
    print(f"  KEY FUNDAMENTALS")
    print(f"    P/E  : {fmt(pe):<10} P/B  : {fmt(pb):<10} PEG  : {fmt(peg)}")
    print(f"    ROE  : {fmt(roe, '%'):<10} D/E  : {fmt(de):<10} Rev Growth: {fmt(rev_growth, '%')}")
    print(f"    Earnings Growth : {fmt(earn_growth, '%'):<10}  Op. Cash Flow: ₹{fmt(ocf/1e7 if ocf else None, ' Cr', decimals=0)}")
    print("-" * 66)
    print(f"  ✅  VERDICT")
    print(f"    {ticker} is a strong candidate with {verdict_str}.")
    print(f"    This stock has good potential to compound wealth over the")
    print(f"    medium to long term based on current fundamentals.")
    print("-" * 66)
    print(f"  ⚠️   DISCLAIMER")
    print(f"    This outlook is valid under normal market conditions.")
    print(f"    Risks for {sector} sector: {risk_str}")
    print(f"    THIS IS NOT FINANCIAL ADVICE. Do your own due diligence.")
    print("=" * 66 + "\n")


# =============================================================================
# SECTION 8: MAIN EXECUTION
# =============================================================================

def main():
    print("\n" + "=" * 66)
    print(f"  NSE STOCK SCREENER | Universe: {CONFIG['universe']}")
    print(f"  Run Date : {datetime.now().strftime('%d %B %Y | %I:%M %p')}")
    print("=" * 66)

    tickers, sector_map = get_universe_tickers(CONFIG["universe"])
    master_df = build_master_dataframe(tickers, sector_map)

    master_df.to_csv("raw_data.csv", index=False)
    filtered_df = apply_filters(master_df)

    if filtered_df.empty:
        print("\n[WARNING] No stocks passed all filters. Try relaxing thresholds in CONFIG.")
        return

    top_df = calculate_scores(filtered_df)
    top_df.to_csv(CONFIG["output_csv"], index=False)

    print("\n" + "=" * 66)
    print(f"  TOP {len(top_df)} STOCK RECOMMENDATIONS")
    print("=" * 66 + "\n")

    for _, row in top_df.iterrows():
        try:
            generate_verdict(row)
        except Exception as e:
            pass

    print(f"  ✅  Screening Complete.")
    print(f"  📁  Full Report : {CONFIG['output_csv']}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] Script interrupted by user.")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] Script failed: {e}")
