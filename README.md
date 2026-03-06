# 📈 NSE Stock Recommendation Engine

### Version 5.1 — Hardened & Config-Robust Edition

An open-source, professional-grade stock screener and recommendation engine
for the Indian Stock Market (NSE). Built in Python. Free to use, modify,
and share.

---

## 🧠 What Is This?

This tool is a **fully automated stock analysis engine** that scans the NSE
universe (Nifty 50 to Nifty 500+), applies institutional-grade filters, and
produces a **Tickertape-style Stock Scorecard** for every qualifying company.

Instead of giving you raw numbers and leaving you to interpret them, it gives
you a **plain-English verdict** for each stock — just like the scorecards you
see on Tickertape or Screener.in:

```
  📋  HDFCBANK.NS STOCK SCORECARD
  ──────────────────────────────────────────────────────
  🟢  Performance    [ High ]
     Price return of 22.4% is strong, outperforming Nifty by 16.1%.

  🟢  Valuation      [ Attractive ]
     PE of 14.2x is highly attractive vs sector median of 18.5x.

  🟢  Growth         [ High ]
     Revenue CAGR of 18.2% and Profit CAGR of 21.5% — outstanding growth.

  🟢  Profitability  [ High ]
     Showing strong profitability with ROE of 17.3% and stable margins.

  🟢  Entry Point    [ Good ]
     RSI of 38.2 — stock is underpriced and not in the overbought zone.

  🟢  Red Flags      [ Low ]
     No red flag found.
```

This is not a toy screener. The engine is built around the same logic that
professional Indian financial platforms use.

---

## 🌟 Key Features at a Glance

| Feature | Details |
|---|---|
| 📊 6-Category Scorecard | Performance, Valuation, Growth, Profitability, Entry Point, Red Flags |
| 📡 Live Price Data | Price, RSI-14, SMA-50/200 always fetched fresh on every run |
| 🛡️ Config Validator | Auto-corrects wrong/missing/out-of-range CONFIG values on startup |
| 🏎️ Parallel Fetching | ThreadPoolExecutor (15 workers) cuts fetch time by ~80% |
| 🧭 Nifty-Relative Strength | Compares 1Y stock return vs Nifty 50 index return |
| 🏦 Financial Sector Logic | Separate scoring model for Banks, NBFCs, and Insurance |
| 📦 Smart Cache System | Fundamentals cached 7 days, prices always live |
| 🧬 Multi-Year CAGR | 3Y and 5Y Revenue & Profit CAGR from actual financial statements |
| 🎭 Archetypes | 5 pre-built strategies: Growth, Cash Machine, Quality Compounder, etc. |
| 👥 Peer Comparison Table | Shows 3 closest sector peers side by side |
| 📁 CSV Export | Saves full report and raw data as CSV files |

---

## 🆕 What's New in v5.1 — Hardened Edition

| Fix | Details |
|---|---|
| ⚠️ Config Validator | Auto-corrects wrong/missing/out-of-range CONFIG values on startup |
| 🔧 Top800_Custom | Fully fixed: parallel fetch, dedup, and sector merge now work correctly |
| 📊 Sector Medians | 3-tier fallback for sparse sectors (sector → market → constant) |
| 🐛 yfinance Shape Bug | Fixed crash when fetching a single ticker vs multiple tickers |
| 💥 Zero Results Warning | Prints actionable tips when all stocks get filtered out |
| 📁 CSV Save | Fallback filename used automatically if output_csv path fails |
| 🔒 Cache JSON Bug | numpy int64/float64 types now safely serialized to JSON |
| 🛡️ Peer Table | Fully crash-proof — no more KeyError on missing columns |

---

## 🗂️ What It Produces

For every top stock, you get a full terminal output containing:

1. **Price & Technical Block** — CMP, 52W High/Low, % below high, SMA50,
   SMA200, RSI-14, volume surge, trend direction.
2. **Fundamentals Snapshot** — PE, PB, ROE, D/E vs Sector Median. Revenue
   & Profit CAGR (3Y & 5Y). OCF, FCF Yield, EPS Yield vs Bond Yield.
3. **6-Category Stock Scorecard** — Each tier has a tag (High/Avg/Low) and
   a one-line plain-English explanation.
4. **Peer Comparison Table** — 3 sector peers ranked by market cap proximity,
   showing their PE, PB, ROE, and 1Y Return.
5. **Sector Risk Disclaimer** — Custom risk statement per sector.

And two output files:
- `Top_Stocks_Report.csv` — Top N ranked stocks with all scores and metrics.
- `raw_data.csv` — Full data for all stocks in the universe (pre-filter).

---

## 🚀 Getting Started

### Step 1: Install Python

You need **Python 3.9 or higher**. Download it from [python.org](https://www.python.org).

Verify your installation:
```bash
python --version
```

---

### Step 2: Install Required Libraries

Open your Terminal (Mac/Linux) or Command Prompt (Windows) and run:

```bash
pip install pandas numpy yfinance requests urllib3
```

No paid APIs, no accounts, no subscriptions required.

---

### Step 3: Download the Script

Download `nse_screener.py` and place it in a folder on your computer:

```
📁 my_screener/
   └── nse_screener.py
```

---

### Step 4: Run It

Navigate to that folder in your terminal:

```bash
cd my_screener
python nse_screener.py
```

The engine will:
- Auto-download the Nifty 500 constituent list from NSE
- Fetch fundamental data for all ~500 stocks (first run: ~10–15 mins)
- Fetch live price + RSI + Nifty RS data (every run: ~2–3 mins)
- Apply all 6 filter gates and produce a ranked scorecard list
- Save results to `Top_Stocks_Report.csv`

**Second run and beyond:** Fundamentals are cached for 7 days. Only live
price data is fetched fresh. The script will finish in **under 3 minutes**.

---

## 🛠️ How to Customize — The CONFIG Block

Open `nse_screener.py` in any text editor (Notepad, VS Code, etc.). At the
top of the file you will find the `CONFIG` block. This is the **only thing
you need to change** to customize the screener.

### Choose Your Universe

```python
"universe": "Nifty500",
```

| Option | Stocks | Best For | Run Time |
|---|---|---|---|
| `"Nifty50"` | 50 | Safe large-cap picks only | ~30 sec |
| `"Nifty100"` | 100 | Conservative investors | ~1 min |
| `"Nifty500"` | 500 | **Most users — recommended** | ~3 min (cached) |
| `"Midcap150"` | 150 | Mid-cap growth hunters | ~1 min |
| `"Smallcap250"` | 250 | High-risk, high-reward hunting | ~2 min |
| `"Top800_Custom"` | ~800 | Widest possible scan | ~5 min |

---

### Use a Pre-Built Archetype (Easiest Customization)

Change one line:

```python
"archetype": "Growth_Bargain",
```

| Archetype | What It Looks For |
|---|---|
| `"Custom"` | Uses your exact CONFIG settings below |
| `"Growth_Bargain"` | 18%+ profit CAGR AND trading below sector PE |
| `"Cash_Machine"` | Near-zero debt, huge FCF, ROE > 20% |
| `"Low_Debt_Midcap"` | Midcap stocks with very clean balance sheets (D/E < 0.1) |
| `"Quality_Compounder"` | Consistent 15%+ multi-year growth + high ROE |
| `"Near_52W_Low"` | Beaten-down 30%+ from highs but fundamentally strong |

---

### Full CONFIG Reference

```python
CONFIG = {

    # ── Universe ──────────────────────────────────────────────────────────────
    # "Nifty50" | "Nifty100" | "Nifty500" | "Midcap150" | "Smallcap250" | "Top800_Custom"
    "universe": "Nifty500",

    # ── Archetype ─────────────────────────────────────────────────────────────
    # "Custom" | "Growth_Bargain" | "Cash_Machine" | "Low_Debt_Midcap"
    # "Quality_Compounder" | "Near_52W_Low"
    "archetype": "Custom",

    # ── Cache ─────────────────────────────────────────────────────────────────
    "use_cache": True,
    "cache_folder":            "cache/",
    "cache_tickers_subfolder": "cache/tickers/",
    "cache_fundamental_days":  7,       # Days before fundamentals re-fetched
    "cache_universe_days":     30,      # Days before index CSV re-downloaded
    "cache_force_refresh":     False,   # True = wipe cache, reset to False after!
    "cache_show_stats":        True,

    # ⚠️  CRITICAL: Must match the script version number EXACTLY.
    # A mismatch forces a full 15-minute re-fetch on every single run.
    # DO NOT change this manually — only update when upgrading the script.
    "cache_schema_version": "5.1",

    # ── Gate 1: Liquidity ─────────────────────────────────────────────────────
    "min_market_cap_cr":    1000,       # Try: 500 (lenient) | 2000 (large-cap only)
    "min_avg_daily_volume": 300000,     # Try: 100000 (lenient) | 1000000 (liquid only)

    # ── Gate 2: Red Flag Auto-Reject ──────────────────────────────────────────
    "reject_negative_ocf":          True,   # Profits + negative OCF = red flag
    "reject_current_ratio_below":   1.0,    # Below 1.0 = can't pay short-term bills
    "reject_shrinking_margins":     True,   # Shrinking margins = weakening business

    # ── Gate 3: Quality (Non-Financial only) ──────────────────────────────────
    "max_debt_to_equity": 0.5,     # Try: 0.1 (debt-free) | 0.5 (default) | 1.0 (lenient)
    "min_roe_pct":        15,      # Try: 10 (lenient) | 15 (default) | 20 (premium)

    # ── Gate 4: Growth ────────────────────────────────────────────────────────
    "min_revenue_cagr_3y_pct":    10,   # Try: 5 (lenient) | 10 (default) | 15 (strict)
    "min_netincome_cagr_3y_pct":  12,   # Try: 8 (lenient) | 12 (default) | 18 (strict)

    # ── Gate 5: Sector-Relative Valuation ─────────────────────────────────────
    "max_pe_vs_sector_mult":  1.2,  # 1.2 = up to 20% above sector PE median
    "min_roe_vs_sector_mult": 0.9,  # 0.9 = ROE must be ≥ 90% of sector median
    # ⚠️  PEG data is frequently missing for Indian stocks on yfinance.
    # Keep at 2.0 to avoid silently over-filtering on sparse data.
    "max_peg_ratio": 2.0,           # Try: 1.5 (strict) | 2.0 (default for India)

    # ── Gate 6: Financials (Banks, NBFCs, Insurance) ──────────────────────────
    "max_pb_financials": 2.5,       # Try: 1.5 (cheap banks) | 2.5 (default) | 3.5 (lenient)
    "financial_sectors": ["Bank", "NBFC", "Insurance", "Financial Services", "Finance"],

    # ── Fetching ──────────────────────────────────────────────────────────────
    # Too high = Yahoo Finance rate-limit errors
    "fetch_max_workers": 15,        # Try: 5 (safe) | 15 (default) | 20 (fast/risky)

    # ── Technical ─────────────────────────────────────────────────────────────
    "sma_short":  50,   # Try: 20 (fast) | 50 (default)
    "sma_long":   200,  # Try: 100 (faster) | 200 (default — institutional standard)
    "rsi_period": 14,   # Try: 9 (fast) | 14 (default — Wilder) | 21 (smooth)

    # ── India 10Y Bond Yield ──────────────────────────────────────────────────
    # Update periodically. Check: https://www.rbi.org.in
    # As of March 2026: ~6.7% (RBI rate-cutting cycle since late 2025)
    "india_10y_bond_yield_pct": 6.7,

    # ── Output ────────────────────────────────────────────────────────────────
    "top_n_results": 15,                        # Try: 5 | 10 | 15 (default) | 25
    "output_csv":    "Top_Stocks_Report.csv",   # Add date for archives
}
```

---

## 📖 Understanding the 6-Category Scorecard

### 🔵 Performance
**What:** How the stock's 1-year price return compares to the Nifty 50 index.

| Tag | Meaning |
|---|---|
| `High` | Stock returned 15%+ MORE than Nifty over 1 year |
| `Avg` | Stock returned roughly in line with the market |
| `Low` | Stock significantly underperformed the Nifty |

---

### 🔵 Valuation
**What:** Whether the stock's PE ratio is cheap or expensive vs its sector peers.

| Tag | Meaning |
|---|---|
| `Attractive` | PE is 20%+ below sector median — potential bargain |
| `Avg` | PE is roughly in line with peers (within ±20%) |
| `Expensive` | PE is 20%+ higher than sector median |

*Note: For Banks and NBFCs, P/B ratio is used instead of P/E.*

---

### 🔵 Growth
**What:** How fast the company's revenue and profits have grown over 3 years.

| Tag | Meaning |
|---|---|
| `High` | Average of Revenue + Profit CAGR > 15% |
| `Avg` | Average CAGR between 5–15% |
| `Low` | Growth is stagnant, below 5%, or negative |

---

### 🔵 Profitability
**What:** How efficiently the company generates returns from shareholders' money.

| Tag | Meaning |
|---|---|
| `High` | ROE > 15% with stable or improving margins |
| `Avg` | ROE between 8–15% |
| `Low` | ROE below 8% or company has posted recent losses |

---

### 🔵 Entry Point
**What:** Whether NOW is a technically good time to buy, based on RSI and SMAs.

| Tag | Meaning |
|---|---|
| `Good` | RSI below 40 or in neutral zone above 200 SMA — not overbought |
| `Average` | RSI between 55–70 — moderate momentum, proceed carefully |
| `Risky` | RSI above 70 — stock is overbought, pullback risk |

The **RSI (Relative Strength Index)** measures recent price momentum on a
0–100 scale. Below 30 = oversold. Above 70 = overbought. Calculated over
14 days using Wilder's smoothing method.

---

### 🔵 Red Flags
**What:** Automatic detection of financial warning signs.

| Tag | Meaning |
|---|---|
| `Low` | No issues found — clean bill of health |
| `Moderate` | 1 potential concern detected |
| `High` | 2 or more serious issues detected |

**What we check:**
- Debt-to-Equity > 1.5 (dangerously high leverage)
- Current Ratio < 1.0 (can't pay short-term bills)
- Negative Operating Cash Flow (not generating real cash)
- Loss in any of the last 4 years (inconsistent profitability)
- Earnings quality concern (reported profit > actual cash flow)
- Gross margin declining sharply year-over-year

---

## 📊 Understanding the Peer Comparison Table

For each top stock, the engine shows the 3 closest sector peers by market cap:

```
  📊  SECTOR PEERS (Information Technology)
  Ticker               PE      PB      ROE    1Y Ret
  --------------------  ------  ------  --------  --------
  TCS.NS               28.5    12.1     42.5%     18.2%  ← THIS STOCK
  INFY.NS              25.1    10.8     32.2%     12.4%
  WIPRO.NS             22.4     4.2     17.8%      5.1%
  HCLTECH.NS           24.8     7.1     23.5%     16.8%
```

Instantly answers: Is this stock cheap vs peers? Growing faster? Better ROE?

---

## 📁 Output Files

| File | Contents |
|---|---|
| `Top_Stocks_Report.csv` | Top N ranked stocks with all metrics and scorecard tags |
| `raw_data.csv` | Full universe data before any filters — useful for your own analysis |
| `cache/` folder | Auto-created. Cached fundamentals (JSON per ticker). Safe to delete for a fresh start |

---

## ⚡ Performance & Speed

| Run Type | Estimated Time |
|---|---|
| First run — 500 stocks, no cache | 10–15 minutes |
| Second run same day (cache hit) | 2–3 minutes |
| Nifty50 only | Under 1 minute |
| Top800_Custom, no cache | 20–25 minutes |
| Top800_Custom, cache hit | 3–5 minutes |

**Why is it fast from the second run?**
The script caches all fundamental data (PE, ROE, CAGR, margins etc.) locally
for 7 days. These change slowly — quarterly at most. Only live price data
(current price, RSI, SMAs, volume) is fetched fresh every run.

---

## 🧩 Project Structure

```
📁 nse_screener/
│
├── nse_screener.py          ← Main script (everything in one file)
│
├── Top_Stocks_Report.csv    ← Auto-generated: top ranked stocks
├── raw_data.csv             ← Auto-generated: full universe data
│
└── cache/                   ← Auto-generated: data cache
    ├── universe_Nifty500.csv
    ├── universe_Nifty500_meta.json
    ├── master_fundamentals.pkl
    ├── cache_metadata.json
    └── tickers/
        ├── TCS_NS_fundamental.json
        ├── HDFCBANK_NS_fundamental.json
        └── ...
```

---

## ❓ Frequently Asked Questions

**Q: My second run is still taking 10–15 minutes. Cache is not working.**
A: Your `cache_schema_version` in CONFIG does not match the script version.
Make sure it says `"5.1"` exactly. Even a tiny mismatch (e.g. `"5.0"`)
forces a complete re-fetch on every single run.

**Q: I get a `FileNotFoundError` about the universe CSV.**
A: The script downloads it automatically from NSE's website. If your
internet blocks niftyindices.com, download the CSV manually from
[niftyindices.com](https://www.niftyindices.com) and save it as
`Nifty500.csv` in the same folder as the script.

**Q: Some stocks show "N/A" in the scorecard.**
A: yfinance did not have enough historical data for that metric. Common for
recently listed companies or stocks with limited coverage on Yahoo Finance.

**Q: The script found 0 stocks after filtering.**
A: Your filters are too strict. The script will print specific suggestions.
Quick fixes: reduce `min_roe_pct` to 10, reduce `min_netincome_cagr_3y_pct`
to 8, increase `max_debt_to_equity` to 1.0, or set
`reject_shrinking_margins` to False.

**Q: Can I run this on Google Colab?**
A: Yes. Upload `nse_screener.py` to Colab and run:
```python
!pip install yfinance requests urllib3
!python nse_screener.py
```

**Q: Why do some sector medians look off?**
A: Sectors with fewer than 5 stocks use the overall market median as a
fallback instead of the sector median, which is statistically unstable
with small samples.

**Q: PEG ratio is filtering out too many good stocks.**
A: This is a known issue with yfinance for Indian stocks — PEG data is
frequently missing. Increase `max_peg_ratio` to `2.0` (already the
recommended default in v5.1).

**Q: Top800_Custom seems to skip some stocks.**
A: If one of the three sub-indexes (Nifty500, Midcap150, Smallcap250)
fails to download, the script continues with the others and warns you.
Check your internet connection or try again after a few minutes.

---

## 🤝 Contributing

Contributions are welcome! Priority areas:

- Additional data sources beyond yfinance for better Indian stock coverage
- Promoter pledge % from BSE shareholding pattern data
- Streamlit web dashboard frontend
- Simple backtesting: "How did last month's top picks perform?"
- Better handling of corporate actions (splits, bonuses) in CAGR math
- Nifty sectoral index support (NiftyIT, NiftyBank, NiftyPharma etc.)

Please fork the repo, make your changes, and submit a Pull Request with a
clear description of what you improved.

---

## ⚖️ Disclaimer

**This software is strictly for educational and research purposes only.**

The outputs, scorecards, rankings, and recommendations generated by this
script do NOT constitute financial advice, investment recommendations, or
any solicitation to buy or sell any securities.

Stock market investments are subject to market risks. Past performance is
not indicative of future results. The data provided by Yahoo Finance (via
yfinance) may be delayed, inaccurate, or incomplete.

Always consult a SEBI-registered financial advisor and conduct your own
thorough due diligence before making any investment decisions.

**The creators and contributors of this open-source tool assume absolutely
no liability for any financial losses arising from the use of this software.**

---

*Built with ❤️ for the Indian investing community.*
*Version 5.1 | March 2026*
