# 📈 NSE Stock Recommendation Engine (v4.0)

An open-source, professional-grade stock screener and recommendation engine for the Indian Stock Market (NSE). 

Unlike basic screeners that rely on static thresholds (e.g., "PE < 20"), this tool uses **Tickertape-style multi-dimensional analysis**. It compares stocks against their specific sector peers, computes historical multi-year growth from financial statements, flags "Smart Money" institutional buying, and outputs a dynamic "Bull Case" explaining *why* a stock is highly ranked right now.

---

## ✨ Key Features

*   **🧠 Multi-Axis Grading System:** Stocks are scored out of 30 across three distinct pillars: **Quality** (10), **Valuation** (10), and **Momentum/Confidence** (10). Visual terminal bars show you exactly where a stock excels.
*   **⚡ Smart Two-Layer Caching:** Fundamental data (which changes quarterly) is cached for 7 days to speed up execution. However, **Price and Technical data is always fetched LIVE** on every run, ensuring your technical signals are never stale.
*   **📊 Computed Multi-Year Growth (CAGR):** Instead of relying on buggy snapshot data, the engine downloads historical income statements to mathematically calculate 3-Year and 5-Year Revenue and Profit growth.
*   **🏦 Dedicated Financial Sector Logic:** Banks and NBFCs are evaluated using entirely different math (focusing on P/B vs Sector and ROE vs Sector, ignoring Debt-to-Equity).
*   **🧭 Sector-Relative Valuation:** A PE of 25 is cheap for FMCG but expensive for Steel. This engine calculates real-time sector medians and scores stocks based on how they compare to their direct peers.
*   **💡 The "Bull Case" Generator:** The engine dynamically writes a plain-English explanation of why a stock is recommended (e.g., *"Trading at 20% discount to sector PE despite exceptional ROE of 24%..."*).
*   **🎭 Pre-Built Archetypes:** Not a quantitative expert? Just change one word in the config to run pre-built strategies like `Growth_Bargain`, `Cash_Machine`, or `Quality_Compounder`.

---

## 🚀 Installation & Setup

### 1. Prerequisites
You will need Python 3.9+ installed on your system. 

### 2. Install Required Libraries
Open your terminal or command prompt and install the dependencies:
```bash
pip install pandas numpy yfinance requests urllib3
```

### 3. Download and Run
Clone this repository or download the `nse_screener.py` file, then run it:
```bash
python nse_screener.py
```

*Note: The first run will take 10–15 minutes as it downloads 5 years of financial history for 500 stocks. Subsequent runs on the same day will take **under 15 seconds** thanks to the smart caching engine!*

---

## 🛠️ How to Use & Customize (The `CONFIG` Block)

Open `nse_screener.py` in any text editor. At the very top, you will find the `CONFIG` block. You can change these settings to fit your investing style.

### Changing the Universe
By default, the script scans the top 500 companies in India. You can change this to scan midcaps or smallcaps:
```python
"universe": "Nifty500",  # Options: Nifty50, Nifty100, Midcap150, Smallcap250, Top800_Custom
```

### Using Pre-Built Archetypes (Easiest Method)
If you don't want to tweak 20 different financial filters manually, just change the `archetype`:
```python
"archetype": "Growth_Bargain", 
```
**Available Archetypes:**
*   `Custom` (Uses your manual filter settings)
*   `Growth_Bargain` (High earnings growth, PE below sector average)
*   `Cash_Machine` (Zero debt, massive operating cash flow, high ROE)
*   `Low_Debt_Midcap` (Scans Midcap150 for clean balance sheets)
*   `Quality_Compounder` (Consistent high ROE/ROCE, 15%+ multi-year growth)
*   `Near_52W_Low` (Beaten-down stocks that still have good fundamentals)

---

## 🖥️ Understanding the Output

The script outputs a beautifully formatted terminal interface and saves a `Top_Stocks_Report.csv` to your folder. 

Here is an example of what a final recommendation looks like in the terminal:

```text
======================================================================
  #1  TCS.NS                    Information Technology
  🟢 STRONG  |  Final Score: 26.5/30
  Quality: 9.0/10 [█████████░]
  Valuation: 8.5/10 [████████░░]
  Momentum: 9.0/10 [█████████░]
----------------------------------------------------------------------
  PRICE & TECHNICALS
    CMP     : ₹4120.5      Mkt Cap : ₹14,50,000 Cr
    52W     : ₹3800.0 — ₹4250.0   Below High: 3.0%
    Trend   : Uptrend ✅          Vol Surge: ✅ Yes
    SMA50   : ₹4010.5      SMA200  : ₹3950.0
----------------------------------------------------------------------
  VALUATION (Stock vs Sector)
    P/E     : 28.5         Sector P/E : 32.1
    P/B     : 12.1         Sector P/B : 10.5
    PEG     : 1.1          EPS Yield  : 3.5%  ❌ Below Bond (7.1%)
    FCF Yield: 4.2%
----------------------------------------------------------------------
  FUNDAMENTALS
    ROE     : 42.5%        Sector ROE : 22.0%
    D/E     : 0.0          Curr Ratio : 2.5
    Rev CAGR: 3Y=15.2%     5Y=14.8%
    PAT CAGR: 3Y=18.5%     5Y=16.2%
    Op. CF  : ₹45,000 Cr   FCF    : ₹42,000 Cr
    Inst. Hold: 28.5%      Data: 5 years
----------------------------------------------------------------------
  💡 THE BULL CASE
    Trading near 52-week highs with 18.5% 3Y net income CAGR — 
    indicates strong business momentum and market confidence.
----------------------------------------------------------------------
  ⚠️  DISCLAIMER
    Sector risk (Information Technology):
    Global IT spending cuts, USD/INR volatility, client concentration.
    THIS IS NOT FINANCIAL ADVICE. Always do your own due diligence.
======================================================================
```

---

## ⚙️ How the Engine Works (The Pipeline)

For developers and quants, here is the architecture of the script:

1.  **Universe Load:** Fetches the live index constituents directly from the NSE website.
2.  **Cache Engine:** Checks `cache/tickers/` for fundamental data younger than 7 days.
3.  **Live Price Fetch:** Batches a request to Yahoo Finance for live OHLCV data.
4.  **Math Engine:** Computes 3Y/5Y CAGRs and calculates Sector Medians.
5.  **Filter Engine (Hard Rejects):** 6 sequential gates drop companies with negative cash flow, shrinking margins, excessive debt, or poor liquidity.
6.  **Scoring Engine:** Surviving stocks are graded on Quality, Valuation, and Momentum.
7.  **Explainability:** Generates a dynamic "Bull Case" based on specific triggered logic conditions.

---

## 🤝 Contributing
Contributions are welcome! If you want to add new indicators (e.g., RSI, MACD), improve the financial extraction logic for Indian stocks, or add new Archetypes, please fork the repository and submit a Pull Request.

---

## ⚖️ Disclaimer
**This software is for educational and research purposes only.** The outputs generated by this script do not constitute financial advice, investment recommendations, or an offer to buy or sell any securities. Stock market investments are subject to market risks. Always consult a registered financial advisor and conduct your own due diligence before making any investment decisions. The creators of this open-source tool assume no liability for any financial losses.