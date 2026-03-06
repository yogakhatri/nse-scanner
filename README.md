# 📈 NSE Stock Screener

A self-contained Python script that automatically identifies fundamentally strong, undervalued, and high-growth NSE stocks. It analyzes the data and provides a plain-English verdict on **why** each stock is a good candidate, along with sector-specific risk disclaimers.

---

## 🚀 Features

- **Broad Coverage:** Screens the Nifty 50, 100, 500, Midcap 150, or Smallcap 250 universes.
- **5-Gate Filtering:** Sequentially filters by Liquidity → Quality → Growth → Valuation → Financials.
- **Smart Scoring:** Ranks surviving stocks using a weighted formula (Growth 30%, Quality 30%, Valuation 20%, Balance Sheet 20%).
- **Sector Aware:** Applies different fundamental rules for Banks/NBFCs (using P/B instead of PEG/Debt).
- **Human-Readable Verdicts:** Generates formatted, easy-to-read text boxes for your terminal.
- **Zero Cost:** Uses 100% free data via `yfinance` and official NSE CSVs. No API keys required.

---

## 💻 Complete Setup & Run Instructions

Follow these exact steps in your terminal to set up the environment and run the script safely without affecting your system-wide Python installation.

### Step 1: Prepare the Project Folder
Open your terminal and run the following commands to create a folder and navigate into it:
```bash
mkdir nse_screener
cd nse_screener
```
*(Now, save the provided `nse_screener.py` file into this exact folder).*

### Step 2: Create a Local Virtual Environment
Create an isolated Python environment inside the folder:

**For Mac/Linux:**
```bash
python3 -m venv .venv
```
**For Windows:**
```cmd
python -m venv .venv
```

### Step 3: Activate the Virtual Environment
You must activate the environment every time you open a new terminal to run the script.

**For Mac/Linux:**
```bash
source .venv/bin/activate
```
**For Windows (Command Prompt):**
```cmd
.venv\Scripts\activate.bat
```
*(You will know it worked if you see `(.venv)` at the beginning of your terminal prompt).*

### Step 4: Install Required Libraries
With the environment active, install the necessary packages:
```bash
pip install pandas numpy yfinance requests
```

### Step 5: Run the Script
Ensure your terminal is in the `nse_screener` folder and the `(.venv)` is active, then run:
```bash
python nse_screener.py
```

> ⏱️ **Note on Speed:** Fetching 500 stocks one by one with a polite delay (to avoid IP bans) will take about **20 to 35 minutes**. Let it run in the background.

---

## ⚙️ Configuration (How to Tweak It)

All settings live in the `CONFIG` dictionary at the very top of `nse_screener.py`. Open the file in any text editor to change them.

### Changing the Universe
By default, it scans the `Nifty500`. To change this, edit line 1 of the `CONFIG`:
```python
"universe": "Midcap150"  # Options: Nifty50, Nifty100, Nifty500, Midcap150, Smallcap250
```

### Adjusting Strictness
If **0 stocks** survive the filters (meaning the market is currently expensive or your rules are too strict), relax the thresholds:
- Change `"min_roe_pct": 15` to `12`
- Change `"max_peg_ratio": 1.5` to `2.0`
- Change `"min_profit_cagr_3yr_pct": 15` to `10`

---

## 📁 Output Files

When the script finishes, it generates two files in the same folder:

1. **`raw_data.csv`** — The raw dump of all metrics for every stock fetched (useful for your own custom spreadsheet analysis).
2. **`Top_Stocks_Report.csv`** — The final, ranked list of surviving stocks with their 0-100 composite scores.

The script will also print a highly readable report directly in your terminal.

---

## 🖥️ Sample Terminal Output

```text
==================================================================
  🏢  TATACONSUM.NS        |  Sector: Consumer Goods
  💯  Composite Score : 84.32 / 100
  💰  Current Price   : ₹920.50
  📊  52W High/Low    : ₹1050.00 / ₹760.00
  📉  Below 52W High  : 12.3%
------------------------------------------------------------------
  KEY FUNDAMENTALS
    P/E  : 38.2       P/B  : 6.1        PEG  : 1.1
    ROE  : 22.1%      D/E  : 0.3        Rev Growth: 18.4%
    Earnings Growth : 22.6%    Op. Cash Flow: ₹1,240 Cr
------------------------------------------------------------------
  ✅  VERDICT
    TATACONSUM.NS is a strong candidate with robust earnings growth
    of 22.6%, healthy ROE of 22.1%, and an attractive valuation (PEG: 1.1).
    This stock has good potential to compound wealth over the
    medium to long term based on current fundamentals.
------------------------------------------------------------------
  ⚠️   DISCLAIMER
    This outlook is valid under normal market conditions.
    Risks for Consumer Goods sector: Input cost inflation, rural demand
    slowdown, or GST rate changes.
    THIS IS NOT FINANCIAL ADVICE. Do your own due diligence.
==================================================================
```

---

## 🐛 Troubleshooting

| Error Message | Solution |
| :--- | :--- |
| `ModuleNotFoundError` | You forgot to activate the virtual environment (`source .venv/bin/activate`) before running. |
| `ConnectionError` on NSE URL | NSE occasionally blocks scraper traffic. The script will auto-fallback to a local `Nifty500.csv` if you download it manually from the NSE website to your folder. |
| `No stocks passed all filters` | The market is trading at a premium. Relax the `CONFIG` valuation metrics (like PE or PEG). |

---

## ⚠️ Disclaimer

This tool is built for **research, automation, and educational purposes only**. It does not constitute financial advice. The Indian stock market is subject to volatility. Always perform your own due diligence, analyze the technical charts, and consider consulting a SEBI-registered financial advisor before executing trades.
```