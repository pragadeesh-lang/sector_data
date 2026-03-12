# Sectoral Intelligence Dashboard 📈

An exhaustive, metric-driven dashboard for monitoring Indian macroeconomic and sectoral trends (Auto, Banking & Mutual Funds).

## 🚀 Live Dashboard
Hosted on GitHub Pages: `https://<your-username>.github.io/sector_data/`

## 🛠️ How to Update Data
The dashboard is powered by a Python data pipeline that fetches data from AMFI, VAHAN, and RBI.

### 1. Requirements
Ensure you have Python 3.9+ installed.
```bash
pip install -r requirements.txt
```

### 2. Running the Pipeline
To ingest the latest data and update the dashboard:
```bash
# Update all sectors (AMFI, RBI, VAHAN)
python3 orchestrator.py --sectors amfi rbi vahan
```

### 3. Deploying Changes
After running the orchestrator, a fresh `data/dashboard_data.js` is generated. To push these updates live:
```bash
git add data/dashboard_data.js
git commit -m "Update: Latest Feb 2026 data"
git push origin main
```

## 📊 Sector Coverage
- **Banking & Payments**: UPI, IMPS, RTGS, NEFT, Card Transactions, and Bank Credit.
- **Auto (VAHAN)**: Monthly vehicle registrations by class and fuel type.
- **AMC (AMFI)**: Mutual Fund industry aggregates (AUM, Inflows, Folios).

---
*Created with the help of Antigravity AI.*
