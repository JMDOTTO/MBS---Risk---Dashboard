# Fixed Income MBS Trades – Synthetic Risk & Query Sandbox

This repository is a small **Python + SQLite lab** to play with synthetic
**Agency MBS (UMBS)** trade data.

It was built to practice:

- Python for data pipelines
- SQL (SQLite) for querying and aggregation
- Basic trade analytics for fixed income / MBS

The idea is to have a lightweight “mini backoffice + analytics” environment
that you can extend later into a full **MBS Risk Lab** (simulations, VaR, etc.).

---

## Features

- 🔹 **Synthetic trade generator** (UMBS 30y, multiple CUSIPs)
- 🔹 **SQLite database** with a `trades` table:
  - `trade_id`
  - `trade_date`
  - `cusip`
  - `product_type`
  - `side`
  - `quantity`
  - `price`
- 🔹 **ETL pipeline**:
  - generate CSV with trades
  - load CSV into SQLite
- 🔹 **Query module** (`queries.py`) with handy functions:
  - `get_trades(limit)` – sample trades
  - `pool_activity(cusip)` – trades by CUSIP, ordered by date
  - `trades_by_dates(start, end)` – trades in a date range
  - `volume_per_cusip(cusip)` – total volume + number of trades for a CUSIP
  - `volume_by_day(trade_date)` – volume + number of trades on a given date
  - `dates_with_trades()` – distinct trade dates
  - `summary_by_date()` – volume + # of trades per date
  - `summary_by_cusip()` – volume + # of trades per CUSIP

---

## Project structure

```text
fixed-income-risk-dashboard/
│
├── src/
│   ├── generate_trades.py   # generates synthetic CSV (trades.csv)
│   ├── database.py          # DB paths, init_db, load_trades_from_csv
│   ├── queries.py           # all SQL SELECTs / summaries
│   └── main.py              # demo entrypoint (runs pipeline + prints summaries)
│
├── data/
│   ├── trades.csv           # synthetic trades (generated)
│   └── trades.db            # SQLite database (generated)
│
└── README.md
