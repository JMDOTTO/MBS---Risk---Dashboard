from datetime import date,timedelta
import random
from pathlib import Path
import sqlite3



DB_PATH = Path(__file__).resolve().parents[1]/"data"/"trades.db"


def get_trades(limit=10):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT * FROM trades LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


def pool_activity(cusip):
    conn=sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT * FROM trades WHERE CUSIP = ? ORDER BY trade_date DESC", (cusip,))
    rows = cur.fetchall()
    conn.close()
    return rows

def trades_by_dates(act_from, act_to):
    conn=sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT * FROM trades where trade_date BETWEEN ? AND ?", (act_from,act_to))
    rows = cur.fetchall()
    conn.close()
    return rows

def volume_per_cusip(cusip):
    conn=sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT SUM(quantity), COUNT(trade_id) from trades " \
    "WHERE CUSIP = ? ", (cusip,))
    rows = cur.fetchone()
    conn.close()
    return rows

def volume_by_day(trade_date):
    conn=sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT SUM(quantity), COUNT(trade_id) from trades " \
    "WHERE trade_date = ?", (trade_date,))
    rows = cur.fetchone()
    conn.close()
    return rows

def dates_with_trades():
    """Lists all dates where there was trade activity"""
    conn=sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT trade_date from trades")
    rows = cur.fetchall()
    conn.close()
    return rows

def summary_by_date():
    """Returns trade volume by each date"""
    conn=sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT trade_date, SUM(quantity), COUNT(trade_id) from trades " \
    "GROUP BY trade_date")
    rows = cur.fetchall()
    conn.close()
    return rows

def summary_by_cusip():
    """Returns trade volume by CUSIP"""
    conn=sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT cusip, SUM(quantity), COUNT(trade_id) from trades " \
    "GROUP BY cusip")
    rows = cur.fetchall()
    conn.close()
    return rows