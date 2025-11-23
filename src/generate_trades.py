import csv
from datetime import date,timedelta
import random
from pathlib import Path
import sqlite3


#File where simulated data will be saved

OUTPUT_PATH = Path(__file__).resolve().parents[1]/"data"/"trades.csv"
DB_PATH = Path(__file__).resolve().parents[1]/"data"/"trades.db"
print(OUTPUT_PATH)
#Range of dates of trading activity

END_DATE = date.today()
START_DATE = END_DATE - timedelta(days = 180)

#Example Cusips UMBS30

CUSIPS = [
        ("912UMBS30A", "UMBS30"),
    ("912UMBS30B", "UMBS30"),
    ("912UMBS30C", "UMBS30"),
]

def daterange(start: date, end: date):
    """Generates dates from Start to End date, by one day"""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)

def is_business_day(d: date) -> bool:
    """Returns True if d is weekday."""
    return d.weekday() < 5

headers = ["trade_id", "trade_date", "cusip", "product", "amount", "price"]

def generate_trades_csv():
    with open(OUTPUT_PATH,'w',newline= '') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        trade_id = 1
        for day in daterange(START_DATE,END_DATE):
            if is_business_day(day) is True:
                for num in range(random.randint(0,10)):
                    trade_id +=1
                    cusip,product = random.choice(CUSIPS)
                    qty = ["1000000", "2000000", "3000000", "4000000", "5000000",
                                           "6000000", "7000000", "8000000", "9000000", 
                                           "10000000","12000000", "15000000", "18000000",
                                            "20000000", "22000000","25000000", "30000000", 
                                            "35000000", "40000000", "50000000"]
                    amount = random.choice(qty)
                    price = random.normalvariate(mu = 100, sigma=0.2)
                    writer.writerow([trade_id, day.isoformat(),cusip,product,amount,price])

if __name__ == "__main__":
    generate_trades_csv()


def init_db():
    #set up connection
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    #Deefine SQL to create table if it doesn't exists

    create_table_sql = """CREATE TABLE IF NOT EXISTS trades (
    trade_id INTEGER PRIMARY KEY,
    trade_date TEXT NOT NULL,
    cusip TEXT NOT NULL,
    product_type TEXT NOT NULL,
    side TEXT CHECK(side IN ('BUY', 'SELL')) NOT NULL,
    quantity REAL NOT NULL,
    price REAL NOT NULL
);
"""

    cur.execute(create_table_sql)
    conn.commit()
    conn.close()

def load_trades_from_csv():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("DELETE FROM trades")

    with open(OUTPUT_PATH, 'r', newline='') as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)

        for row in csv_reader:
#            print(row, len(row))
            cur.execute(
                """INSERT INTO trades (trade_id, trade_date, cusip, product_type, side, quantity, price)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    row[0],      # trade_id
                    row[1],      # trade_date
                    row[2],      # cusip
                    row[3],      # product_type
                    "BUY",       # side (lo hardcodeamos por ahora)
                    row[4],      # quantity
                    row[5],      # price
                )
            )

    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
    load_trades_from_csv()


def get_trades(limit=10):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT * FROM trades LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows

    
get_trades()


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

def summary_volume():
    """Returns trade volume by each date"""
    conn=sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT trade_date, SUM(quantity), COUNT(trade_id) from trades " \
    "GROUP BY trade_date")
    rows = cur.fetchall()
    conn.close()
    return rows