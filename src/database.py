from pathlib import Path
import csv
import sqlite3


#File where simulated data will be saved

OUTPUT_PATH = Path(__file__).resolve().parents[1]/"data"/"trades.csv"
DB_PATH = Path(__file__).resolve().parents[1]/"data"/"trades.db"

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
