from database import init_db, load_trades_from_csv
from queries import summary_by_cusip, summary_by_date, get_trades


def print_header(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


if __name__ == "__main__":
    # 1) Inicializar base y cargar datos
    init_db()
    load_trades_from_csv()

    # 2) Mostrar algunos ejemplos de consultas
    print_header("Sample trades (first 5 rows)")
    for row in get_trades(5):
        print(row)

    print_header("Summary by CUSIP (volume, number of trades)")
    for cusip, volume, n_trades in summary_by_cusip():
        print(f"{cusip}: volume={volume:.0f}, trades={n_trades}")

    print_header("Summary by date (volume, number of trades)")
    for trade_date, volume, n_trades in summary_by_date():
        print(f"{trade_date}: volume={volume:.0f}, trades={n_trades}")
