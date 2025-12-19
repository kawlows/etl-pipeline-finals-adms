import pandas as pd
import sqlite3
import os


def generate_analytics():
    """Generate 5 key insights from the consolidated BIG_TABLE."""
    # Use the same DB as load.py
    db_path = os.path.join("data", "Staging", "etl_database.db")
    conn = sqlite3.connect(db_path)

    # Read the consolidated table
    big_table = pd.read_sql_query("SELECT * FROM BIG_TABLE", conn)

    # Clean column names (strip quotes/spaces if any)
    big_table.columns = [c.strip().strip("'").strip('"') for c in big_table.columns]

    print("\n" + "=" * 70)
    print("ANALYTICS: 5 KEY INSIGHTS FROM ETL CONSOLIDATED DATA")
    print("=" * 70)

    # 1. Total revenue by store (price is already in USD)
    print("\n1. TOTAL REVENUE BY STORE (in USD):")
    print("-" * 70)
    revenue_by_store = big_table.groupby("store")["price"].sum()
    for store, revenue in revenue_by_store.items():
        print(f" {store}: ${revenue:,.2f}")
    total_revenue = revenue_by_store.sum()
    print(f" TOTAL: ${total_revenue:,.2f}")

    # 2. Number of transactions by store (invoice_id)
    print("\n2. TRANSACTION VOLUME BY STORE:")
    print("-" * 70)
    transactions_by_store = big_table.groupby("store")["invoice_id"].nunique()
    for store, count in transactions_by_store.items():
        print(f" {store}: {count:,} transactions")
    print(f" TOTAL: {transactions_by_store.sum():,} transactions")

    # 3. Average transaction value by store
    print("\n3. AVERAGE TRANSACTION VALUE BY STORE (in USD):")
    print("-" * 70)
    avg_transaction_value = big_table.groupby("store")["price"].mean()
    for store, avg_value in avg_transaction_value.items():
        print(f" {store}: ${avg_value:,.2f}")
    print(f" OVERALL AVERAGE: ${big_table['price'].mean():,.2f}")

    # 4. Top 3 items by quantity (product_id as proxy; adapt if you have name)
    print("\n4. TOP 3 BEST-SELLING PRODUCTS (by quantity):")
    print("-" * 70)
    top_items = big_table.groupby("product_id")["quantity"].sum().nlargest(3)
    for idx, (pid, qty) in enumerate(top_items.items(), 1):
        print(f" {idx}. Product {pid}: {qty:,} units")

    # 5. Market share analysis by store
    print("\n5. MARKET SHARE ANALYSIS:")
    print("-" * 70)
    store_revenue = big_table.groupby("store")["price"].sum()
    store_market_share = store_revenue / store_revenue.sum() * 100
    for store, share in store_market_share.items():
        print(f" {store}: {share:.1f}% of total revenue")

    print("\n" + "=" * 70)
    print("SUMMARY STATISTICS")
    print("=" * 70)
    print(f"Total Records in BIG_TABLE: {len(big_table):,}")
    print(f"Total Items Sold: {big_table['quantity'].sum():,} units")
    print(
        f"Average Items per Transaction: "
        f"{big_table.groupby('invoice_id')['quantity'].sum().mean():.2f} units"
    )
    print(f"Total Revenue: ${big_table['price'].sum():,.2f}")
    print(f"Number of Unique Products: {big_table['product_id'].nunique()}")
    print(f"Number of Stores: {big_table['store'].nunique()}")
    print("=" * 70 + "\n")

    conn.close()


if __name__ == "__main__":
    generate_analytics()
