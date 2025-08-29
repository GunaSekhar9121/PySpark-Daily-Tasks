from Connection_sql import get_engine
import pandas as pd

DB_NAME = "Transactions_DB"
engine = get_engine(DB_NAME)

# 1) Load customers + orders into DataFrames
customers = pd.read_sql("SELECT * FROM cleaned_customers;", engine)
orders = pd.read_sql("SELECT * FROM orders;", engine)

# 2) Merge on customer_id
df = pd.merge(customers, orders, on="customer_id", how="left")

# 3) Aggregate performance metrics per customer
customer_perf = (
    df.groupby(["customer_id", "name", "email", "loyalty_status", "country"], dropna=False)
    .agg(
        total_orders=("order_id", "nunique"),
        total_spent=("order_amount", lambda x: x[df.loc[x.index, "order_status"]=="Completed"].sum()),
        completed_orders=("order_status", lambda x: (x=="Completed").sum()),
        cancelled_orders=("order_status", lambda x: (x=="Cancelled").sum())
    )
    .reset_index()
    .sort_values("total_spent", ascending=False)
)

print(customer_perf.head())
customer_perf.to_csv("customer_sales_performance.csv", index=False)
