
from Connection_sql import get_engine
import pandas as pd

DB_NAME = "Transactions_DB"
TABLE   = "orders"   
def load_orders(engine) -> pd.DataFrame:
    sql = f"""
    SELECT 
        order_id,
        customer_id,
        order_date,
        order_amount,
        order_status,
        product_category
    FROM {TABLE};
    """
    return pd.read_sql(sql, engine)
def sales_by_month(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df[df["order_status"] == "Completed"]
        .assign(order_month=pd.to_datetime(df["order_date"]).dt.to_period("M").astype(str))
        .groupby("order_month")
        .agg(total_sales=("order_amount", "sum"),
             total_orders=("order_id", "nunique"))
        .reset_index()
        .sort_values("order_month")
    )

def sales_by_category(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df[df["order_status"] == "Completed"]
        .groupby("product_category")
        .agg(category_sales=("order_amount", "sum"),
             total_orders=("order_id", "nunique"))
        .reset_index()
        .sort_values("category_sales", ascending=False)
    )

def top_customers(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    return (
        df[df["order_status"] == "Completed"]
        .groupby("customer_id")
        .agg(total_spent=("order_amount", "sum"),
             order_count=("order_id", "nunique"))
        .reset_index()
        .sort_values("total_spent", ascending=False)
        .head(top_n)
    )

def orders_by_status(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("order_status")
        .agg(num_orders=("order_id", "nunique"),
             total_value=("order_amount", "sum"))
        .reset_index()
        .sort_values("num_orders", ascending=False)
    )

def category_by_status(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["product_category", "order_status"])
        .agg(num_orders=("order_id", "nunique"),
             total_value=("order_amount", "sum"))
        .reset_index()
        .sort_values(["product_category", "order_status"])
    )

if __name__ == "__main__":
    engine = get_engine(DB_NAME)

    df = load_orders(engine)

    df_month   = sales_by_month(df);     df_month.to_csv("sales_by_month.csv", index=False)
    df_cat     = sales_by_category(df);  df_cat.to_csv("sales_by_category.csv", index=False)
    df_topcust = top_customers(df, 10);  df_topcust.to_csv("top_customers.csv", index=False)
    df_status  = orders_by_status(df);   df_status.to_csv("orders_by_status.csv", index=False)
    df_catstat = category_by_status(df); df_catstat.to_csv("category_by_status.csv", index=False)

    print("Orders analysis complete. Results saved as CSVs:")
    print("sales_by_month.csv")
    print("sales_by_category.csv")
    print("top_customers.csv")
    print("orders_by_status.csv")
    print("category_by_status.csv")
