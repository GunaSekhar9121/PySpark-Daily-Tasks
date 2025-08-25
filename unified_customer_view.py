import pandas as pd
from transform_customers import clean_customer_data

def create_unified_view():
    df_customers = pd.read_csv("/Users/gunasekharsiddabathuni/env/Datasets/us_customer_data.csv")
    df_orders = pd.read_csv("/Users/gunasekharsiddabathuni/env/Datasets/order_data.csv")

    df_customers = clean_customer_data(df_customers)

    df_unified = pd.merge(df_customers, df_orders, on='customer_id', how='inner')

    df_unified.to_csv("unified_customer_view.csv", index=False)
    print("Unified customer view created.")

if __name__ == "__main__":
    create_unified_view()
