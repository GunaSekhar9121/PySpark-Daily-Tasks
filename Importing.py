import pandas as pd
from sqlalchemy import create_engine
from db_connection import get_sql_connection


def load_csv_to_sql():
    conn = get_sql_connection()
    if conn:
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect="
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=192.168.1.249,1433;"
            f"DATABASE=Python_Pipelines;"
            f"UID=pythonuser;"
            f"PWD=TEJA@GUNA123s;"
            f"Encrypt=no;TrustServerCertificate=yes"
        )

        df_customers = pd.read_csv('/Users/gunasekharsiddabathuni/env/Datasets/us_customer_data.csv')
        df_orders = pd.read_csv('/Users/gunasekharsiddabathuni/env/Datasets/order_data.csv')

        df_customers.to_sql('customers', engine, index=False, if_exists='replace')
        df_orders.to_sql('orders', engine, index=False, if_exists='replace')

        print("Data loaded into SQL Server.")

if __name__ == "__main__":
    load_csv_to_sql()
