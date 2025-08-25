import pandas as pd
from sqlalchemy import create_engine
import urllib

# SQL Server credentials and connection settings
server = '192.168.1.249'
database = 'Python_Pipelines'
username = 'pythonuser'
password = 'TEJA@GUNA123s'  # special characters handled via URL encoding
driver = 'ODBC Driver 18 for SQL Server'

# URL-encoded connection string for SQLAlchemy + pyodbc
params = urllib.parse.quote_plus(
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    f"Encrypt=no;"
    f"TrustServerCertificate=yes;"
    f"Connection Timeout=30;"
)

# Create SQLAlchemy engine
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def upload_final_view():
    print("Connecting to SQL Server...")
    df = pd.read_csv("/Users/gunasekharsiddabathuni/env/unified_customer_view.csv")
    print("Data loaded successfully!")
    df.to_sql("unified_customer_view", con=engine, if_exists='replace', index=False)
    print("Uploaded 'unified_customer_view' to SQL Server!")

if __name__ == "__main__":
    upload_final_view()
