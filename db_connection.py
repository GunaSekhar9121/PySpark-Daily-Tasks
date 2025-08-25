import pandas as pd
import pyodbc
import re

def get_sql_connection():
    server = '192.168.1.249'
    database = 'Bikestores'
    username = 'pythonuser'
    password = 'TEJA@GUNA123s'
    driver = 'ODBC Driver 18 for SQL Server'

    try:
        conn = pyodbc.connect(
            f"DRIVER={{{driver}}};"
            f"SERVER={server},1433;"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )
        print("Connected to SQL Server successfully!")
        return conn
    except Exception as e:
        print("Connection failed:", e)
        return None
