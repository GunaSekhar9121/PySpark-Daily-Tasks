import pandas as pd

def clean_orders_data(df):
    # Convert order_date to datetime.date (drop time)
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce').dt.date

    # Remove duplicates
    df = df.drop_duplicates()

    return df

if __name__ == "__main__":
    # Load the raw orders data
    df = pd.read_csv("/Users/gunasekharsiddabathuni/env/Datasets/order_data.csv")

    # Clean the data
    cleaned_df = clean_orders_data(df)

    # Save to a new CSV
    cleaned_df.to_csv("cleaned_orders.csv", index=False)
    print("Orders data cleaned and saved as 'cleaned_orders.csv'")
