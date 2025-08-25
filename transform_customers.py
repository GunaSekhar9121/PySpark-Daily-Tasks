import pandas as pd
import re

def clean_customer_data(df):
    # Remove common prefixes and suffixes from names
    def clean_name(name):
        name = re.sub(r'\b(Mr\.|Mrs\.|Miss|Dr\.)\s*', '', name)
        name = re.sub(r'\s*(Jr\.|Sr\.|II|III)\b', '', name)
        return name.strip()

    df['cleaned_name'] = df['name'].apply(clean_name)

    # Split into First Name and Last Name
    df[['first_name', 'last_name']] = df['cleaned_name'].str.split(pat=' ', n=1, expand=True)

    # Extract Country Code from address
    def extract_country(address):
        # Simplified logic — can be customized based on known formats
        if 'FPO AE' in address:
            return 'US'
        elif 'UK' in address:
            return 'UK'
        elif 'CA' in address:
            return 'CA'
        elif 'AU' in address:
            return 'AU'
        else:
            return 'US'

    df['country'] = df['address'].apply(extract_country)

    # Map to dialing codes
    dialing_codes = {'US': '+1', 'UK': '+44', 'CA': '+1', 'AU': '+61'}
    df['dialing_code'] = df['country'].map(dialing_codes)
    df['formatted_phone'] = df['dialing_code'].fillna('') + df['phone'].astype(str)

    # Customer Tier Mapping
    tier_map = {'Gold': 2, 'Silver': 1, 'Bronze': 0}
    df['customer_tier'] = df['loyalty_status'].map(tier_map)

    return df

if __name__ == "__main__":
    df = pd.read_csv("/Users/gunasekharsiddabathuni/env/Datasets/us_customer_data.csv")
    transformed = clean_customer_data(df)
    transformed.to_csv("transformed_customers.csv", index=False)
    print("Customer data transformed.")
