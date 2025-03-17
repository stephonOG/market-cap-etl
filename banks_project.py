import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
import datetime

# Function to log progress in a file and print updates
def log_progress(message):
    # Logs progress to a file code_log.txt
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("code_log.txt", "a") as log_file:
        log_file.write(f"{timestamp} - {message}\n")
    print(f"{message}")

# Function to extract data from the webpage
def extract(url, table_attribs):
    log_progress("Preliminaries complete. Initiating ETL process")
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    
    # Find all tables and select the first one
    tables = data.find_all('table')
    
    correct_table = tables[0]  # Selecting the first table directly
    
    # Extract table rows
    rows = correct_table.find('tbody').find_all('tr')
    df = pd.DataFrame(columns=table_attribs)
    
    for row in rows:
        col = row.find_all('td')
        
        if len(col) >= 3:
            bank_name = col[1].text.strip()
            market_cap = col[2].text.strip().replace(',', '')  
            
            data_dict = {
                    "Name": bank_name,
                    "MC_USD_Billion": float(market_cap)
            }
            df1 = pd.DataFrame([data_dict])
            df = pd.concat([df, df1], ignore_index=True)
    
    return df

# Function to transform extracted data by converting USD market cap to GBP, EUR, and INR
def transform(df, csv_path):
    exchange_rates = pd.read_csv(csv_path)

    # Extract exchange rates for GBP, EUR, and INR from CSV file
    usd_to_gbp = exchange_rates.loc[exchange_rates["Currency"] == "GBP", "Rate"].values[0]
    usd_to_eur = exchange_rates.loc[exchange_rates["Currency"] == "EUR", "Rate"].values[0]
    usd_to_inr = exchange_rates.loc[exchange_rates["Currency"] == "INR", "Rate"].values[0]
    
    # Convert Market Cap to different currencies and round to 2 d.p
    df["MC_GBP_Billion"] = (df["MC_USD_Billion"] * usd_to_gbp).round(2)
    df["MC_EUR_Billion"] = (df["MC_USD_Billion"] * usd_to_eur).round(2)
    df["MC_INR_Billion"] = (df["MC_USD_Billion"] * usd_to_inr).round(2)
    return df

# Function to save transformed data to a CSV file
def load_to_csv(df, output_path):
    df.to_csv(output_path, index = False)

# Function to load transformed data into an SQLite database table
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Function to execute SQL queries on the database
def run_query(query_statement, sql_connection):
    result = pd.read_sql(query_statement, sql_connection)
    return result

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]

# Extract data from the webpage
df_extracted = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

# Path for exchange rate CSV file
exchange_rate_csv = "exchange_rate.csv" 

# Transform extracted data
df_transformed = transform(df_extracted, exchange_rate_csv)
log_progress("Data transformation complete. Initiating Loading process")

# Save transformed data to CSV
csv_output_path = "./Largest_banks_data.csv"
load_to_csv(df_transformed, csv_output_path)
log_progress("Data saved to CSV file")

# Connect to SQLite database
connection = sqlite3.connect("Banks.db")
log_progress("SQL Connection initiated")

# Load data into the database table
table_name = "Largest_banks"
load_to_db(df_transformed, connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

# Execute queries and print results
query_all = run_query("SELECT * FROM Largest_banks;", connection)
print(query_all)
query_avg = run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks;", connection)
print(query_avg)
query_limit = run_query("SELECT Name FROM Largest_banks LIMIT 5;", connection)
print(query_limit)
log_progress("Process Complete")

connection.close()
log_progress("Server Connection closed")







