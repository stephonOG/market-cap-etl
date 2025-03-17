# ETL Pipeline for Market Capitalization Analysis

## Project Overview
This project was developed for the final exam of my **IBM Python Data Engineering Project on Coursera**. The goal is to build an **ETL pipeline** that extracts, transforms, and loads data about the **largest banks in the world ranked by market capitalization**.

## Data Pipeline Workflow
- **Extract** - Web scrapes banking data from Wikipedia.  
- **Transform** - Converts USD market cap to GBP, EUR, and INR using exchange rates.  
- **Load** - Stores cleaned data into a **CSV file** and an **SQLite database**.  
- **Query** - Runs SQL queries to analyze the top banks.

## Tech Stack
- **Python** (Pandas, BeautifulSoup, SQLite)
- **Web Scraping** (requests, BeautifulSoup)
- **SQL** (SQLite3)
- **Logging** (Progress tracking for debugging also with DateTime module to attach a timestamp to processes logged in the log file)

## Installation & Usage
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/market-cap-etl.git
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the ETL script:
   ```bash
   python3 banks_project.py
   ```

## Sample Queries
```sql
SELECT * FROM Largest_banks;
SELECT AVG(MC_GBP_Billion) FROM Largest_banks;
SELECT Name FROM Largest_banks LIMIT 5;
