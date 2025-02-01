# Chess Results Project Documentation

## Overview

The Chess Results project is designed to scrape, process, and visualize tournament data from chess-results.com. It comprises several modules that fetch tournament listings, start lists, and game results; clean and merge data; and display interactive dashboards.

## Modules and Components

### Dashboard
- **File:** `dashboard.py`
- **Description:**  
  Utilizes Streamlit, Pandas, Matplotlib, and Seaborn to provide an interactive web dashboard. Users can explore various datasets (e.g., results, games, tournaments, and start lists) with multiple tabs for overview, statistics, and insights.
- **Key Features:**  
  - Dataset selection via sidebar.
  - Display of basic metrics (rows, columns, list of columns).
  - Visualization tools: histograms, box plots, and correlation heatmaps.

### Chess Results Scraper
- **File:** `chess_results_scraper.py`
- **Description:**  
  Implements asynchronous web scraping of tournament result pages using `aiohttp` and `BeautifulSoup`.  
- **Key Functions:**  
  - `get_column_structure`: Extracts the table header structure.
  - `parse_result_row`: Parses individual rows from the results table.
  - `fetch_tournament_results` and `process_tournaments_async`: Handle asynchronous fetching and merging of results.

### Chess Start List Scraper
- **File:** `chess_start_list_scraper.py`
- **Description:**  
  Scrapes tournament start lists. Uses asynchronous HTTP requests to fetch start list pages and BeautifulSoup for parsing HTML content.
- **Key Features:**  
  - Extracts player data and associated URLs.
  - Processes and converts scraped data into a Pandas DataFrame.

### Chess Tournament Selenium Scraper
- **File:** `chess_tournament_selenium_scraper.py`
- **Description:**  
  Uses Selenium WebDriver to navigate and scrape tournament listings. Handles dynamic web elements, pop-ups, and cookie consent.
- **Key Functions:**  
  - `setup_driver`: Initializes the Selenium WebDriver.
  - `fetch_tournaments`: Navigates to the target page, fills forms, and retrieves tournament data.
  - `create_checkpoint`: Saves scraped tournament data progressively to manage memory.

### Data Cleaning and Merging
- **Jupyter Notebooks / Scripts:**  
  - `data_cleaning.ipynb` provides examples of cleaning and merging data:
    - Converting date strings to `datetime` objects.
    - Extracting tournament IDs from URLs.
    - Replacing empty strings with NaN.
    - Converting numeric columns to appropriate types.
  - Merges data from games, tournaments, and start lists to provide complete insights.

## Dependencies

- **Python Packages:**
  - `streamlit`
  - `pandas`
  - `matplotlib`
  - `seaborn`
  - `aiohttp`
  - `BeautifulSoup` (from `bs4`)
  - `selenium`
  - `numpy`

- **Additional Tools:**
  - ChromeDriver (if using Selenium; ensure it is compatible with your version of Chrome).