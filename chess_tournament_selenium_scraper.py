from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import os


class ChessTournamentSeleniumScraper:
    def __init__(
        self,
        base_url="https://chess-results.com/TurnierSuche.aspx?lan=3",
        output_file="chess_tournaments_selenium.csv",
        start_years=20,  # How many years to go back from today, to make sure we don't miss any tournaments
        countries_alpha3=None,
        headless=True
    ):
        """
        :param base_url: The page on chess-results.com to search for tournaments.
        :param output_file: Where to append or save the results of the scraping.
        :param start_years: How many years in the past to start searching.
        :param countries_alpha3: List of country codes to scrape. If None, a default list is used.
        :param headless: Whether to run Chrome in headless mode.
        """
        self.base_url = base_url
        self.output_file = output_file
        self.start_years = start_years
        self.countries_alpha3 = countries_alpha3 or [
            "AFG", "ALA", "ALB", "ALG", "ASM", "AND", "ANG", "ANT", "ARG", "ARM", "ARU", "ACF", "AUS",
            "AUT", "AZE", "BAH", "BRN", "BAN", "BAR", "BLR", "BEL", "BIZ", "BEN", "BER", "BHU", "BOL",
            "BES", "BIH", "BOT", "BRA", "IOT", "IVB", "BRU", "BUL", "BUR", "BDI", "CAM", "CMR", "CAN",
            "CPV", "CAT", "CAY", "CAF", "CHA", "CHI", "CHN", "TPE", "COL", "COM", "CCA", "COK", "CRC",
            "CRO", "CUB", "CUW", "CYP", "CZE", "COD", "DEN", "DJI", "DMA", "DOM", "ECU", "EGY", "ESA",
            "ENG", "GEQ", "ERI", "EST", "ETH", "ECX", "EU", "FLK", "FAI", "FID", "FIJ", "FIN", "FRM",
            "FRA", "GUF", "PYF", "GAB", "GAM", "GEO", "GER", "GHA", "GIB", "GRE", "GRL", "GRN", "GLP",
            "GUM", "GUA", "GCI", "GIN", "GNB", "GUY", "HAI", "VAT", "HON", "HKG", "HUN", "ISL", "IND",
            "INA", "IRI", "IRQ", "IRL", "IOM", "IMN", "ISR", "ITA", "CIV", "JAM", "JPN", "JCI", "JOR",
            "KAZ", "KEN", "KIR", "PRK", "KOS", "KUW", "KGZ", "LAO", "LAT", "LBN", "LES", "LBR", "LBA",
            "LIE", "LTU", "LUX", "MAC", "MAD", "MAW", "MAS", "MDV", "MLI", "MLT", "MHL", "MTQ", "MTN",
            "MRI", "MYT", "MEX", "FSM", "MDA", "MNC", "MGL", "MNE", "MSR", "MAR", "MOZ", "MYA", "NAM",
            "NRU", "NEP", "NED", "AHO", "NCL", "NZL", "NCA", "NIG", "NGR", "NIU", "AFR", "NFK", "NIR",
            "MKD", "MNP", "NOR", "OMA", "ONL", "PAK", "PLW", "PLE", "PAN", "PNG", "PAR", "PER", "PHI",
            "POL", "POR", "PUR", "QAT", "REU", "ROU", "RUS", "RWA", "SKN", "LCA", "MAF", "VIN", "WSM",
            "SMR", "STP", "KSA", "SCO", "SEN", "SRB", "SEY", "SLE", "SGP", "SXM", "SVK", "SLO", "SOL",
            "SOM", "RSA", "KOR", "SSD", "ESP", "SRI", "SPM", "SUD", "SUR", "SWZ", "SWE", "SUI", "SYR",
            "TJK", "TAN", "THA", "TLS", "TOG", "TKL", "TON", "TTO", "TUN", "TUR", "TKM", "TCA", "TUV",
            "UGA", "UKR", "UAE", "USA", "XXX", "YYY", "ZZZ", "---", "URU", "ISV", "UZB", "VAN", "VUT",
            "VEN", "VIE", "WLS", "WLF", "WFC", "YEM", "SCG", "ZAM", "ZIM"
        ]
        self.driver = None
        self.headless = headless

    def setup_driver(self):
        """
        Sets up the Selenium Chrome driver with professional configuration.
        """
        chrome_options = Options()
        
        # Professional User-Agent
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        if self.headless:
            chrome_options.add_argument("--headless=new")
        
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")
        
        
        self.driver = webdriver.Chrome(options=chrome_options)
        

    def convert_date_format(date_str):
        """
        Convert YYYY/MM/DD to DD.MM.YYYY format.
        """
        date_obj = datetime.strptime(date_str, '%Y/%m/%d')
        return date_obj.strftime("%d.%m.%Y")

    
    def is_bogus_date(date_str):
        """
        Returns True if date_str is not a valid date in YYYY/MM/DD; else False.
        """
        try:
            datetime.strptime(date_str, "%Y/%m/%d")
            return False
        except ValueError:
            return True

    
    def time_delta_between_dates(date1, date2):
        """
        Returns the difference in days between two dates in DD.MM.YYYY format.
        """
        d1 = datetime.strptime(date1, "%d.%m.%Y")
        d2 = datetime.strptime(date2, "%d.%m.%Y")
        return (d2 - d1).days

    
    def parse_tournaments(self, html):
        """
        Parse the HTML page_source for tournament data and return a list of dicts.
        """
        soup = BeautifulSoup(html, "html.parser")
        tournaments = []

        # The table with class 'CRs2' has results; skip header row.
        rows = soup.select("table.CRs2 tr:has(td)")[1:]
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 6:
                # columns: 0=No, 1=Name, 2=Country, 3=?, 4=?, 5=StartDate, 6=EndDate
                tournaments.append({
                    "end_date": cols[6].text.strip(),
                    "start_date": cols[5].text.strip(),
                    "country": cols[2].text.strip(),
                    "name": cols[1].text.strip(),
                    "url": cols[1].find('a')['href'] if cols[1].find('a') else None
                })
        return tournaments

    
    def create_checkpoint(self, output_file, all_tournaments, country):
        """
        Write tournaments to CSV, appending if file exists. Clears all_tournaments afterwards.
        """
        if all_tournaments:
            df = pd.DataFrame(all_tournaments)
            # Append if the main file already exists
            write_header = not os.path.exists(output_file)
            df.to_csv(
                output_file,
                mode='a' if not write_header else 'w',
                header=write_header,
                index=False
            )
            print(f"Saved {len(all_tournaments)} tournaments for {country}")
            # Clear list to free memory
            all_tournaments.clear()

    
    def find_last_valid_date(response):
        """
        Find the last valid end_date (YYYY/MM/DD) in a list of tournament dictionaries.
        """
        for i in range(1, len(response) + 1):
            last_date = response[-i]["end_date"]
            if not ChessTournamentSeleniumScraper.is_bogus_date(last_date):
                return last_date
        return None

    def fetch_tournaments(self, start_date, end_date, country):
        """
        Given a start_date, end_date, and a country code, fills the form on the site
        and returns the parsed tournament data.
        """
        driver = self.driver
        try:
            driver.get(self.base_url)

            # Accept cookies for the first run with the first country in the list (optional)
            if country == self.countries_alpha3[0]:
                WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located(
                        (By.ID, "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll")
                    )
                )
                driver.find_element(
                    By.ID, "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"
                ).click()

            # Wait for search button (P1_cb_suchen) to appear
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "P1_cb_suchen"))
            )

            # Date inputs
            start_date_field = driver.find_element(By.ID, "P1_txt_von_tag")
            end_date_field = driver.find_element(By.ID, "P1_txt_bis_tag")
            start_date_field.clear()
            end_date_field.clear()
            start_date_field.send_keys(start_date)
            end_date_field.send_keys(end_date)

            # Only finished tournaments
            driver.find_element(By.ID, "P1_cbox_zuEnde").click()

            # Results per page
            results_dropdown = Select(driver.find_element(By.ID, "P1_combo_anzahl_zeilen"))
            results_dropdown.select_by_visible_text("2000")

            # Country filter
            country_dropdown = Select(driver.find_element(By.ID, "P1_combo_land"))
            country_dropdown.select_by_value(country)

            # Sort by "Start date descending" (value=4)
            sort_dropdown = Select(driver.find_element(By.ID, "P1_combo_sort"))
            sort_dropdown.select_by_value("4")

            # Submit form
            search_button = driver.find_element(By.ID, "P1_cb_suchen")
            search_button.click()

            # Wait for results to load
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "P1_cb_suchen"))
            )

            time.sleep(1)  # Extra buffer
            return self.parse_tournaments(driver.page_source)

        except Exception as e:
            print(f"Error fetching {country}-{end_date}: {str(e)}")
            return []

    def fetch_data(self, start_date, end_date, country):
        """
        Simple wrapper for fetch_tournaments, if you need extra logic in the future.
        """
        return self.fetch_tournaments(start_date, end_date, country)

    def crawl_all_tournaments(self):
        """
        Main method that controls the multi-year, multi-country scraping logic.
        Iterates over countries_alpha3, going back self.start_years from today,
        collecting tournament data until fewer than 2000 results appear.
        """
        self.setup_driver()
        all_tournaments = []

        try:
            for country in self.countries_alpha3:
                print(f"Processing country: {country}")
                # Start from 'today' and go back self.start_years years
                end_date = datetime.now().strftime("%d.%m.%Y")
                current_end_date = end_date
                start_date = (datetime.now() - timedelta(days=self.start_years * 365))
                current_start_date = start_date.strftime("%d.%m.%Y")

                country_tournaments = []

                while True:
                    response = self.fetch_data(current_start_date, current_end_date, country)
                    country_tournaments.extend(response)
                    all_tournaments.extend(response)

                    # If fewer than 2000 results are returned, we've exhausted the range
                    if len(response) < 2000:
                        self.create_checkpoint(self.output_file, country_tournaments, country)
                        break  # Move on to next country

                    # If 2000 results, we likely have more to fetch. Update date range.
                    last_date = self.find_last_valid_date(response)
                    if not last_date:
                        # No valid date found, break to avoid infinite loop
                        break

                    # Move end_date backward by one day from the last valid date
                    # Note: last_date is in YYYY/MM/DD; we convert to a datetime,
                    # then shift one day
                    last_date_dt = datetime.strptime(last_date, "%Y/%m/%d")
                    last_date_dt = last_date_dt - timedelta(days=1)
                    current_end_date = last_date_dt.strftime("%d.%m.%Y")

                    # Save partial results
                    self.create_checkpoint(self.output_file, country_tournaments, country)

        finally:
            self.driver.quit()

        return pd.DataFrame(all_tournaments)


