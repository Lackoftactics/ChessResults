import os
import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup


class ChessResultsScraper:
    def __init__(self, data_path="data", base_url="https://chess-results.com/", max_concurrency=20):
        """
        :param data_path: Path to the 'data' folder, which contains 'start_lists' and 'results' subfolders.
        :param base_url: Base URL for chess-results.com
        :param max_concurrency: Maximum number of concurrent requests.
        """
        self.data_path = data_path
        self.base_url = base_url
        self.max_concurrency = max_concurrency

    def get_column_structure(self, soup: BeautifulSoup):
        """
        Extract column structure from the results table header.
        Try 'CRng1b' first, then 'CRg1b' as fallback.
        Returns a dict mapping {col_name -> col_index}.
        """
        header_row = soup.select_one("table.CRs1 tr.CRng1b")
        if not header_row:
            header_row = soup.select_one("table.CRs1 tr.CRg1b")

        if not header_row:
            return {}

        columns = {}
        header_cells = header_row.find_all(["th", "td"])
        for i, col in enumerate(header_cells):
            col_name = col.get_text(strip=True)
            if not col_name:  # If blank or &nbsp;
                col_name = f"col_{i}"
            columns[col_name] = i

        return columns

    def parse_result_row(self, row, column_map):
        """
        Parse a single <tr> of data based on the column_map.
        Returns a dict with {column_name: cell_text}.
        """
        cells = row.find_all("td")
        row_data = {}
        for col_name, idx in column_map.items():
            if idx < len(cells):
                cell_text = cells[idx].get_text(strip=True)
                row_data[col_name] = cell_text
            else:
                row_data[col_name] = None
        return row_data

    async def fetch_tournament_results(self, session, relative_url):
        """
        Asynchronously fetch a single tournament's results page and parse it into a DataFrame.
        """
        url = f"{self.base_url}{relative_url}&art=5&zeilen=99999"

        try:
            async with session.get(url) as response:
                html = await response.text()

            soup = BeautifulSoup(html, "lxml")

            # 1) Get column structure
            col_map = self.get_column_structure(soup)
            if not col_map:
                print(f"No header row found at {url}")
                return pd.DataFrame()

            # 2) Parse each data row
            table = soup.select_one("table.CRs1")
            if not table:
                print(f"No 'CRs1' table found at {url}")
                return pd.DataFrame()

            rows = []
            for tr in table.find_all("tr"):
                row_classes = tr.get("class", [])
                # Skip header rows
                if "CRng1b" in row_classes or "CRg1b" in row_classes:
                    continue

                tds = tr.find_all("td")
                if len(tds) < 2:
                    continue

                # Parse row
                row_data = self.parse_result_row(tr, col_map)
                row_data["tournament_url"] = relative_url

                # If row_data is not completely empty, add to list
                if any(v for v in row_data.values()):
                    rows.append(row_data)

            return pd.DataFrame(rows)

        except Exception as e:
            print(f"Error fetching/parsing {url}: {e}")
            return pd.DataFrame()

    async def process_tournaments_async(self, tournament_urls, max_concurrency=None):
        """
        Asynchronously fetch and parse multiple tournament URLs in parallel.
        Returns a single DataFrame containing all rows from all tournaments.
        """
        if max_concurrency is None:
            max_concurrency = self.max_concurrency

        semaphore = asyncio.Semaphore(max_concurrency)
        all_frames = []

        async with aiohttp.ClientSession() as session:
            async def fetch_with_limit(url):
                async with semaphore:
                    return await self.fetch_tournament_results(session, url)

            tasks = [asyncio.create_task(fetch_with_limit(u)) for u in tournament_urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, pd.DataFrame):
                all_frames.append(res)
            else:
                print(f"Got an exception: {res}")

        if all_frames:
            return pd.concat(all_frames, ignore_index=True)
        else:
            return pd.DataFrame()

    async def run_main(self):
        """
        Main entry point for processing:
          1) Reads country start lists from data/start_lists
          2) Filters for completed tournaments (in your actual code, you'd define what 'finished' means)
          3) Fetches results and saves to data/results
        """
        start_lists_path = os.path.join(self.data_path, "start_lists")
        results_path = os.path.join(self.data_path, "results")

        start_lists = os.listdir(start_lists_path)
        # Get first part of the file name before _
        countries = [s.split("_")[0] for s in start_lists]
        sorted_countries = sorted(countries)

        for country in sorted_countries:
            output_file = os.path.join(results_path, f"{country}_results.csv")
            if os.path.exists(output_file):
                print(f"Skipping {country}")
                continue

            print(f"Processing {country}")
            start_list_file = os.path.join(start_lists_path, f"{country}_start_list.csv")
            start_list = pd.read_csv(start_list_file)

            # Get unique URLs from the start list
            tournament_urls = start_list['tournament_url'].unique()

            # Process tournaments
            combined_df = await self.process_tournaments_async(tournament_urls)

            # Save to CSV
            combined_df.to_csv(output_file, index=False)
            print(f"Saved {len(combined_df)} rows for {country}")

        print("All done!")
