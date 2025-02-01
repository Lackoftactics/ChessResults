import os
import asyncio
import aiohttp
import requests
import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer

class ChessStartListScraper:
    def __init__(
        self,
        base_url="https://chess-results.com/",
        data_path="data",
        max_concurrency=20
    ):
        """
        :param base_url: Base URL for the chess-results site.
        :param data_path: Directory where 'chess_tournaments_selenium.csv' and output folders live.
        :param max_concurrency: Maximum number of concurrent requests (for aiohttp).
        """
        self.base_url = base_url
        self.data_path = data_path
        self.max_concurrency = max_concurrency

    def parse_player(self, row, column_map):
        """Parse player data using column mapping."""
        cols = row.find_all("td")
        player = {}
        
        for col_name, idx in column_map.items():
            if idx < len(cols):
                cell = cols[idx]
                # Handle links in cells
                link = cell.find('a')
                if link:
                    player[f"{col_name}_url"] = link.get('href')
                    player[col_name] = link.text.strip()
                else:
                    player[col_name] = cell.text.strip()
        return player

    def get_column_structure(self, soup: BeautifulSoup):
        """Extract column structure from the start list header row."""
        # We look for table.CRs1 and the row with class="CRg1b"
        header_row = soup.select_one("table.CRs1 tr.CRg1b")
        if not header_row:
            return {}
        
        columns = {}
        for i, col in enumerate(header_row.find_all("th")):
            col_name = col.text.strip()
            columns[col_name] = i
        return columns

    async def fetch_start_list(self, session, tournament_url):
        """
        Fetch and parse a single tournament's start list page (div id="F7")
        using an aiohttp session.
        """
        try:
            full_url = f"{self.base_url}{tournament_url}"
            async with session.get(full_url) as response:
                html = await response.text()

            # We only parse the <div id="F7"> section
            only_f7 = SoupStrainer("div", id="F7")
            soup = BeautifulSoup(html, "lxml", parse_only=only_f7)

            # If there's no 'Lista startowa' (Polish for 'Start list') heading, skip
            if not soup.select_one("h2", string="Lista startowa"):
                return []

            # Extract column structure
            column_map = self.get_column_structure(soup)
            if not column_map:
                return []

            players = []
            # Skip the header row: [1:] after selecting all <tr> in table.CRs1
            for row in soup.select("table.CRs1 tr")[1:]:
                player = self.parse_player(row, column_map)
                if player:
                    player["tournament_url"] = tournament_url
                    players.append(player)

            return players

        except Exception as e:
            print(f"Error processing {tournament_url}: {str(e)}")
            return []

    async def process_tournaments_async(self, tournament_urls):
        """
        Asynchronously fetch and parse multiple tournament URLs in parallel.
        Returns a single DataFrame with all parsed data.
        """
        results = []
        semaphore = asyncio.Semaphore(self.max_concurrency)

        async with aiohttp.ClientSession() as session:
            async def fetch_with_limit(url):
                async with semaphore:
                    return await self.fetch_start_list(session, url)

            tasks = [asyncio.create_task(fetch_with_limit(url)) for url in tournament_urls]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

        for res in responses:
            if isinstance(res, list):
                results.extend(res)
            else:
                print(f"Got an exception: {res}")

        return pd.DataFrame(results)

    async def run_main(self):
        """
        Main entry point:
        1) Load tournament data from CSV (which has a 'country' column and a 'url' column).
        2) Group them by country.
        3) Scrape each country's tournaments (if not already saved).
        4) Save each country's start list to CSV under data/start_lists/.
        """
        # Load your tournament data
        tournaments_csv = os.path.join(self.data_path, "chess_tournaments_selenium.csv")
        tournaments = pd.read_csv(tournaments_csv)

        # Group by country -> [list of urls]
        countries_with_urls = tournaments.groupby("country")["url"].apply(
            lambda x: x.dropna().unique().tolist()
        )

        # We'll keep track of all results in a single DataFrame if desired
        all_start_lists = pd.DataFrame()

        for country, urls in countries_with_urls.items():
            # skip if CSV already exists
            country_start_list_path = os.path.join(
                self.data_path, "start_lists", f"{country}_start_list.csv"
            )
            if os.path.exists(country_start_list_path):
                print(f"Skipping {country}")
                continue

            print(f"Processing {country}: {len(urls)} tournaments")

            # Asynchronously scrape the tournaments
            country_start_lists = await self.process_tournaments_async(urls)

            # Save to CSV
            os.makedirs(os.path.dirname(country_start_list_path), exist_ok=True)
            country_start_lists.to_csv(country_start_list_path, index=False)
            print(f"Saved {len(country_start_lists)} players for {country}")

            all_start_lists = pd.concat([all_start_lists, country_start_lists], ignore_index=True)

        # Optionally, save a combined CSV with all start lists
        all_start_lists_path = os.path.join(self.data_path, "tournament_players.csv")
        all_start_lists.to_csv(all_start_lists_path, index=False)
        print("All done!")
