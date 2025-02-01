import re
import pandas as pd
import glob
import os

class ResultsConverter:
    def __init__(self, results_folder="data/results", games_folder="data/games"):
        """
        :param results_folder: Path to folder containing CSV files like 'USA_results.csv'
        :param games_folder:   Path to folder where output (e.g. 'USA_games.csv') should be saved
        """
        self.results_folder = results_folder
        self.games_folder = games_folder
        os.makedirs(self.games_folder, exist_ok=True)  # Create output folder if not exists

    @staticmethod
    def parse_round_cell(cell_value):
        """
        Parse a cell like "16w1", "21b½", etc.
        Returns a tuple (opponent_number, color, result) or None if no match.
        """
        cell_value = str(cell_value).strip()
        if not cell_value or cell_value.lower() == "nan":
            return None
        
        match = re.match(r"(\d+)([wb])([10½/\+\-])?", cell_value)
        if not match:
            return None
        
        opp_str, color, result = match.groups()
        opp_num = int(opp_str)
        return (opp_num, color, result)

    def convert_all_results(self):
        """
        Convert all '*_results.csv' files in 'self.results_folder' into
        corresponding '*_games.csv' in 'self.games_folder'.
        If the output file already exists, that country is skipped.
        """
        # Grab all CSVs in the results folder that match the pattern "<something>_results.csv"
        csv_files = glob.glob(os.path.join(self.results_folder, "*_results.csv"))

        for file_path in csv_files:
            country = os.path.basename(file_path).replace("_results.csv", "")
            out_path = os.path.join(self.games_folder, f"{country}_games.csv")

            # If we've already converted this country, skip it
            if os.path.exists(out_path):
                print(f"Skipping {country}, games file already exists.")
                continue

            print(f"Converting results for {country}...")

            # Read the input file
            df = pd.read_csv(file_path)

            print(f"Loaded {len(df)} rows from {file_path}")

            # check if has column Rg in df
            if 'Rg' not in df.columns or 'Fed' not in df.columns:
                print(f"Skipping {country}, no ratings found.")
                continue

            # Identify how many rounds there are by searching for columns like "1.Rd", "2.Rd", etc.
            # This code assumes columns like "1.Rd", "2.Rd", ...
            rounds_num = (
                df.columns
                  .str.extract(r"(\d+)\.Rd")
                  .dropna()
                  .astype(int)
                  .max()
                  .item()
            )

            all_games = []

            for i, row in df.iterrows():
                # Player "start number" is typically in column 'Nr' (change if needed)
                player_id = row["Nr"]
                player_name = row["Nazwisko"]
                player_rating = row["Rg"]
                player_fed = row["Fed"]

                # For each round, parse the cell if it exists
                for r in range(1, rounds_num + 1):
                    col_name = f"{r}.Rd"
                    if col_name not in df.columns:
                        break  # or continue if columns may be missing sporadically

                    cell_val = row[col_name]
                    parsed = self.parse_round_cell(cell_val)
                    if not parsed:
                        continue

                    opp_num, color, result = parsed

                    # Only record game if opponent's start number is larger => avoid duplicates
                    if opp_num <= player_id:
                        continue

                    # Prepare row data
                    if color == 'w':
                        # Current player is White
                        white_id = player_id
                        white_name = player_name
                        white_rating = player_rating
                        white_fed = player_fed

                        # Opponent is Black
                        opp_row = df.loc[opp_num - 1]
                        black_id = opp_num
                        black_name = opp_row["Nazwisko"]
                        black_rating = opp_row["Rg"]
                        black_fed = opp_row["Fed"]

                        # Result string from White's perspective
                        if result == '1':
                            final_result = "1-0"
                        elif result == '0':
                            final_result = "0-1"
                        elif result in ['½', '1/2']:
                            final_result = "1/2-1/2"
                        else:
                            final_result = f"{result}"
                    else:
                        # Current player is Black
                        black_id = player_id
                        black_name = player_name
                        black_rating = player_rating
                        black_fed = player_fed

                        # Opponent is White
                        opp_row = df.loc[opp_num - 1]
                        white_id = opp_num
                        white_name = opp_row["Nazwisko"]
                        white_rating = opp_row["Rg"]
                        white_fed = opp_row["Fed"]

                        # Result string given is from Black's perspective
                        if result == '1':
                            # Black won => from White's perspective it's 0-1
                            final_result = "0-1"
                        elif result == '0':
                            # Black lost => from White's perspective it's 1-0
                            final_result = "1-0"
                        elif result in ['½', '1/2']:
                            final_result = "1/2-1/2"
                        else:
                            final_result = f"{result}"

                    # Append to list of all games
                    all_games.append({
                        "Round": r,
                        "WhiteStartNumber": white_id,
                        "WhiteFed": white_fed,
                        "WhiteName": white_name,
                        "WhiteRating": white_rating,
                        "BlackStartNumber": black_id,
                        "BlackFed": black_fed,
                        "BlackName": black_name,
                        "BlackRating": black_rating,
                        "Result": final_result,
                        "tournament_url": row.get("tournament_url", "")  # handle missing columns safely
                    })

            # Create a DataFrame of all games and save
            games_df = pd.DataFrame(all_games)
            if not games_df.empty:
                # Sort if desired
                games_df.sort_values(by=["Round", "WhiteName"], inplace=True)

            # Write to CSV
            games_df.to_csv(out_path, index=False)
            print(f"Saved {len(games_df)} games to {out_path}")
