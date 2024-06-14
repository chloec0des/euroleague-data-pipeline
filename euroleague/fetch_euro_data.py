import os
import sys
from datetime import date
import pandas as pd
from euroleague_api.shot_data import ShotData
from euroleague_api.team_stats import TeamStats

# Add the base directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from dags.lib.euroleague.utils import create_directories  # Now this import should work

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

def fetch_euro_data_from_api():
    create_directories()  # Ensure directories exist before fetching data
    euro_data = {
        'game_shots': get_euro_game_shots_data(),
        'team_stats': get_euro_team_stats_data()
    }
    store_euro_data(euro_data)

def get_euro_game_shots_data():
    shot_data = ShotData(competition='E')
    data_frames = []
    for game_code in range(1, 10):  # Adjust range as needed
        df = shot_data.get_game_shot_data(season=2023, gamecode=game_code)
        if not df.empty:
            data_frames.append(df)
    if data_frames:
        return pd.concat(data_frames, ignore_index=True)
    else:
        return pd.DataFrame()

def get_euro_team_stats_data():
    team_stats = TeamStats()
    endpoint = 'traditional'  # Choose one of the valid endpoints: 'traditional', 'advanced', 'opponentsTraditional'
    df = team_stats.get_team_stats(endpoint)  # Provide the required 'endpoint' argument
    return df if not df.empty else pd.DataFrame()

def store_euro_data(euro_data):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/euroleague/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    for key, df in euro_data.items():
        df.to_csv(f"{TARGET_PATH}{key}.csv", index=False)
        print(f"Data stored: {TARGET_PATH}{key}.csv")

if __name__ == "__main__":
    fetch_euro_data_from_api()
