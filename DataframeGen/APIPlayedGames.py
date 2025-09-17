from Utils.dotenv_util import return_steam_key, return_steam_id
from Utils.database_util import transfer_df_to_sql
import requests
import pandas as pd

def fetch_played_games_data() -> pd.DataFrame:
    """Fetches played games data from the Steam API and returns it as a pandas DataFrame."""
    api_key = return_steam_key()
    steam_id = return_steam_id()
    url = f"https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/"
    params = {'key': api_key, 'steamid': steam_id, 'format': 'json', 'include_played_free_games': 1, 'include_free_sub': 1, 'skip_unvetted_apps': 0}

    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an error for bad responses

    data = response.json()

    # Extract the list of played games
    games = data.get('response', {}).get('games', [])

    # Convert to DataFrame
    df_played_games = pd.DataFrame(games)

    return df_played_games

if __name__ == "__main__":
    df = fetch_played_games_data()
    transfer_df_to_sql(df, table_name='SteamPlayedGames', replace_table=True)