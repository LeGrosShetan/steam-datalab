from Utils.dotenv_util import return_steam_key, return_steam_id
from Utils.database_util import transfer_df_to_sql, get_friends_list_from_db
import requests
import pandas as pd

def fetch_played_games_data(steam_id : int) -> pd.DataFrame:
    """Fetches played games data from the Steam API and returns it as a pandas DataFrame."""
    api_key = return_steam_key()
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

def iterate_over_steam_ids_and_fetch_data(steam_ids: list) -> pd.DataFrame:
    all_data = pd.DataFrame()
    for steam_id in steam_ids:
        df = fetch_played_games_data(steam_id)
        df['steam_id'] = steam_id  # Add a column to identify the user
        all_data = pd.concat([all_data, df], ignore_index=True)

    all_data = all_data.astype(
        {"appid": 'Int64', "playtime_forever": 'Int64', "playtime_2weeks": 'Int64', "rtime_last_played": 'Int64',
         "playtime_windows_forever": 'Int64', "playtime_mac_forever": 'Int64', "playtime_linux_forever": 'Int64',
         "playtime_deck_forever": 'Int64', "playtime_disconnected": 'Int64'})

    return all_data

def return_steam_ids_from_db() -> list:
    friends_df = get_friends_list_from_db()['steamid'].tolist()
    friends_df.append(int(return_steam_id()))  # Add own Steam ID to the list

    return friends_df


if __name__ == "__main__":
    steam_ids = return_steam_ids_from_db()
    df = iterate_over_steam_ids_and_fetch_data(steam_ids)
    transfer_df_to_sql(df, table_name='SteamPlayedGames', replace_table=True)