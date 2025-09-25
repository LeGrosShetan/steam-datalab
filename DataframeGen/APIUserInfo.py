from Utils.dotenv_util import return_steam_key, return_steam_id
from Utils.database_util import transfer_df_to_sql, get_friends_list_from_db
import requests
import pandas as pd

def fetch_user_info(steam_id: int) -> pd.DataFrame:
    """Fetches user info data from the Steam API and returns it as a pandas DataFrame."""
    url = f"https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/"
    params = {'key': return_steam_key(), 'steamids': steam_id, 'format': 'json'}

    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an error for bad responses

    data = response.json()

    # Extract the list of apps
    players = data.get('response', {}).get('players', [])

    # Convert to DataFrame
    df_user_info = pd.DataFrame(players)

    return df_user_info

def iterate_over_steam_ids_and_fetch_data(steam_ids: list) -> pd.DataFrame:
    all_data = pd.DataFrame()
    for steam_id in steam_ids:
        df = fetch_user_info(steam_id)
        all_data = pd.concat([all_data, df], ignore_index=True)

    all_data = all_data.astype({"steamid": 'Int64', "communityvisibilitystate": 'Int64', "profilestate": 'Int64',
                                "commentpermission": 'Int64', "lastlogoff": 'Int64', "personastate": 'Int64',
                                "primaryclanid": 'Int64', "timecreated": 'Int64', "personastateflags": 'Int64',
                                "loccityid": 'Int64'})
    return all_data

def return_steam_ids_from_db() -> list:
    friends_df = get_friends_list_from_db()['steamid'].tolist()
    friends_df.append(int(return_steam_id()))  # Add own Steam ID to the list

    return friends_df


if __name__ == "__main__":
    steam_ids = return_steam_ids_from_db()
    df = iterate_over_steam_ids_and_fetch_data(steam_ids)
    transfer_df_to_sql(df, table_name='SteamUserInfo', replace_table=True)