from Utils.dotenv_util import return_steam_key, return_steam_id
from Utils.database_util import transfer_df_to_sql
import requests
import pandas as pd

def fetch_friend_list() -> pd.DataFrame:
    """Fetches the friend list of a Steam user from the Steam API and returns it as a pandas DataFrame."""
    api_key = return_steam_key()
    steam_id = return_steam_id()
    url = f"http://api.steampowered.com/ISteamUser/GetFriendList/v1"
    params = {
        'key': api_key,
        'steamid': steam_id
    }

    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an error for bad responses

    data = response.json()

    # Extract the list of friends
    friends = data.get('friendslist', {}).get('friends', [])

    # Convert to DataFrame
    df_friend_list = pd.DataFrame(friends)

    return df_friend_list

if __name__ == '__main__':
    df = fetch_friend_list()
    transfer_df_to_sql(df, table_name='SteamFriendList', replace_table=True)