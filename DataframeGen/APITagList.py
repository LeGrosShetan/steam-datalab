from Utils.dotenv_util import return_steam_key
from Utils.database_util import transfer_df_to_sql
import requests
import pandas as pd

def fetch_tag_list() -> pd.DataFrame:
    """Fetches the list of Steam tags from the Steam API and returns it as a pandas DataFrame."""
    url = "https://api.steampowered.com/IStoreService/GetTagList/v1/"
    params = {'key': return_steam_key(), 'language': 'french'}

    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an error for bad responses

    data = response.json()

    # Extract the list of apps
    tags = data.get('response', {}).get('tags', [])

    # Convert to DataFrame
    df_tag_list = pd.DataFrame(tags)

    return df_tag_list

if __name__ == "__main__":
    df = fetch_tag_list()
    transfer_df_to_sql(df, table_name='SteamTagList', replace_table=True)