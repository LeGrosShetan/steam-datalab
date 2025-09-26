from Utils.dotenv_util import return_steam_key
from Utils.database_util import transfer_df_to_sql
import requests
import pandas as pd

def fetch_app_categories() -> pd.DataFrame:
    """Fetches the list of Steam app categories from the Steam API and returns it as a pandas DataFrame."""
    url = "https://api.steampowered.com/IStoreBrowseService/GetStoreCategories/v1/"
    params = {'key': return_steam_key(), "language": 'french'}

    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an error for bad responses

    data = response.json()

    categories = data.get('response', {}).get('categories', [])
    df_categories = pd.DataFrame(categories)

    return df_categories

if __name__ == "__main__":
    df = fetch_app_categories()
    transfer_df_to_sql(df, table_name='SteamCategories', replace_table=True)