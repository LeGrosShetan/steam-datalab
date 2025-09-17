from Utils.dotenv_util import return_steam_key
from Utils.database_util import transfer_df_to_sql
import requests
import pandas as pd

def fetch_app_list() -> pd.DataFrame:
    """Fetches the list of all Steam applications from the Steam API and returns it as a pandas DataFrame."""
    api_key = return_steam_key()
    url = f"https://api.steampowered.com/ISteamApps/GetAppList/v2/?key={api_key}"

    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses

    data = response.json()

    # Extract the list of apps
    apps = data.get('applist', {}).get('apps', [])

    # Convert to DataFrame
    df_app_list = pd.DataFrame(apps)

    return df_app_list

if __name__ == "__main__":
    df = fetch_app_list()
    transfer_df_to_sql(df, table_name='SteamLightAppList', replace_table=True)