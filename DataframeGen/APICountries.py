from Utils.dotenv_util import return_steam_key
from Utils.database_util import transfer_df_to_sql
import requests
import pandas as pd

def fetch_countries_data() -> pd.DataFrame:
    """Fetches country data from the Steam API and returns it as a pandas DataFrame."""
    api_key = return_steam_key()
    url = f"https://api.steampowered.com/IStoreTopSellersService/GetCountryList/v1/?key={api_key}"

    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses

    data = response.json()

    # Extract the list of countries
    countries = data.get('response', {}).get('countries', [])

    # Convert to DataFrame
    df_countries = pd.DataFrame(countries)

    return df_countries

if __name__ == "__main__":
    df = fetch_countries_data()
    transfer_df_to_sql(df, table_name='SteamCountries', replace_table=True)
