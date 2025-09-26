from Utils.database_util import transfer_df_to_sql, get_app_list_from_db
from datetime import datetime
import requests
import time
import pandas as pd

def fetch_app_info(appid: int, retries = 0) -> pd.DataFrame:
    if retries > 3:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    """Fetches app info data from the Steam API and returns it as a pandas DataFrame."""
    url = "https://store.steampowered.com/api/appdetails/"
    params = {'l': 'french', 'cc': 'FR', 'format': 'json', 'appids': int(appid)  }

    try :
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an error for bad responses

    except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
        print(f"HTTP error occurred for app ID {appid}: {e}. Retrying ({retries+1}/3)...")
        time.sleep(5)  # Wait for 5 seconds before retrying
        return fetch_app_info(appid, retries + 1)


    data = response.json()

    # Extract the list of apps
    apps = data.get(str(appid), {}).get('data', {})
    data = {}

    data["type"] = apps.get("type", None)
    data["name"] = apps.get("name", None)
    data["steam_appid"] = apps.get("steam_appid", None)
    data["required_age"] = apps.get("required_age", None)
    data["is_free"] = apps.get("is_free", None)
    data["controller_support"] = apps.get("controller_support", None)
    data["detailed_description"] = apps.get("detailed_description", None)
    data["about_the_game"] = apps.get("about_the_game", None)
    data["short_description"] = apps.get("short_description", None)
    data["supported_languages"] = apps.get("supported_languages", None)
    data["header_image"] = apps.get("header_image", None)
    data["capsule_image"] = apps.get("capsule_image", None)
    data["capsule_imagev5"] = apps.get("capsule_imagev5", None)
    data["website"] = apps.get("website", None)
    data["legal_notice"] = apps.get("legal_notice", None)
    data["developers"] = ", ".join(apps.get("developers") or [])
    data["publishers"] = ", ".join(apps.get("publishers") or [])
    data["background"] = apps.get("background", None)
    data["background_raw"] = apps.get("background_raw", None)
    data["metacritic_score"] = apps.get("metacritic", {}).get("score", None)
    data["total_recommendations"] = apps.get("recommendations", {}).get("total", None)
    data["is_coming_soon"] = apps.get("release_date", {}).get("coming_soon", None)
    data["release_date"] = apps.get("release_date", {}).get("date", None)
    data["fullgame_appid"] = apps.get("fullgame", {}).get("appid", None)

    packages = apps.get("packages", {})
    packages_df = pd.DataFrame({"app_id":appid, "package_id":packages})
    packages_df = packages_df.astype({"package_id": 'Int64'})

    categories_list = [category['id'] for category in apps.get('categories', [])]
    categories_df = pd.DataFrame({"app_id":appid, "category_id":categories_list})
    categories_df = categories_df.astype({"category_id": 'Int64'})

    # Convert to DataFrame
    df_app_info = pd.DataFrame(data, index=[0])
    df_app_info = df_app_info.astype({"fullgame_appid": 'Int64'})

    return df_app_info, packages_df, categories_df

def iterate_over_app_ids_and_fetch_data(app_ids: list) -> pd.DataFrame:
    app_data = pd.DataFrame()
    package_data = pd.DataFrame()
    category_data = pd.DataFrame()

    for app_id in app_ids:
        app_id = int(app_id)

        time.sleep(2)
        currentTime = datetime.now()
        print(f"{currentTime.hour}:{currentTime.minute}:{currentTime.second} - Fetching data for app ID: {app_id}")
        app_df, package_df, category_df = fetch_app_info(app_id)

        app_data = pd.concat([app_data, app_df], ignore_index=True)
        package_data = pd.concat([package_data, package_df], ignore_index=True)
        category_data = pd.concat([category_data, category_df], ignore_index=True)

    return app_data, package_data, category_data

def return_app_ids_from_db() -> list:
    app_ids = get_app_list_from_db()['appid'].astype("Int64")

    return app_ids.tolist()

if __name__ == "__main__":
    app_ids = return_app_ids_from_db()
    app_df, package_df, category_df = iterate_over_app_ids_and_fetch_data(app_ids)

    transfer_df_to_sql(app_df, table_name='SteamAppInfo', replace_table=True)
    transfer_df_to_sql(package_df, table_name='SteamAppPackages', replace_table=True)
    transfer_df_to_sql(category_df, table_name='SteamAppCategories', replace_table=True)