from dotenv import load_dotenv
import os

# Chargement des variables d'environnement
load_dotenv()

def return_steam_key() -> str:
    return os.getenv("STEAM_KEY")

def return_steam_id() -> str:
    return os.getenv("STEAM_ID")

def return_db_server() -> str:
    return os.getenv("DB_SERVER")

def return_db_name() -> str:
    return os.getenv("DB_NAME")

def return_db_user() -> str:
    return os.getenv("DB_USER")

def return_db_pass() -> str:
    return os.getenv("DB_PASSWORD")