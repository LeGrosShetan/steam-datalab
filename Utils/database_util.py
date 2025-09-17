import pyodbc
import urllib
import pandas as pd
from sqlalchemy import create_engine
from Utils.dotenv_util import return_db_server, return_db_name, return_db_user, return_db_pass

def return_connection() -> pyodbc.Connection:
    return pyodbc.connect('DRIVER={SQL Server};SERVER='+return_db_server()+';DATABASE='+return_db_name()+';UID='+return_db_user()+';PWD='+ return_db_pass())

def transfer_df_to_sql(df: pd.DataFrame, table_name: str, replace_table: bool) -> None:
    params = urllib.parse.quote_plus(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={return_db_server()};'
        f'DATABASE={return_db_name()};'
        f'UID={return_db_user()};'
        f'PWD={return_db_pass()}'
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    replace_table_str = 'replace' if replace_table else 'append'
    df.to_sql(name=table_name, con=engine, if_exists=replace_table_str, index=False, chunksize=1000)