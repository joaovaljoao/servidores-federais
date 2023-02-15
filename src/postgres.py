
from sqlalchemy import create_engine, text
import os
import dotenv
import pandas as pd

dotenv.load_dotenv()
host = os.getenv("RDS_HOST")
database = os.getenv("RDS_DB")
user = os.getenv("RDS_USER")
password = os.getenv("RDS_PASSWORD")
port = os.getenv("RDS_PORT")


def get_table(table_name):
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    conn = engine.connect()
    statement = text("SELECT * FROM " + table_name + ";")
    rows = conn.execute(statement)
    df = pd.DataFrame(rows.fetchall())
    return df

def create_table(df, table_name, if_exists='replace'):
    host = os.getenv("RDS_HOST")
    database = os.getenv("RDS_DB")
    user = os.getenv("RDS_USER")
    password = os.getenv("RDS_PASSWORD")
    port = os.getenv("RDS_PORT")
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)