from sqlalchemy import create_engine
import os
import dotenv
import pandas as pd

dotenv.load_dotenv()

host = os.getenv("RDS_HOST")
database = os.getenv("RDS_DB")
user = os.getenv("RDS_USER")
password = os.getenv("RDS_PASSWORD")
port = os.getenv("RDS_PORT")

def create_table(df, table_name, if_exists='replace'):
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)

def get_table(table_name):
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    return df
