
from sqlalchemy import create_engine, text
import psycopg2
import os
import dotenv
import pandas as pd

dotenv.load_dotenv()

class Servidores:
    def __init__(self):
        self.host = ""
        self.database = ""
        self.user = ""
        self.password = ""
        self.port = ""

    def get_table(self, table_name):
        host = os.getenv("RDS_HOST")
        database = os.getenv("RDS_DB")
        user = os.getenv("RDS_USER")
        password = os.getenv("RDS_PASSWORD")
        port = os.getenv("RDS_PORT")
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        conn = engine.connect()
        statement = text("SELECT * FROM " + table_name + ";")
        rows = conn.execute(statement)
        df = pd.DataFrame(rows.fetchall())
        return df

    def create_table(self, df, table_name):
        host = os.getenv("RDS_HOST")
        database = os.getenv("RDS_DB")
        user = os.getenv("RDS_USER")
        password = os.getenv("RDS_PASSWORD")
        port = os.getenv("RDS_PORT")
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        df.to_sql(table_name, engine, if_exists='replace', index=False)

    def populate_column(self, df, column_name, table_name):
        host = os.getenv("RDS_HOST")
        database = os.getenv("RDS_DB")
        user = os.getenv("RDS_USER")
        password = os.getenv("RDS_PASSWORD")
        port = os.getenv("RDS_PORT")
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        conn = engine.connect()
        for index, row in df.iterrows():
            statement = text("UPDATE " + table_name + " SET " + column_name + " = " + str(row[column_name]) + " WHERE id_servidor_portal = " + str(row["id_servidor_portal"]) + ";")
            conn.execute(statement)