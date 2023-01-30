import os
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()

class Servidor(Base):
    __tablename__ = 'servidores'

    id_servidor_portal = Column(Integer, primary_key=True)
    nome = Column(String)
    cpf = Column(String)
    matricula = Column(String)
    cnpq_id = Column(Integer)
    cnpq_web_id = Column(Integer)

def create_table_in_rds(df):
    host = os.getenv("RDS_HOST")
    database = os.getenv("RDS_DB")
    user = os.getenv("RDS_USER")
    password = os.getenv("RDS_PASSWORD")
    port = os.getenv("RDS_PORT")
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    #drop servidores table
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    
    # Create a new session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    for i, row in df.iterrows():
        servidor = Servidor(id_servidor_portal=row['Id_SERVIDOR_PORTAL'], nome=row['NOME'], cpf=row['CPF'], matricula=row['MATRICULA'], cnpq_id=row['CNPQ_ID'], cnpq_web_id=row['CNPQ_WEB_ID'])
        session.add(servidor)
    session.commit()
    
    session.close()

def get_servidores():
    host = os.getenv("RDS_HOST")
    database = os.getenv("RDS_DB")
    user = os.getenv("RDS_USER")
    password = os.getenv("RDS_PASSWORD")
    port = os.getenv("RDS_PORT")
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    conn = engine.connect()
    rows = conn.execute("SELECT * FROM servidores")
    return rows.fetchall()
