from sqlalchemy import create_engine
import pyodbc as pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

SERVER = os.getenv('SERVER')
TARGET_DATABASE = os.getenv('TARGET_DATABASE')
SOURCE_DATABASE = os.getenv('SOURCE_DATABASE')


def getEngineSource():
    driver = 'ODBC Driver 17 for SQL Server'
    print(SERVER, SOURCE_DATABASE, TARGET_DATABASE)
    connection_string = f'mssql+pyodbc://@{SERVER}/{SOURCE_DATABASE}?driver={driver}'
    engine = create_engine(connection_string)
    return engine

def getEngineTarget():
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = f'mssql+pyodbc://@{SERVER}/{TARGET_DATABASE}?driver={driver}'
    engine = create_engine(connection_string)
    return engine
