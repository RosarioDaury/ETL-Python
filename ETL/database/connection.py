from config import SERVER, TARGET_DATABASE, SOURCE_DATABASE
from sqlalchemy import create_engine
import pyodbc as pyodbc

def getEngineSource():
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = f'mssql+pyodbc://@{SERVER}/{SOURCE_DATABASE}?driver={driver}'
    engine = create_engine(connection_string)
    return engine

def getEngineTarget():
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = f'mssql+pyodbc://@{SERVER}/{TARGET_DATABASE}?driver={driver}'
    engine = create_engine(connection_string)
    return engine
