from database.connection import getEngineSource, getEngineTarget
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pandas
import time

def QueryExecutorSource(query):
    result = pandas.read_sql(query, getEngineSource())
    return result

def QueryExecutorTarget(query):
    engine = getEngineTarget()
    with engine.connect() as con:
        truncate_query = text('DELETE FROM SALESUMMARY')
        con.execute(truncate_query)

def InsertExecutorTarget(data, table_name):
    try:
        engine = getEngineTarget()
        data.to_sql(table_name, engine, if_exists = 'replace', index = False)
    except SQLAlchemyError as e:
        print('DEAD LOCK ERROR, retrying transaction...')
        time.sleep(3)
        InsertExecutorTarget(data, table_name)
