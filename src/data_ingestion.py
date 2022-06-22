import psycopg2
from datetime import datetime
import pytz
import pandas as pd
from round2 import round2

from Utils_Python.database_connector.DatabaseConnector import DatabaseConnector
from trading.IG import IG



SCHEMA = "FOREX_MINI"
TABLE = "GBPUSD_15MIN"
TARGET_EPIC = 'CS.D.GBPUSD.MINI.IP'
RESOLUTION = '15Min'
INITIAL_TIMESTAMP = '2022-01-01 00:00:00'

TEMP_START_TIMESTAMP = '2022-01-01 00:00:00' # Temporary
TEMP_END_TIMESTAMP  = '2022-07-01 00:00:00' # Temporary



def openIgAPIconnection(ig, loginType):
    config_live = ig.getLoginConfig(loginType)
    ig_service = ig.getIgService(config_live)
    ig.getIgAccountDetails(ig_service)
    return ig_service


def closeDatabaseConnection(databaseConnector, cur, connObject):
    cur.close()
    databaseConnector.closeConnection(connObject)


def getLatestTimestamp(connTag=None, ti=None, taskIDs=None):
    if ti is not None:
        connTag = ti.xcom_pull(key='return_value', task_ids=taskIDs['getData'])['connTag']

    databaseConnector = DatabaseConnector()
    connObject = databaseConnector.openConnection(connTag)
    conn = connObject['connection']
    cur = conn.cursor()

    query = (f"SELECT MAX(datetime) "
             f"FROM \"{SCHEMA}\".\"{TABLE}\" ")
    try:
        cur.execute(query)
    except Exception as e:
        closeDatabaseConnection(databaseConnector, cur, connObject)
        raise Exception(f"Could not execute: {query}\n{e}")
    res = cur.fetchone()    
    closeDatabaseConnection(databaseConnector, cur, connObject)

    if res[0] is None:
        startDate = TEMP_START_TIMESTAMP
    else:
        startDate = res[0]

    return startDate


def getHistoricalData(ig_service=None, startDate=None, ti=None, taskIDs=None):
    if ti is not None:
        ig_service = ti.xcom_pull(key='return_value', task_ids=taskIDs['getData'])['ig_service']
        startDate = ti.xcom_pull(key='return_value', task_ids=taskIDs['getLatestTimestamp'])
    
    currentTimestamp = datetime.now(pytz.timezone('Asia/Kuala_Lumpur')).strftime('%Y-%m-%d %H:%M:%S')
    # currentTimestamp = TEMP_END_TIMESTAMP # Temporary
    print(f"Start Timestamp: {startDate}")
    print(f"End Timestamp: {currentTimestamp}")
    try:
        res = ig_service.fetch_historical_prices_by_epic_and_date_range(epic=TARGET_EPIC,
                                                                                resolution=RESOLUTION, 
                                                                                start_date=startDate, 
                                                                                end_date=currentTimestamp)
    except Exception as e:
        raise Exception(f"Could not 'fetch_historical_prices_by_epic_and_date_range'\n{e}")
    df = pd.DataFrame.from_dict(res['prices'])
    df = df.reset_index()
    return df


def averageTwoFloats(x,y):
    return round2((x+y)/2, 5)


def calculateMidValues(history=None, ti=None, taskIDs=None):
    if ti is not None:
        history = ti.xcom_pull(key='return_value', task_ids=taskIDs['getHistoricalData'])
    
    df = history.copy()

    df[('mid', 'Open')]  = df.apply(lambda x: averageTwoFloats(x[('bid', 'Open')],  x[('ask', 'Open')]), axis=1)
    df[('mid', 'High')]  = df.apply(lambda x: averageTwoFloats(x[('bid', 'High')],  x[('ask', 'High')]), axis=1)
    df[('mid', 'Low')]   = df.apply(lambda x: averageTwoFloats(x[('bid', 'Low')],   x[('ask', 'Low')]), axis=1)
    df[('mid', 'Close')] = df.apply(lambda x: averageTwoFloats(x[('bid', 'Close')], x[('ask', 'Close')]), axis=1)

    res = df[[('DateTime', ''), ('mid', 'Open'), ('mid', 'High'), ('mid', 'Low'), ('mid', 'Close'), ('last', 'Volume')]].copy()
    res.columns = res.columns.droplevel()
    res.columns = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']

    print(res)

    return res


def deleteDirtyData(connTag=None, startDate=None, ti=None, taskIDs=None):
    if ti is not None:
        connTag = ti.xcom_pull(key='return_value', task_ids=taskIDs['getData'])['connTag']
        startDate = ti.xcom_pull(key='return_value', task_ids=taskIDs['getLatestTimestamp'])

    databaseConnector = DatabaseConnector()
    connObject = databaseConnector.openConnection(connTag)
    conn = connObject['connection']
    cur = conn.cursor()

    query = (f"DELETE FROM \"{SCHEMA}\".\"{TABLE}\" " 
             f"WHERE datetime >= \'{startDate}\' ")
    try:
        cur.execute(query)
        conn.commit()
        print(f"---------- Deleted dirty data from {SCHEMA}.{TABLE} starting from '{startDate}' onwards.")
    except Exception as e:
        conn.rollback()
        closeDatabaseConnection(databaseConnector, cur, connObject)
        raise Exception(f"Could not execute: {query}\n{e}")
    closeDatabaseConnection(databaseConnector, cur, connObject)


def pushDataToDatabase(connTag=None, history=None, ti=None, taskIDs=None):
    if ti is not None:
        connTag = ti.xcom_pull(key='return_value', task_ids=taskIDs['getData'])['connTag']
        history = ti.xcom_pull(key='return_value', task_ids=taskIDs['calculateMidValues'])
    
    databaseConnector = DatabaseConnector()
    connObject = databaseConnector.openConnection(connTag)
    conn = connObject['connection']
    cur = conn.cursor()

    query = (f"INSERT INTO \"{SCHEMA}\".\"{TABLE}\" " 
                f"(datetime, open, high, low, close, volume) " 
             f"VALUES (%s, %s, %s, %s, %s, %s) ")
    try:
        cur.executemany(query, history.values.tolist())
        conn.commit()
        print(history.values.tolist())
    except Exception as e:
        conn.rollback()
        closeDatabaseConnection(databaseConnector, cur, connObject)
        return print(f"Could not execute: {query}\n{e}")
        # raise Exception(f"Could not execute: {query}\n{e}")
    closeDatabaseConnection(databaseConnector, cur, connObject)


def getData(connTag, loginType):
    ig = IG()
    ig_service = openIgAPIconnection(ig, loginType)
    return {'connTag':connTag, 'ig_service':ig_service}
