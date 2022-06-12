import sys
from configparser import ConfigParser
import psycopg2
from datetime import datetime
import pandas as pd
from round2 import round2

sys.path.append(f"../")
from src.utils.IG import IG


SCHEMA = "FOREX_MINI"
TABLE = "GBPUSD_15MIN"
TARGET_EPIC = 'CS.D.GBPUSD.MINI.IP'
RESOLUTION = '15Min'
INITIAL_TIMESTAMP = '2022-01-01 00:00:00'

TEMP_START_TIMESTAMP = '2022-01-01 00:00:00' # Temporary
TEMP_END_TIMESTAMP  = '2022-07-01 00:00:00' # Temporary


def getDatabaseConfig(parser, connTag, filePath):
    parser.read(filePath)
    # Create a Config file (database_connection.ini) in the credentials folder with the following format:-
    # 
    # [PostgresqlIgTrading]
    # host=<HOST_IP_ADDRESS>
    # port=5432
    # database=<DATABASE_NAME>
    # user=<USER>
    # password=<PASSWORD>
    #
    db = {}
    if parser.has_section(connTag):
        params = parser.items(connTag)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f"Section '{connTag}' not found at {filePath}.")
    return db


def openIgAPIconnection(ig):
    config_live = ig.getLoginConfig('live', "./pipelines/ScalpFX/credentials/trading_ig_config.ini")
    ig_service_live = ig.getIgService(config_live)
    ig.getIgAccountDetails(ig_service_live)
    return ig_service_live


def openDatabaseConnection(dbConfig):
    conn = psycopg2.connect(host=dbConfig['host'],
                            dbname=dbConfig['database'],
                            user=dbConfig['user'],
                            password=dbConfig['password'])
    cur = conn.cursor()
    return cur, conn


def closeDatabaseConnection(cur, conn):
    cur.close()
    conn.close()


def getLatestTimestamp(dbConfig=None, ti=None, taskIDs=None):
    if ti is not None:
        dbConfig = ti.xcom_pull(key='dbConfig', task_ids=taskIDs)

    cur, conn = openDatabaseConnection(dbConfig)
    query = (f"SELECT MAX(datetime) "
             f"FROM \"{SCHEMA}\".\"{TABLE}\" ")
    try:
        cur.execute(query)
    except Exception as e:
        closeDatabaseConnection(cur, conn)
        raise Exception(f"Could not execute: {query}\n{e}")
    res = cur.fetchone()    
    closeDatabaseConnection(cur, conn)

    if res[0] is None:
        startDate = TEMP_START_TIMESTAMP
    else:
        startDate = res[0]

    try:
        ti.xcom_push(key='startDate', value=startDate)
    except Exception as e:
        if ti is None:
            print("XCom is not available")
        else:
            raise Exception(f"Could not push data to XCom\n{e}")
    return startDate


def getHistoricalData(ig_service_live, startDate):
    currentTimestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # currentTimestamp = TEMP_END_TIMESTAMP # Temporary
    try:
        res = ig_service_live.fetch_historical_prices_by_epic_and_date_range(epic=TARGET_EPIC,
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


def calculateMidValues(history):
    df = history.copy()

    df[('mid', 'Open')]  = df.apply(lambda x: averageTwoFloats(x[('bid', 'Open')],  x[('ask', 'Open')]), axis=1)
    df[('mid', 'High')]  = df.apply(lambda x: averageTwoFloats(x[('bid', 'High')],  x[('ask', 'High')]), axis=1)
    df[('mid', 'Low')]   = df.apply(lambda x: averageTwoFloats(x[('bid', 'Low')],   x[('ask', 'Low')]), axis=1)
    df[('mid', 'Close')] = df.apply(lambda x: averageTwoFloats(x[('bid', 'Close')], x[('ask', 'Close')]), axis=1)

    res = df[[('DateTime', ''), ('mid', 'Open'), ('mid', 'High'), ('mid', 'Low'), ('mid', 'Close'), ('last', 'Volume')]].copy()
    res.columns = res.columns.droplevel()
    res.columns = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']

    return res


def deleteDirtyData(dbConfig, startDate):
    cur, conn = openDatabaseConnection(dbConfig)
    query = (f"DELETE FROM \"{SCHEMA}\".\"{TABLE}\" " 
             f"WHERE datetime >= \'{startDate}\' ")
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        closeDatabaseConnection(cur, conn)
        raise Exception(f"Could not execute: {query}\n{e}")
    closeDatabaseConnection(cur, conn)


def pushDataToDatabase(dbConfig, history):
    cur, conn = openDatabaseConnection(dbConfig)
    query = (f"INSERT INTO \"{SCHEMA}\".\"{TABLE}\" " 
                f"(datetime, open, high, low, close, volume) " 
             f"VALUES (%s, %s, %s, %s, %s, %s) ")
    try:
        cur.executemany(query, history.values.tolist())
        conn.commit()
    except Exception as e:
        conn.rollback()
        closeDatabaseConnection(cur, conn)
        raise Exception(f"Could not execute: {query}\n{e}")
    closeDatabaseConnection(cur, conn)


def getData(connTag, filePath, ti=None):
    # Instantiate objects 
    ig = IG()
    parser = ConfigParser()

    dbConfig = getDatabaseConfig(parser, connTag, filePath)
    ig_service_live = openIgAPIconnection(ig)

    try:
        ti.xcom_push(key='dbConfig', value=dbConfig)
        ti.xcom_push(key='ig_service_live', value=ig_service_live)
    except Exception as e:
        if ti is None:
            print("XCom is not available")
        else:
            raise Exception(f"Could not push data to XCom\n{e}")
            
    return {'dbConfig':dbConfig, 'ig_service_live':ig_service_live}
