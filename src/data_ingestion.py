import sys
from configparser import ConfigParser
import psycopg2
from datetime import datetime
import pandas as pd
from round2 import round2

sys.path.append("/ScalpFX/")
from src.utils.IG import IG


SCHEMA = "FOREX_MINI"
TABLE = "GBPUSD_15MIN"
TARGET_EPIC = 'CS.D.GBPUSD.MINI.IP'
RESOLUTION = '15Min'
INITIAL_TIMESTAMP = '2022-01-01 00:00:00'
# DATA_POINTS = 5000

TEMP_START_TIMESTAMP = '2022-01-01 00:00:00' # Temporary
TEMP_END_TIMESTAMP  = '2022-07-01 00:00:00' # Temporary


def getDatabaseConfig(parser, connTag):
    parser.read("/ScalpFX/credentials/database_connection.ini")
    db = {}
    if parser.has_section(connTag):
        params = parser.items(connTag)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f"Section '{connTag}' not found in the database connection ini file")
    return db


def openIgAPIconnection(ig):
    config_live = ig.getLoginConfig('live')
    ig_service_live = ig.getIgService(config_live)
    ig.getIgAccountDetails(ig_service_live)
    return ig_service_live


def closeDatabaseConnection(cur, conn):
    cur.close()
    conn.close()


def getLatestTimestamp(dbConfig):
    conn = psycopg2.connect(host=dbConfig['host'],
                            dbname=dbConfig['database'],
                            user=dbConfig['user'],
                            password=dbConfig['password'])
    cur = conn.cursor()
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
        return TEMP_START_TIMESTAMP
    else:
        return res[0]


def getHistoricalData(ig_service_live, startDate):
    currentTimestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # currentTimestamp = TEMP_END_TIMESTAMP # Temporary
    try:
        # res = ig_service_live.fetch_historical_prices_by_epic(epic=TARGET_EPIC, 
        #                                                         resolution=RESOLUTION, 
        #                                                         start_date=startDate, 
        #                                                         end_date=currentTimestamp, 
        #                                                         numpoints=DATA_POINTS)
        res = ig_service_live.fetch_historical_prices_by_epic_and_date_range(epic=TARGET_EPIC,
                                                                                resolution=RESOLUTION, 
                                                                                start_date=startDate, 
                                                                                end_date=currentTimestamp)
    except Exception as e:
        raise Exception(f"Could not 'fetch_historical_prices_by_epic'\n{e}")
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
    conn = psycopg2.connect(host=dbConfig['host'],
                            dbname=dbConfig['database'],
                            user=dbConfig['user'],
                            password=dbConfig['password'])
    cur = conn.cursor()
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
    conn = psycopg2.connect(host=dbConfig['host'],
                            dbname=dbConfig['database'],
                            user=dbConfig['user'],
                            password=dbConfig['password'])
    cur = conn.cursor()
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


def main():
    # Instantiate objects 
    ig = IG()
    parser = ConfigParser()
    dbConfig = getDatabaseConfig(parser, "PostgresqlIgTrading")
    ig_service_live = openIgAPIconnection(ig)

    # Get the latest timestamp available in the database
    startDate = getLatestTimestamp(dbConfig)
    print(f"Start date: {startDate}")
    
    # Get historical data, then calculate the Mid values
    history = getHistoricalData(ig_service_live, startDate)
    mid_df = calculateMidValues(history)

    # Delete dirty data, then push historical mid-value data to database
    deleteDirtyData(dbConfig, startDate)
    pushDataToDatabase(dbConfig, mid_df)


if __name__ == "__main__":
    main()