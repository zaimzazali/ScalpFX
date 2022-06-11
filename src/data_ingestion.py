import sys
from configparser import ConfigParser
import psycopg2
from datetime import datetime
import pandas as pd

sys.path.append("/ScalpFX/")
from src.utils.IG import IG
from credentials import database_connection

# sys.path.append("./utils/")


SCHEMA = "FOREX_MINI"
TABLE = "GBPUSD_15MIN"
TARGET_EPIC = 'CS.D.GBPUSD.MINI.IP'
RESOLUTION = '15Min'
INITIAL_TIMESTAMP = '2022-01-01 00:00:00'
DATA_POINTS = 10000


def getDatabaseConfig(parser, connTag):
    parser.read(database_connection)
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
    query = ("SELECT MAX(datetime) "
             "FROM \"{SCHEMA}\".\"{TABLE}\" ")
    try:
        cur.execute(query)
    except Exception as e:
        closeDatabaseConnection(cur, conn)
        raise Exception(f"Could not execute: {query}\n{e}")
    res = cur.fetchone()    
    closeDatabaseConnection(cur, conn)
    if res is None:
        return INITIAL_TIMESTAMP
    else:
        return res[0]


def getHistoricalData(ig_service_live, startDate):
    currentTimestamp = datetime.now()
    try:
        res = ig_service_live.fetch_historical_prices_by_epic_and_date_range(epic=TARGET_EPIC, 
                                                                                resolution=RESOLUTION, 
                                                                                start_date=startDate, 
                                                                                end_date=currentTimestamp, 
                                                                                numpoints=DATA_POINTS)
    except Exception as e:
        raise Exception(f"Could not 'fetch_historical_prices_by_epic_and_date_range'\n{e}")
    df = pd.DataFrame.from_dict(res['prices'])
    df = df.reset_index()
    return df


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

    # Get historical data, delete dirty data, and push to database
    history = getHistoricalData(ig_service_live, startDate)
    deleteDirtyData(dbConfig, startDate)
    pushDataToDatabase(dbConfig, history)


if __name__ == "__main__":
    main()