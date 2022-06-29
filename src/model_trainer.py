import pandas as pd

import data_transformer as dataTransformer
from Utils_Python.database_connector.DatabaseConnector import DatabaseConnector



ROW_COUNT = 96 * 5 * 2  # Points in 1 day * Number of Days * Number of Weeks
TABLE_NAME = 'GBPUSD_15MIN'
TMP_DF_PATH = '/ScalpFX/src/data/tmp_df.pkl'
TMP_TRANS_PATH = '/ScalpFX/src/data/tmp_trans.pkl'
TMP_NORM_PATH = '/ScalpFX/src/data/tmp_norm.pkl'


def closeDatabaseConnection(databaseConnector, cur, connObject):
    cur.close()
    databaseConnector.closeConnection(connObject)


def getTrainingData(connTag, verbose=False):
    databaseConnector = DatabaseConnector()
    connObject = databaseConnector.openConnection(connTag)
    conn = connObject['connection']
    cur = conn.cursor()

    query = (   
                f"SELECT * "
                f"FROM \"{TABLE_NAME}\" "\
                f"ORDER BY datetime DESC "
                f"LIMIT {ROW_COUNT}"
            )

    colNames = None
    try:
        cur.execute(query)
        colNames = [desc[0] for desc in cur.description]
    except Exception as e:
        closeDatabaseConnection(databaseConnector, cur, connObject)
        raise Exception(f"Could not execute: {query}\n{e}")

    res = cur.fetchall()    
    closeDatabaseConnection(databaseConnector, cur, connObject)

    df = pd.DataFrame(res, columns=colNames)
    
    if verbose:
        print(df)

    df.to_pickle(TMP_DF_PATH)
    print(f"Training Data has been saved to '{TMP_DF_PATH}'")


def transformData(verbose=False):
    df = pd.read_pickle(TMP_DF_PATH)

    working_df = dataTransformer.calcFeaturesEngineering(df)
    working_df = dataTransformer.calcTA(working_df)

    if verbose:
        print(working_df)

    working_df.to_pickle(TMP_TRANS_PATH)
    print(f"Transformed Data has been saved to '{TMP_TRANS_PATH}'")
    

def normalisedDataframe(verbose=False):
    df = pd.read_pickle(TMP_TRANS_PATH)

    working_df = dataTransformer.normalised(df, mode='train')

    if verbose:
        print(working_df)

    working_df.to_pickle(TMP_NORM_PATH)
    print(f"Normalised Data has been saved to '{TMP_NORM_PATH}'")


def prepareDataToTrain():
    df = pd.read_pickle(TMP_NORM_PATH)


def trainModel():
    pass