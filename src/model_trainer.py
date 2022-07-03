import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.losses import MeanSquaredError
from tensorflow.keras.metrics import MeanAbsoluteError, MeanSquaredError, RootMeanSquaredError
from tensorflow.keras.optimizers import Adam

import data_transformer as dataTransformer
from Utils_Python.database_connector.DatabaseConnector import DatabaseConnector
from Utils_Python.file_writer.FileWriter import FileWriter
from Utils_Python.file_reader.FileReader import FileReader


POINT_IN_A_DAY = 96
ADJUSTER = 2.103
ROW_COUNT = (POINT_IN_A_DAY * 5 * 2) * ADJUSTER  # (Points in 1 day * Number of Days * Number of Weeks) * n
DATABASE_NAME = 'IG_TRADING'
SCHEMA_NAME = 'FOREX_MINI'
TABLE_NAME = 'GBPUSD_15MIN'
DATA_PATH = '/ScalpFX/src/data'
MODEL_PATH = '/ScalpFX/src/model'


RANDOM_SEED = 42


def closeDatabaseConnection(databaseConnector, cur, connObject):
    cur.close()
    databaseConnector.closeConnection(connObject)


def getTrainingData(filePathToWrite, connTag, verbose=False):
    databaseConnector = DatabaseConnector()
    connObject = databaseConnector.openConnection(connTag)
    conn = connObject['connection']
    cur = conn.cursor()

    query = (   
                f"SELECT * "
                f"FROM \"{DATABASE_NAME}\".\"{SCHEMA_NAME}\".\"{TABLE_NAME}\" "
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
    df = df.sort_values(['datetime'], ascending=True).reset_index()

    if verbose:
        print('getTrainingData()')
        print(f"{df}\n")

    df.to_pickle(filePathToWrite)
    print(f"Training Data has been saved to '{filePathToWrite}'\n")


def removeColumns(filePathToRead, filePathToWrite, columnNames, verbose=False):
    df = pd.read_pickle(filePathToRead)

    df.index = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')
    working_df = df.drop(columns=columnNames)

    if verbose:
        print('removeColumns()')
        print(f"{working_df}\n")

    working_df.to_pickle(filePathToWrite)
    print(f"Removed Columns' Training Data has been saved to '{filePathToWrite}'\n")


def transformData(filePathToRead, filePathToWrite, verbose=False):
    df = pd.read_pickle(filePathToRead)

    working_df = dataTransformer.calcFeaturesEngineering(df, verbose=verbose)
    working_df = dataTransformer.calcTA(working_df, verbose=verbose)

    working_df.to_pickle(filePathToWrite)
    print(f"Transformed Data has been saved to '{filePathToWrite}'\n")
    

def normalisedDataframe(filePathToRead, filePathToWrite, verbose=False):
    df = pd.read_pickle(filePathToRead)

    working_df = dataTransformer.normalised(df, mode='train')

    if verbose:
        print('normalisedDataframe()')
        print(f"{working_df}\n")

    working_df.to_pickle(filePathToWrite)
    print(f"Normalised Data has been saved to '{filePathToWrite}'\n")


def createDataset(X, y, windowSize=1):
    Xs, ys = [], []
    for i in range(len(X) - windowSize):
        v = X.iloc[i:(i+windowSize)].to_numpy()
        Xs.append(v)
        ys.append(y.iloc[i+windowSize])
    return np.array(Xs), np.array(ys)


def prepareDataToTrain(filePathToRead, windowSize, verbose=False):
    df = pd.read_pickle(filePathToRead)

    trainSize = int(len(df) * 0.9)
    train, test = df.iloc[:trainSize], df.iloc[trainSize:]

    print(train.shape, test.shape)

    targetColumn = 'close'
    X_train, y_train = createDataset(train, train[targetColumn], windowSize)
    X_test, y_test = createDataset(test, test[targetColumn], windowSize)

    print(X_train.shape, y_train.shape)
    print(X_test.shape, y_test.shape)

    print(X_train[0][0])
    print(y_train[95:98])

    fileReader = FileWriter()
    fileReader.writeJSONfile(f"{DATA_PATH}/X_test.json", {'X_test':X_test.tolist()})
    fileReader.writeJSONfile(f"{DATA_PATH}/y_test.json", {'y_test':y_test.tolist()})
    fileReader.writeJSONfile(f"{DATA_PATH}/X_train.json", {'X_train':X_train.tolist()})
    fileReader.writeJSONfile(f"{DATA_PATH}/y_train.json", {'y_train':y_train.tolist()})
    
    print(f"Prepared Dataset has been saved!\n")


def trainModel():
    fileReader = FileReader()
    X_train = np.asarray(fileReader.readJSONfile(f"{DATA_PATH}/X_train.json")['X_train'])
    y_train = np.asarray(fileReader.readJSONfile(f"{DATA_PATH}/y_train.json")['y_train'])

    print(X_train.shape, y_train.shape)
    print(X_train.shape[1], X_train.shape[2])

    print(X_train[0][0])
    print(X_train[0][1])
    print(y_train[0])

    lstm_model = tf.keras.models.Sequential()
    lstm_model.add(tf.keras.layers.InputLayer(input_shape=(X_train.shape[1], X_train.shape[2])))
    lstm_model.add(
        tf.keras.layers.Bidirectional(
            tf.keras.layers.LSTM(
                units=128,
                return_sequences=True,
                input_shape=(X_train.shape[1], X_train.shape[2])
            )
        )
    )
    lstm_model.add(tf.keras.layers.Dropout(rate=0.2))
    lstm_model.add(tf.keras.layers.Dense(units=8, activation='relu'))
    lstm_model.add(tf.keras.layers.Dense(units=1))
    lstm_model.build()

    print(lstm_model.summary())

    lstm_model.compile(optimizer=Adam(), loss=MeanSquaredError())

    cp = tf.keras.callbacks.ModelCheckpoint(MODEL_PATH, save_best_only=True)
    history = lstm_model.fit(
        X_train, y_train,
        epochs=1000000,
        batch_size=32,
        validation_split=0.1,
        shuffle=False,
        callbacks=[cp]
    )


    
