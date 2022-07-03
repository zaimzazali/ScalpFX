from Utils_Python.folder_importer.FolderImporter import FolderImporter
FolderImporter()
from model_trainer import *


TMP_DATA_PATH = '/ScalpFX/src/data/tmp_df.pkl'
TMP_DF_PATH = '/ScalpFX/src/data/tmp_df.pkl'
TMP_TRANS_PATH = '/ScalpFX/src/data/tmp_trans.pkl'
TMP_NORM_PATH = '/ScalpFX/src/data/tmp_norm.pkl'
TMP_PREP_PATH = '/ScalpFX/src/data/tmp_prep.json'



def main():
    # getTrainingData(TMP_DATA_PATH, "PostgresqlIgTrading", verbose=True)
    # removeColumns(TMP_DATA_PATH, TMP_DF_PATH, ['index', 'datetime', 'inserted_on_myt'], verbose=True)
    # transformData(TMP_DF_PATH, TMP_TRANS_PATH, verbose=True)
    # normalisedDataframe(TMP_TRANS_PATH, TMP_NORM_PATH, verbose=True)
    prepareDataToTrain(TMP_NORM_PATH, 96, verbose=True)
    trainModel()


if __name__ == "__main__":
    main()
