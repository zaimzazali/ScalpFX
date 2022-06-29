from Utils_Python.folder_importer.FolderImporter import FolderImporter
FolderImporter()
from model_trainer import *


def main():
    getTrainingData("PostgresqlIgTrading", verbose=True)
    removeColumns(['inserted_on_myt'], verbose=True)
    transformData(verbose=True)


if __name__ == "__main__":
    main()
