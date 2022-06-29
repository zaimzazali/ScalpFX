from Utils_Python.folder_importer.FolderImporter import FolderImporter
FolderImporter()
from src.data_ingestion import *

def main():
    data = getData("PostgresqlIgTrading", "live")

    # Get the latest timestamp available in the database
    startDate = getLatestTimestamp(data['connTag'])
    
    # Get historical data, then calculate the Mid values
    history = getHistoricalData(data['ig_service_live'], startDate)
    mid_df = calculateMidValues(history)

    # Delete dirty data, then push historical mid-value data to database
    deleteDirtyData(data['connTag'], startDate)
    pushDataToDatabase(data['connTag'], mid_df)


if __name__ == "__main__":
    main()
