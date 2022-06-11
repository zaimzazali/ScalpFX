from data_ingestion import *

def main():
    data = getData()

    # Get the latest timestamp available in the database
    startDate = getLatestTimestamp(data['dbConfig'])
    print(f"Start date: {startDate}")
    
    # Get historical data, then calculate the Mid values
    history = getHistoricalData(data['ig_service_live'], startDate)
    mid_df = calculateMidValues(history)

    # Delete dirty data, then push historical mid-value data to database
    deleteDirtyData(data['dbConfig'], startDate)
    pushDataToDatabase(data['dbConfig'], mid_df)


if __name__ == "__main__":
    main()
