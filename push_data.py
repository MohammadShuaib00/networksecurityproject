import os
import sys
import json
from dotenv import load_dotenv
import certifi
import pandas as pd
import numpy as np
import pymongo
from networksecurity.logger.logging import logging
from networksecurity.exception.exception import NetworkSecurityException

# Load environment variables from a .env file
load_dotenv()

# Get the MongoDB URL from the environment variables
MONGODB_URL = os.getenv("MONGO_DB_URL")

# Get the CA certificates for secure connection
ca = certifi.where()


class NetworkDataExtract:
    def __init__(self):
        try:
            # Initialize MongoDB client
            self.mongo_client = pymongo.MongoClient(MONGODB_URL, tlsCAFile=ca)

        except Exception as e:
            raise NetworkSecurityException(e, sys.exc_info())

    def cv_to_json_convert(self, file_path):
        try:
            logging.info("Data is being converted into JSON")
            # Read CSV data into a DataFrame
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            # Convert DataFrame to JSON records
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys.exc_info())

    def insert_data_mongodb(self, records, database, collection):
        try:
            # Connect to the specified database and collection
            self.database = database
            self.collection = collection
            self.records = records

            # Connect to MongoDB
            self.mongo_client = pymongo.MongoClient(MONGODB_URL)
            self.database = self.mongo_client[self.database]
            self.collection = self.database[self.collection]

            # Insert the records into the collection
            self.collection.insert_many(self.records)  # Use self.records instead of self.collection
            return len(self.records)

        except Exception as e:
            raise NetworkSecurityException(e, sys.exc_info())


if __name__ == "__main__":
    FILE_PATH = "networksdata\phisingData.csv"  # Make sure the path is correct
    DATABASE = "myDatabase"
    COLLECTION = "NetworkData"
    
    # Create an instance of the NetworkDataExtract class
    networkobj = NetworkDataExtract()
    
    # Convert the CSV data to JSON records
    records = networkobj.cv_to_json_convert(file_path=FILE_PATH)
    print(records)  # Output the records for verification
    
    # Insert the records into MongoDB
    no_of_records = networkobj.insert_data_mongodb(records, DATABASE, COLLECTION)
    print("Number of inserted rows: ", no_of_records)
