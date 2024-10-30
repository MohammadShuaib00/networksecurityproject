import os
import sys

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logger.logging import logging

## configuration of the Data Ingestion Config
from networksecurity.entity.config_entity import DataIngestionConfig
import pymongo
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv
from networksecurity.entity.artifact_entity import DataIngestionArtifact

# Load environment variables
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(
                f"Error in DataIngestion __init__: {str(e)}", sys.exc_info()
            )

    def export_collection_as_dataframe(self):
        try:
            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            collection = self.mongo_client[database_name][collection_name]

            # Convert collection to DataFrame
            df = pd.DataFrame(list(collection.find()))
            if "_id" in df.columns.tolist():
                df = df.drop(columns=["_id"], axis=1)
            df.replace({"na": np.nan}, inplace=True)
            return df

        except Exception as e:
            raise NetworkSecurityException(
                f"Error in export_collection_as_dataframe: {str(e)}", sys.exc_info()
            )

    def export_data_into_feature_store(self, dataframe: pd.DataFrame):
        try:
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            # Create directory if it doesn't exist
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            return dataframe

        except Exception as e:
            raise NetworkSecurityException(
                f"Error in export_data_into_feature_store: {str(e)}", sys.exc_info()
            )

    def split_data_as_train_test(self, dataframe: pd.DataFrame):
        try:
            train_set, test_set = train_test_split(
                dataframe, test_size=self.data_ingestion_config.train_test_split_ratio
            )
            logging.info("Performed train-test split on the dataframe")
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)

            # Export train and test datasets
            train_set.to_csv(
                self.data_ingestion_config.training_file_path, index=False, header=True
            )
            test_set.to_csv(
                self.data_ingestion_config.testing_file_path, index=False, header=True
            )

            logging.info(f"Exported train and test files successfully.")
        except Exception as e:
            raise NetworkSecurityException(
                f"Error in split_data_as_train_test: {str(e)}", sys.exc_info()
            )

    def initiate_data_ingestion(self):
        try:
            logging.info("Reading the data from MongoDB")
            dataframe = self.export_collection_as_dataframe()
            dataframe = self.export_data_into_feature_store(dataframe)
            self.split_data_as_train_test(dataframe)

            data_ingestion_artifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path,
            )
            logging.info(
                f"Data ingestion completed and artifact: {data_ingestion_artifact}"
            )
            return data_ingestion_artifact

        except Exception as e:
            raise NetworkSecurityException(
                f"Error in initiate_data_ingestion: {str(e)}", sys.exc_info()
            )
