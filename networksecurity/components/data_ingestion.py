from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logger.logging import logging
import os, sys

## configuration of the Data Ingestion Config
from networksecurity.entity.config_entity import DataIngestionConfig
import pymongo
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv
from networksecurity.entity.artifact_entity import DataIngestionArtifact

# laod env
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")


class DataIngestion:
    def __init__(self, data_ingestion_Config: DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_Config
        except Exception as e:
            raise NetworkSecurityException(e, sys.exc_info())

    def export_collection_as_dataframe(self):
        try:
            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            collection = self.mongo_client[database_name][collection_name]

            # Convert json into datafrme
            df = pd.DataFrame(list(collection.find()))
            if "_id" in df.columns.tolist():
                df = df.drop(columns=["_id"], axis=1)
            df.replace({"na": np.nan}, inplace=True)
            return df

        except Exception as e:
            raise NetworkSecurityException(e, sys.exc_info())

    def export_data_into_feature_store(self, datafrme: pd.DataFrame):
        try:
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            # creating folder
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            datafrme.to_csv(feature_store_file_path, index=False, header=True)
            return datafrme

        except Exception as e:
            raise NetworkSecurityException(e, sys.exc_info())

    def split_data_as_train_test(self, dataframe: pd.DataFrame):
        try:
            train_set, test_set = train_test_split(
                dataframe, test_size=self.data_ingestion_config.train_test_split_ratio
            )
            logging.info("Performed train test split on the dataframe")
            logging.info(
                "Exited split_data_as_train_test method of Data_Ingestion class"
            )
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)
            logging.info("Exporting train and test file path")
            train_set.to_csv(
                self.data_ingestion_config.training_file_path, index=False, header=True
            )
            test_set.to_csv(
                self.data_ingestion_config.testing_file_path, index=False, header=True
            )

            logging.info(f"Exported train and test file path.")
        except Exception as e:
            raise NetworkSecurityException(e, sys.exc_info())

    def initiate_data_ingestion(self):
        try:
            logging.info("Reading the data from mongodb")
            datafrme = self.export_collection_as_dataframe()
            datafrme = self.export_data_into_feature_store(datafrme)
            self.split_data_as_train_test(datafrme)

            data_ingestio_artifacts = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path,
            )
            return data_ingestio_artifacts

        except Exception as e:
            raise NetworkSecurityException(e, sys.exc_info())
