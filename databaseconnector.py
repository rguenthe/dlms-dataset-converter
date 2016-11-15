from pymongo import MongoClient
from datetime import datetime


class MongoDBConnector(object):

    def __init__(self, db_server_ip, db_server_port, database_name):
        self.client = MongoClient(db_server_ip, db_server_port)
        self.db = self.client[str(database_name)]

    def insert_datapoints(self, datapoints=[]):
        """Insert a list of datapoints into the database"""
        result = self.db['datapoints'].insert_many(datapoints)
        return result

    def insert_dataset(self, datapoints=[], datasetname='dataset_dummy'):
        """Insert a dataset into the database"""
        dataset = {}
        dataset['name'] = datasetname;
        dataset['points'] = len(datapoints)
        dataset['start_time'] = datetime.fromtimestamp(datapoints[0]['unix_time']).isoformat()
        dataset_result = self.db['datasets'].insert(dataset)

        # copy the dataset id to datapoints list
        for dp in datapoints:
            dp['dataset_id'] = dataset_result

        datapoints_result = self.insert_datapoints(datapoints)
        return [dataset_result, datapoints_result]
