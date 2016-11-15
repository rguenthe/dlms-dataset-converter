from pymongo import MongoClient


class MongoDBConnector(object):

    def __init__(self, db_server_ip, db_server_port, database_name):
        self.client = MongoClient(db_server_ip, db_server_port)
        self.db = self.client[str(database_name)]

    def insert_dataset(self, data):
        result = self.db['datapoints'].insert_many(data)
        return result
