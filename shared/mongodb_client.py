from pymongo import MongoClient

class MongoDBClient:
    def __init__(self, host="localhost", port=27017):
        self.host = host
        self.port = port
        self.client = MongoClient(host, port)
        self.database = self.client["MongoDB_"]
        self.collection = self.database["sensors"]

    def close(self):
        self.client.close()
    
    def ping(self):
        return self.client.db_name.command('ping')
    
    def getDatabase(self, database):
        self.database = self.client[database]
        return self.database

    def getCollection(self, collection):
        self.collection = self.database[collection]
        return self.collection
    
    def clearDb(self,database):
        self.client.drop_database(database)

    def insert(self, document):
        return self.collection.insert_one(document)
    
    def delete(self, query):
        return self.collection.delete_one(query)
    
    def getDocuments(self, query):
        return self.collection.find(query, {'_id':0})
    
    def set_sensor(self,document):
        return self.collection.insert_one(document)
    
    def get_sensor(self,query):
        return self.collection.find_one(query, {"_id":0})


