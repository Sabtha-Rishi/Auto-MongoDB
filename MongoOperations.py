import pandas as pd
import pymongo

class MongoOperations:

    """
    To perform basic MongoDB operations such as
     """

    def __init__(self,client):
        self.client = client
        self.connection  = pymongo.MongoClient(self.client)

    def create_database(self, DBname):
        self.DBname = DBname
        Database = self.connection[self.DBname]
        return Database

    def delete_database(self , DBname):
        self.DBname = DBname
        self.connection.drop_database(self.DBname)
        return "Database deletion successful"

    def create_collection(self, DBname, COLname):
        self.DBname = DBname
        self.COLname = COLname
        db = self.connection[self.DBname]
        collection = db[self.COLname]
        return collection

    def insert_csv(self, DBname, COLname, file_path):
        self.DBname = DBname
        self.COLname = COLname
        self.file_path = file_path

        db = self.connection[self.DBname]
        collection = db[self.COLname]
        df = pd.read_csv(self.file_path)
        data = df.to_dict("records")
        collection.insert_many(data)
        return "Data inserted successfully in dictionary format"

    def insert_one(self, DBname, COLname, data_dict):
        self.data_dict = data_dict
        self.DBname = DBname
        self.COLname = COLname
        db = self.connection[self.DBname]
        collection = db[self.COLname]
        collection.insert_one(self.data_dict)
        return "Values inserted successfully"

    def delete_records(self,  DBname, COLname, filter):
        self.filter = filter
        self.DBname = DBname
        self.COLname = COLname
        db = self.connection[self.DBname]
        collection = db[self.COLname]
        collection.delete_many(self.filter)
        return "Records deleted successfully"

    def delete_collection(self, DBname, COLname):
        self.DBname = DBname
        self.COLname = COLname
        db = self.connection[self.DBname]
        db.drop_collection(self.COLname)
        return "Collection deleted successfully"

    def collection_to_(self, DBname, COLname, saving_path, format="csv", drop_mongoid=True, ):
        self.DBname = DBname
        self.COLname = COLname
        self.saving_path = saving_path
        self.drop_mongoid = drop_mongoid
        self.format = format
        db = self.connection[self.DBname]
        collection = db[self.COLname]
        data_with_id = pd.DataFrame(list(collection.find()))
        data_without_id = data_with_id.drop("_id", axis=1, inplace=True)
        path = (str(self.saving_path) + "\\" + str(self.COLname) + ".csv")
        if self.format == "csv":
            if self.drop_mongoid:
                data_with_id.to_csv(path)
            else:
                data_without_id.to_csv(path)
        elif self.format == "dictionary":
            if self.drop_mongoid:
                data_with_id.to_dict(path)
            else:
                data_without_id.to_dict(path)
        else:
            if self.drop_mongoid:
                data_with_id.to_json(path)
            else:
                data_without_id.to_json(path)
        return "Collection export successfully"



