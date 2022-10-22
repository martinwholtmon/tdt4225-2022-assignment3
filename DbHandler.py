"""The database handler.
"""
from DbConnector import DbConnector


class DbHandler:
    """The Database handler. Containing all functionality to interact with the database"""

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        """Create a colletions in the DB

        Args:
            collection_name (str): Name of a collection
        """
        collection = self.db.create_collection(collection_name)
        print("Created collection: ", collection)

    def insert_documents(self, collection_name, docs: list[dict]) -> list:
        """Insert documents into the DB
        Format:
        docs = [
            {
                "_id": 1,
                "name": "Bobby",
                "courses":
                    [
                        {'code':'TDT4225', 'name': ' Very Large, Distributed Data Volumes'},
                        {'code':'BOI1001', 'name': ' How to become a boi or boierinnaa'}
                    ]
            },
            ...
        ]


        Args:
            collection_name (_type_): Name of a collection
            docs (list[dict]): Documents to be inserted

        Returns:
            list: inserted ids
        """
        collection = self.db[collection_name]
        results = collection.insert_many(docs)
        return results.inserted_ids

    def update_document(self, collection_name, document_id, data):
        """Update a document in a collection
        Example of data = {
            "field_name_to_update": new_value]
        }

        Args:
            collection_name (str): name of the collection
            document_id (ObjectID): id of the document
            data (dict): data to update
        """
        collection = self.db[collection_name]
        collection.update_one({"_id": document_id}, {"$set": data}, upsert=False)

    def fetch_documents(self, collection_name) -> list:
        """Fetch all documents in a collection from the database

        Args:
            collection_name (str): Name of a collection

        Returns:
            list: documents for a given collection
        """
        collection = self.db[collection_name]
        return list(collection.find({}))

    def get_nr_documents(self, collection_name) -> int:
        """Get number of documents in a collection

        Args:
            collection_name (str): Name of a collection

        Returns:
            int: number of documents for a given collection
        """
        collection = self.db[collection_name]
        return int(collection.count_documents({}))

    def find_documents(self, collection_name, query={}, fields={}):
        """find documents in a given collection provided queries.
        You can spesificy which fields you would like in return as well.

        Exmaple of
        - query = {"activities.transportation_mode": "taxi"}
        - fields = {"_id": 1}

        Args:
            collection_name (_type_): _description_
            query (_type_): _description_
            fields (dict, optional): _description_. Defaults to {}.

        Returns:
            _type_: _description_
        """
        collection = self.db[collection_name]
        return collection.find(query, fields)

    def aggregate(self, collection_name, pipeline: list):
        """Perform aggregation, and return computed results.
        Each stage is provided in the pipeline list

        Args:
            collection_name (str): Name of the collection
            pipeline (list): Stages in the aggregation

        Returns:
            ~pymongo.command_cursor.CommandCursor:
                The results of the aggregation.
                CommandCursor is iterable (for-each loop).
                list(CommandCursor) to print the results.
        """
        collection = self.db[collection_name]
        return collection.aggregate(pipeline)

    def drop_coll(self, collection_name):
        """Remove a collection from the database

        Args:
            collection_name (str): Name of a collection
        """
        collection = self.db[collection_name]
        collection.drop()

    def drop_all_coll(self):
        """Remove all collections in the db"""
        collections = self.db.list_collection_names()
        for collection in collections:
            self.db.drop_collection(collection)

    def get_coll(self) -> list:
        """Returns all the collections

        Returns:
            list: All collections for the db
        """
        return self.db.list_collection_names()
