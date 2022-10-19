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
            "trackpoint": [ObjectID(...), ObjectID(...), ...]
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
        collection = self.db[collection_name]
        return int(collection.count_documents({}))

    def drop_coll(self, collection_name):
        """Remove a collection from the database

        Args:
            collection_name (str): Name of a collection
        """
        collection = self.db[collection_name]
        collection.drop()

    def drop_all_coll(self):
        """Remove all collections in the db"""
        coll = self.db.list_collection_names()
        for c in coll:
            self.db.drop_collection(c)

    def get_coll(self) -> list:
        """Returns all the collections

        Returns:
            list: All collections for the db
        """
        return self.db.list_collection_names()
