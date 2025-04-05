from typing import TypeVar, Type
from pydantic import BaseModel
from typing import Generic
from pymongo import MongoClient, errors
from loguru import logger
from bson import ObjectId

from apps.brain_ai_assistant.settings import settings

T = TypeVar("T", bound = BaseModel)

class MongoDBService(Generic[T]):
    """
    Generic MongoDB service class to handle CRUD operations for any Pydantic model

    This class abstracts common database operations such as connecting to a MongoDB collection, inserting, retrieving, and clearing documents,
    and ensures that all stored and retrieved data is validated against the provided Pydantic model

    """
    def __init__(
        self,
        model : Type[T],
        collection_name : str,
        database_name : str = settings.MONGODB_DATABASE_NAME,
        mongodb_uri : str = "mongodb://brainaiassistant:brainaiassistant@localhost:27017/?directConnection=true" 
    ) -> None:
        """
        Initialise a connection to the MongoDB database collection

        Args : 
            model_class : Pydantic model for document serialisation
            collection_name : Target collectio name
            database_name : Mondodb database name. Default to value form the settings
            mongodb_uri : Mongodb uri name. Default to value from settings
        """
        print(f"Attempting to connect with URI: {mongodb_uri}")
        self.model_class = model
        self.collection_name = collection_name
        self.database_name = database_name
        self.mongodb_uri = mongodb_uri

        try:
            self.client = MongoClient(host = mongodb_uri)
            
            # Verify connection is working
            self.client.admin.command("ping")
        except Exception as e:
            logger.error(f"Failed to initialise MongoDBService : {e}")
            raise
        
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]

        logger.info(f"Database connection established : \n URI {mongodb_uri} \n Database : {database_name} \n Collection : {collection_name}")

    def __enter__(self) -> "MongoDBService" :
        """
        Context manager entry point

        Returns : 
            MongoDBService : Current instance
        """
        return self

    def __exit__(self, exception_type, exception_value, excetpion_traceback) -> None:
        """
        Release resoruces when exiting context
        
        Args:
            exc_type: Exception type if raised
            exc_val: Exception value if raised
            exc_tb: Exception traceback if raised
        """
        self.disconnect()
    
    def disconnect(self) -> None:
        """
        Release database connection resources. Should be called when repository is no longer needed
        """
        self.client.close()
        logger.debug("Database connection resources released.")

    def clear_collection(self):
        """
        Removes all the documents from collection

        Raises : 
            errors.PyMongoError : If deletion operation fails
        """
        try:
            result = self.collection.delete_many({})
            logger.info(f"Cleared collection. Removed {result.deleted_count} documents.")

        except errors.PyMongoError as e:
            logger.error("Error resetting the collection : {e}")
            raise

    def store_documents(self, documents : list[T]) -> None:
        """
        Store multiple documents in the collection

        Args:
            documents: List of model instances to store

        Raises:
            ValueError: If input is empty or contains invalid items
            errors.PyMongoError: If insertion operation fails
        """
        try:
            # Validate input
            if not documents or not all(isinstance(doc, BaseModel) for doc in documents):
                raise ValueError("Input must contain valid model instances")
            
            # Prepare documents for sharing
            serialised_documents = [document.model_dump() for document in documents]

            # Ensure MondoDb handles id generationn to avoid duplicate key errors
            for document in serialised_documents:
                document.pop("_id", None)

            
            # Store documents
            self.collection.insert_many(serialised_documents)
            logger.debug(f"Successfully stored {len(documents)} documents.")

        except errors.PyMongoError as e:
            logger.error(f"Document storage operation failed : {e}")
            raise
    
    def fetch_documents(self, limit : int, query : dict) -> list[T]:
        """
        Retrieve documetns from MongoDBB collection based on a query

        Args :
            limit : Maximum number of documents to fetch
            query : MongoDB query filter to apply

        Raises:
            Exception : If the query operation fails
        """
        try:
            documents = list(self.collection.find(query).limit(limit))
            logger.debug(f"Fetched {len(documents)} documents with query {query}")
            return self.__deserialise_documents(documents)
        except Exception as e:
            logger.error(f"Error fetching documents : {e}")

    def __deserialise_documents(self, raw_documents : list[dict]) -> list[T]:
        """
        Handle MongoDB documents to Pydantic model instance
        
        Args :
            raw_documents : Database documents to deserialise

        Returns:
            List of validated model instances
        """
        deserialised_documents = []

        for raw_document in raw_documents:
            # Convert ObjectId to string representation
            for key, value in raw_documents.items():
                if isinstance(value, ObjectId):
                    raw_document[key] = str(value)
            
            # Handle document ID field conversion
            document_id = raw_document.pop("_id", None)
            raw_document["id"] = document_id

            # Create and validate model instance
            model_instance = self.model_class.model_validate(raw_document)
            deserialised_documents.append(model_instance)
        
        return deserialised_documents

    def get_collection_count(self) -> int:
        """
        Count total documents in the collection

        Returns:
            Document count

        Raises:
            errors.PyMongoError: If count operation fails
        """
        try:
            return self.collection.count_documents({})
        except errors.PyMongoError as e:
            logger.error(f"Document counting operation failed: {e}")
            raise
