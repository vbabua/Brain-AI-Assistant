from zenml.steps import step, get_step_context
from pydantic import BaseModel
from typing_extensions import Annotated
from loguru import logger

from apps.brain_ai_assistant.infrastructure.mongo.service import MongoDBService


@step
def store_to_mongodb(
    document_models : list[BaseModel], 
    target_collection : str,
    clear_existing_data : bool = True
) -> Annotated[int, "output"]:
    """
    ZenML step to store document models in MongoDB

    Args :
        document_models : List of Pydantic BaseModel instances to store in MongoDB
        target_collection : Target MongoDB collection name
        clear_existing_data : If true clears the existing collection data before insertion. Defaults to true

    Returns :
        int : Total document cound in collection after operation
    
    Raises :
        ValueError : When empty model list is provided
    """
    if not document_models:
        raise ValueError("Empty model list provided for database insertion")
    
    model_class_type = type(document_models[0])

    logger.info(f"Storing {len(document_models)} {model_class_type.__name__} objects to MomngoDB collection '{target_collection}'")

    # Use context manager for automatic resource cleanup
    with MongoDBService(model = model_class_type, collection_name = target_collection) as mongodb_service:
        # Optionally clear  existing collection data before insertion
        if clear_existing_data:
            logger.warning(f"Clear existing data is set to True. Removing all existing data from {target_collection}")

            mongodb_service.clear_collection()
        
        # Perform bulk insertion of documents
        mongodb_service.store_documents(document_models)

        # Verify final document count for reporting
        final_documet_count = mongodb_service.get_collection_count()

        logger.info(f"Datbase operation completed : {final_documet_count} documents now in collection '{target_collection}'")

        # Read operational metrics in the pipeline context for monitoring
        step_context = get_step_context()
        step_context.add_output_metadata(
            output_name = "output",
            metadata = {
                "total_count" : final_documet_count
            }
        )

        return final_documet_count

    
