from zenml import pipeline
from pathlib import Path
from loguru import logger

from apps.brain_ai_assistant.steps.infrastructure import (
    read_documents_from_disk,
    save_documents_to_disk,
    store_to_mongodb
)
from apps.brain_ai_assistant.steps.etl import (
    crawl,
    add_quality_score
)

@pipeline
def etl(
    storage_path : Path,
    database_collection_name : str,
    concurrent_tasks : int = 10,
    quality_agent_model_id : str = "gpt-4o-mini",
    use_mock_quality_agent : bool = True
    ) -> None:
    """
    Pipeline for extracting, transforming and laoding documents.

    Args:
        storage_path: Base directory for storing data files
        database_collection_name: Name of the MongoDB collection to store documents in
        concurrent_tasks: Maximum number of tasks to run in parallel for processing
        quality_agent_model_id: Model identifier for the quality assessment language model
        use_mock_quality_agent: If True, uses synthetic responses instead of actual model calls
    
    """
    notion_storage_path = storage_path / "notion"
    logger.info(f"Loading notion document from {notion_storage_path}")

    crawled_data_path = storage_path / "crawled"
    logger.info(f"Target directory for processed content : {crawled_data_path}")

    documents = read_documents_from_disk(storage_path = notion_storage_path, nesting_level = 1)

    crawled_documents = crawl(source_documents = documents, concurrent_tasks = concurrent_tasks)

    # Enhance document quality with evaluation scores
    processed_documents = add_quality_score(
        documents = crawled_documents,
        model_name = quality_agent_model_id,
        use_mock_quality_agent = use_mock_quality_agent,
        concurrent_tasks = concurrent_tasks
    )

    save_documents_to_disk(documents = processed_documents, output_storage_path = crawled_data_path)

    store_to_mongodb(
        document_models = processed_documents,
        target_collection = database_collection_name,
        clear_existing_data = True
    )


