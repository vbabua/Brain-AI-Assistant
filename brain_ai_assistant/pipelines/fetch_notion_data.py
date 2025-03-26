from pathlib import Path
from loguru import logger
from zenml import pipeline

from brain_ai_assistant.steps.fetch_notion_data import (
    fetch_notion_documents,
    fetch_notion_documents_metadata,
)
from brain_ai_assistant.steps.infrastructure import save_documents_to_disk


@pipeline
def fetch_notion_data(
    database_ids: list[str], 
    storage_path: Path
) -> None:
    """
    Pipeline to fetch data from Notion databases and save it to disk.
    
    Args:
        database_ids: List of Notion database IDs to fetch data from.
        storage_path: Path to store the fetched data.
    """
    notion_storage_path = storage_path / "notion"
    notion_storage_path.mkdir(parents=True, exist_ok=True)

    invocation_ids = []
    
    for index, database_id in enumerate(database_ids):
        logger.info(f"Processing notion database '{database_id}' and retrieving pages")
        notion_documents_metadata = fetch_notion_documents_metadata(database_id=database_id)
        notion_documents_data = fetch_notion_documents(documents_metadata=notion_documents_metadata)

        result = save_documents_to_disk(
            documents = notion_documents_data,
            output_storage_path = notion_storage_path / f"database_{index}",
        )
        invocation_ids.append(result.invocation_id)

   