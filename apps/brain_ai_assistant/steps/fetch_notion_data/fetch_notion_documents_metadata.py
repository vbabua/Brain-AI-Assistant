
from typing_extensions import Annotated
from loguru import logger
from zenml import get_step_context, step

from apps.brain_ai_assistant.domain import DocumentMetadata
from apps.brain_ai_assistant.infrastructure.notion import NotionDatabaseClient

@step
def fetch_notion_documents_metadata(
    database_id : str,
) -> Annotated[list[DocumentMetadata], "notion_documents_metadata"]:
    """
    Fetch metadata from Notion documents in a specified database.

    Args:
        database_id : The ID of the Notion database to query.

    Returns:
        A list of DocumentMetadata objects containing the fetched information.
    """

    client = NotionDatabaseClient()
    documents_metadata = client.query_notion_database(database_id)

    logger.info(f"Fetched {len(documents_metadata)} documents metadata from database {database_id}")

    step_context = get_step_context()
    step_context.add_output_metadata(
        output_name="notion_documents_metadata",
        metadata={
            "database_id": database_id,
            "documents_metadata_count": len(documents_metadata),
        },
    )

    return documents_metadata
