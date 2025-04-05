from typing_extensions import Annotated
from zenml import get_step_context, step

from apps.brain_ai_assistant.domain import Document, DocumentMetadata
from apps.brain_ai_assistant.infrastructure.notion import NotionDocumentClient


@step
def fetch_notion_documents(
    documents_metadata: list[DocumentMetadata],
) -> Annotated[list[Document], "notion_documents"]:
    """
    Fetch content from multiple Notion documents.

    Args:
        documents_metadata: List of document metadata to fetch content from.

    Returns:
        list[Document]: List of documents with their fetched content.
    """

    # Initialise Notion client for fetching notion pages
    client = NotionDocumentClient()
    document_collection = []
    
    # Process each document metadata entry to fetch full page content
    for document_metadata in documents_metadata:
        document_collection.append(client.fetch_document(document_metadata))

    # Store processing metrics in step context for observability
    step_context = get_step_context()
    step_context.add_output_metadata(
        output_name="notion_documents",
        metadata={
            "document_count": len(document_collection),
        },
    )

    return document_collection