import shutil
from typing_extensions import Annotated
from pathlib import Path
from zenml import get_step_context, step

from brain_ai_assistant.domain import Document


@step
def save_documents_to_disk(
    documents: Annotated[list[Document], "documents"],
    output_storage_path: Path,
) -> Annotated[str, "output"]:
    """
    Save a list of documents to disk.
    
    Args:
        documents: List of documents to save.
        output_storage_path: Path to store the documents.
    
    Returns:
        str: Path to the directory containing the saved documents
    """

    # Clean existing directory if it exists to avoid file conflicts
    if output_storage_path.exists():
        shutil.rmtree(output_storage_path)

    # Create fresh directory
    output_storage_path.mkdir(parents=True)

    # Save each document with privacy protection enabled
    for document in documents:
        document.save(output_path=output_storage_path, anonymise = True, create_text_copy=True)

    # Record processing metrics in step context for monitoring
    step_context = get_step_context()
    step_context.add_output_metadata(
        output_name="output",
        metadata={
            "document_count": len(documents),
            "output_storage_path": str(output_storage_path),
        },
    )

    return str(output_storage_path)