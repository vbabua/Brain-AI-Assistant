from zenml.steps import step, get_step_context
from pathlib import Path
from typing import Annotated
from loguru import logger

from apps.brain_ai_assistant.domain.document import Document

@step
def read_documents_from_disk(
    storage_path : Path, 
    nesting_level : int = 0
    ) -> Annotated[list[Document], "documents"]:
    """
    Load and deserialize document files from a filesystem directory.
    
    Args:
        storage_path : Path containing document files
        nesting_level : How many subdirectory levels to search
    
    Returns:
        List of deserialized Document objects
        
    Raises:
        FileNotFoundError: If the source directory doesn't exist
    """
    document_collection : list[Document] = []

    logger.info(f"Beginning document loading from {storage_path}'")

    if not storage_path.exists():
        raise FileNotFoundError(f"Source directory not found: '{storage_path}'")

    json_files = __get_json_files(
        data_directory = storage_path, nesting_level = nesting_level
    )
    
    for json_file in json_files:
        document = Document.from_file(json_file)
        document_collection.append(document)

    logger.info(f"Document loading complete - processed {len(document_collection)} files")

    # Record processing metrics
    step_context = get_step_context()
    step_context.add_output_metadata(
        output_name="documents",
        metadata={
            "count": len(document_collection),
        },
    )

    return document_collection

def __get_json_files(data_directory : Path, nesting_level : int = 0) -> list[Path]:
    """
    Recursively locate JSON document files within a directory structure.
   
    Args:
        data_directory: The root directory to begin searching
        nesting_level: How many subdirectory levels to recursively search
            0 = search only the root directory
            1+ = search N levels of subdirectories
   
    Returns:
        List of paths to JSON files found within the directory structure
    """
    # For the base case, return only JSON files in the current directory
    if nesting_level == 0:
        return list(data_directory.glob("*.json"))
    
    # For recursive case, search subdirectories
    else:
        json_files = []

        # Iterate through each item in the current directory
        for subdirectory in data_directory.iterdir():
            if subdirectory.is_dir():

                # Recursively search each subdirectory, decrementing nesting level
                nested_json_files = __get_json_files(
                    data_directory=subdirectory, nesting_level=nesting_level - 1
                )
                json_files.extend(nested_json_files)

        return json_files