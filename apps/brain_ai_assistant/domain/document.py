import json
from pathlib import Path
from pydantic import BaseModel, Field

from apps.brain_ai_assistant import utils

class DocumentMetadata(BaseModel):
    """
    Metadata for a document stored in the system.
    Contains identifying information and properties of the document without the actual content.
    """
    id : str
    url : str
    title : str
    properties : dict

    def anonymise(self) -> "DocumentMetadata":
        """
        Replace sensitive identifiers with random values for privacy.

        Returns:
            DocumentMetadata: A new DocumentMetadata instance with anonymised data.
        """
        # Remove dashes from ID for consistent replacement
        original_id = self.id.replace("-", "")

        # Generate a random ID with the same length for anonymisation
        anonymised_id = utils.generate_random_hex(len(original_id))

        # Replace the original ID with the anonymised version
        self.id = anonymised_id
        self.url = self.url.replace(original_id, anonymised_id)

        return self


class Document(BaseModel):
    """
    Represents a document in the system with content and metadata.
    Stores the complete document including content, metadata, and relationships to the other documents.
    """
    id : str = Field(default_factory=lambda: utils.generate_random_hex(length=32))
    metadata : DocumentMetadata
    parent_metadata : DocumentMetadata | None = None
    content : str
    content_quality_score : float | None = None
    summary : str | None = None
    child_urls : list[str] = Field(default_factory=list)

    @classmethod
    def from_file(cls, 
                  file_path: Path
                  )-> "Document":
        """
        Create a Document object from a JSON file.

        Args:
            file_path: Path to the JSON file containing document data.

        Returns:
            Document: A new Document instance constructed from the file data.

        Raises:
            FileNotFoundError: If the file path does not exist.
            ValidationError: If the file data is not valid JSON.
        """
        # Read and parse the JSON file
        json_data = file_path.read_text(encoding="utf-8")

        # Convert JSON data to a Document object
        return cls.model_validate_json(json_data)

    def add_summary(self, 
                    summary: str
                    ) -> "Document":
        """
        Add a summary to the document.

        Args:
            summary: A brief summary of the document content.
        
        Returns:
            Document: A reference to the current Document
        """
        self.summary = summary

        return self

    def add_quality_score(self, 
                          score: float
                          ) -> "Document":
        self.content_quality_score = score

        return self

    def save(
        self, 
        output_path: Path, 
        anonymise : bool = False, 
        create_text_copy: bool = False
    ) -> None:
        """
        Save document data to file, optionally anonymising sensitive information.

        Args:
            output_path: Path to the directory where the document data will be saved.
            anonymise: If True, sensitive information will be replaced with random values.
            create_text_copy: If True, content will also be saved as a text file.
        """

        output_path.mkdir(parents=True, exist_ok=True)

        if anonymise:
            self.anonymise()

        # Anonymise document if requested
        serialised_data = self.model_dump()

        json_file_path = output_path / f"{self.id}.json"
        with open(json_file_path, "w", encoding="utf-8") as file_handle:
            json.dump(
                serialised_data,
                file_handle,
                indent=4,
                ensure_ascii=False,
            )

        if create_text_copy:
            text_file_path = json_file_path.with_suffix(".txt")
            with open(text_file_path, "w", encoding="utf-8") as file_handle:
                file_handle.write(self.content)

    def anonymise(self) -> "Document":
        """
        Create an anonymised version of this document by modifying in place.

        Returns:
            Document: A reference to the current Document with anonymised data.
        """

        self.metadata = self.metadata.anonymise()
        self.parent_metadata = (self.parent_metadata.anonymise() if self.parent_metadata else None)
        self.id = self.metadata.id
        return self

    def __eq__(self, 
               other: object
               ) -> bool:
        """
        Compare two Document objects for equality based on their IDs.

        Args:
            other: The object to compare against.

        Returns:
            bool: True if the objects are equal, False otherwise.
        """
        if not isinstance(other, Document):
            return False
        return self.id == other.id

    def __hash__(self
                 ) -> int:
        """
        Generate a hash value for the Document based on its ID.

        Returns:
            int: A hash value for the Document.
        """
        return hash(self.id)
