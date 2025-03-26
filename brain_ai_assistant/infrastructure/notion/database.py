import json
import requests
from typing import Any
from loguru import logger

from brain_ai_assistant.config import settings
from brain_ai_assistant.domain import DocumentMetadata


class NotionDatabaseClient:
    """
    Client for interacting with Notion API to extract database content.

    Attributes:
        api_key: The Notion API key to use for authentication
    """

    def __init__(self, 
                 api_key: str | None = settings.NOTION_API_KEY
                 ) -> None:
        """
        Initialize the NotionDatabaseClient.

        Args:
            api_key: The Notion API key to use for authentication.
        """
        assert api_key is not None, (
            "NOTION_API_KEY environment variable is required. Please set it in your .env file."
        )
        self.api_key = api_key

    def query_notion_database(
        self, 
        database_id : str, 
        query_params : str | None = None
    ) -> list[DocumentMetadata]:
        """
        Query a Notion database to fetch page metadata.

        Args:
            database_id: The ID of the Notion database to query.
            query_json: Optional JSON string to filter the query.

        Returns:
            A list of dictionaries containing the query results.
        """

        api_endpoint = f"https://api.notion.com/v1/databases/{database_id}/query"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        }

        # Parse query parameters if provided
        request_payload = {}
        if query_params and query_params.strip():
            try:
                request_payload = json.loads(query_params)
            except json.JSONDecodeError:
                logger.opt(exception=True).debug("Invalid JSON format in query parameters")
                return []

        try:
            # Send request to Notion API
            response = requests.post(
                api_endpoint, headers=headers, json=request_payload, timeout=10
            )
            response.raise_for_status()
            results = response.json()
            results = results["results"]
        except requests.exceptions.RequestException:
            logger.opt(exception=True).debug("Error while querying Notion database")
            return []
        except KeyError:
            logger.opt(exception=True).debug(
                "Unexpected response structure from Notion API"
            )
            return []
        except Exception:
            logger.opt(exception=True).debug("Error querying Notion database")
            return []
        
        # Transform raw pages into PageMetadata objects
        return [self.__create_page_metadata(page) for page in results]

    def __create_page_metadata(self, 
                               page: dict[str, Any]
                               )-> DocumentMetadata:
        """
        Create a PageMetadata object from a Notion page dictionary.

        Args:
            page: Dictionary containing Notion page data.

        Returns:
            A PageMetadata object containing the processed page data.
        """
        page_properties = self.__flatten_properties(page.get("properties", {}))
        page_title = page_properties.pop("Name")

        if page.get("parent"):
            page_properties["parent"] = {
                "id": page["parent"]["database_id"],
                "url": "",
                "title": "",
                "properties": {},
            }

        return DocumentMetadata(
            id=page["id"], url=page["url"], title=page_title, properties=page_properties
        )

    def __flatten_properties(self, 
                             page_properties: dict
                             ) -> dict:
        """
        Flatten Notion properties dictionary into a simpler key-value format.

        Args:
            properties: Dictionary of Notion properties to flatten.

        Returns:
            A flattened dictionary with simplified key-value pairs.
        """
        flattened = {}

        # Process each property based on its type
        for key, value in page_properties.items():
            prop_type = value.get("type")

            if prop_type == "select":
                select_value = value.get("select", {}) or {}
                flattened[key] = select_value.get("name")
            elif prop_type == "multi_select":
                flattened[key] = [
                    item.get("name") for item in value.get("multi_select", [])
                ]
            elif prop_type == "title":
                flattened[key] = "\n".join(
                    item.get("plain_text", "") for item in value.get("title", [])
                )
            elif prop_type == "rich_text":
                flattened[key] = " ".join(
                    item.get("plain_text", "") for item in value.get("rich_text", [])
                )
            elif prop_type == "number":
                flattened[key] = value.get("number")
            elif prop_type == "checkbox":
                flattened[key] = value.get("checkbox")
            elif prop_type == "date":
                date_value = value.get("date", {})
                if date_value:
                    flattened[key] = {
                        "start": date_value.get("start"),
                        "end": date_value.get("end"),
                    }
            elif prop_type == "database_id":
                flattened[key] = value.get("database_id")
            else:
                flattened[key] = value

        return flattened
