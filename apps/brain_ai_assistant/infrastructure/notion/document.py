import requests
from loguru import logger

from apps.brain_ai_assistant.settings import settings
from apps.brain_ai_assistant.domain import Document, DocumentMetadata


class NotionDocumentClient:
    """
    Client for fetching content from Notion documents.
    """

    def __init__(self, 
                 api_key: str | None = settings.NOTION_API_KEY
                 )-> None:
        """
        Initialize the Notion client.

        Args:
            api_key: The Notion API key to use for authentication.
        """

        assert api_key is not None, (
            "NOTION_API_KEY environment variable is required. Please set it in your .env file."
        )

        self.api_key = api_key

    def fetch_document(self, document_metadata: DocumentMetadata) -> Document:
        """
        Fetch content from a Notion document.

        Args:
            document_metadata: Metadata of the document to fetch.

        Returns:
            Document: Document object containing the fetched content and metadata.
        """

        # Fetch child blocks from the document
        blocks = self.__fetch_child_blocks(document_metadata.id)

        # Process blocks into text content and extract URLs
        content, urls = self.__process_blocks(blocks)

        # Handle parent metadata if present
        parent_metadata = document_metadata.properties.pop("parent", None)
        if parent_metadata:
            parent_metadata = DocumentMetadata(
                id=parent_metadata["id"],
                url=parent_metadata["url"],
                title=parent_metadata["title"],
                properties=parent_metadata["properties"],
            )

        return Document(
            id=document_metadata.id,
            metadata=document_metadata,
            parent_metadata=parent_metadata,
            content=content,
            child_urls=urls,
        )

    def __fetch_child_blocks(
        self, 
        block_id: str, 
        page_size: int = 100
    ) -> list[dict]:
        """
        Fetch child blocks from a Notion block.

        Args:
            block_id: The ID of the block to retrieve children from.
            page_size: Number of blocks to retrieve per request.

        Returns:
            list[dict]: List of block data.
        """
        # Construct the URL for fetching child blocks
        blocks_url = f"https://api.notion.com/v1/blocks/{block_id}/children?page_size={page_size}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Notion-Version": "2022-06-28",
        }
        
        try:
            # Send request to Notion API
            blocks_response = requests.get(blocks_url, headers=headers, timeout=40)
            blocks_response.raise_for_status()
            blocks_data = blocks_response.json()
            return blocks_data.get("results", [])
        except requests.exceptions.RequestException as e:
            error_message = f"Error: Failed to fetch Notion page content. {e}"
            if hasattr(e, "response") and e.response is not None:
                error_message += f" Status code: {e.response.status_code}, Response: {e.response.text}"
            logger.exception(error_message)
            return []
        except Exception:
            logger.exception("Error fetching Notion page content")
            return []

    def __process_blocks(
        self, 
        blocks: list[dict], 
        depth: int = 0
    ) -> tuple[str, list[str]]:
        """
        Process Notion blocks into text content and extract URLs.

        Args:
            blocks: List of Notion block objects to parse.
            depth: Current recursion depth for parsing nested blocks.

        Returns:
            tuple[str, list[str]]: A tuple containing:
                - Parsed text content as a string
                - List of extracted URLs
        """
        content = ""
        urls = []

        # Process each block based on its type
        for block in blocks:
            block_type = block.get("type")
            block_id = block.get("id")
            
            # Handle heading blocks
            if block_type in {
                "heading_1",
                "heading_2",
                "heading_3",
            }:
                content += f"# {self.__parse_rich_text(block[block_type].get('rich_text', []))}\n\n"
                urls.extend(self.__find_urls(block[block_type].get("rich_text", [])))
            
            # Handle paragraph and quote blocks
            elif block_type in {
                "paragraph",
                "quote",
            }:
                content += f"{self.__parse_rich_text(block[block_type].get('rich_text', []))}\n"
                urls.extend(self.__find_urls(block[block_type].get("rich_text", [])))
            
            # Handle bulleted and numbered list items
            elif block_type in {"bulleted_list_item", "numbered_list_item"}:
                content += f"- {self.__parse_rich_text(block[block_type].get('rich_text', []))}\n"
                urls.extend(self.__find_urls(block[block_type].get("rich_text", [])))
            
            # Handle to-do items
            elif block_type == "to_do":
                content += f"[] {self.__parse_rich_text(block['to_do'].get('rich_text', []))}\n"
                urls.extend(self.__find_urls(block[block_type].get("rich_text", [])))
            
            # Handle code blocks
            elif block_type == "code":
                content += f"```\n{self.__parse_rich_text(block['code'].get('rich_text', []))}\n````\n"
                urls.extend(self.__find_urls(block[block_type].get("rich_text", [])))
            
            # Handle image blocks
            elif block_type == "image":
                content += f"[Image]({block['image'].get('external', {}).get('url', 'No URL')})\n"
            
            # Handle divider blocks
            elif block_type == "divider":
                content += "---\n\n"
            
            # Handle child pages
            elif block_type == "child_page" and depth < 3:
                child_id = block["id"]
                child_title = block.get("child_page", {}).get("title", "Untitled")
                content += f"\n\n<child_page>\n# {child_title}\n\n"

                child_blocks = self.__fetch_child_blocks(child_id)
                child_content, child_urls = self.__process_blocks(child_blocks, depth + 1)
                content += child_content + "\n</child_page>\n\n"
                urls += child_urls

            # Handle link preview blocks
            elif block_type == "link_preview":
                url = block.get("link_preview", {}).get("url", "")
                content += f"[Link Preview]({url})\n"

                urls.append(self.__standardize_url(url))
            else:
                logger.warning(f"Unknown block type: {block_type}")

            # Process nested blocks(not for child pages which are handled separately)
            if (
                block_type != "child_page"
                and "has_children" in block
                and block["has_children"]
            ):
                child_blocks = self.__fetch_child_blocks(block_id)
                child_content, child_urls = self.__process_blocks(child_blocks, depth + 1)
                content += (
                    "\n".join("\t" + line for line in child_content.split("\n"))
                    + "\n\n"
                )
                urls += child_urls

        urls = list(set(urls))

        return content.strip("\n "), urls

    def __parse_rich_text(self, rich_text: list[dict]) -> str:
        """
        Parse Notion rich text blocks into plain text with markdown formatting.

        Args:
            rich_text: List of Notion rich text objects to

        Returns:
            str: Formatted text content.
        """
        text = ""
        for segment in rich_text:

            # Hanle links with markdown formatting
            if segment.get("href"):
                text += f"[{segment.get('plain_text', '')}]({segment.get('href', '')})"
            else:
                text += segment.get("plain_text", "")
        return text

    def __find_urls(self, rich_text: list[dict]) -> list[str]:
        """
        Find URLs from Notion rich text blocks.

        Args:
            rich_text: List of Notion rich text objects to extract URLs from.

        Returns:
            list[str]: List of normalized URLs found in the rich text.
        """
        urls = []
        for text in rich_text:
            url = None

            # Check href and annotations for URLs
            if text.get("href"):
                url = text["href"]
            elif "url" in text.get("annotations", {}):
                url = text["annotations"]["url"]

            if url:
                urls.append(self.__standardize_url(url))

        return urls

    def __standardize_url(self, url: str) -> str:
        """
        Standardize a URL by ensuring it ends with a forward slash.

        Args:
            url: URL to normalize.

        Returns:
            str: Normalized URL with trailing slash.
        """
        if not url.endswith("/"):
            url += "/"
        return url
