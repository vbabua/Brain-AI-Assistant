import asyncio
import os
import psutil
from loguru import logger
from crawl4ai import AsyncWebCrawler, CacheMode

from apps.brain_ai_assistant.domain import Document, DocumentMetadata
from apps.brain_ai_assistant import utils


class Crawl4AICrawler:
    """
    A crawler implementation using crawl4ai library to effectively retrieve contents from URLs
    Implements concurrent request handling to optimise crawling performance while managing system resources

    Attributes :
        max_concurrent_taks : Upper request for simulataneous HTTP requests
    """

    def __init__(
            self, 
            max_concurrent_tasks : int = 10
            ) -> None:
        """
        Set up crawler with specified concurrency settings

        Args :
            max_concurrent_tasks : Limit on parallel HTTP requests. Defaults to 10.
        """
        self.max_concurrent_tasks = max_concurrent_tasks
    
    def __call__(
            self, 
            documents : list[Document]
            ) -> list[Document]:
        """
        Process and extract content from URLs contained in the provided documents.

        Args :
            documents: Collection of documents containing URLs to be crawled.

        Returns:
            list[Document]: New documents generated from successfully crawled URLs.
        """
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            # No active loop, create and execute a new one
            return asyncio.run(self.__crawl_batch(documents))
        else:
            # Utilize existing loop
            return current_loop.run_until_complete(self.__crawl_batch(documents))
    
    async def __crawl_batch(
            self, 
            documents: list[Document]
            ) -> list[Document]:
        """
        Perform asynchronous crawling of all URLs found in the document collection.

        Args:
            documents: Collection of documents with embedded URLs to crawl.

        Returns:
            list[Document]: Collection of new documents created from successful crawl operations.
        """
        # Track resource usage for performance monitoring
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        logger.debug(
            f"Beginning batch crawl with {self.max_concurrent_tasks} concurrent requests. "
            f"Initial memory consumption: {initial_memory // (1024 * 1024)} MB"
        )

        # Implement concurrency control
        semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
        crawled_results = []

        # Initialize crawler with fresh content setting
        async with AsyncWebCrawler(cache_mode=CacheMode.BYPASS) as crawler:
            # Process each source document
            for document in documents:
                # Create parallel tasks for all URLs in document
                crawl_tasks = [
                    self.__crawl_url(crawler, document, url, semaphore)
                    for url in document.child_urls
                ]
                # Execute tasks concurrently
                batch_results = await asyncio.gather(*crawl_tasks)
                crawled_results.extend(batch_results)

        # Evaluate resource impact post-crawling
        final_memory = process.memory_info().rss
        memory_difference = final_memory - initial_memory
        logger.debug(
            f"Batch crawl completed. "
            f"Final memory consumption: {final_memory // (1024 * 1024)} MB, "
            f"Incremental memory usage: {memory_difference // (1024 * 1024)} MB"
        )

        # Retain only successful crawl results
        successful_documents = [result for result in crawled_results if result is not None]

        # Report crawling effectiveness metrics
        success_count = len(successful_documents)
        failed_count = len(crawled_results) - success_count
        total_attempts = len(crawled_results)
        logger.info(
            f"Crawling summary: "
            f"{success_count}/{total_attempts} succeeded "
            f"{failed_count}/{total_attempts} failed"
        )

        return successful_documents
    
    async def __crawl_url(
        self,
        crawler: AsyncWebCrawler,
        parent_document: Document,
        url: str,
        request_limiter: asyncio.Semaphore,
    ) -> Document | None:
        """
        Extract content from a single URL and transform it into a document.

        Args:
            crawler: Web crawler instance handling HTTP operations
            parent_document: Source document containing the target URL
            url: Web address to retrieve content from
            request_limiter: Concurrency control mechanism

        Returns:
            Document | None: Newly created document or None if crawling failed
        """

        async with request_limiter:
            # Retrieve content from target URL
            crawl_result = await crawler.arun(url=url)
            # Introduce delay for responsible crawling
            await asyncio.sleep(0.5)  # Rate limiting

            # Handle unsuccessful outcomes
            if not crawl_result or not crawl_result.success:
                logger.warning(f"Failed to crawl URL: {url}")
                return None

            if crawl_result.markdown is None:
                logger.warning(f"No content extracted from URL: {url}")
                return None

            # Process discovered links
            extracted_links = [
                link["href"]
                for link in crawl_result.links["internal"] + crawl_result.links["external"]
            ]
            
            # Extract page title
            if crawl_result.metadata:
                page_title = crawl_result.metadata.pop("title", "") or ""
            else:
                page_title = ""

            # Create unique identifier
            document_id = utils.generate_random_hex(length=32)

            # Construct document from crawled data
            return Document(
                id=document_id,
                metadata=DocumentMetadata(
                    id=document_id,
                    url=url,
                    title=page_title,
                    properties=crawl_result.metadata or {},
                ),
                parent_metadata=parent_document.metadata,
                content=str(crawl_result.markdown),
                child_urls=extracted_links,
            )