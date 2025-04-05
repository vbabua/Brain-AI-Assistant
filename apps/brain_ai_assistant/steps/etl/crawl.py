from loguru import logger
from typing_extensions import Annotated
from zenml import get_step_context, step

from apps.brain_ai_assistant.application.crawlers import Crawl4AICrawler
from apps.brain_ai_assistant.domain import Document


@step
def crawl(
    source_documents: list[Document],
    concurrent_tasks : int = 10
) -> Annotated[list[Document], "crawled_documents"]:
    """
    Extract content from the URLs referenced in the source documents

    Args :
        source_documents : List of documents containing URLs to process
        concurrent_tasks : Maximum number of concurrent tasks. Default to 10
    """
    crawler = Crawl4AICrawler(max_concurrent_tasks=concurrent_tasks )
    child_pages = crawler(source_documents)

    # Combine original documents with newly discovered content
    comprehensive_collection = source_documents.copy()
    comprehensive_collection.extend(child_pages)
    comprehensive_collection = list(set(comprehensive_collection))

    logger.info(f"Initial Document count : {len(source_documents)}")
    logger.info(f"Final Document count : {len(comprehensive_collection)}")
    logger.info(
        f"After crawling, we have {len(comprehensive_collection) - len(source_documents)} new documents"
    )

    step_context = get_step_context()
    step_context.add_output_metadata(
        output_name="crawled_documents",
        metadata={
            "initial_document_count": len(source_documents),
            "final_document_count": len(comprehensive_collection),
            "newly_added_documents": len(comprehensive_collection) - len(source_documents),
        },
    )

    return comprehensive_collection
