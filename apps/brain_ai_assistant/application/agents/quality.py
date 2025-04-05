import asyncio
import json
import os
import psutil
from litellm import acompletion
from loguru import logger
from pydantic import BaseModel
from tqdm.asyncio import tqdm

from apps.brain_ai_assistant import utils
from apps.brain_ai_assistant.domain import Document


class QualityScoreResponseFormat(BaseModel):
    """Format for quality score responses from the language model.

    Attributes:
        score: A float between 0.0 and 1.0 representing the quality score.
    """
    score: float

class ModelBasedQualityAgent:
    """Evaluates the quality of documents using LiteLLM with async support.

    This class handles the interaction with language models through LiteLLM to
    evaluate document quality based on relevance, factual accuracy, and information
    coherence. It supports both single and batch document processing.

    Attributes:
        model_id: The ID of the language model to use for quality evaluation.
        mock: If True, returns mock quality scores instead of using the model.
        max_concurrent_tasks: Maximum number of concurrent API requests.
    """

    SYSTEM_PROMPT_TEMPLATE = """You are an expert judge tasked with evaluating the quality of a given DOCUMENT.

Guidelines:
1. Evaluate the DOCUMENT based on generally accepted facts and reliable information.
2. Evaluate that the DOCUMENT contains relevant information and not only links or error messages.
3. Check that the DOCUMENT doesn't oversimplify or generalize information in a way that changes its meaning or accuracy.

Analyze the text thoroughly and assign a quality score between 0 and 1, where:
- **0.0**: The DOCUMENT is completely irrelevant containing only noise such as links or error messages
- **0.1 - 0.7**: The DOCUMENT is partially relevant containing some relevant information checking partially guidelines
- **0.8 - 1.0**: The DOCUMENT is entirely relevant containing all relevant information following the guidelines

It is crucial that you return only the score in the following JSON format:
{{
    "score": <your score between 0.0 and 1.0>
}}

DOCUMENT:
{document}
"""

    def __init__(
        self,
        model_name: str = "gpt-4o-mini",
        use_mock: bool = False,
        max_concurrent_tasks: int = 10,
    ) -> None:
        """Initialize the ModelBasedQualityAgent.

        Args:
            model_id: Identifier for the language model to use
            mock: Whether to use mock responses instead of real API calls
            max_concurrent_tasks: Maximum parallel requests to language model
        """
        self.model_id = model_name
        self.mock = use_mock
        self.max_concurrent_tasks = max_concurrent_tasks

    def __call__(
        self, documents: Document | list[Document]
    ) -> Document | list[Document]:
        """Process single document or batch of documents for quality evaluation.

        Args:
            documents: Single Document or list of Documents to evaluate.

        Returns:
            Document | list[Document]: Processed document(s) with quality scores.
        """
        if hasattr(documents, "value"):
            documents = documents.value


        # Handle both single document and document collections
        is_single_document = isinstance(documents, Document)
        docs_list = [documents] if is_single_document else documents

        # Run quality evaluation asynchronously
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            results = asyncio.run(self.__evaluate_quality_batch(docs_list))
        else:
            results = loop.run_until_complete(self.__evaluate_quality_batch(docs_list))

        # Return in the same format as input (single document or list)
        return results[0] if is_single_document else results

    async def __evaluate_quality_batch(
        self, documents: list[Document]
    ) -> list[Document]:
        """Asynchronously evaluate quality for multiple documents with retry mechanism.

        Args:
            documents: List of documents to evaluate.

        Returns:
            list[Document]: Documents with quality scores added.
        """
        # Track memory usage for performance monitoring
        process = psutil.Process(os.getpid())
        start_mem = process.memory_info().rss
        total_docs = len(documents)
        logger.debug(
            f"Starting quality evaluation batch with {self.max_concurrent_tasks} concurrent requests. "
            f"Current memory usage: {start_mem // (1024 * 1024)} MB"
        )

        # First attempt at quality evaluation
        evaluated_documents = await self.__process_batch(documents, await_time_seconds=7)
        documents_with_scores = [
            doc for doc in evaluated_documents if doc.content_quality_score is not None
        ]
        documents_without_scores = [
            doc for doc in evaluated_documents if doc.content_quality_score is None
        ]

        # Retry failed documents with increased wait time to handle rate limiting
        if documents_without_scores:
            logger.info(
                f"Retrying {len(documents_without_scores)} failed evaluations with increased wait time..."
            )
            retry_results = await self.__process_batch(
                documents_without_scores, await_time_seconds=20
            )

            documents_with_scores += retry_results

        # Log memory usage after processing
        end_mem = process.memory_info().rss
        memory_diff = end_mem - start_mem
        logger.debug(
            f"Quality evaluation batch completed. "
            f"Final memory usage: {end_mem // (1024 * 1024)} MB, "
            f"Memory increase: {memory_diff // (1024 * 1024)} MB"
        )

        # Report success/failure metrics
        success_count = len(
            [doc for doc in evaluated_documents if hasattr(doc, "quality_score")]
        )
        failed_count = total_docs - success_count
        logger.info(
            f"Quality evaluation completed: "
            f"{success_count}/{total_docs} succeeded ✓ | "
            f"{failed_count}/{total_docs} failed ✗"
        )

        return evaluated_documents

    async def __process_batch(
        self, documents: list[Document], await_time_seconds: int
    ) -> list[Document]:
        """Process a batch of documents with controlled concurrency.
        
        Args:
            documents: Documents to process
            await_time_seconds: Time to wait between API calls
            
        Returns:
            List of processed documents
        """
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
        tasks = [
            self.__evaluate_document_quality(
                document, semaphore, await_time_seconds=await_time_seconds
            )
            for document in documents
        ]
        
        # Process tasks as they complete with progress tracking
        results = []
        for coro in tqdm(
            asyncio.as_completed(tasks),
            total=len(documents),
            desc="Evaluating document quality",
            unit="doc",
        ):
            result = await coro
            results.append(result)

        return results

    async def __evaluate_document_quality(
        self,
        document: Document,
        semaphore: asyncio.Semaphore | None = None,
        await_time_seconds: int = 2,
    ) -> Document | None:
        """Evaluate quality for a single document.

        Args:
            document: The Document object to evaluate.
            semaphore: Optional semaphore for controlling concurrent requests.
            await_time_seconds: Time to wait after API call to prevent rate limiting.
            
        Returns:
            Document with quality score added or unchanged document if evaluation failed.
        """
        # Return mock score if in mock mode
        if self.mock:
            return document.add_quality_score(score=0.5)

        async def process_document() -> Document:
            # Format prompt with document content
            input_user_prompt = self.SYSTEM_PROMPT_TEMPLATE.format(
                document=document.content
            )
            
            # Truncate prompt to fit model token limits
            try:
                input_user_prompt = utils.clip_tokens(
                    input_user_prompt, max_tokens=8192, model_id=self.model_id
                )
            except Exception as e:
                logger.warning(
                    f"Token clipping failed for document {document.id}: {str(e)}"
                )

            try:
                # Make API call to language model
                response = await acompletion(
                    model=self.model_id,
                    messages=[
                        {"role": "user", "content": input_user_prompt},
                    ],
                    stream=False,
                )
                # Wait to respect rate limits
                await asyncio.sleep(await_time_seconds)

                # Handle empty response
                if not response.choices:
                    logger.warning(
                        f"No quality evaluation generated for document {document.id}"
                    )
                    return document

                # Parse response and add quality score to document
                raw_answer = response.choices[0].message.content
                quality_score = self._parse_model_output(raw_answer)
                if not quality_score:
                    logger.warning(
                        f"Failed to parse model output for document {document.id}"
                    )
                    return document

                return document.add_quality_score(
                    score=quality_score.score,
                )
            except Exception as e:
                logger.warning(f"Quality evaluation failed for document {document.id}: {str(e)}")
                return document

        # Use semaphore to control concurrency if provided
        if semaphore:
            async with semaphore:
                return await process_document()

        return await process_document()

    def _parse_model_output(
        self, answer: str | None
    ) -> QualityScoreResponseFormat | None:
        """Parse JSON output from language model into structured format.
        
        Args:
            answer: Raw text response from the language model
            
        Returns:
            Structured quality score or None if parsing failed
        """
        if not answer:
            return None

        try:
            dict_content = json.loads(answer)
            return QualityScoreResponseFormat(
                score=dict_content["score"],
            )
        except Exception:
            return None


class RuleBasedQualityAgent:
    """A heuristic-based agent for evaluating document quality using simple rules.

    This agent evaluates document quality primarily by analyzing the ratio of URL content
    to total content length, assigning low scores to documents that are primarily
    composed of URLs rather than substantive content.
    """

    def __call__(
        self, documents: Document | list[Document]
    ) -> Document | list[Document]:
        """Process single document or batch of documents for quality scoring.

        Args:
            documents: Single Document or list of Documents to evaluate.

        Returns:
            Document | list[Document]: Processed document(s) with quality scores.
        """
        if hasattr(documents, "value"):
            documents = documents.value
        # Handle both single document and document collections
        is_single_document = isinstance(documents, Document)
        docs_list = [documents] if is_single_document else documents

        # Apply rule-based scoring to each document
        scored_documents = [self.__apply_quality_rules(document) for document in docs_list]

        # Return in the same format as input (single document or list)
        return scored_documents[0] if is_single_document else scored_documents

    def __apply_quality_rules(self, document: Document) -> Document:
        """Apply quality scoring rules to a single document.

        Calculates the ratio of URL content length to total content length.
        Documents with high URL content proportion receive lower quality scores.

        Args:
            document: The Document object to evaluate.

        Returns:
            Document: The input document with quality score added when applicable.
        """
        # Assign zero score to empty documents
        if len(document.content) == 0:
            return document.add_quality_score(score=0.0)

        # Calculate the ratio of URL content to overall content
        url_based_content = sum(len(url) for url in document.child_urls)
        url_content_ratio = url_based_content / len(document.content)

        # Apply scoring rules based on URL ratio thresholds
        if url_content_ratio >= 0.7:
            # Documents that are mostly URLs have minimal value
            return document.add_quality_score(score=0.0)
        elif url_content_ratio >= 0.5:
            # Documents with significant URL content have reduced value
            return document.add_quality_score(score=0.2)

        # For documents with acceptable URL ratios, leave scoring to ModelBasedQualityAgent
        return document