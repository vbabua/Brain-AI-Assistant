from zenml.steps import step, get_step_context
from typing import Annotated
from loguru import logger

from apps.brain_ai_assistant.domain.document import Document
from apps.brain_ai_assistant.application.agents.quality import RuleBasedQualityAgent, ModelBasedQualityAgent
@step
def add_quality_score(
        documents : list[Document],
        model_name = "gpt-4o-mini",
        use_mock_quality_agent : bool = False,
        concurrent_tasks : int = 10
    ) -> Annotated[list[Document], "scored_documents"]:
    """
    Evaluates content quality of documents using rule-based and AI-powered assessment.

    This function processes documents in two stages:
    1. Applies rule-based quality evaluation for efficient first-pass screening
    2. Uses an AI model for more nuanced assessment of documents that weren't evaluated by rules

    Args:
        content_items: List of documents to evaluate for quality
        language_model_name: Identifier for the model to use in quality assessment.
            Defaults to "gpt-4o-mini"
        use_test_mode: If True, uses synthetic responses instead of actual model calls.
            Defaults to False
        parallelism_limit: Maximum number of concurrent quality evaluation operations.
            Defaults to 10

    Returns:
        list[Document]: Documents enhanced with quality scores, annotated as
            "evaluated_content" for pipeline metadata tracking

    Note:
        The function tracks evaluation statistics in the step context for monitoring purposes.
    """
    if hasattr(documents, "value"):
            documents = documents.value
    # First evaluation stage: Apply rule-based scoring for efficient processing
    rule_evaluator = RuleBasedQualityAgent()
    documents_after_rules: list[Document] = rule_evaluator(documents)

    # Separate documents that received scores through rules from those that didn't
    documents_with_rule_scores = [
        doc for doc in documents_after_rules if doc.content_quality_score is not None
    ]
    documents_without_scores = [
        doc for doc in documents_after_rules if doc.content_quality_score is None
    ]

    # Second evaluation stage: Use model-based scoring for remaining documents
    model_evaluator = ModelBasedQualityAgent(
        model_name=model_name, 
        use_mock=use_mock_quality_agent, 
        max_concurrent_tasks=concurrent_tasks
    )
    documents_with_model_scores: list[Document] = model_evaluator(
        documents_without_scores
    )

    # Combine results from both evaluation methods
    scored_documents: list[Document] = documents_with_rule_scores + documents_with_model_scores

    # Calculate final statistics for reporting
    total_documents = len(documents)
    scored_documents_count = len(
        [doc for doc in scored_documents if doc.content_quality_score is not None]
    )
    logger.info(f"Processed {total_documents} documents for quality evaluation")
    logger.info(f"Successfully scored {scored_documents_count} documents")

    step_context = get_step_context()
    step_context.add_output_metadata(
        output_name="scored_documents",
        metadata={
            "total_documents": total_documents,
            "documents_with_scores": scored_documents_count,
            "documents_scored_by_rules": len(documents_with_rule_scores),
            "documents_scored_by_model": len(documents_with_model_scores),
        },
    )

    return scored_documents
