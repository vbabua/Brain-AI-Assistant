import click
from datetime import datetime as dt
from pathlib import Path

from brain_ai_assistant.pipelines import fetch_notion_data

@click.command()
@click.option(
    "--run-fetch-notion-data-pipeline",
    is_flag=True,
    default=False,
    help="Whether to run the collection data from Notion pipeline.",
)
@click.option(
    "--no-cache",
    is_flag=True,
    default=False,
    help="Disable caching for the pipeline run.",
)
def main(
    run_fetch_notion_data_pipeline: bool = False,
    no_cache: bool = False,
) -> None:
    """
    Run the ZenML Second Brain project pipelines.
    """
    
    if not run_fetch_notion_data_pipeline:
        print("Please specify an action to run with --run-fetch-notion-data-pipeline")
        return
    
    pipeline_args = {
        "enable_cache": not no_cache,
    }
    root_dir = Path(__file__).resolve().parent.parent
    
    if run_fetch_notion_data_pipeline:
        run_args = {}
        pipeline_args["config_path"] = root_dir / "configs" / "collect_notion_data.yaml"
        assert pipeline_args["config_path"].exists(), f"Config file not found: {pipeline_args['config_path']}"
        pipeline_args["run_name"] = f"fetch_notion_data_run_{dt.now().strftime('%Y_%m_%d_%H_%M_%S')}"
        fetch_notion_data.with_options(**pipeline_args)(**run_args)


if __name__ == "__main__":
    main()