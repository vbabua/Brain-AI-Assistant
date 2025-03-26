# run_notion.py
from brain_ai_assistant.tools.run import main

if __name__ == "__main__":
    import sys
    args = ["--run-fetch-notion-data-pipeline"]
    sys.argv.extend(args)
    main()