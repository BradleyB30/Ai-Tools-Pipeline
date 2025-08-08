@echo off
call .\.venv\Scripts\activate.bat
set EUDK_RAW_URL=https://raw.githubusercontent.com/eudk/awesome-ai-tools/main/README.md
python -m pipeline.bronze_md_eudk
python -m pipeline.bronze_csv_ingest
python -m pipeline.silver_normalize
python -m pipeline.gold_dedupe
python -m pipeline.load_pg
echo Done.
