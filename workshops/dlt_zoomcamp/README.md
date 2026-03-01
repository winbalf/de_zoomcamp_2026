
uv init dlt_zoomcamp
uv add "dlt["workspace"]"
uv run dlt init dlthub:open_library duckdb

---
Please generate a REST API Source for Open Library API, as specified in @open_library-docs.yaml 

Use the search api and query harry potter books.

Place the code in open_library_pipeline.py and name the pipeline open_library_pipeline. 
If the file exists, use it as a starting point. 
Do not add or modify any other files. 
Use @dlt rest api as a tutorial. 
After adding the endpoints, allow the user to run the pipeline with python open_library_pipeline.py and await further instructions.

---

uv run open_library_pipeline.py
<!-- 
rm -rf .venv
uv sync
uv run dlt --version -->

uv run dlt pipeline open_library_pipeline show

---
https://dlthub.com/docs/general-usage/dataset-access/marimo

using the above reference:
1. create a bar chart showing number of books per author.
2. create a line chart showing books over time.
---

### Install deps (if not already)
pip install -e .

#### Start the marimo app and open the notebook
marimo edit explore_books.py
