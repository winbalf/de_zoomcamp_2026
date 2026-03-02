"""NYC taxi data REST API source using dlt."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

# Cap how many pages to fetch so the run finishes. The API may not return
# a total count and may never return an empty page. Set to None to fetch
# until an empty page (may run indefinitely).
#
# The zoomcamp API uses page-based pagination (?page=1, ?page=2). Use
# page_number paginator so each request gets a different page.
MAXIMUM_PAGE = None  # Set to an int (e.g. 10) to cap pages; None = stop on empty page


@dlt.source
def taxi_pipeline_rest_api_source():
    """Define dlt resources for the NYC taxi data REST API."""
    paginator: dict = {
        "type": "page_number",
        "page_param": "page",
        "base_page": 1,
        "total_path": None,
        "stop_after_empty_page": True,
    }
    if MAXIMUM_PAGE is not None:
        paginator["maximum_page"] = MAXIMUM_PAGE

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
        },
        "resources": [
            {
                "name": "trips",
                "write_disposition": "append",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    "params": {
                        "limit": 1000,
                    },
                    "paginator": paginator,
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)  # noqa: T201
