# Explore Open Library pipeline data
#
# Run from the `dlt_zoomcamp` directory: `marimo edit explore_books.py`

import marimo as mo

app = mo.App()

with app.setup:
    import marimo as mo
    import ibis
    import plotly.express as px


@app.cell
def __():
    title = mo.md(
        """
        # Explore Open Library pipeline data
        Run from the `dlt_zoomcamp` directory: `marimo edit explore_books.py`
        """
    )
    return (title,)


@app.cell
def __():
    DB_PATH = "open_library_pipeline.duckdb"
    conn = ibis.duckdb.connect(DB_PATH)
    database = "open_library_pipeline_dataset"
    books = conn.table("books", database=database)
    books_author_name = conn.table("books__author_name", database=database)
    return DB_PATH, books, books_author_name, conn, database


@app.cell
def __(books_author_name):
    books_per_author = (
        books_author_name
        .group_by(books_author_name.value.name("author"))
        .aggregate(count=ibis._.count())
        .order_by(ibis.desc("count"))
    )
    df_author = books_per_author.execute()
    fig_bar = px.bar(
        df_author,
        x="author",
        y="count",
        title="Number of books per author",
        labels={"author": "Author", "count": "Number of books"},
    )
    fig_bar.update_layout(xaxis_tickangle=-45, height=500)
    return df_author, fig_bar, books_per_author


@app.cell
def __(books):
    books_over_time = (
        books.group_by(books.first_publish_year)
        .aggregate(count=ibis._.count())
        .order_by("first_publish_year")
    )
    df_time = books_over_time.execute()
    fig_line = px.line(
        df_time,
        x="first_publish_year",
        y="count",
        title="Books over time (by first publish year)",
        labels={"first_publish_year": "Year", "count": "Number of books"},
        markers=True,
    )
    fig_line.update_layout(height=400)
    return books_over_time, df_time, fig_line


if __name__ == "__main__":
    app.run()
