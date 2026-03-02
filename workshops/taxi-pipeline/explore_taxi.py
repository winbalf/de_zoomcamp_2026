# Explore taxi_pipeline data with Marimo
#
# Run from the taxi-pipeline directory: marimo edit explore_taxi.py
# Requires: uv sync (installs marimo, ibis, plotly)

import marimo

__generated_with = "0.20.2"
app = marimo.App()

with app.setup:
    import marimo as mo
    import ibis
    import plotly.express as px


@app.cell
def _():
    title=mo.md("""
    # Taxi pipeline – data exploration
    Run from the **taxi-pipeline** directory: `marimo edit explore_taxi.py`
    """)
    return


@app.cell
def _():
    DB_PATH = "taxi_pipeline.duckdb"
    conn = ibis.duckdb.connect(DB_PATH)
    database = "taxi_pipeline_dataset"
    trips = conn.table("trips", database=database)
    return (trips,)


@app.cell
def _(trips):
    # Bar chart: trips by payment type
    by_payment = (
        trips.group_by(trips.payment_type)
        .aggregate(count=ibis._.count())
        .order_by(ibis.desc("count"))
    )
    df_payment = by_payment.execute()
    fig_payment = px.bar(
        df_payment,
        x="payment_type",
        y="count",
        title="Number of trips by payment type",
        labels={"payment_type": "Payment type", "count": "Number of trips"},
        color="count",
        color_continuous_scale="Blues",
    )
    fig_payment.update_layout(xaxis_tickangle=-45, height=400, showlegend=False)
    return (fig_payment,)


@app.cell
def _(fig_payment):
    mo.output.replace(fig_payment)
    return


@app.cell
def _(trips):
    # Line chart: trips over time (by pickup date)
    trips_with_date = trips.mutate(
        pickup_date=trips.trip_pickup_date_time.date()
    )
    by_date = (
        trips_with_date.group_by("pickup_date")
        .aggregate(count=ibis._.count())
        .order_by("pickup_date")
    )
    df_date = by_date.execute()
    fig_trips_time = px.line(
        df_date,
        x="pickup_date",
        y="count",
        title="Trips per day (pickup date)",
        labels={"pickup_date": "Date", "count": "Number of trips"},
        markers=True,
    )
    fig_trips_time.update_layout(height=400)
    return (fig_trips_time,)


@app.cell
def _(fig_trips_time):
    mo.output.replace(fig_trips_time)
    return


@app.cell
def _(trips):
    # Bar chart: total tips by payment type
    tips_by_payment = (
        trips.group_by(trips.payment_type)
        .aggregate(total_tips=trips.tip_amt.sum())
        .order_by(ibis.desc("total_tips"))
    )
    df_tips = tips_by_payment.execute()
    fig_tips = px.bar(
        df_tips,
        x="payment_type",
        y="total_tips",
        title="Total tips by payment type",
        labels={"payment_type": "Payment type", "total_tips": "Total tips ($)"},
        color="total_tips",
        color_continuous_scale="Greens",
    )
    fig_tips.update_layout(xaxis_tickangle=-45, height=400, showlegend=False)
    return (fig_tips,)


@app.cell
def _(fig_tips):
    mo.output.replace(fig_tips)
    return


if __name__ == "__main__":
    app.run()
