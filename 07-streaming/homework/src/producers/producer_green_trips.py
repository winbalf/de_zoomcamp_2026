import argparse
import json
import math
from decimal import InvalidOperation
from time import time
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from kafka import KafkaProducer


REQUIRED_COLUMNS: List[str] = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]

def _sanitize_for_json(value: Any) -> Any:
    """Flink's JSON format uses Jackson without ALLOW_NON_NUMERIC_NUMBERS; ensure no NaN/Inf."""
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)) and not isinstance(value, (bool, np.bool_)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        fv = float(value)
        if not math.isfinite(fv):
            return None
        return fv
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError, InvalidOperation):
        # e.g. some Decimal edge cases when pandas probes "isna"
        pass
    return value


def _sanitize_record(record: Dict) -> Dict:
    return {k: _sanitize_for_json(v) for k, v in record.items()}


# Function to serialize a Python dictionary to JSON-encoded bytes for Kafka transmission
def dict_serializer(value: Dict) -> bytes:
    # Re-sanitize at encode time so nothing non-finite reaches strict-json consumers (Flink).
    clean = _sanitize_record(value)
    return json.dumps(clean, allow_nan=False).encode("utf-8")

# Function to send green trips to a Kafka topic
# Reads a parquet file, filters to only include the required columns,
# converts datetime columns to strings, and sends the records to a Kafka topic.
def send_green_trips(
    parquet_path: str,
    bootstrap_server: str = "localhost:9092",
    topic_name: str = "green-trips",
) -> None:
    # Read the parquet file into a pandas DataFrame
    df = pd.read_parquet(parquet_path)

    # Check for missing required columns
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns in parquet file: {missing_cols}")

    # Keep only the required columns
    df = df[REQUIRED_COLUMNS].copy()

    # Convert datetime columns to strings for JSON serialization
    # This is necessary because Kafka requires all values to be strings.
    for col in ["lpep_pickup_datetime", "lpep_dropoff_datetime"]:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            # Best effort: just cast to string
            df[col] = df[col].astype(str)

    # Parquet often has NaN in float columns (e.g. passenger_count). json.dumps would
    # emit invalid JSON ("NaN") with allow_nan=True (Kafka + Flink then break). Use SQL NULL
    # semantics: missing values -> None -> JSON null.
    # Any numeric dtype must be object first: where(..., None) on float64/int dtypes can keep
    # sentinel NaNs / fail to hold Python None as intended.
    num_cols = df.select_dtypes(include=["number"]).columns
    if len(num_cols) > 0:
        df[num_cols] = df[num_cols].astype(object)
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")

    # Create a Kafka producer instance
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        value_serializer=dict_serializer,
    )

    # Print the number of records to be sent
    print(f"Sending {len(records)} records to topic '{topic_name}'...")

    # Send the records to the Kafka topic
    t0 = time()
    for record in records:
        producer.send(topic_name, value=_sanitize_record(record))

    # Flush the producer to ensure all messages are sent
    producer.flush()
    t1 = time()

    # Print the time taken to send the records
    print(f"took {(t1 - t0):.2f} seconds")


def parse_args() -> argparse.Namespace:
    # Parse command line arguments
    # This allows the script to be run with different parameters from the command line.
    parser = argparse.ArgumentParser(
        description="Send NYC green taxi trips from a parquet file to a Kafka topic."
    )
    # Add a command line argument for the parquet file path
    parser.add_argument(
        "--parquet-path",
        required=True,
        help="Path to the green taxi parquet file.",
    )
    # Add a command line argument for the Kafka bootstrap server address
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server address (default: localhost:9092).",
    )
    # Add a command line argument for the Kafka topic name
    parser.add_argument(
        "--topic",
        default="green-trips",
        help="Kafka topic to send messages to (default: green-trips).",
    )
    return parser.parse_args()

# Main function to send green trips to a Kafka topic
def main() -> None:
    # Parse command line arguments
    args = parse_args()
    # Send green trips to a Kafka topic
    # This is the main function that is called when the script is run.
    send_green_trips(
        parquet_path=args.parquet_path,
        bootstrap_server=args.bootstrap_server,
        topic_name=args.topic,
    )


if __name__ == "__main__":
    main()

