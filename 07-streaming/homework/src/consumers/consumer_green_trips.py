import argparse
import json
from typing import Any, Dict

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# Function to parse trip distance
def parse_trip_distance(value: Any) -> float:
    # Producer sends JSON; depending on pandas->json conversion it might be float or string.
    if value is None:
        return float("nan")
    return float(value)


# Function to consume green trips from a Kafka topic
def consumer_main(
    bootstrap_server: str,
    topic: str,
    threshold_km: float,
) -> None:
    # Create a Kafka consumer instance
    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_server],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        # No group_id needed since we will manually assign+seek.
        group_id=None,
    )

    # Get the partitions for the topic
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        raise RuntimeError(f"No partitions found for topic '{topic}'. Is the topic created?")

    # Assign the partitions to the consumer
    topic_partitions = [TopicPartition(topic, p) for p in sorted(partitions)]
    consumer.assign(topic_partitions)

    # Read everything currently available in the topic
    for tp in topic_partitions:
        consumer.seek_to_beginning(tp)

    # Get the end offsets for the partitions
    end_offsets = consumer.end_offsets(topic_partitions)

    # Initialize counters
    total_trips = 0
    trips_over_threshold = 0

    # Initialize a set of remaining partitions
    remaining = set(topic_partitions)
    try:
        # Poll for records until all partitions are processed
        while remaining:
            # Poll for records with a timeout of 1 second and a maximum of 500 records
            records_map = consumer.poll(timeout_ms=1000, max_records=500)
            if not records_map:
                # Still check positions in case producers already finished
                pass

            for tp in list(remaining):
                # Update counts from any records for this partition
                for record in records_map.get(tp, []):
                    payload: Dict[str, Any] = json.loads(record.value.decode("utf-8"))
                    trip_distance = parse_trip_distance(payload.get("trip_distance"))
                    total_trips += 1
                    if trip_distance > threshold_km:
                        trips_over_threshold += 1

                # Stop when we've reached the end offset for this partition
                if consumer.position(tp) >= end_offsets[tp]:
                    remaining.remove(tp)
    finally:
        # Close the consumer
        consumer.close()

    # Print the total number of trips and the number of trips with trip distance greater than the threshold
    print(f"total trips: {total_trips}")
    print(f"trips with trip_distance > {threshold_km}: {trips_over_threshold}")

# Function to parse command line arguments
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume green-trips from Kafka and count trips by trip_distance."
    )
    # Add a command line argument for the Kafka bootstrap server address
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="green-trips")
    # Add a command line argument for the threshold for trip distance
    parser.add_argument("--threshold-km", type=float, default=5.0)
    return parser.parse_args()

# Main function to consume green trips from a Kafka topic
if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    # Consume green trips from a Kafka topic
    # This is the main function that is called when the script is run.
    consumer_main(
        bootstrap_server=args.bootstrap_server,
        topic=args.topic,
        threshold_km=args.threshold_km,
    )

