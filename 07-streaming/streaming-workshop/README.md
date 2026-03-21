PS1="> "
nano ~/.bashrc

pip install uv
uv init -p 3.12

uv add kafka-python pandas pyarrow

docker compose up redpanda -d
docker compose logs redpanda -f

uv add --dev jupyter

docker compose up postgres -d
docker compose logs postgres -f

uvx pgcli -h localhost -p 5432 -U postgres -d postgres
or
docker compose exec postgres psql -U postgres -d postgres

CREATE TABLE processed_events (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    trip_distance DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    pickup_datetime TIMESTAMP
);

uv add psycopg2-binary

---

PREFIX="https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/07-streaming/workshop"

wget ${PREFIX}/Dockerfile.flink
wget ${PREFIX}/pyproject.flink.toml
wget ${PREFIX}/flink-config.yaml

---

docker compose up jobmanager taskmanager



------------

CREATE TABLE processed_events_aggregated (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)
);


PREFIX="https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/07-streaming/workshop"
wget ${PREFIX}/src/producers/producer_realtime.py -P src/producers/