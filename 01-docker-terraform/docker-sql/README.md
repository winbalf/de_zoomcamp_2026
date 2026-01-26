### Question 1. Understanding Docker images

```bash
docker run python:3.13 bash -c "pip --version"
```

**Ans:** `25.3`

---

### Question 2. Understanding Docker networking and docker-compose

```bash
docker-compose up -d
```

**Ans:**
* Hostname: `db`
* Port: `5432`

---

### Question 3. Counting short trips

```sql
SELECT COUNT(*) AS trip_count
FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
```

**Ans:** `8007`

---

### Question 4. Longest trip for each day

```sql
SELECT 
    DATE(lpep_pickup_datetime) AS pickup_day,
    MAX(trip_distance) AS max_trip_distance
FROM green_taxi_data
WHERE trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_trip_distance DESC
LIMIT 1;
```

**Ans:** `"2025-11-14"`

---

### Question 5. Biggest pickup zone

```sql
SELECT 
    z."Zone" AS pickup_zone,
    SUM(t.total_amount) AS total_amount_sum
FROM green_taxi_data t
JOIN taxi_zone_lookup z ON t."PULocationID" = z."LocationID"
WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z."Zone"
ORDER BY total_amount_sum DESC
LIMIT 1;
```

**Ans:** `East Harlem North`

---

### Question 6. Largest tip

```sql
SELECT 
    dz."Zone" AS dropoff_zone,
    MAX(t.tip_amount) AS total_tip_amount
FROM green_taxi_data t
JOIN taxi_zone_lookup pz ON t."PULocationID" = pz."LocationID"
JOIN taxi_zone_lookup dz ON t."DOLocationID" = dz."LocationID"
WHERE pz."Zone" = 'East Harlem North'
  AND DATE(t.lpep_pickup_datetime) >= '2025-11-01'
  AND DATE(t.lpep_pickup_datetime) < '2025-12-01'
GROUP BY dz."Zone"
ORDER BY total_tip_amount DESC
LIMIT 1;
```

**Ans:** `"Yorkville West"`

---

### Question 7. Terraform Workflow

```bash
terraform init
terraform apply -auto-approve
terraform destroy
```

**Ans:** `terraform init, terraform apply -auto-approve, terraform destroy`
