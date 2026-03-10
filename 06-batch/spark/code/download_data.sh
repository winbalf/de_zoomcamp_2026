
set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`

  URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo -n "downloading ${YEAR}-${FMONTH} ... "
  mkdir -p ${LOCAL_PREFIX}
  if wget -q ${URL} -O ${LOCAL_PATH} 2>/dev/null; then
    echo "ok"
  else
    echo "404 (not in release, skipping)"
    rm -f "${LOCAL_PATH}"
  fi
done
