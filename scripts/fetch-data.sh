#!/usr/bin/env bash
#
# Downloads the Sparkov "Credit Card Transactions Fraud Detection Dataset".
#
# Kaggle needs credentials. Get an API token from https://www.kaggle.com/settings
# ("Create New Token" downloads kaggle.json), then either put it at ~/.kaggle/kaggle.json
# or export KAGGLE_USERNAME and KAGGLE_KEY.
#
# This uses curl rather than the Kaggle CLI on purpose: the CLI is a Python package, and
# this project has no other reason to need Python installed.

set -euo pipefail

DATA_DIR="${DATA_DIR:-data}"
SLUG="kartik2112/fraud-detection"

if compgen -G "${DATA_DIR}/*.csv" > /dev/null; then
  echo "CSV files already present in ${DATA_DIR}/ - nothing to do."
  exit 0
fi

if [[ -z "${KAGGLE_USERNAME:-}" || -z "${KAGGLE_KEY:-}" ]]; then
  CREDS="${HOME}/.kaggle/kaggle.json"
  if [[ -f "$CREDS" ]]; then
    KAGGLE_USERNAME=$(grep -o '"username"[^,}]*' "$CREDS" | cut -d'"' -f4)
    KAGGLE_KEY=$(grep -o '"key"[^,}]*' "$CREDS" | cut -d'"' -f4)
  fi
fi

if [[ -z "${KAGGLE_USERNAME:-}" || -z "${KAGGLE_KEY:-}" ]]; then
  cat <<'EOF'
No Kaggle credentials found.

Either:
  1. Download a token from https://www.kaggle.com/settings and save it to
     ~/.kaggle/kaggle.json, or export KAGGLE_USERNAME and KAGGLE_KEY; or
  2. Download the dataset by hand from
     https://www.kaggle.com/datasets/kartik2112/fraud-detection
     and unzip the CSVs into ./data/

The engine runs fine without it - the scripted scenarios in the traffic generator need
no external data. This dataset is only for the volume replay and the evaluation.
EOF
  exit 1
fi

mkdir -p "$DATA_DIR"
echo "downloading ${SLUG} (several hundred MB)"

curl -fL --progress-bar \
  -u "${KAGGLE_USERNAME}:${KAGGLE_KEY}" \
  -o "${DATA_DIR}/fraud-detection.zip" \
  "https://www.kaggle.com/api/v1/datasets/download/${SLUG}"

echo "unzipping"
unzip -o -q "${DATA_DIR}/fraud-detection.zip" -d "$DATA_DIR"
rm -f "${DATA_DIR}/fraud-detection.zip"

echo
ls -lh "$DATA_DIR"
echo
echo "Ready. Replay with:"
echo "  ./gradlew :traffic-generator:run --args=\"replay --speed=1000 --limit=50000\""
