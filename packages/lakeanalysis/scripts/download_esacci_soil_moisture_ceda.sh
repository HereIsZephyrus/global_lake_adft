#!/usr/bin/env bash
# Download ESA CCI Soil Moisture COMBINED daily NetCDF (v09.2) from CEDA DAP.
# Years 2001–2023 by default; output under /mnt/warehouse/ESACCI/ (override with ESACCI_OUT).
# Browse: https://data.ceda.ac.uk/neodc/esacci/soil_moisture/data/daily_files/COMBINED/v09.2
set -euo pipefail

BASE_URL="${ESACCI_BASE_URL:-https://dap.ceda.ac.uk/neodc/esacci/soil_moisture/data/daily_files/COMBINED/v09.2}"
OUT_DIR="${ESACCI_OUT:-/mnt/warehouse/ESACCI}"
YEAR_START="${ESACCI_YEAR_START:-2001}"
YEAR_END="${ESACCI_YEAR_END:-2023}"

mkdir -p "${OUT_DIR}"

echo "ESA CCI soil moisture COMBINED v09.2 (CEDA DAP)"
echo "  base=${BASE_URL}"
echo "  out_dir=${OUT_DIR}"
echo "  years=${YEAR_START}..${YEAR_END}"
echo ""

for y in $(seq "${YEAR_START}" "${YEAR_END}"); do
  echo ">>> ${y}"
  wget -e robots=off -r -np -nH --cut-dirs=7 -nc -P "${OUT_DIR}" \
    --accept "*.nc" \
    "${BASE_URL}/${y}/"
done

echo ""
echo "Done. Files under: ${OUT_DIR}/<year>/*.nc"
