#!/usr/bin/env bash
set -e

# P5-2: Hardened launch wrapper for the micro-live autonomous probe.
# Ensure we are in the repository root.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

echo "Starting micro-live launch wrapper..."

if [[ ! -f "./.env" ]]; then
    echo "ERROR: .env file not found in repository root. Aborting."
    exit 1
fi

echo "Sourcing .env..."
set -a
source "./.env"
set +a

# Ensure production DB path is used if not overridden
export POLYMARKET_DATA_DIR="${POLYMARKET_DATA_DIR:-/home/polybot/.polybot}"
mkdir -p "${POLYMARKET_DATA_DIR}"

DB_PATH="${POLYMARKET_DATA_DIR}/live.sqlite3"
BUNDLE_DIR="${POLYMARKET_DATA_DIR}/capture_bundles/micro_live_$(date +%Y%m%d_%H%M%S)"
LOG_PATH="${LOG_PATH:-}"
NOTIFY_ON_COMPLETE="${NOTIFY_ON_COMPLETE:-1}"

echo "DB_PATH=${DB_PATH}"
echo "BUNDLE_DIR=${BUNDLE_DIR}"
if [[ -n "${LOG_PATH}" ]]; then
    echo "LOG_PATH=${LOG_PATH}"
fi

# Determine python executable
if [[ -f "./.venv/bin/python" && -x "./.venv/bin/python" ]]; then
    # Test if it actually has our deps, else fallback
    if "./.venv/bin/python" -c "import structlog" 2>/dev/null; then
        PYTHON="./.venv/bin/python"
    else
        PYTHON="python3"
    fi
else
    PYTHON="python3"
fi

echo "Using python: ${PYTHON}"

# Trap signals for graceful shutdown message
trap 'echo "Received termination signal. live_runner should handle graceful shutdown and write the bundle..."' SIGINT SIGTERM

echo "Launching live_runner in 3 seconds..."
sleep 3

set +e
"${PYTHON}" -m v2.short_horizon.short_horizon.live_runner \
    "${DB_PATH}" \
    --mode live \
    --execution-mode live \
    --allow-live-execution \
    --approve-allowances \
    --redeem-resolved \
    --redeem-resolved-interval-seconds 300 \
    --bridge-polygon-usdc-to-usdce \
    --capture-dir "${BUNDLE_DIR}" \
    --max-runtime-seconds 28800 \
    "$@"
status=$?
set -e

if [[ "${NOTIFY_ON_COMPLETE}" != "0" ]]; then
    notify_args=("${DB_PATH}")
    if [[ -n "${LOG_PATH}" ]]; then
        notify_args+=(--log-path "${LOG_PATH}")
    fi
    if ! "${PYTHON}" scripts/notify_probe_completed.py "${notify_args[@]}"; then
        echo "WARNING: completion Telegram notification failed" >&2
    fi
fi

exit "${status}"
