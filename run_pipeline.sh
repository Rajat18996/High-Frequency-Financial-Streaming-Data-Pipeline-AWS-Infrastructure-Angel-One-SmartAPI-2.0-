#!/usr/bin/env bash
# ==============================================================================
# Enterprise High-Frequency Pipeline Ingestion Orchestrator Control Script
# Author: Rajat Shrivastava
# Description: Automated environmental health check, virtual dependency masking,
#              and concurrent multi-process daemon lifecycle configuration.
# ==============================================================================

set -euo pipefail

# System Configuration Parameters
VENV_DIR=".venv"
LOG_DIR="logs"
PRODUCER_SCRIPT="ingestion/market_ingestor.py"
CONSUMER_SCRIPT="consumers/market_consumer.py"

echo "======================================================================"
echo "[INIT] Launching Stock Pipeline Core Orchestrator Ecosystem..."
echo "======================================================================"

# Create system log repositories if absent
if [ ! -d "$LOG_DIR" ]; then
    echo "[SYSTEM] Creating dedicated telemetry log layout space at /$LOG_DIR"
    mkdir "$LOG_DIR"
fi

# Verify local Python 3 environment exists
if ! command -v python3 &> /dev/null; then
    echo "[CRITICAL ERROR] Python3 runtime layer could not be detected on this machine. Halting." >&2
    exit 1
fi

# Provision isolated Python virtual network layer if missing
if [ ! -d "$VENV_DIR" ]; then
    echo "[ENVIRONMENT] Sandbox workspace missing. Instantiating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Activate environment context safely
echo "[ENVIRONMENT] Binding execution parameters to local shell..."
source "$VENV_DIR/bin/activate"

# Sync module alignments with repository criteria
if [ -f "requirements.txt" ]; then
    echo "[ENVIRONMENT] Verifying third-party dependency states against requirements.txt..."
    pip install --upgrade pip --quiet
    pip install -r requirements.txt --quiet
else
    echo "[WARNING] No requirements.txt found. Proceeding with active environment parameters."
fi

# Ensure underlying files have executable permissions flag assigned
chmod +x "$PRODUCER_SCRIPT"
chmod +x "$CONSUMER_SCRIPT"

echo "----------------------------------------------------------------------"
echo "[DAEMON] Starting dual-path application components in background..."
echo "----------------------------------------------------------------------"

# Spawn high-performance consumer background worker process
echo "[DAEMON 1/2] Spawning Market Data Consumer Engine..."
python3 "$CONSUMER_SCRIPT" > "$LOG_DIR/consumer_stdout.log" 2> "$LOG_DIR/consumer_stderr.log" &
CONSUMER_PID=$!
echo "[DAEMON 1/2] Consumer online. Process ID tracking tag: $CONSUMER_PID"

# Sleep briefly to let SQS consumers establish background listener matrices
sleep 2

# Spawn ultra-lean binary producer ingestor process
echo "[DAEMON 2/2] Spawning AngelOne WebSocket Producer Engine..."
python3 "$PRODUCER_SCRIPT" > "$LOG_DIR/producer_stdout.log" 2> "$LOG_DIR/producer_stderr.log" &
PRODUCER_PID=$!
echo "[DAEMON 2/2] Producer online. Process ID tracking tag: $PRODUCER_PID"

echo "======================================================================"
echo "[SUCCESS] Real-time engine matrix is fully active and processing ticks."
echo "[TELEMETRY] To audit execution performance, track live outputs via:"
echo "            -> tail -f logs/producer_stdout.log"
echo "            -> tail -f logs/consumer_stdout.log"
echo "======================================================================"
echo "To terminate all active processes run: kill $PRODUCER_PID $CONSUMER_PID"
