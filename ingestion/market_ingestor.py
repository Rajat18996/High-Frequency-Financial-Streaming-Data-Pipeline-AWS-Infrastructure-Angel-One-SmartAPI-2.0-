#!/usr/bin/env python3
"""
Production-Grade Resilient Market Ingestion Engine (Ultra-Lean Binary Ingestor)
Author: Rajat Shrivastava
Description: Connects to AngelOne SmartStream, captures pure binary frames,
             and ships raw hexadecimal payloads directly to AWS SQS FIFO.
             Zero processing/decoding is performed on this layer to protect
             the main asyncio network loop from I/O bottlenecks.
"""

import asyncio
import json
import boto3
import pyotp
import websockets
import time
import sys
from concurrent.futures import ThreadPoolExecutor

# =====================================================================
# SYSTEM CORE TARGET & PIPELINE CODES
# =====================================================================
AWS_REGION = "ap-south-1"
SQS_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/AWS_USER_ID/market-ticks-queue.fifo"

# Initialize native structural service boundaries
sqs_client = boto3.client('sqs', region_name=AWS_REGION)
ssm_client = boto3.client('ssm', region_name=AWS_REGION)

# Thread pool dedicated entirely to handling synchronous AWS SQS network requests
executor = ThreadPoolExecutor(max_workers=20)

# Global Performance State Tracking
tick_counter = 0

def get_secret_parameters():
    """Fetches session credentials out of AWS Systems Manager Parameter Store."""
    try:
        api_key = ssm_client.get_parameter(Name="/stockpipeline/angelone/api_key", WithDecryption=True)['Parameter']['Value']
        client_code = ssm_client.get_parameter(Name="/stockpipeline/angelone/client_code", WithDecryption=True)['Parameter']['Value']
        password = ssm_client.get_parameter(Name="/stockpipeline/angelone/password", WithDecryption=True)['Parameter']['Value']
        totp_seed = ssm_client.get_parameter(Name="/stockpipeline/angelone/totp_seed", WithDecryption=True)['Parameter']['Value']
        return api_key, client_code, password, totp_seed
    except Exception as e:
        print(f"[BOOTSTRAP EXCEPTION] Parameter lookup mismatch inside SSM: {str(e)}", file=sys.stderr, flush=True)
        raise e

def generate_angelone_session():
    """Executes authentication handshake protocol with the exchange gateway."""
    from SmartApi import SmartConnect
    api_key, client_code, password, totp_seed = get_secret_parameters()
    totp = pyotp.TOTP(totp_seed)
    current_otp = totp.now()

    print(f"[AUTH CORE] Authenticating credential payload matrix for user: {client_code}", flush=True)

    smart_api = SmartConnect(api_key=api_key)
    login_session = smart_api.generateSession(client_code, password, current_otp)

    if not login_session.get('status'):
        raise ConnectionError(f"AngelOne Core Network Handshake Rejected: {login_session.get('message')}")

    print("[AUTH CORE] Session context verified. Security parameters propagated.", flush=True)
    return {
        "jwtToken": login_session['data']['jwtToken'],
        "feedToken": smart_api.getfeedToken(),
        "api_key": api_key,
        "client_code": client_code
    }

def sync_sqs_send(raw_hex, arrival_timestamp_ms):
    """Dispatches the raw binary hexadecimal string directly to SQS FIFO queue."""
    try:
        payload = {
            "arrival_timestamp_ms": arrival_timestamp_ms,
            "raw_hex": raw_hex
        }

        # PRODUCER FIFO OPTIMIZATION:
        # Using a unified MessageGroupId allows maximum sequential grouping alignment,
        # while using the unique hex snapshot prevents collisions in deduplication.
        sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(payload),
            MessageGroupId="StockGroup_AngelOne_Mode3_Stream",
            MessageDeduplicationId=f"Dedupe_M3_{arrival_timestamp_ms}_{raw_hex[:16]}"
        )
    except Exception as sqs_error:
        print(f"[PIPELINE DROP ALERT] SQS structural rejection: {sqs_error}", file=sys.stderr, flush=True)

async def send_heartbeat_loop(websocket):
    """Emits structural connection heartbeat pings to prevent gateway timeout drops."""
    try:
        while True:
            await asyncio.sleep(20)  # Core application heartbeat interval
            heartbeat_payload = {"action": 3, "params": {}}
            await websocket.send(json.dumps(heartbeat_payload))
    except asyncio.CancelledError:
        pass
    except Exception as err:
        print(f"[HEARTBEAT MESH WARNING] Keepalive ping delayed: {err}", file=sys.stderr, flush=True)

async def stream_ticks_to_pipeline():
    """Manages connection routing lifecycle across the streaming network architecture."""
    global tick_counter
    loop = asyncio.get_running_loop()
    session = generate_angelone_session()
    ws_url = "wss://smartapisocket.angelone.in/smart-stream"
    reconnect_delay = 2

    while True:
        headers = {
            "Authorization": f"Bearer {session['jwtToken']}",
            "x-api-key": session['api_key'],
            "x-client-code": session['client_code'],
            "x-feed-token": session['feedToken']
        }

        print("[STREAM ENGINE] Opening ultra-lean binary tunnel to SmartStream...", flush=True)
        heartbeat_task = None

        try:
            # Clear native protocol-level ping configurations.
            async with websockets.connect(
                ws_url,
                additional_headers=headers,
                ping_interval=None,
                ping_timeout=None
            ) as websocket:

                print("[STREAM ENGINE] Transport layer verified. Pipeline connection SECURED.", flush=True)
                reconnect_delay = 2  # Reset exponential backoff state

                subscription_payload = {
                    "correlationID": f"aws_pipeline_{int(time.time())}",
                    "action": 1,
                    "params": {
                        "mode": 3,
                        "tokenList": [
                            {"exchangeType": 1, "tokens": ["2885"]},
                            {"exchangeType": 1, "tokens": ["999001"]}
                        ]
                    }
                }
                await websocket.send(json.dumps(subscription_payload))
                heartbeat_task = asyncio.create_task(send_heartbeat_loop(websocket))

                async for raw_packet in websocket:
                    if isinstance(raw_packet, bytes):
                        # Capture exact arrival time metrics before offloading
                        arrival_timestamp_ms = int(time.time() * 1000)
                        preserved_hex = raw_packet.hex()

                        tick_counter += 1
                        if tick_counter % 500 == 0:
                            print(f"[INGEST] Ingested {tick_counter} pure binary messages to SQS.", flush=True)

                        # Instantly offload raw hex string delivery to thread pool execution
                        loop.run_in_executor(
                            executor,
                            sync_sqs_send,
                            preserved_hex,
                            arrival_timestamp_ms
                        )

        except (websockets.ConnectionClosed, OSError) as net_err:
            print(f"[DISCONNECT DETECTED] Tunnel failure: {net_err}. Reconnecting in {reconnect_delay}s...", file=sys.stderr, flush=True)
        except Exception as system_anomaly:
            print(f"[RUNTIME EXCEPTION IN LOOP] Halting operational task: {system_anomaly}", file=sys.stderr, flush=True)
        finally:
            if heartbeat_task and not heartbeat_task.done():
                heartbeat_task.cancel()
            # Apply progressive backoff to cushion connection bursts
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

if __name__ == "__main__":
    try:
        asyncio.run(stream_ticks_to_pipeline())
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Data pipeline execution intercepted manually by systems engineer.", flush=True)
