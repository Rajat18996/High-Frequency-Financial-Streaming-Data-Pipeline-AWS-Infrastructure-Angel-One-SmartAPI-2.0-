#!/usr/bin/env python3
"""
Production-Grade High-Velocity Parquet Target Audit & Realtime Hot-Path Consumer
Author: Rajat Shrivastava
Description: Maximizes SQS drain throughput while calculating live order book
             alpha features and updating a local high-speed hot-path CSV for
             the Streamlit predictive UI engine.
"""
import os
import sys
import json
import time
import asyncio
import boto3
import struct
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor

# ==============================================================================
# CONFIGURATION ENVIRONMENT DEFINITIONS
# ==============================================================================
AWS_REGION = "ap-south-1"
SQS_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/AWS_USER_ID/market-ticks-queue.fifo"
S3_TARGET_BUCKET = "realtime-streaming-stock-market-datapipeline"
HOT_PATH_FILE = "live_market_hot.csv"

# High-Performance Buffer Settings Optimized for High-Velocity Draining
MAX_MESSAGES_PER_FLUSH = 500
SQS_BATCH_SIZE = 10
WAIT_TIME_SECONDS = 2
FLUSH_INTERVAL_SECONDS = 3
MAX_CONSUMER_CONCURRENCY = 6

# Decoupled Thread Pools
process_executor = ThreadPoolExecutor(max_workers=12)
s3_io_executor = ThreadPoolExecutor(max_workers=30)

sqs_client = boto3.client('sqs', region_name=AWS_REGION)
s3_client = boto3.client('s3', region_name=AWS_REGION)

# ==============================================================================
# PYARROW SCHEMA DEFINITIONS FOR PARQUET CORRELATION
# ==============================================================================
order_level_struct = pa.struct([
    ('level', pa.int32()), ('quantity', pa.int32()), ('price', pa.float64()),
    ('num_orders', pa.int32()), ('flag', pa.int32())
])
raw_audit_schema = pa.schema([
    ('subscription_mode', pa.int32()), ('exchange_type', pa.int32()), ('token', pa.string()),
    ('sequence_number', pa.int64()), ('exchange_timestamp', pa.int64()), ('last_traded_price', pa.float64()),
    ('last_traded_quantity', pa.int64()), ('avg_traded_price', pa.float64()), ('open_price', pa.float64()),
    ('high_price', pa.float64()), ('low_price', pa.float64()), ('close_price', pa.float64()),
    ('last_traded_timestamp', pa.int64()), ('open_interest', pa.int64()), ('oi_change_percent', pa.float64()),
    ('market_analytics', pa.struct([
        ('total_buy_quantity', pa.float64()), ('total_sell_quantity', pa.float64()), ('volume_traded', pa.int64())
    ])),
    ('best_buy_orders', pa.list_(order_level_struct)), ('best_sell_orders', pa.list_(order_level_struct)),
    ('extended_analytics', pa.struct([
        ('upper_circuit_limit', pa.float64()), ('lower_circuit_limit', pa.float64()),
        ('fifty_two_week_high', pa.float64()), ('fifty_two_week_low', pa.float64())
    ]))
])

def decode_mode3_binary(packet_bytes):
    """Executes structural binary parsing on raw incoming bytes."""
    if len(packet_bytes) != 379:
        return None
    try:
        subscription_mode = struct.unpack('<B', packet_bytes[0:1])[0]
        exchange_type     = struct.unpack('<B', packet_bytes[1:2])[0]
        token              = packet_bytes[2:27].split(b'\x00')[0].decode('utf-8')
        sequence_number    = struct.unpack('<Q', packet_bytes[27:35])[0]
        exchange_timestamp = struct.unpack('<Q', packet_bytes[35:43])[0]
        last_traded_price    = struct.unpack('<q', packet_bytes[43:51])[0] / 100.0
        last_traded_quantity = struct.unpack('<q', packet_bytes[51:59])[0]
        avg_traded_price     = struct.unpack('<q', packet_bytes[59:67])[0] / 100.0
        volume_traded        = struct.unpack('<q', packet_bytes[67:75])[0]
        total_buy_quantity   = struct.unpack('<d', packet_bytes[75:83])[0]
        total_sell_quantity  = struct.unpack('<d', packet_bytes[83:91])[0]
        open_price  = struct.unpack('<q', packet_bytes[91:99])[0] / 100.0
        high_price  = struct.unpack('<q', packet_bytes[107:115])[0] / 100.0 if struct.unpack('<q', packet_bytes[99:107])[0] == 0 else struct.unpack('<q', packet_bytes[107:115])[0] / 100.0
        low_price   = struct.unpack('<q', packet_bytes[107:115])[0] / 100.0
        close_price = struct.unpack('<q', packet_bytes[115:123])[0] / 100.0
        last_traded_timestamp = struct.unpack('<q', packet_bytes[123:131])[0]
        open_interest         = struct.unpack('<q', packet_bytes[131:139])[0]
        open_interest_change_percent = struct.unpack('<d', packet_bytes[139:147])[0]

        best_five_raw = packet_bytes[147:347]
        buy_levels, sell_levels = [], []
        LEVEL_SIZE = 20
        DOCUMENTATION_FORMAT = '<hqqh'

        for i in range(5):
            offset = i * LEVEL_SIZE
            flag, qty, price_raw, num_orders = struct.unpack(DOCUMENTATION_FORMAT, best_five_raw[offset : offset + LEVEL_SIZE])
            buy_levels.append({"level": i + 1, "quantity": qty, "price": price_raw / 100.0, "num_orders": num_orders, "flag": flag})

            offset = 100 + (i * LEVEL_SIZE)
            flag, qty, price_raw, num_orders = struct.unpack(DOCUMENTATION_FORMAT, best_five_raw[offset : offset + LEVEL_SIZE])
            sell_levels.append({"level": i + 1, "quantity": qty, "price": price_raw / 100.0, "num_orders": num_orders, "flag": flag})

        upper_circuit_limit       = struct.unpack('<q', packet_bytes[347:355])[0] / 100.0
        lower_circuit_limit       = struct.unpack('<q', packet_bytes[355:363])[0] / 100.0
        fifty_two_week_high_price = struct.unpack('<q', packet_bytes[363:371])[0] / 100.0
        fifty_two_week_low_price  = struct.unpack('<q', packet_bytes[371:379])[0] / 100.0

        return {
            "subscription_mode": subscription_mode, "exchange_type": exchange_type, "token": token,
            "sequence_number": sequence_number, "exchange_timestamp": exchange_timestamp,
            "last_traded_price": last_traded_price, "last_traded_quantity": last_traded_quantity,
            "avg_traded_price": avg_traded_price, "open_price": open_price, "high_price": high_price,
            "low_price": low_price, "close_price": close_price, "last_traded_timestamp": last_traded_timestamp,
            "open_interest": open_interest, "oi_change_percent": round(open_interest_change_percent, 4),
            "market_analytics": {
                "total_buy_quantity": total_buy_quantity, "total_sell_quantity": total_sell_quantity, "volume_traded": volume_traded
            },
            "best_buy_orders": buy_levels, "best_sell_orders": sell_levels,
            "extended_analytics": {
                "upper_circuit_limit": upper_circuit_limit, "lower_circuit_limit": lower_circuit_limit,
                "fifty_two_week_high": fifty_two_week_high_price, "fifty_two_week_low": fifty_two_week_low_price
            }
        }
    except Exception:
        return None

def generate_hot_path_features(records):
    """
    HOT PATH ENGINE: Aggregates the last batch of microsecond records into a single
    5-second predictive window feature matrix row and saves it to local disk.
    """
    try:
        if not records:
            return

        latest = records[-1]
        volume_velocity = sum(int(r['last_traded_quantity']) for r in records)

        obi_l1_list = []
        obi_l5_list = []
        spread_l1_list = []

        for r in records:
            bid_q1 = r['best_buy_orders'][0]['quantity']
            ask_q1 = r['best_sell_orders'][0]['quantity']
            denom_l1 = bid_q1 + ask_q1
            obi_l1 = (bid_q1 - ask_q1) / denom_l1 if denom_l1 > 0 else 0.0
            obi_l1_list.append(obi_l1)

            bid_p1 = r['best_buy_orders'][0]['price']
            ask_p1 = r['best_sell_orders'][0]['price']
            spread_l1_list.append(ask_p1 - bid_p1)

            total_bid_q = sum(level['quantity'] for level in r['best_buy_orders'])
            total_ask_q = sum(level['quantity'] for level in r['best_sell_orders'])
            denom_l5 = total_bid_q + total_ask_q
            obi_l5 = (total_bid_q - total_ask_q) / denom_l5 if denom_l5 > 0 else 0.0
            obi_l5_list.append(obi_l5)

        avg_obi_l1 = sum(obi_l1_list) / len(obi_l1_list)
        avg_deep_obi_l5 = sum(obi_l5_list) / len(obi_l5_list)
        max_spread_l1 = max(spread_l1_list)

        current_bid = latest['best_buy_orders'][0]['price']
        current_ask = latest['best_sell_orders'][0]['price']
        current_interval_mid_price = (current_bid + current_ask) / 2.0

        hot_df = pd.DataFrame([{
            'volume_velocity': volume_velocity,
            'avg_obi_l1': avg_obi_l1,
            'avg_deep_obi_l5': avg_deep_obi_l5,
            'max_spread_l1': max_spread_l1,
            'current_interval_mid_price': current_interval_mid_price
        }])

        temp_hot_path = f"{HOT_PATH_FILE}.tmp"
        hot_df.to_csv(temp_hot_path, index=False)
        os.rename(temp_hot_path, HOT_PATH_FILE)

    except Exception as e:
        sys.stderr.write(f"[HOT PATH ERROR] Failed to calculate real-time features: {e}\n")
        sys.stderr.flush()

def s3_direct_upload(key, memory_buffer):
    """Executes file payload transfers directly out of system RAM."""
    try:
        s3_client.put_object(Bucket=S3_TARGET_BUCKET, Key=key, Body=memory_buffer)
    except Exception as e:
        sys.stderr.write(f"[S3 WORKER ERROR] Put object transmission dropped for key {key}: {e}\n")
        sys.stderr.flush()

def purge_sqs_batch(receipt_handles):
    """Purges messages from the SQS FIFO queue in background worker threads."""
    try:
        for i in range(0, len(receipt_handles), 10):
            chunk = receipt_handles[i:i+10]
            entries = [{'Id': str(idx), 'ReceiptHandle': h} for idx, h in enumerate(chunk)]
            sqs_client.delete_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
    except Exception as e:
        sys.stderr.write(f"[SQS PURGE ERROR] Failed to clear messages: {e}\n")
        sys.stderr.flush()

def sync_deferred_processor(raw_messages, receipt_handles):
    """Transforms raw logs into unified Parquet buffers entirely within memory and updates Hot Path."""
    if not raw_messages:
        return

    decoded_records = []
    try:
        for msg_body in raw_messages:
            envelope = json.loads(msg_body)
            raw_hex = envelope["raw_hex"]
            packet_bytes = bytes.fromhex(raw_hex)

            decoded_json = decode_mode3_binary(packet_bytes)
            if decoded_json:
                decoded_records.append(decoded_json)

        if not decoded_records:
            s3_io_executor.submit(purge_sqs_batch, receipt_handles)
            return

        decoded_records.sort(key=lambda x: int(x['sequence_number']))
        seen_sequences = set()
        deduplicated = []
        for r in decoded_records:
            if r['sequence_number'] not in seen_sequences:
                seen_sequences.add(r['sequence_number'])
                deduplicated.append(r)

        if not deduplicated:
            s3_io_executor.submit(purge_sqs_batch, receipt_handles)
            return

        generate_hot_path_features(deduplicated)

        table = pa.Table.from_pylist(deduplicated, schema=raw_audit_schema)

        first_seq = deduplicated[0]['sequence_number']
        last_seq = deduplicated[-1]['sequence_number']
        parquet_file = f"audit_seq_{first_seq}_{last_seq}_{int(time.time_ns() / 1000)}.parquet"

        ts_base = pd.to_datetime(deduplicated[0]['exchange_timestamp'], unit='ms')
        p_year, p_month, p_day = ts_base.strftime('%Y'), ts_base.strftime('%m'), ts_base.strftime('%d')
        parquet_key = f"market_zone_parquet/year={p_year}/month={p_month}/day={p_day}/{parquet_file}"

        memory_buffer = io.BytesIO()
        pq.write_table(table, memory_buffer, compression='SNAPPY')
        memory_buffer.seek(0)
        payload_bytes = memory_buffer.read()

        s3_future = s3_io_executor.submit(s3_direct_upload, parquet_key, payload_bytes)
        s3_future.result()

        s3_io_executor.submit(purge_sqs_batch, receipt_handles)

        sys.stdout.write(f"[PIPELINE SYNC] S3 Cold-Path updated. Local Hot-Path frame refreshed with {len(deduplicated)} rows.\n")
        sys.stdout.flush()

    except Exception as err:
        sys.stderr.write(f"[CRITICAL CONSUMER ERROR] Core extraction step collapsed: {str(err)}\n")
        sys.stderr.flush()

async def poll_sqs_worker(queue: asyncio.Queue):
    """Low-overhead async connection manager fetching messages from SQS."""
    loop = asyncio.get_running_loop()
    while True:
        try:
            response = await loop.run_in_executor(
                None,
                lambda: sqs_client.receive_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MaxNumberOfMessages=SQS_BATCH_SIZE,
                    WaitTimeSeconds=WAIT_TIME_SECONDS,
                    AttributeNames=['All']
                )
            )
            messages = response.get('Messages', [])
            for msg in messages:
                await queue.put(msg)
        except Exception as e:
            sys.stderr.write(f"[WORKER WARNING] Ingestion pipeline drop: {e}\n")
            await asyncio.sleep(1)

async def orchestrate_buffer_manager(queue: asyncio.Queue):
    """Drains memory queues using an optimized adaptive window structure."""
    loop = asyncio.get_running_loop()
    body_buffer = []
    handle_buffer = []
    last_flush_time = time.time()

    while True:
        try:
            while len(body_buffer) < MAX_MESSAGES_PER_FLUSH:
                try:
                    msg = queue.get_nowait()
                    body_buffer.append(msg['Body'])
                    handle_buffer.append(msg['ReceiptHandle'])
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break

            time_since_last_flush = time.time() - last_flush_time

            if len(body_buffer) >= MAX_MESSAGES_PER_FLUSH or (time_since_last_flush >= FLUSH_INTERVAL_SECONDS and body_buffer):
                bodies_copy = list(body_buffer)
                handles_copy = list(handle_buffer)

                body_buffer.clear()
                handle_buffer.clear()
                last_flush_time = time.time()

                loop.run_in_executor(process_executor, sync_deferred_processor, bodies_copy, handles_copy)

            await asyncio.sleep(0.01)

        except Exception as critical_err:
            sys.stderr.write(f"[CORE MONITOR EXCEPTION] Accumulator core failed: {critical_err}\n")
            sys.stderr.flush()

async def main():
    sys.stdout.write("[INIT] Launching Dual-Path Memory-Optimized Parquet Stream Processor...\n")
    sys.stdout.flush()

    shared_queue = asyncio.Queue(maxsize=20000)
    workers = [asyncio.create_task(poll_sqs_worker(shared_queue)) for _ in range(MAX_CONSUMER_CONCURRENCY)]
    manager = asyncio.create_task(orchestrate_buffer_manager(shared_queue))

    await asyncio.gather(*workers, manager)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.stdout.write("\n[SHUTDOWN] Terminating active consumer matrix safely.\n")
        sys.exit(0)
