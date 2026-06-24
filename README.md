# High-Frequency Financial Streaming Data Pipeline (AWS Infrastructure & Angel One SmartAPI 2.0)
Production-grade real-time high-frequency stock market data ingestion pipeline utilizing AngelOne SmartAPI WebSocket 2.0, AWS SQS FIFO, Lambda, and S3.

## Overview
This production-grade data platform implements an asynchronous, real-time, high-frequency stock market data streaming architecture designed to ingest, decode, buffer, transform, and permanently store tick-by-tick financial market feeds. The pipeline establishes a persistent, stateful connection to the National Stock Exchange (NSE) and Bombay Stock Exchange (BSE) through the institutional-grade Angel One SmartAPI WebSocket 2.0 interface. 

By eliminating legacy batch-oriented scheduling and processing systems, this architecture bridges the operational gap between millisecond-level data creation at the exchange level and centralized analytics engines. It handles high-velocity streaming data by capturing tick packets, processing binary payload formats, and executing structural validations within highly isolated data perimeters before writing partitioned, columnar datasets directly into an enterprise cloud data lake.

---

## Key Takeaways
* **Asynchronous Multi-Threaded Ingestion Engine:** Engineered a high-throughput Python ingestion client running on compute-optimized cloud infrastructure, utilizing advanced non-blocking network sockets to absorb continuous streaming ticks without data frame degradation.
* **Low-Latency Structural Buffering Architectures:** Developed an event-driven, decoupled integration layer utilizing FIFO (First-In, First-Out) queuing mechanisms to prevent down-stream processing bottlenecks, ensuring strict message ordering and preventing data loss during burst traffic conditions.
* **Columnar Storage Optimization & Compaction Topologies:** Implemented zero-copy data processing frameworks using advanced in-memory columnar layouts to transform raw, unpacked tick streams into compressed, highly optimized storage configurations before execution writes.
* **Hive-Style Data Lake Partitioning & Time-Zone Alignments:** Architected a multi-tier cloud storage hierarchy utilizing historical data indexing patterns, explicitly sorted by instrument identification and date tracking, while handling global time shifting back to true Indian Standard Time (IST).
* **Serverless Schema Synchronization & Analytical Access Layers:** Configured automated metadata discovery crawlers to dynamically update unified data catalogs, providing an optimized, serverless analytical access layer capable of running ad-hoc, low-latency business logic directly over cold storage objects.

---

## Objectives
* **Build a Scalable, Fault-Tolerant Data Pipeline Infrastructure:** Design and implement a top-down dependent streaming architecture on AWS capable of scaling horizontally to absorb massive volumes of simultaneous tick updates across thousands of financial instruments.
* **Bypass Batch Limitations via Real-Time WebSockets:** Implement a persistent network pipeline that eliminates traditional API polling, shifting system performance limits to capture, unpack, and make new ticks visible within seconds of their publication on the exchange floor.
* **Enforce Strict Relational Integrity & Zero-Data-Loss Guarantees:** Establish definitive data handling boundaries that isolate raw ingest parameters, strip processing artifacts, and leverage algorithmic deduplication to maintain full mathematical and structural validation checks.
* **Optimize Cloud Storage Overheads & Computational Economics:** Minimize corporate cloud spend by implementing cost-optimized machine lifecycles and utilizing high-ratio data compression formats, allowing fast SQL execution engines to process petabyte-scale financial queries with minimal cloud overhead.

---

## Use Case: High-Frequency Algorithmic Execution & Market Microstructure Analytics

In the institutional financial landscape, proprietary trading desks, hedge funds, and quantitative asset managers face the relentless challenge of managing structural latency arbitrage. Financial markets operate on millisecond-level cycles where order book fluctuations, price discoveries, and liquidation imbalances occur across thousands of instruments concurrently. Traditional data architectures rely on periodic API polling or batch-oriented ETL schedules (e.g., pulling data via Apache Spark every 5 to 15 minutes). This creates a critical operational blind spot: by the time an analytical model detects a volume surge, cross-market spread divergence, or sudden liquidity wipeout, the market window has closed, rendering downstream trading signals obsolete.

To capture alpha and execute real-time risk calculations, institutions must transition from reactive polling to proactive, stateful, event-driven ingestion frameworks. Implementing a persistent, low-latency stream processing pipeline directly from exchange-level web sockets provides immediate visibility into market microstructure shifts. 

By leveraging cutting-edge cloud infrastructure and optimized memory layouts to ingest, buffer, and permanently commit tick-by-tick financial market feeds, quantitative systems can run live mathematical calculations, backtest high-frequency algorithms, and track algorithmic execution fills with zero data frame degradation.

### Stream Processing Advantages for Financial Markets
* **Sub-Second Tick Capitalization:** Captures full market-depth and last traded price (LTP) modifications the exact millisecond they are processed on the exchange floor, eliminating network polling overhead.
* **Deterministic Network Sockets:** Maintains stateful TCP handshakes through institutional endpoints, eliminating connection teardown and setup latencies during peak volume periods.
* **Granular Time-Series Integrity:** Eliminates the aggregation artifacts common in batch-based windows, ensuring that highly volatile macro-bursts and fleeting price spikes are explicitly preserved for historical quantitative analysis.

---

## Contents
1. [The Dataset & Financial Instruments Profiles](#the-dataset--financial-instruments-profiles)
2. [Master Project Architecture Blueprint](#master-project-architecture-blueprint)
3. [End-to-End High-Frequency Streaming Pipeline Lifecycle](#end-to-end-high-frequency-streaming-pipeline-lifecycle)
4. [Live System Execution & Demo Highlights](#live-system-execution--demo-highlights)
5. [Production Technical Challenges & Core Engineering Solutions](#production-technical-challenges--core-engineering-solutions)
6. [Architectural Conclusion](#architectural-conclusion)
7. [Enterprise Future Enhancements](#enterprise-future-enhancements)
8. [Professional Registry & Contact Information](#professional-registry--contact-information)
9. [Technical Appendix & Reference Materials](#technical-appendix--reference-materials)

---

## Master Project Architecture Blueprint

The platform leverages specialized cloud engineering primitives and dedicated data streaming patterns to create a highly performant, decoupled, low-latency transaction processing environment. By matching physical infrastructure choices to specific analytical workloads, the pipeline eliminates structural bottleneck points across multiple operational boundaries.

The data processing pipeline is organized into four main operational phases:
* **Connect:** Establishing a stateful network perimeter that initializes cryptographic authentication handshakes and continuously extracts raw, multi-instrument binary frames directly from institutional trading desks.
* **Processing:** Decoupling multi-threaded consumer workloads, unpacking streaming data payloads in-memory, performing schema mappings, and applying micro-batch windowing boundaries.
* **Storage:** Managing a partitioned, transactional cloud data lake that organizes incoming records into highly optimized, compressed, self-describing columnar structural formats.
* **Visualization:** Providing interactive dashboards and low-latency, ad-hoc serverless query interfaces to interpret market microstructure fluctuations and historical analytics.

---

## High-Frequency Streaming Data Processing Pipeline

The pipeline lifecycle begins when an edge ingestion client runs an active network socket connection within a security-hardened compute perimeter, establishing a persistent TCP connection to the Angel One SmartAPI 2.0 WebSocket server. The exchange pushes real-time, tick-by-tick binary payloads across this stream. The ingestion engine absorbs these frames, unpacks them in-memory, and streams the validated JSON messages into an Amazon SQS FIFO queue to guarantee message ordering.

A multi-threaded, thread-isolated consumer process reads structured chunks from the SQS queue, converts the records into PyArrow tables, and writes highly optimized Snappy-compressed Parquet files directly into Amazon S3. Storage objects are laid out using a strict Hive-style directory partitioning topology based on instrument token identifications and calendar dates. 

AWS Glue crawlers periodically inspect these cold-storage directories to discover partition structures and update a centralized metadata catalog. This allows Amazon Athena to run serverless, ad-hoc SQL queries using time-zone alignments, which feed real-time Streamlit executive dashboard tracking layers.

---

## Detailed Pipeline Process Flow

### 1. Connect

* **Persistent Network Sockets:** The ingestion client maintains stateful TCP handshakes through institutional endpoints, bypassing the connection setup overhead characteristic of REST APIs.
* **Secure Parameter Orchestration:** Authentication credentials, structural API keys, and session tokens are retrieved securely from AWS Systems Manager (SSM) Parameter Store namespaces at runtime initialization, preventing sensitive security data from leaking into configuration environments.
* **Asynchronous Frame Interception:** The network thread absorbs uninterrupted binary streams during live exchange trading hours, ensuring the socket buffer never overflows.

### 2. Processing

The system splits operational processing across isolated logical segments to prevent down-stream ingestion stalls:
* **Binary Unpacking & Token Mapping:** Raw bytes are parsed and mapped back to strict data schemas.
* **High-Throughput SQS FIFO Buffering:** Unpacked JSON frames are dispatched to an Amazon SQS FIFO queue. SQS handles message buffering and eliminates down-stream message duplication through strict deduplication hash checks.
* **Thread-Isolated Consumer Processing:** A multi-threaded, zero-copy producer-consumer wrapper handles SQS queue reading and PyArrow memory operations inside separate, dedicated software perimeters.
* **Parquet File Compaction:** PyArrow transforms transient arrays into highly structured columnar chunks, utilizing Snappy compression algorithms to build production-grade storage objects.

### 3. Storage

* **Hive-Style S3 Partition Topologies:** Optimized bucket structures group financial data into highly discoverable directories organized by target analytical attributes:
  `s3://angelone-streaming-data-lake/market_zone_parquet/instrument_token=XXXXX/year=YYYY/month=MM/day=DD/`
* **Automated Data Lifecycle Policies:** Storage perimeters enforce automated rules to transition aged analytical files to cold storage tiers, optimizing long-term cloud storage economics.

### 4. Visualization

* **AWS Glue Catalog Metadata Synchronization:** Automated data crawlers inspect active S3 storage namespaces to map newly written files, dynamically discovering partition schemas and publishing table definitions to the central data catalog.
* **Serverless Presto Analytics via Amazon Athena:** Users execute low-latency, ad-hoc SQL queries directly across the partitioned Parquet files using Amazon Athena's serverless engine. SQL views handle timezone-shifting calculations to convert UTC timestamps back to true Indian Standard Time (IST).
* **Streamlit Dynamic Executive Dashboards:** A localized, high-performance web tracking application connects directly to Athena, pulling metrics, mid-price trajectories, and historical parameters without data frame degradation.

---


## 🪧 Live System Execution & Demo Highlights

### 1. Project Setup & Dependencies Blueprint

To establish a highly predictable, repeatable computational environment for the ingestion and consumption services, the pipeline avoids system-wide Python configurations. Instead, it utilizes isolated virtual environments and precise dependency pinning. This prevents package version drift and ensures complete environment isolation across development and deployment scopes.

#### a. Python Virtual Environment & System Package Ingestion Setup

Follow these exact steps to update your base system utilities, initialize a dedicated virtual environment sandbox, and pull down the foundational libraries needed to maintain network stream states:

```bash
# 1. Update system package repository definition listings to avoid broken command configurations
sudo apt update && sudo apt upgrade -y

# 2. Install core Python development tools, virtual environment managers, and build utilities
sudo apt install python3-pip python3-venv build-essential -y

# 3. Create a clean, isolated Python virtual environment inside your project root
python3 -m venv venv

# 4. Activate the isolated virtual environment to hook your terminal to local binary execution paths
source venv/bin/activate

# 5. Upgrade the baseline package manager (pip) to the absolute latest distribution standard
pip install --upgrade pip

# 6. Install the high-performance core libraries required for streaming ingestion, memory tracking, and storage compaction
pip install websocket-client pyarrow pandas smartapi-python boto3 streamlit
```
---

### 2. Finalized Project Directory Architecture

To ensure clean isolation of operational concerns and simple long-term troubleshooting, the repository layout splits ingestion mechanics, queue processing wrappers, configuration values, and frontend visualization code into dedicated modular perimeters.

```text
high-frequency-financial-pipeline/
├── config/
│   └── credentials.txt           # Secure session variables and operational API parameters
├── dashboards/
│   └── streamlit_app.py          # Streamlit real-time canvas tracking and UI plotting layout
├── src/
│   ├── ingestion/
│   │   └── websocket_client.py   # Persistent connection thread mapping to Angel One API
│   └── consumer/
│       └── sqs_to_parquet.py     # Multi-threaded SQS buffer worker and PyArrow compactor
├── venv/                         # Isolated Python development virtual environment files
├── .gitignore                    # Prevents credentials and local binary cache leaks to Git
└── README.md                     # Core production platform technical architecture layout
```
---

### 3. Identity and Access Management (IAM) & Parameter Orchestration Setup

To secure data assets and orchestrate network access tokens without exposing system environmental values, the data infrastructure isolates computational permissions using a strict least-privilege model via AWS IAM and stores critical variables inside AWS Systems Manager (SSM) Parameter Store.

#### a. Setup AWS IAM Security Boundaries
The pipeline utilizes an administrative automation structure where fine-grained access policies are bound directly to specialized user groups. This allows granular resource control and eliminates unmanaged credential proliferation:

* **IAM User Group Configuration:** Established a high-security user group populated with administrative access policies. Users under this group inherit precise structural entitlements.
* **IAM Security Profiles:** A dedicated development user operates within this group perimeter to manage programmatic operations over cloud resources like Amazon SQS, Amazon S3, and AWS Glue.

#### b. Secure Session Variables & SSM Parameter Configuration
Instead of hardcoding the Angel One SmartAPI 2.0 institutional endpoints, application keys, or session configurations into local code matrices, all structural keys are securely maintained within the AWS Systems Manager (SSM) Parameter Store. 

The ingestion client requests these runtime tokens securely via localized `boto3` parameters at startup:

```text
/angelone/api_key          -> Your registered SmartAPI application access token
/angelone/client_code      -> Institutional trading account identifier
/angelone/password         -> Encrypted security access password
/angelone/totp_secret      -> Time-Based One-Time Password seed key for programmatic 2FA bypass
```
---

#### d. Setup of GitHub Actions CI/CD Workflow

To enforce system reliability and prevent runtime syntax errors from corrupting the live trading tick stream, the repository leverages an automated GitHub Actions CI/CD workflow. This automation replaces manual execution tests with structured verification rings.

The workflow architecture enforces the following rigorous validations:
* **Linting & Code Formatting Hygiene:** Automatically scans Python modules via `flake8` to detect unreferenced variables, missing imports, or non-compliant formatting before any code merge.
* **Structural Dependency Auditing:** Validates that all streaming, processing, and analytical core libraries (`pyarrow`, `pandas`, `websocket-client`, `boto3`) compile smoothly under clean virtual environment layers.
* **Asynchronous Integration Testing:** Executes verification scripts to confirm that the asynchronous WebSocket producer loops and SQS FIFO queue client handlers interact with standard AWS software definitions flawlessly.

##### CI/CD Engineering Considerations:
* **Decoupled Execution Windows:** The pipeline is strategically configured to trigger code quality checks whenever code is pushed to the `dev` branch, or via Pull Requests target-merged into the `main` branch. This shields the master branch from volatile modifications.
* **Clipped Pipeline Caching:** To minimize continuous integration run durations, standard action layers cache Pip download wheels across runners, saving overhead time during routine verification cycles.
* **Failure Alerts and Status Logging:** If an validation step fails, standard output execution logs are retained for debugging, and automated pipeline notifications trigger immediate awareness across development loops.

---

### 4. Overall key components, main cdk stack and construct creation process

#### a. Overall architecture diagram
Below is the architecture diagram with highlighted important steps on overall pipeline

#### b. Payload

The high-frequency financial data contract undergoes immediate memory serialization, transforming asynchronous streaming stock ticks into clean, structured records ready for immediate processing:

![payload overview](./images/project_payload_overview.png)

Initial Live Stream Structure (Raw JSON Tick Packet from WebSocket Client):

```json
{
  "tick_sequence": 14920423,
  "instrument_token": "NSE_EQ|25",
  "trading_symbol": "SBIN-EQ",
  "exchange_timestamp": "2026-06-23 10:51:56",
  "last_traded_price": 842.15,
  "volume_traded": 14205,
  "bid_price": 842.10,
  "ask_price": 842.20
}
```

#### c. Authentication and Ingestion Security Considerations

Securing high-frequency transactional data streams requires an isolated authentication plane that protects downstream Kinesis buffers without introducing network handshake overhead. Three distinct options were evaluated for this project stack:

* **Lambda Authorizer (Selected Implementation)**
    * **Justification:** Chosen for its low latency and absolute architectural flexibility. It allows programmatic validation of dynamic credentials passed by automated ingestion engines or third-party webhooks without requiring heavy enterprise overhead.
    * **Process Flow:**
        1.  **Request Capture:** The Python ingestion script or streaming client initiates an HTTPS connection, delivering an authorization token inside the request metadata/headers.
        2.  **Gateway Interception:** AWS API Gateway captures the inbound traffic and forwards the authorization contextual parameters to the decoupled custom Lambda Authorizer.
        3.  **Dynamic Verification:** The Lambda Authorizer runs a sub-millisecond cryptographic evaluation against token rules (cross-checking variables decoupled via AWS Secrets Manager).
        4.  **Policy Generation:** Upon validation, the Authorizer programmatically builds and signs an ephemeral, least-privilege IAM Policy string defining explicit access allowances or denials.
        5.  **Downstream Execution:** API Gateway caches the response context for the policy lifetime and drops valid frames directly into the Kinesis Data Stream proxy handler.

* **Amazon Cognito User Pools (Evaluated & Omitted)**
    * **Justification:** Cognito is designed for user-facing applications (handling registration, multi-factor login screens, and session tokens). For an automated, machine-to-machine high-throughput streaming node, invoking an external Cognito user pool infrastructure adds unnecessary authentication handshakes, slowing down real-time ingestion rates.

* **Static API Keys (Evaluated & Omitted)**
    * **Justification:** While native API Keys provide low latency, they utilize long-lived static values passed within `x-api-key` headers. In a financial ingestion pipeline, relying on unexpiring static tokens increases data leakage vectors, lacking the granular security controls required for enterprise data governance.

---

#### d. Design considerations

* **Network Backpressure & Event Loop Resource Boundaries**
    * Unlike batch or Lambda-on-demand setups, this high-frequency pipeline runs as a persistent, stateful background daemon (`cloud_ingest.py`) inside a `tmux` session on a dedicated compute node (EC2). Managing resources means optimizing the asynchronous Python `asyncio` loop and avoiding network backpressure.
    * [cite_start]Under heavy market volatility (e.g., market open at 9:15 AM), a multiplexed subscription tracking 100+ liquid tokens can easily experience bursts of **300 to 500 binary packets per second** over the AngelOne SmartAPI WebSocket 2.0 pipeline[cite: 720, 721]. [cite_start]If the synchronous processing or logging inside the event loop takes more than a few milliseconds, the underlying network receive buffer inflates rapidly[cite: 722, 724]. [cite_start]This creates latency drift and risks forcing the remote exchange gateway to drop the connection for failing to maintain pace[cite: 724].
    * To validate and monitor the system footprint, system metrics are tracked across the long-running daemon execution scopes:
        * [cite_start]**Handshake and Keep-Alives:** Connection lifecycles are completely decoupled from library-level ping handlers by disabling default timeouts (`ping_interval=None`, `ping_timeout=None`)[cite: 149, 156]. [cite_start]An explicit application-level coroutine manages the 20–30 second token heartbeat rules specified by the exchange specification[cite: 592, 594, 367].
        * [cite_start]**Exponential Connection Backoff:** Network exceptions are captured cleanly by a state-machine wrapper that applies progressive cool-down windows, preventing thread starvation or remote IP banning during transit failures[cite: 150, 157].

* **Architecture Bottleneck Mitigation Strategies**
    The fundamental architectural vulnerability of this system sits at the interface between raw stream ingestion and downstream storage commits. To decouple the lightning-fast inbound I/O layer from heavy file-compaction math, a strict **Producer-Consumer Design Pattern** has been implemented across the following grid[cite: 725, 726]:

    * **Producer: Asynchronous Ingestion to SQS FIFO**
        * [cite_start]The `cloud_ingest.py` worker focuses solely on transport preservation: it reads binary byte data arrays, extracts the token index context, and instantly queues them into Amazon SQS FIFO (`market-ticks-queue.fifo`)[cite: 152, 235, 298]. [cite_start]It executes no mathematical aggregations and handles zero file writing[cite: 728].
        * [cite_start]**SQS Capacity Optimization:** By utilizing SQS FIFO's High Throughput Mode, the pipeline scales safely to a ceiling of 3,000 messages per second, comfortably ahead of the nominal volatility workload of 10 to 50 ticks per second[cite: 747, 748, 750].
        * [cite_start]**Deduplication Structuring:** Messages are assigned a highly specific structural composite key using an arrival timestamp and a unique slice of the raw payload hex data (`MessageDeduplicationId`)[cite: 366]. [cite_start]This prevents duplicate processing of identical ticks across connection-recovery boundaries[cite: 366].

    * **Consumer: Deferred Multi-Threaded Processing & S3 Compaction**
        * [cite_start]The decoupled consumer engine (`cloud_consumer.py`) acts as the workhorse[cite: 194, 298]. [cite_start]It retrieves payloads in batches from SQS and stages them inside an in-memory accumulation array[cite: 332].
        * [cite_start]**Thread/Process Pool Segregation:** If S3 writes are executed entirely in-line inside a restricted worker array, thread starvation can occur[cite: 340]. [cite_start]The consumer uses an adaptive window structure (`orchestrate_buffer_manager`) to isolate task blocks[cite: 461]:
            * [cite_start]An internal `ProcessPoolExecutor` processes and unpacks raw binary data arrays (Little-Endian formatting) into data-science schemas without blocking the event loop[cite: 505, 340, 341].
            * [cite_start]A high-performance concurrent `ThreadPoolExecutor` array handles the triple-destination concurrent flushes to AWS S3 (Binary snapshots, JSON structures, and Snappy-compressed Apache Parquet logs)[cite: 341, 342, 347].

    * **Frontend Rendering and Memory Management (Streamlit)**
        * [cite_start]To maintain responsive dashboard refreshes every 2 to 5 seconds without introducing layout instability, the UI app (`app_streaming.py`) is entirely decoupled from heavy query layers[cite: 450]. [cite_start]It points directly to a high-speed local memory cache or hot file path updated by the consumer daemon[cite: 451].
        * **Canvas State Purging:** Rather than instantiating cumulative data elements inside nested variable loops, the engine assigns an immutable master container footprint (`main_canvas = st.empty()`) outside the cycling window. On every loop iteration, the context is cleared and redrawn in-place via `main_canvas.container()`, eliminating stale interface artifacts and preventing long-running memory leaks.

---

