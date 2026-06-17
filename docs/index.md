# Streaming Data

This site provides documentation for this project.
Use the navigation to explore module-specific materials.

## Custom Project

### Dataset

The producer utilizes a local dataset named `sales.csv`. It contains real-time e-commerce transactional records, simulating an active storefront. Each record includes essential transactional attributes such as `order_id`, `timestamp`, `product_id`, `quantity`, `price`, `region_id`, and `discount_code`.

For this module, I modified the original consumer file to include data-contract edge cases, purposefully omitting required validation fields on selected rows. This allows me to test the real-time structural validation and downstream resilience of the streaming pipeline.

### Kafka Messages

Describe the messages sent through Kafka.

The Kafka producer reads rows sequentially from the source CSV file, serializes the data payloads into JSON strings, and streams them asynchronously.

* **Kafka Topic:** The messages are published to a dedicated topic named `streaming-06-scenarios-case-0`.
* **Message Key:** To ensure strict ordering and logical partitioning by geographical market, the `region_id` is applied as the Kafka message key.
* **Message Fields:** The fields were kept the same, to be lightweight to maximize throughput; structural enrichment and derived fields are intentionally deferred to the consumer side of the pipeline.

### Consumer Processing

The consumer is configured to poll the `streaming-06-scenarios-case` topic, reading sequentially from the earliest available offset (`OFFSET_BEGINNING`).

* **Volume:** It is configured to consume up to a maximum threshold of 1,000 messages or halt automatically if a 10-second idle timeout elapses.
* **Logging & Real-Time Statistics:** For every message pulled, the consumer logs the raw payload, validates it against a strict data contract, and calculates continuous metrics. It prints a live stream of rolling analytics to the console, including cumulative subtotal, calculated tax, order totals, and a running tally of key financial metrics (total sales, mean, minimum, and maximum transaction values).
* **Storage & Filtering:** Valid records are enriched using reference lookups (mapping `region_id` to its corresponding tax rate percentage) to compute derived financial fields. The fully processed, structured dictionary is then filtered and committed to two persistent sinks: appended as an audit row in `data/output/consumed_sales.csv` and written as an analytical record into a local **DuckDB** database file (`sales.duckdb`).

### Experiments

To test and optimize the architecture, I implemented two modifications to kafka_consume_case:

* **Phase 4 (Small Technical Change):** I injected a **Continuous Intelligence Data Quality Metric** directly into the core processing loop. Using the pre-tracked `consumed_count` and `skipped_count`, I modified the script to calculate a real-time *Data Contract Failure Rate* percentage. This change outputs live pipeline data-health feedback directly to the console logs both during execution and inside a newly structured final stream summary panel.
* **Phase 5 (Application Change):** I upgraded the consumer architecture to utilize a robust **Dead Letter Queue (DLQ)** pattern for exception isolation. I wrapped the core business enrichment logic (`enrich_message`) in a `try/except` block. If an unforeseen data anomaly occurs—such as a missing key in the reference file throwing a `KeyError`—the consumer intercepts the exception, flags a non-fatal terminal warning, and isolates the broken payload into a standalone file (`data/output/dead_letter_payloads.log`), allowing the main stream to process subsequent records without crashing.

### Results

When executing the pipeline, the producer initialized successfully, recognized the KRaft-managed broker cluster, and began publishing serialized messages. Upon running the consumer terminal command (`uv run python -m streaming.consumer_analytics`), the system verified connectivity and successfully isolated the message partition.

The consumer processed the incoming stream sequentially. As valid records passed through, the console updated dynamically, and a Matplotlib live chart (`sales_chart_case.png`) refreshed in the background to visually chart transaction volume trends. When intentionally corrupted data frames passed through the stream, the system did not collapse; instead, the validation logs immediately caught the error, calculated the climbing contract breach rate, isolated the bad payloads into the dead letter log, and smoothly consumed the remainder of the queue until hitting the 10-second idle timeout.

### Interpretation

The Kafka streaming workflow demonstrated the architectural difference between traditional batch transformations and decouple-based event streaming. Unlike static systems where a single data processing error can halt a nightly ETL pipeline, this streaming architecture proved that data contract enforcement and fault isolation can maintain continuous system uptime.

By observing messages move from the producer through the broker partitions to the consumer, I learned how asynchronous message queuing enables decoupled microservices to process heavy analytical operations independently.

For a business, this continuous intelligence framework provides immediate operational transparency. Rather than waiting for end-of-day reporting, the stream gives stakeholders a real-time window into transaction velocity and revenue metrics. Furthermore, by surfacing instantaneous data quality and failure rates, the stream acts as an early-warning diagnostic tool—immediately alerting data engineers to upstream schema drift or product-catalog mismatches before bad data can pollute production data warehouses.

## How-To Guide

Many instructions are common to all our projects.

See
[⭐ **Workflow: Apply Example**](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to get these projects running on your machine.

## Project Documentation Pages (docs/)

- **Home** - this documentation landing page
- **Project Instructions** - instructions specific to this module
- **Your Files** - how to copy from examples and make them yours
- **Glossary** - project terms and concepts
- **API** - autogenerated look at the code interface
