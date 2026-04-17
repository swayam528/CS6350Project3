"""
NER Spark Structured Streaming
================================
Reads news articles from Kafka topic `news-raw`, extracts named entities
with spaCy, maintains a *running count* across all micro-batches, and at
every trigger interval publishes the complete entity-count table to a second
Kafka topic `ner-counts`.

Pipeline
--------
  Kafka (news-raw)
      → parse JSON  → extract "text" field
      → spaCy NER pandas UDF  → explode to (entity, label) rows
      → groupBy(entity, label).count()   ← stateful, complete mode
      → foreachBatch: print top-20 to console + flush all rows to ner-counts

Message written to ner-counts
------------------------------
    key:   "<entity>||<label>"
    value: {"entity": "...", "label": "PERSON", "count": 42}

Usage
-----
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        ner_spark_streaming.py [bootstrap_servers]

    bootstrap_servers  Default: localhost:9092

Prerequisites
-------------
    pip install pyspark kafka-python spacy pandas
    python -m spacy download en_core_web_sm
"""

import json
import sys
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

INPUT_TOPIC     = "news-raw"
OUTPUT_TOPIC    = "ner-counts"
TRIGGER_SECS    = "30 seconds"
CHECKPOINT_DIR  = "/tmp/checkpoint_ner_counts"

# ── spaCy NER ──────────────────────────────────────────────────────────────────

def _load_nlp(cache: dict = {}) -> object:          # noqa: B006
    """Load spaCy model once per Python worker process (cache via mutable default)."""
    if "model" not in cache:
        import spacy                                # import inside UDF for serialisation
        cache["model"] = spacy.load("en_core_web_sm")
    return cache["model"]


def _extract_raw(text: str) -> list:
    """Return a list of 'entity||LABEL' strings for one piece of text."""
    if not text:
        return []
    nlp  = _load_nlp()
    doc  = nlp(str(text)[:5_000])                  # cap length for performance
    seen = set()
    out  = []
    for ent in doc.ents:
        surface = ent.text.strip()
        if not surface:
            continue
        key = f"{surface}||{ent.label_}"
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


@F.pandas_udf(ArrayType(StringType()))
def extract_entity_strings(texts: pd.Series) -> pd.Series:
    """Vectorised pandas UDF: maps a Series of text strings → Series of entity lists."""
    return texts.apply(_extract_raw)


# ── Kafka output helper (runs on the driver inside foreachBatch) ───────────────

def _make_kafka_producer(bootstrap_servers: str):
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        retries=5,
    )


def make_batch_processor(bootstrap_servers: str, output_topic: str):
    """
    Returns the foreachBatch callback.

    The closure keeps a lazily-initialised KafkaProducer so it is reused
    across trigger intervals without being re-created every time.
    """
    _state: dict = {}          # holds {"producer": KafkaProducer}

    def process_batch(batch_df, epoch_id: int) -> None:
        # Lazy-init Kafka producer (driver-side only)
        if "producer" not in _state:
            _state["producer"] = _make_kafka_producer(bootstrap_servers)
        producer = _state["producer"]

        # Collect the full aggregated state (sorted desc for display)
        rows = batch_df.orderBy(F.col("count").desc()).collect()

        # ── Console output ─────────────────────────────────────────────────
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        print(f"\n{'='*65}")
        print(f"  Trigger {epoch_id}  |  {ts}  |  {len(rows)} unique entities")
        print(f"{'='*65}")
        if rows:
            print(f"  {'LABEL':<14}  {'ENTITY':<42}  COUNT")
            print(f"  {'-'*60}")
            for row in rows[:20]:
                print(f"  {row.label:<14}  {row.entity:<42}  {row['count']}")
        else:
            print("  (no entities yet)")

        # ── Publish ALL running counts to Kafka topic2 ─────────────────────
        for row in rows:
            key   = f"{row.entity}||{row.label}"
            value = json.dumps({
                "entity": row.entity,
                "label":  row.label,
                "count":  int(row["count"]),
            })
            producer.send(output_topic, key=key, value=value)

        producer.flush()
        print(f"\n  Flushed {len(rows)} entity-count messages to '{output_topic}'\n")

    return process_batch


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    bootstrap_servers = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"

    spark = (
        SparkSession.builder
        .appName("NERStructuredStreaming")
        # Reduce shuffle partitions for local/small-scale operation
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ── Article schema ────────────────────────────────────────────────────
    article_schema = StructType([
        StructField("url",          StringType()),
        StructField("source",       StringType()),
        StructField("published_at", StringType()),
        StructField("fetched_at",   StringType()),
        StructField("text",         StringType()),
    ])

    # ── Read from Kafka topic1 ────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        # Use the group-based consumer offset reader instead of the admin API,
        # which avoids the describeTopics timeout seen with some WSL2/Docker setups.
        .option("kafka.group.id", "spark-ner-consumer")
        # Generous timeouts for WSL2 → Docker networking
        .option("kafka.request.timeout.ms", "120000")
        .option("kafka.session.timeout.ms", "60000")
        .option("kafka.heartbeat.interval.ms", "20000")
        .option("kafka.default.api.timeout.ms", "120000")
        .option("kafka.metadata.max.age.ms", "60000")
        .load()
        .selectExpr("CAST(value AS STRING) AS json_str")
    )

    # Parse JSON and extract text field
    news_df = (
        raw_stream
        .select(F.from_json(F.col("json_str"), article_schema).alias("a"))
        .select(F.col("a.text").alias("text"))
        .where(F.col("text").isNotNull() & (F.col("text") != ""))
    )

    # ── Named-entity extraction ───────────────────────────────────────────
    entity_rows = (
        news_df
        .select(
            F.explode(extract_entity_strings(F.col("text"))).alias("entity_str")
        )
        .select(
            F.split(F.col("entity_str"), r"\|\|").getItem(0).alias("entity"),
            F.split(F.col("entity_str"), r"\|\|").getItem(1).alias("label"),
        )
        .where(F.col("entity").isNotNull() & (F.col("entity") != ""))
    )

    # ── Running count (stateful aggregation, complete output mode) ─────────
    entity_counts = entity_rows.groupBy("entity", "label").count()

    # ── Write: console + Kafka topic2 via foreachBatch ─────────────────────
    query = (
        entity_counts
        .writeStream
        .outputMode("complete")
        .foreachBatch(make_batch_processor(bootstrap_servers, OUTPUT_TOPIC))
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_SECS)
        .start()
    )

    print(
        f"[ner-stream] Listening on '{INPUT_TOPIC}' → "
        f"publishing counts to '{OUTPUT_TOPIC}' every {TRIGGER_SECS} …"
    )
    query.awaitTermination()
