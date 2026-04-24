# CS 6350 Big Data Management and Analytics
## Assignment 3 — Kafka + Spark Structured Streaming + GraphFrames

This repo contains both parts of Assignment 3:

| Part | Folder / Entry point                         | Topic                                                                  |
|------|----------------------------------------------|------------------------------------------------------------------------|
| 1    | repo root (`news_producer.py`, `ner_spark_streaming.py`, `docker-compose.yml`, `logstash/`, `kibana_setup.py`) | NewsAPI → Kafka → PySpark NER → Kafka → Logstash → Elasticsearch → Kibana |
| 2    | [`p2/`](p2/)                                 | musae-github (SNAP) social-network analysis with Spark GraphFrames     |

Each part is fully self-contained — no hard-coded local paths in the
analysis code, dataset is downloaded automatically (Part 2), and Part 1's
infrastructure runs from a single `docker compose up`.

---

# Part 1 — Real-time NER on news headlines

End-to-end pipeline:

```
NewsAPI ──► news_producer.py ──► Kafka topic: news-raw
                                         │
                                         ▼
                              ner_spark_streaming.py
                              (PySpark Structured Streaming
                               + spaCy NER + running count)
                                         │
                                         ▼
                              Kafka topic: ner-counts
                                         │
                                         ▼
                              Logstash → Elasticsearch
                                         │
                                         ▼
                              Kibana (top-10 bar chart)
```

## Prerequisites

- **Docker Desktop** (for Kafka, Zookeeper, Elasticsearch, Logstash, Kibana)
- **WSL2 / Linux** with **Java 11** and **Python 3.10+**
- **A NewsAPI key** — free at <https://newsapi.org/>

## 1.1 Install Python dependencies

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

[requirements.txt](requirements.txt) pins `pyspark>=3.5.0`, `kafka-python`,
`spacy`, `pandas`, `requests`, `newsapi-python`, `python-dotenv`.

## 1.2 Bring the infrastructure up

```bash
docker compose up -d
```

This starts five containers on a `pipeline` network:

| Service       | Host port | Purpose                                       |
|---------------|-----------|-----------------------------------------------|
| zookeeper     | —         | Coordinates the Kafka broker                  |
| kafka         | 9092      | Brokers `news-raw` and `ner-counts` topics    |
| elasticsearch | 9200      | Stores `ner-entity-counts` index              |
| logstash      | —         | Bridges Kafka `ner-counts` → ES               |
| kibana        | 5601      | UI / dashboard                                |

Wait until `docker compose ps` shows everything as `(healthy)` (~60 s).

> **Note on `KAFKA_WSL_HOST`** — only relevant if you run the Spark job
> from inside WSL while Docker is running on the Windows side. The default
> in [docker-compose.yml](docker-compose.yml) is `172.24.176.1`; override
> with your own WSL gateway IP via `export KAFKA_WSL_HOST=<IP>`. If you
> run everything inside WSL (recommended), `localhost:9092` works as-is
> and you can ignore this variable.

## 1.3 Set your NewsAPI key

```bash
export API_KEY="<your_newsapi_key>"
```

(There is a fallback key compiled into [news_producer.py:42](news_producer.py#L42)
so the script also runs without `API_KEY`, but please use your own.)

## 1.4 Start the producer (terminal #1)

```bash
python news_producer.py
# or, with a non-default broker:
python news_producer.py localhost:9092
```

The producer rotates through 15 search queries (technology, politics,
economy, …), polls NewsAPI every 60 s, dedups by article URL, and pushes
each new article to Kafka topic `news-raw` as JSON.

## 1.5 Start the Spark NER job (terminal #2)

```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    ner_spark_streaming.py
```

It reads `news-raw`, runs spaCy NER on each article's text, maintains a
**running count per (entity, label)** across micro-batches, and at every
30-second trigger:

- prints the top-20 entities to the console, and
- publishes the *complete* current count table to Kafka topic `ner-counts`.

## 1.6 Wire up Kibana (terminal #3, run once)

```bash
python kibana_setup.py
```

This idempotently creates:

1. The Elasticsearch mapping for `ner-entity-counts`.
2. A Kibana index pattern.
3. The horizontal bar visualisation **"Top 10 Named Entities by Count"**.
4. A dashboard embedding the visualisation.

Open <http://localhost:5601> → Dashboards → **NER Entity Counts Dashboard**.
Enable auto-refresh (e.g. every 30 s) to watch the bars update live.

## 1.7 Snapshots

Four PNG snapshots taken from a real run are committed at the repo root:

- [snapshot_15min.png](snapshot_15min.png)
- [snapshot_30min.png](snapshot_30min.png)
- [snapshot_45min.png](snapshot_45min.png)
- [snapshot_60min.png](snapshot_60min.png)

They were generated from real Spark listener output by
[generate_charts.py](generate_charts.py) (snapshot data is embedded in that
script). To regenerate:

```bash
python generate_charts.py
```

## Part 1 — file map

| File                              | Role                                                           |
|-----------------------------------|----------------------------------------------------------------|
| [news_producer.py](news_producer.py)               | NewsAPI → Kafka `news-raw` producer                            |
| [ner_spark_streaming.py](ner_spark_streaming.py)   | PySpark structured streaming NER, writes to Kafka `ner-counts` |
| [docker-compose.yml](docker-compose.yml)           | Kafka, Zookeeper, ES, Logstash, Kibana                         |
| [logstash/pipeline/logstash.conf](logstash/pipeline/logstash.conf) | Kafka → ES upsert pipeline                       |
| [logstash/config/logstash.yml](logstash/config/logstash.yml)       | Logstash node config                             |
| [kibana_setup.py](kibana_setup.py)                 | One-shot Kibana index pattern + viz + dashboard creator        |
| [generate_charts.py](generate_charts.py)           | Regenerates the four snapshot PNGs from captured run data      |
| [requirements.txt](requirements.txt)               | Python dependencies                                            |
| [structured_kafka_wordcount.py](structured_kafka_wordcount.py) | Unmodified Apache reference example, included for reference |


Note: Future works may include improving word capture and tuning spaCy output/filtering (ex: re-evaluate "date" importance/relevance and remove or add more context)
      We also may add more context in general but need an updated infrastructure to handle and dislay it

---

# Part 2 — GraphFrames analysis of the musae-github network

See the dedicated [p2/README.md](p2/README.md) for full details.

Summary of the workflow:

```bash
cd p2
spark-submit \
  --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
  part2.py
```

The single entrypoint downloads the SNAP musae-github archive, builds a
property GraphFrame (37,700 vertices, 578,006 directed edges = 2 × 289,003
undirected edges), and runs all five queries from Section 2.3:

| Query | Output file                          | What it computes                                                |
|-------|--------------------------------------|-----------------------------------------------------------------|
| 2.3a  | `p2/output/2_3a_top_outdegree.txt`   | Top 5 vertices by outdegree                                     |
| 2.3b  | `p2/output/2_3b_top_indegree.txt`    | Top 5 vertices by indegree                                      |
| 2.3c  | `p2/output/2_3c_top_pagerank.txt`    | Top 5 vertices by PageRank (`resetProbability=0.15, maxIter=10`)|
| 2.3d  | `p2/output/2_3d_top_components.txt`  | Top 5 connected components by vertex count                      |
| 2.3e  | `p2/output/2_3e_top_triangles.txt`   | Top 5 vertices by triangle count (ties broken with seeded rand) |

---

# Submission contents

- **Code:** all the files listed above.
- **Part 1 snapshots:** `snapshot_15min.png`, `snapshot_30min.png`,
  `snapshot_45min.png`, `snapshot_60min.png`.
- **Part 2 outputs:** `p2/output/2_3*.txt` (five files).
- **Reports / READMEs:** this file plus [p2/README.md](p2/README.md).
