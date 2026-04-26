# CS 6350 Big Data Management and Analytics 
## Assignment 3: 
> Kafka Spark Structure Streaming and GraphX/Graphframes

---

## Part 1: Spark Structured Streaming with NewsAPI and Kafka

### Prerequisites

- Docker Desktop (with WSL2 integration enabled)
- Python 3.8+
- Apache Spark 3.5.x with `spark-submit` on your PATH
- A NewsAPI key (free tier at https://newsapi.org)

Install Python dependencies:

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

Set your NewsAPI key as an environment variable (or leave the fallback in `news_producer.py`):

```bash
export API_KEY=your_newsapi_key_here
```

---

### Step 1 — Start Kafka (and optionally the full ELK stack)

From the project directory in WSL:

```bash
# Kafka only (for producer + Spark):
docker compose up -d zookeeper kafka

# Full stack including Elasticsearch, Logstash, Kibana:
docker compose up -d
```

Wait for containers to be healthy:

```bash
docker compose ps
```

---

### Step 2 — Run the NewsAPI Producer

Opens a terminal and runs the producer. It polls NewsAPI every 60 seconds, rotating through keyword queries (technology, politics, economy, sports, etc.) and publishes each unique article as a JSON record to Kafka topic `news-raw`.

```bash
python news_producer.py localhost:9092
```

Expected output:
```
[producer] Connected to Kafka at localhost:9092
[producer] Polling NewsAPI every 60s → topic 'news-raw' …
[producer] 15:06:30 UTC  query=technology  sent= 99  total_seen=  99  sleeping 60s …
```

---

### Step 3 — Run the NER Spark Streaming Job

In a second terminal, submit the PySpark job. It reads from `news-raw`, applies spaCy NER to each article, maintains running entity counts, and publishes results to Kafka topic `ner-counts` every 30 seconds.

```bash
# Clear any stale checkpoint first (required after Kafka topic resets):
rm -rf /tmp/checkpoint_ner_counts

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  ner_spark_streaming.py localhost:9092
```

Expected output every 30 seconds:
```
=================================================================
  Trigger 1  |  15:08:00 UTC  |  142 unique entities
=================================================================
  LABEL           ENTITY                                      COUNT
  ------------------------------------------------------------
  DATE            Wednesday                                   127
  GPE             Iran                                         91
  PERSON          Trump                                        44
  ...
  Flushed 142 entity-count messages to 'ner-counts'
```

---

### Step 4 — Set Up Kibana Dashboard (optional)

Once the Spark job has fired at least one trigger:

```bash
python kibana_setup.py
```

This automatically creates an Elasticsearch index mapping, Kibana index pattern, horizontal bar chart visualization, and dashboard. The script prints the direct dashboard URL on completion.

Open Kibana at http://localhost:5601, navigate to the printed dashboard URL, and set the time picker to **All time** to view all accumulated entity counts.

---

### Step 5 — Generate Snapshot Bar Charts

To generate the four interval bar charts (15 / 30 / 45 / 60 minutes) from captured listener data:

```bash
python generate_charts.py
```

Output PNGs are saved to the `part1results/` directory.

---

### File Reference

| File | Description |
|---|---|
| `news_producer.py` | Polls NewsAPI and publishes articles to Kafka `news-raw` |
| `ner_spark_streaming.py` | PySpark job: NER extraction and running counts → `ner-counts` |
| `kibana_setup.py` | Auto-creates Kibana index pattern, visualization, and dashboard |
| `generate_charts.py` | Generates matplotlib bar chart snapshots from listener data |
| `docker-compose.yml` | ZooKeeper, Kafka, Elasticsearch, Logstash, Kibana |
| `logstash/pipeline/logstash.conf` | Logstash pipeline: `ner-counts` → Elasticsearch |
| `requirements.txt` | Python dependencies |

Part 2 — GraphFrames analysis of the musae-github network
See the dedicated [p2/README.md](p2/README.md) for full details.

Summary of the workflow:

cd p2
spark-submit \
  --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
  part2.py


The single entrypoint downloads the SNAP musae-github archive, builds a
property GraphFrame (37,700 vertices, 578,006 directed edges = 2 × 289,003
undirected edges), and runs all five queries from Section 2.3:

| Query | Output file                          | What it computes                                                |
|-------|--------------------------------------|-----------------------------------------------------------------|
| 2.3a  | p2/output/2_3a_top_outdegree.txt   | Top 5 vertices by outdegree                                     |
| 2.3b  | p2/output/2_3b_top_indegree.txt    | Top 5 vertices by indegree                                      |
| 2.3c  | p2/output/2_3c_top_pagerank.txt    | Top 5 vertices by PageRank (resetProbability=0.15, maxIter=10)|
| 2.3d  | p2/output/2_3d_top_components.txt  | Top 5 connected components by vertex count                      |
| 2.3e  | p2/output/2_3e_top_triangles.txt   | Top 5 vertices by triangle count (ties broken with seeded rand) |

---

Submission contents
Code: all the files listed above.
Part 1 snapshots: snapshot_15min.png, snapshot_30min.png,snapshot_45min.png, snapshot_60min.png.
Part 2 outputs: p2/output/2_3*.txt (five files).
Reports / READMEs: this file plus [p2/README.md](p2/README.md).
