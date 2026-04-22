"""
CS 6350 Big Data - Assignment 3, Part 2
Analyzing Social Networks using GraphFrames

Dataset: musae-github from SNAP
  https://snap.stanford.edu/data/github-social.html
  Archive: https://snap.stanford.edu/data/git_web_ml.zip

The script is fully self-contained: it downloads and extracts the dataset
at runtime so no local data files are required. Intended to be run via
`spark-submit` inside WSL (or any Spark 3.5 environment).

Run example (from p2/):
    spark-submit \
        --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
        part2.py

The GraphFrames Python module also needs to be on PYTHONPATH, which happens
automatically when the package is resolved via --packages.

Optional environment variables:
    P2_WORK_DIR     directory used to cache the downloaded dataset
                    (default: a fresh temp directory)
    P2_OUTPUT_DIR   directory where query outputs are written
                    (default: ./output relative to cwd)
"""

from __future__ import annotations

import os
import sys
import tempfile
import urllib.request
import zipfile
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# GraphFrames is pulled in via spark-submit's --packages flag. Importing it at
# module load time fails fast if the package wasn't supplied.
from graphframes import GraphFrame


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATASET_URL = "https://snap.stanford.edu/data/git_web_ml.zip"
ARCHIVE_ROOT = "git_web_ml"            # folder name inside the zip
EDGES_FILENAME = "musae_git_edges.csv"   # columns: id_1, id_2 (undirected)
TARGETS_FILENAME = "musae_git_target.csv"  # columns: id, name, ml_target


# ---------------------------------------------------------------------------
# 2.1 Loading Data
# ---------------------------------------------------------------------------

def download_dataset(work_dir: str) -> Tuple[str, str]:
    """Download and extract the SNAP musae-github archive.

    Returns a (edges_csv_path, targets_csv_path) tuple on the local FS.
    The archive is cached in `work_dir` so reruns skip the download.
    """
    os.makedirs(work_dir, exist_ok=True)
    zip_path = os.path.join(work_dir, "git_web_ml.zip")

    if not os.path.exists(zip_path):
        print(f"[2.1] Downloading dataset: {DATASET_URL}")
        urllib.request.urlretrieve(DATASET_URL, zip_path)
    else:
        print(f"[2.1] Using cached archive: {zip_path}")

    extract_dir = os.path.join(work_dir, ARCHIVE_ROOT)
    if not os.path.isdir(extract_dir):
        print(f"[2.1] Extracting archive to: {work_dir}")
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(work_dir)

    edges_path = os.path.join(extract_dir, EDGES_FILENAME)
    targets_path = os.path.join(extract_dir, TARGETS_FILENAME)
    for p in (edges_path, targets_path):
        if not os.path.exists(p):
            raise FileNotFoundError(f"Expected dataset file missing: {p}")
    return edges_path, targets_path


def parse_raw(
    spark: SparkSession, edges_path: str, targets_path: str
) -> Tuple[DataFrame, DataFrame]:
    """Parse the two SNAP CSVs.

    - `musae_git_target.csv` -> vertex DataFrame with columns (id, name, ml_target)
    - `musae_git_edges.csv`  -> raw UNDIRECTED edge DataFrame (id_1, id_2)
    """
    def _as_uri(p: str) -> str:
        # Spark's CSV reader wants a URI on some platforms; file:// works for
        # local paths on WSL/Linux. Convert Windows-style backslashes just in
        # case the script is exercised outside WSL.
        return "file://" + p.replace(os.sep, "/")

    raw_vertices = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(_as_uri(targets_path))
    )
    raw_edges = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(_as_uri(edges_path))
    )
    return raw_vertices, raw_edges


def undirected_to_directed(raw_edges: DataFrame) -> DataFrame:
    """Expand each undirected edge (a, b) into two directed edges a->b and b->a.

    The musae-github edges are mutual follower relationships (undirected), so
    per the assignment spec we materialise both directions. Duplicates that
    would arise from the file already containing a symmetric entry are
    deduplicated defensively via distinct().
    """
    forward = raw_edges.select(
        F.col("id_1").alias("src"),
        F.col("id_2").alias("dst"),
    )
    backward = raw_edges.select(
        F.col("id_2").alias("src"),
        F.col("id_1").alias("dst"),
    )
    return forward.unionByName(backward).distinct()


# ---------------------------------------------------------------------------
# 2.2 Create Graphs
# ---------------------------------------------------------------------------

def build_graph(raw_vertices: DataFrame, directed_edges: DataFrame) -> GraphFrame:
    """Construct a property GraphFrame from the parsed DataFrames.

    Vertex schema: (id: long, name: string, ml_target: int)
        - `id`        : required by GraphFrames as the unique vertex key
        - `name`      : GitHub username (property)
        - `ml_target` : 1 = machine-learning developer, 0 = web developer

    Edge schema:   (src: long, dst: long, relationship: string)
        - `src`, `dst`    : required by GraphFrames
        - `relationship` : literal "follows"; gives the graph edge-typing so
                            it is a genuine property graph (as requested in
                            2.2) rather than a bare topology.

    Vertices referenced by edges but missing from the target CSV would cause
    GraphFrames to drop those edges silently. The musae-github target file
    covers every node (ids 0..37699), but we defensively pull the set of
    endpoints from the edges as well and left-join with the target metadata,
    so orphan edges never get lost.
    """
    vertices_from_edges = (
        directed_edges.select(F.col("src").alias("id"))
        .unionByName(directed_edges.select(F.col("dst").alias("id")))
        .distinct()
    )

    vertices = (
        vertices_from_edges
        .join(raw_vertices, on="id", how="left")
        .select("id", "name", "ml_target")
    )

    edges = directed_edges.withColumn("relationship", F.lit("follows"))

    return GraphFrame(vertices, edges)


# ---------------------------------------------------------------------------
# 2.3 Running Queries - shared helpers
# ---------------------------------------------------------------------------

def save_top_k(df: DataFrame, output_dir: str, name: str, title: str) -> None:
    """Print the DataFrame and persist it as a single plaintext file.

    Results for every 2.3 query are at most 5 rows, so we `.collect()` to
    the driver and write a readable text file (no Spark part-files). The
    same rows are also printed to stdout for visibility in the driver log.
    """
    rows = df.collect()
    columns = df.columns
    out_path = os.path.join(output_dir, f"{name}.txt")

    widths = [
        max(len(c), *(len(str(r[c])) for r in rows)) if rows else len(c)
        for c in columns
    ]
    header = " | ".join(c.ljust(w) for c, w in zip(columns, widths))
    sep = "-+-".join("-" * w for w in widths)
    body = [
        " | ".join(str(r[c]).ljust(w) for c, w in zip(columns, widths))
        for r in rows
    ]

    content = "\n".join([title, "=" * len(title), header, sep, *body, ""])
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"\n--- {title} ---")
    print(content)
    print(f"[write] {out_path}")


# ---------------------------------------------------------------------------
# 2.3a Top 5 nodes by outdegree
# ---------------------------------------------------------------------------

def query_top_outdegree(graph: GraphFrame, k: int = 5) -> DataFrame:
    """Top-k vertices by outdegree (count of outgoing edges).

    GraphFrames exposes `outDegrees` as a DataFrame with (id, outDegree).
    We enrich it with the vertex metadata (name, ml_target) so the result
    file is self-describing rather than showing bare numeric ids.
    """
    return (
        graph.outDegrees
        .join(graph.vertices, on="id", how="left")
        .select("id", "name", "ml_target", "outDegree")
        .orderBy(F.desc("outDegree"), F.asc("id"))
        .limit(k)
    )


# ---------------------------------------------------------------------------
# 2.3b Top 5 nodes by indegree
# ---------------------------------------------------------------------------

def query_top_indegree(graph: GraphFrame, k: int = 5) -> DataFrame:
    """Top-k vertices by indegree (count of incoming edges).

    Because musae-github is undirected and we mirrored every edge in 2.1,
    indegree == outdegree for every vertex. The ranking therefore matches
    2.3a; we still compute it through GraphFrames' `inDegrees` property so
    the query path is independent and the output file stands on its own.
    """
    return (
        graph.inDegrees
        .join(graph.vertices, on="id", how="left")
        .select("id", "name", "ml_target", "inDegree")
        .orderBy(F.desc("inDegree"), F.asc("id"))
        .limit(k)
    )


# ---------------------------------------------------------------------------
# 2.3c PageRank
# ---------------------------------------------------------------------------

# resetProbability = 1 - damping factor. 0.15 is the canonical Brin/Page value.
# maxIter = 10 is a common default for GraphFrames; PageRank on a 37k-node
# symmetric graph is very well-behaved and converges quickly. We prefer a
# fixed iteration budget over a tolerance threshold so the runtime is
# predictable across reruns / graders.
PAGERANK_RESET_PROB = 0.15
PAGERANK_MAX_ITER = 10


def query_top_pagerank(graph: GraphFrame, k: int = 5) -> DataFrame:
    """Top-k vertices by PageRank score.

    Returns a DataFrame with (id, name, ml_target, pagerank).
    """
    ranked = graph.pageRank(
        resetProbability=PAGERANK_RESET_PROB,
        maxIter=PAGERANK_MAX_ITER,
    )
    return (
        ranked.vertices
        .select("id", "name", "ml_target", "pagerank")
        .orderBy(F.desc("pagerank"), F.asc("id"))
        .limit(k)
    )


# ---------------------------------------------------------------------------
# 2.3d Connected Components
# ---------------------------------------------------------------------------

def query_top_components(graph: GraphFrame, k: int = 5) -> DataFrame:
    """Top-k connected components ranked by number of member vertices.

    GraphFrames' `connectedComponents` labels every vertex with the id of a
    representative vertex for its component. We then `groupBy(component)`
    and count to measure component size. A checkpoint directory must be set
    on the SparkContext before calling; the caller wires that up.
    """
    components = graph.connectedComponents()
    return (
        components.groupBy("component")
        .agg(F.count("*").alias("num_vertices"))
        .orderBy(F.desc("num_vertices"), F.asc("component"))
        .limit(k)
    )


# ---------------------------------------------------------------------------
# 2.3e Triangle Counts
# ---------------------------------------------------------------------------

TRIANGLE_TIE_BREAK_SEED = 42


def query_top_triangles(graph: GraphFrame, k: int = 5) -> DataFrame:
    """Top-k vertices by triangle count.

    GraphFrames' `triangleCount` returns a vertices DataFrame with a `count`
    column (number of triangles that vertex participates in). The algorithm
    treats edges as undirected, which is exactly what we want given the
    dataset semantics.

    The spec allows random tie-breaking; we use a seeded `F.rand()` as the
    secondary sort key so ties are resolved randomly but reproducibly.
    """
    triangles = graph.triangleCount()
    return (
        triangles
        .select("id", "name", "ml_target", F.col("count").alias("triangle_count"))
        .withColumn("_tiebreak", F.rand(seed=TRIANGLE_TIE_BREAK_SEED))
        .orderBy(F.desc("triangle_count"), F.asc("_tiebreak"))
        .drop("_tiebreak")
        .limit(k)
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CS6350-P2-GitHubSocialNetwork")
        .getOrCreate()
    )


def main() -> int:
    work_dir = os.environ.get("P2_WORK_DIR") or tempfile.mkdtemp(prefix="p2_musae_")
    output_dir = os.environ.get(
        "P2_OUTPUT_DIR", os.path.join(os.getcwd(), "output")
    )
    os.makedirs(output_dir, exist_ok=True)
    print(f"[init] work_dir   = {work_dir}")
    print(f"[init] output_dir = {output_dir}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # GraphFrames' connectedComponents uses an iterative Pregel-style job
    # that requires a checkpoint directory to truncate lineage between
    # iterations. Park it under the work_dir so it gets cleaned up with
    # the rest of the transient artifacts.
    checkpoint_dir = os.path.join(work_dir, "checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)
    spark.sparkContext.setCheckpointDir("file://" + checkpoint_dir.replace(os.sep, "/"))

    # --- 2.1 Loading Data -------------------------------------------------
    edges_path, targets_path = download_dataset(work_dir)
    raw_vertices, raw_edges = parse_raw(spark, edges_path, targets_path)

    print("[2.1] Raw vertices (id, name, ml_target) sample:")
    raw_vertices.show(5, truncate=False)
    v_count = raw_vertices.count()
    print(f"[2.1] Vertex count: {v_count}")

    print("[2.1] Raw undirected edges (id_1, id_2) sample:")
    raw_edges.show(5)
    und_count = raw_edges.count()
    print(f"[2.1] Undirected edge count: {und_count}")

    directed_edges = undirected_to_directed(raw_edges).cache()
    print("[2.1] Directed edges after mirroring undirected pairs (src, dst):")
    directed_edges.show(5)
    dir_count = directed_edges.count()
    print(f"[2.1] Directed edge count: {dir_count} (expected ~= 2 x {und_count})")

    # --- 2.2 Create Graphs ------------------------------------------------
    graph = build_graph(raw_vertices, directed_edges)
    # Cache the graph's backing DataFrames; every 2.3 query below scans them.
    graph.vertices.cache()
    graph.edges.cache()

    print("[2.2] GraphFrame vertex schema:")
    graph.vertices.printSchema()
    print("[2.2] Sample vertices:")
    graph.vertices.show(5, truncate=False)

    print("[2.2] GraphFrame edge schema:")
    graph.edges.printSchema()
    print("[2.2] Sample edges:")
    graph.edges.show(5)

    print(f"[2.2] GraphFrame |V| = {graph.vertices.count()}, "
          f"|E| = {graph.edges.count()}")

    # --- 2.3a Top 5 nodes by outdegree ------------------------------------
    top_out = query_top_outdegree(graph, k=5)
    save_top_k(
        top_out,
        output_dir,
        name="2_3a_top_outdegree",
        title="2.3a Top 5 nodes by outdegree (outgoing edges)",
    )

    # --- 2.3b Top 5 nodes by indegree -------------------------------------
    top_in = query_top_indegree(graph, k=5)
    save_top_k(
        top_in,
        output_dir,
        name="2_3b_top_indegree",
        title="2.3b Top 5 nodes by indegree (incoming edges)",
    )

    # --- 2.3c Top 5 nodes by PageRank -------------------------------------
    top_pr = query_top_pagerank(graph, k=5)
    save_top_k(
        top_pr,
        output_dir,
        name="2_3c_top_pagerank",
        title=(
            f"2.3c Top 5 nodes by PageRank "
            f"(resetProbability={PAGERANK_RESET_PROB}, "
            f"maxIter={PAGERANK_MAX_ITER})"
        ),
    )

    # --- 2.3d Top 5 connected components by size --------------------------
    top_cc = query_top_components(graph, k=5)
    save_top_k(
        top_cc,
        output_dir,
        name="2_3d_top_components",
        title="2.3d Top 5 connected components by vertex count",
    )

    # --- 2.3e Top 5 vertices by triangle count ----------------------------
    top_tri = query_top_triangles(graph, k=5)
    save_top_k(
        top_tri,
        output_dir,
        name="2_3e_top_triangles",
        title="2.3e Top 5 vertices by triangle count (ties broken randomly)",
    )

    print(f"\n[done] All query outputs written under: {output_dir}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
