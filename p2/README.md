# CS 6350 Assignment 3 — Part 2

Analyzing the **musae-github** social network from SNAP with Spark GraphFrames.

- Dataset page: https://snap.stanford.edu/data/github-social.html
- Archive (downloaded automatically at runtime): https://snap.stanford.edu/data/git_web_ml.zip
- ~37,700 GitHub developers, ~289,003 mutual-follower edges (undirected),
  each doubled into two directed edges per the assignment spec.

Everything runs from a single spark-submit invocation — no local data files
required and no hard-coded paths.

---

## Prerequisites (WSL)

Tested with Ubuntu on WSL2. Any Spark 3.5 + Python 3.10+ environment works.

1. **Java 11** (Spark 3.5 runs on Java 8/11/17; 11 is the easy choice):
   ```bash
   sudo apt-get install openjdk-11-jdk
   ```
2. **Python packages** (PySpark + deps):
   ```bash
   pip install pyspark==3.5.0
   ```
   GraphFrames' Python bindings are shipped inside the Maven package and
   are added to `PYTHONPATH` automatically by `spark-submit --packages`.
3. **Outbound network access** — the script pulls the dataset from
   `snap.stanford.edu` and the GraphFrames jar from Maven Central on first
   run.

---

## Run

From inside this folder (`p2/`):

```bash
cd p2
spark-submit \
  --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
  part2.py
```

> If you forget to `cd` into `p2/` first, `spark-submit` will report
> `python3: can't open file '.../part2.py': [Errno 2] No such file or directory`
> because `part2.py` lives inside the `p2/` folder.

That's it. All five queries (2.3a through 2.3e) run end-to-end and write
their results under `./output/`. End-to-end runtime is roughly **5–15
minutes** on a laptop; PageRank finishes quickly, but Connected Components
and (especially) Triangle Count are the slow parts. The job is done when
you see `[done] All query outputs written under: ...` in the log.

### Optional environment variables

| Variable        | Purpose                                     | Default                              |
|-----------------|---------------------------------------------|--------------------------------------|
| `P2_WORK_DIR`   | Cache dir for the downloaded dataset + CC checkpoints | fresh `tempfile.mkdtemp()`           |
| `P2_OUTPUT_DIR` | Where query outputs are written             | `./output` relative to cwd           |

Example — reuse a cached download between runs:

```bash
export P2_WORK_DIR=~/.cache/cs6350-p2
spark-submit --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 part2.py
```

---

## What gets produced

`./output/` is populated with one plaintext file per query:

```
output/
├── 2_3a_top_outdegree.txt
├── 2_3b_top_indegree.txt
├── 2_3c_top_pagerank.txt
├── 2_3d_top_components.txt
└── 2_3e_top_triangles.txt
```

Each file contains a header and the top-5 rows for that query.

The driver stdout also prints every result (and dataset sanity counts for
2.1 / 2.2) so the full run is visible in the Spark log.

---

## Expected sanity numbers

After the run you should see in the driver log:

- Vertex count: **37,700**
- Undirected edge count: **289,003**
- Directed edge count: **~578,006** (≈ 2 × undirected; any deviation is the
  defensive `.distinct()` absorbing pre-existing symmetric pairs)
- `|V|`, `|E|` on the GraphFrame matching the numbers above.

---

## Notes on the results

- **Output columns** — every result file has columns `id`, `name`, and the
  query-specific metric (`outDegree`, `inDegree`, `pagerank`,
  `num_vertices`, or `triangle_count`). The vertex `ml_target` attribute
  is carried in the GraphFrame (so 2.2 still satisfies "property graph")
  but intentionally omitted from the result files because the spec doesn't
  ask for it.
- **2.3a vs 2.3b** — since musae-github is undirected and we materialise both
  directions, every vertex has `indegree == outdegree`. The two ranking files
  therefore show the same top 5. That's the correct answer given the spec
  requires the undirected → 2-directed conversion; the files are kept
  separate because the spec asks for both queries as independent outputs.
- **2.3c PageRank** — run with `resetProbability=0.15` (canonical 0.85
  damping factor) and `maxIter=10`. 10 iterations give stable top-k rankings
  for this graph size. Note: GraphFrames returns *unnormalised* PageRank
  scores — every vertex starts with rank 1 (not 1/N), so the sum over all
  vertices is ≈ N = 37,700 and individual scores can be much greater than 1.
  The ranking is identical to the textbook normalised form; divide by
  37,700 if you want probability-style values in `[0, 1]`.
- **2.3d Connected Components** — requires a checkpoint directory, which the
  script sets up automatically under `P2_WORK_DIR/checkpoints`.
- **2.3e Triangle Count** — ties are broken with a seeded `F.rand()`
  (seed = 42) so the output is reproducible across runs but randomised
  within tied triangle counts, as the spec allows.

---

## Files

| File        | What it is                                              |
|-------------|---------------------------------------------------------|
| `part2.py`  | Single spark-submit entrypoint (2.1 + 2.2 + 2.3a–e)     |
| `README.md` | This file                                               |
| `output/`   | Query results, produced at runtime                      |


---

## outputs

Musae-Github Network Analysis:

We used analyzed the **musae-github** social network from SNAP with Spark GraphFrames.

- Dataset page: https://snap.stanford.edu/data/github-social.html
- Archive (downloaded automatically at runtime): https://snap.stanford.edu/data/git_web_ml.zip
- ~37,700 GitHub developers, ~289,003 mutual-follower edges (undirected),
  each doubled into two directed edges per the assignment spec.
- Tested with Ubuntu on WSL2. Any Spark 3.5 + Python 3.10+ environment works.


2.3a Top 5 nodes by outdegree (outgoing edges)
==============================================
id    | name              | outDegree
------+-------------------+----------
31890 | dalinhuang99      | 9458     
27803 | nfultz            | 7085     
35773 | addyosmani        | 3324     
19222 | Bunlong           | 2958     
13638 | gabrielpconceicao | 2468     

>> Very simple count of the number of accounts these users follow a.k.a. the outdegree in this case. This lists the top 5 users with most outgoing connections. From this we can also infer they are active users, part of a large community on GitHub.


2.3b Top 5 nodes by indegree (incoming edges)
=============================================
id    | name              | inDegree
------+-------------------+---------
31890 | dalinhuang99      | 9458    
27803 | nfultz            | 7085    
35773 | addyosmani        | 3324    
19222 | Bunlong           | 2958    
13638 | gabrielpconceicao | 2468    

>> Very simple count of the number of followers each has a.k.a. the indegree in this case. These the are most popular users in terms of follower count or well-known and well connected users.


2.3c Top 5 nodes by PageRank (resetProbability=0.15, maxIter=10)
================================================================
id    | name              | pagerank          
------+-------------------+-------------------
31890 | dalinhuang99      | 634.0502986779188 
27803 | nfultz            | 433.4045041751695 
35773 | addyosmani        | 190.0493832173293 
19222 | Bunlong           | 178.00658569996676
13638 | gabrielpconceicao | 147.84127656784105

>> Ran the PageRank algorithm to determine the most well-known/popular/influential GitHub users based on their connections (follows/following vs followers). Here we are not just worried about the count of their connections but their value or influence as well. High scores indicate a well connect user that is also well connected to other influential users. In other words, these users are part of a very popular group or highly influential and close-knit/well-known community of GitHub like an upper echelon of its users by account.


2.3d Top 5 connected components by vertex count
===============================================
component | num_vertices
----------+-------------
0         | 37700       

>> The number of components is 1 since all users are connection each other at least once in this graph and can be reachable from each other. Connected components are parts or subgraphs that have a connection between all its nodes regardless of the length of the paths to reach each. If a subgraph doesn’t have any connection to another subgraph, they are separate components. However, in our case, the GitHub community seems to be all connected overall.


2.3e Top 5 vertices by triangle count (ties broken randomly)
============================================================
id    | name              | triangle_count
------+-------------------+---------------
27803 | nfultz            | 80286         
31890 | dalinhuang99      | 63205         
19222 | Bunlong           | 27873         
35773 | addyosmani        | 22758         
13638 | gabrielpconceicao | 19762         

>> Triangle counts are counts of 3 mutually connected individual GitHub accounts. (ex: A, B, C have connections A  B, A  C, & B  C). These users have the most triangle count connections or, in other words, have tight community connections to fellow GitHub users and not just a lot of followers.






