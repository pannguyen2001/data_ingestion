# Research Specification — Batch Ingestion Learning Project

**Project:** `ingestion-loading-poc` (snapshot-diff based batch ingestion, learning target before returning to `data-pipeline-2026`)
**Scope rule:** foundation theory only for what you are actually building — a snapshot-diff CDC engine in Polars/DuckDB. Nothing here should require tools or concepts beyond your current stack (Python, Polars, DuckDB, Pandas fundamentals).
**Depth rule:** Tier 1 = build with it now. Tier 2 = know why it exists and how it differs from what you built, but do not implement it. Tier 3 = named for awareness only; revisit after this project ships and after the main `data-pipeline-2026` project matures.

---

## Tier 1 — Learn now, apply directly in this project

### Checkpoint 1: Batch ingestion fundamentals (full vs. incremental, ETL vs. ELT)
*Goal: know what "ingestion" means as a pipeline stage and where full-reload vs. incremental loading fits.*

1. Joe Reis & Matt Housley, *Fundamentals of Data Engineering* (O'Reilly), Chapter 7 "Ingestion" — the reference text. https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/
2. Chapter-7 reading notes (fast digest, read this first, the book second if you want depth) — https://medium.com/@jchen001/book-notes-fundamentals-of-data-engineering-chapter-7-ingestion-3751da59ccad
3. Data Pipeline Design Patterns (dataskew.io) — practical overview of idempotency, backfilling, CDC in one place. https://dataskew.io/blog/data-pipeline-design-patterns/
4. Data Ingestion Design Patterns: Batch, Streaming & CDC — batch/streaming/CDC compared side by side. https://medium.com/@sanaakeef/data-ingestion-design-patterns-batch-streaming-cdc-a-practical-guide-30867b49de33
5. Data Pipeline: Batch, Streaming, and Idempotent Patterns (ml4devs.com). https://www.ml4devs.com/what-is/data-pipeline/

**How much depth you need now:** enough to explain, in your own words, why a full reload doesn't scale and why incremental loading needs a way to detect "what changed." Skip the streaming/Kappa/Lambda sections — not relevant to a batch snapshot project.

---

### Checkpoint 2: Change detection theory (the 4 methods, and why snapshot-diff is the correct one here)
*Goal: justify your architecture choice, not just apply it.*

1. Redpanda — CDC approaches, architectures, and best practices (clearest side-by-side of the methods). https://www.redpanda.com/guides/fundamentals-of-data-engineering-cdc-change-data-capture
2. Fivetran — CDC: Tools, benefits, and best practices. https://www.fivetran.com/blog/change-data-capture-what-it-is-and-how-to-use-it
3. DataCamp — What is CDC? A Beginner's Guide. https://www.datacamp.com/blog/change-data-capture
4. GeeksforGeeks — CDC in system design. https://www.geeksforgeeks.org/system-design/change-data-capture-cdc/
5. Qlik — CDC definition and best practices. https://www.qlik.com/us/change-data-capture/cdc-change-data-capture

**How much depth you need now:** be able to say, in an interview, "my source is a periodic full-file extract with no log access, so snapshot-diff is the right method — log-based CDC needs a live transactional database I don't have here." That sentence is the whole checkpoint.

---

### Checkpoint 3: Row/field diff mechanics at scale (Polars & DuckDB)
*Goal: the actual technique — full outer join + hash comparison — and how it holds up past in-memory size.*

1. DuckDB GitHub Discussion #18025, "Dataset Diffs" — the exact anti-join/join pattern for classifying insert/update/delete, in SQL. Read this line by line next to your own Polars code. https://github.com/duckdb/duckdb/discussions/18025
2. QuantCo Engineering — `diffly`: comparing Polars DataFrames — the Polars-native version of the same idea, from a team running this in production. https://tech.quantco.com/blog/diffly
3. DuckDB GitHub Discussion #18354 — confirms this approach holds at 100M-row scale, so you know the ceiling. https://github.com/duckdb/duckdb/discussions/18354
4. Medium — 10 DuckDB Joins That Scale With Your Data (chunking behavior once data won't fit in memory). https://medium.com/@bhagyarana80/10-duckdb-joins-that-scale-with-your-data-465353318289
5. Medium — Speed up large set operations with DuckDB (a real Postgres-vs-DuckDB benchmark for this exact diff workload). https://medium.com/doctrine/speed-up-large-set-operations-with-duckdb-70e2ea9bb7c7

**Known caveat to carry forward:** hash-based row comparison assumes "same hash ⇒ same data." This is a probabilistic guarantee, not an absolute one — collision odds are negligible at any volume you'll hit in this project, but say this out loud in your write-up rather than implying certainty. Also watch for nulls: `pl.concat_str(...)` needs `ignore_nulls=True`, or one null field silently poisons the whole row hash.

---

### Checkpoint 4: Idempotency & checkpointing (make re-runs safe)
*Goal: this is the theory behind the `Checkpoint` / `LoadDataCheckpoint` dataclasses you already wrote — turn them from bookkeeping into an actual reliability mechanism.*

1. Unstructured.io — Incremental and Continuous Data Ingestion Strategies (clean, short definitions of watermark vs. checkpoint vs. exactly-once — read this one first). https://unstructured.io/insights/incremental-data-ingestion-strategies-for-continuous-pipelines
2. apxml.com — Idempotency in Pipelines (short, dense pattern catalogue: insert-overwrite, upsert, dedup-at-read). https://apxml.com/courses/intro-data-lake-architectures/chapter-3-ingestion-pipelines/idempotency-in-pipelines
3. Towards Data Engineering (Medium) — Building Idempotent Data Pipelines (maps idempotency onto Bronze/Silver/Gold — same shape as your `data-pipeline-2026` architecture). https://medium.com/towards-data-engineering/building-idempotent-data-pipelines-a-practical-guide-to-reliability-at-scale-2afc1dcb7251
4. Airbyte — Idempotency in Data Pipelines: A Complete Guide. https://airbyte.com/data-engineering-resources/idempotency-in-data-pipelines
5. Designing idempotent processing in a Databricks environment (blog) — practical merge/upsert pattern, ignore the Databricks-specific parts. https://dnavin.wordpress.com/2024/12/22/designing-idempotent-processing-in-a-databricks-environment/

**How much depth you need now:** know that "safe to re-run" means using the primary key to upsert (never blind-append), and that your checkpoint file's job is to record exactly how far the last run got. That's it — don't chase distributed-systems consistency theory yet.

---

## Tier 2 — Understand conceptually, do not implement

### Checkpoint 5: Historical state tracking (current-state table vs. change-log/history table)
*Goal: know why you keep two outputs (a "what does it look like now" table and an append-only change log), without adopting the full Kimball dimensional-modeling vocabulary.*

1. Wikipedia — Slowly changing dimension (single-page overview; read only the Type 1/Type 2 sections). https://en.wikipedia.org/wiki/Slowly_changing_dimension
2. DataCamp — Mastering Slowly Changing Dimensions (hands-on, modern tooling, skip the star-schema history). https://www.datacamp.com/tutorial/mastering-slowly-changing-dimensions-scd
3. Xebia — Practical Guide to SCD Type 2 in dbt, Part 1 (bridges theory to a real implementation you can compare your `DataTracking` dataclass against). https://xebia.com/blog/a-practical-guide-to-creating-slowly-changing-dimensions-type-2-in-dbt-part-1/
4. Kimball Group — Type 2: Add New Row (primary-source reference, read only if the summaries above leave a gap). https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/
5. Kimball Group — Slowly Changing Dimensions, Part 2 (same — reference only, not required reading). https://www.kimballgroup.com/2008/09/slowly-changing-dimensions-part-2/

**Explicit boundary:** you do not need surrogate keys, star schemas, or SCD Types 3–7 for this project. The only thing to take from this checkpoint is: "update = old row closed out, new row opened, both kept" vs. "update = overwrite in place." Your `DataTracking` change-log table already gives you this — this checkpoint is just naming what you built.

### Checkpoint 6: Log-based CDC (Debezium/WAL/binlog) — context only
*Goal: know what the "other" method is and why it doesn't apply to your source, so you can speak to it in an interview — not build it.*

1. Debezium official docs — Features (skim only). https://debezium.io/documentation/reference/stable/features.html
2. Debezium blog — Five Advantages of Log-Based CDC (the one page that matters here — explains the polling-vs-log tradeoff plainly). https://debezium.io/blog/2018/07/19/advantages-of-log-based-change-data-capture/
3. Materialize — MySQL CDC with Debezium in Production (shows the initial-snapshot-then-log-tail pattern — structurally similar to your own bootstrap-vs-incremental split, worth 10 minutes). https://materialize.com/guides/mysql-cdc/
4. Conduktor — Debezium CDC with Kafka: Setup & Examples (skim architecture diagram only). https://www.conduktor.io/glossary/implementing-cdc-with-debezium
5. PostgreSQL docs — Write-Ahead Logging (WAL) intro (primary source, one paragraph is enough). https://www.postgresql.org/docs/current/wal-intro.html

**Explicit boundary:** do not install Kafka, Debezium, or set up a WAL-tailing pipeline for this project. That is a different source type (live OLTP DB) and a different tool category entirely. Revisit this only when a real project gives you direct database log access.

---

## Tier 3 — Named for awareness, defer until later

- **Martin Kleppmann, *Designing Data-Intensive Applications*** — the deepest reference on distributed-systems reliability and consistency. Extremely valuable, but it's a multi-week read aimed at systems beyond a single-machine batch-diff POC. Park it for after this project and after `data-pipeline-2026` reaches its streaming-module phase — don't start it now, it will slow you down without changing what you ship this month.
- **Full Kimball dimensional-modeling framework (SCD Types 3–7, mini-dimensions, surrogate-key management at warehouse scale)** — relevant once you're designing an actual analytics warehouse for BI consumption, not for a change-capture learning exercise.

---

## Suggested reading order

1 → 2 → 3 → 4 (build and ship the diff engine using these) → 5 (name what you already built) → 6 (context, one sitting) → stop. Tier 3 is a "later" list, not a "next" list.