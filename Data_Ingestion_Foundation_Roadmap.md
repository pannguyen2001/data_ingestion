# Data Ingestion Foundation Learning Roadmap (ChatGPT)

## Goal

Build a mini **Batch Data Ingestion Engine** from scratch to deeply
understand:

-   How data ingestion works
-   Batch processing
-   Load strategies
-   Change detection
-   Metadata
-   Production architecture

------------------------------------------------------------------------

# Knowledge Map

``` text
1. Data Engineering Foundation
                │
                ▼
2. Storage System
                │
                ▼
3. File Format
                │
                ▼
4. Batch Processing
                │
                ▼
5. Load Strategy
                │
                ▼
6. Change Detection
                │
                ▼
7. Data Modeling
                │
                ▼
8. Metadata & Checkpoint
                │
                ▼
9. Data Quality
                │
                ▼
10. Performance
                │
                ▼
11. Reliability
                │
                ▼
12. Production Architecture
```

------------------------------------------------------------------------

# 1. Data Engineering Foundation

## Research

-   What is Data Engineering?
-   OLTP vs OLAP
-   Data lifecycle
-   ETL vs ELT
-   Batch vs Streaming
-   Data Warehouse
-   Data Lake
-   Lakehouse

## Expected Understanding

Business → Application DB → Data Ingestion → Raw → Clean → Warehouse →
Analytics

## Top 5 Sources

1.  Fundamentals of Data Engineering --- Joe Reis & Matt Housley
2.  Designing Data-Intensive Applications --- Martin Kleppmann
3.  Google Cloud Data Engineering learning path
4.  Microsoft Learn -- Data Engineering
5.  Databricks Lakehouse Fundamentals

------------------------------------------------------------------------

# 2. Storage System

## Research

-   Object Storage
-   Block Storage
-   Local File System
-   Partitioning
-   Compression
-   Small File Problem

## Understand

CSV → Parquet → Delta/Iceberg

## Top 5 Sources

1.  Fundamentals of Data Engineering
2.  Apache Parquet Documentation
3.  Delta Lake Documentation
4.  Apache Iceberg Documentation
5.  DuckDB Documentation

------------------------------------------------------------------------

# 3. File Format

## Research

-   CSV
-   JSON
-   Parquet
-   Avro
-   ORC
-   Schema Evolution
-   Columnar vs Row Format

## Top 5 Sources

1.  Apache Parquet Docs
2.  Apache Avro Docs
3.  DuckDB Blog
4.  MotherDuck Blog
5.  Databricks Documentation

------------------------------------------------------------------------

# 4. Batch Processing

## Research

-   Batch Window
-   Job
-   Pipeline
-   Workflow
-   Retry
-   Checkpoint
-   Idempotency
-   Late Data
-   Partition
-   Scheduling

## Top 5 Sources

1.  Fundamentals of Data Engineering
2.  Google Cloud Data Pipelines
3.  Microsoft Data Factory Docs
4.  Airflow Best Practices
5.  Netflix Data Engineering Blogs

------------------------------------------------------------------------

# 5. Load Strategy

## Research

-   Full Load
-   Append
-   Incremental
-   CDC
-   Snapshot
-   Merge
-   Watermark
-   High Watermark
-   SCD

## Top 5 Sources

1.  Kimball Data Warehouse Toolkit
2.  dbt Snapshot Documentation
3.  Delta Lake MERGE
4.  Snowflake MERGE
5.  BigQuery Incremental Loading

------------------------------------------------------------------------

# 6. Change Detection (Core)

## Research

-   Primary Key
-   Business Key
-   Surrogate Key
-   Hash
-   Checksum
-   Row Hash
-   Field Comparison
-   Join Strategy
-   Diff Algorithm
-   Audit Log
-   CDC

## Algorithm

1.  Read yesterday snapshot
2.  Read today snapshot
3.  Join by primary key
4.  Detect CREATE
5.  Detect DELETE
6.  Compare row hash
7.  Compare changed fields
8.  Generate Change Log

## Top 5 Sources

1.  Kimball Toolkit
2.  Delta Lake MERGE
3.  Apache Hudi Docs
4.  Apache Iceberg Docs
5.  dbt Snapshot Implementation

------------------------------------------------------------------------

# 7. Data Modeling

## Research

-   Star Schema
-   Snowflake Schema
-   Fact
-   Dimension
-   SCD Type 1
-   SCD Type 2
-   Business Key

## Top 5 Sources

1.  Kimball Toolkit
2.  Microsoft Fabric Modeling
3.  Google BigQuery Modeling
4.  dbt Documentation
5.  Databricks Modeling Guide

------------------------------------------------------------------------

# 8. Metadata & Checkpoint

## Research

-   Operational Metadata
-   Technical Metadata
-   Business Metadata
-   Batch Metadata
-   Lineage
-   Audit
-   Checkpoint

## Metadata Tables

-   job_history
-   batch_history
-   change_log
-   checkpoint
-   error_log
-   metrics

## Top 5 Sources

1.  OpenLineage
2.  Marquez
3.  Airflow Metadata DB
4.  Fundamentals of Data Engineering
5.  Netflix Metadata Blogs

------------------------------------------------------------------------

# 9. Data Quality

## Research

-   Completeness
-   Accuracy
-   Consistency
-   Validity
-   Uniqueness
-   Freshness
-   Referential Integrity

## Top 5 Sources

1.  Great Expectations
2.  Soda
3.  Monte Carlo Data
4.  dbt Tests
5.  Google Cloud Data Quality

------------------------------------------------------------------------

# 10. Performance

## Research

-   Lazy Execution
-   Predicate Pushdown
-   Projection Pushdown
-   Parallelism
-   Partition Pruning
-   Vectorized Execution
-   Hash Join
-   Streaming Execution

## Top 5 Sources

1.  Polars User Guide
2.  DuckDB Internals
3.  DuckDB Blog
4.  Apache Arrow Documentation
5.  MotherDuck Engineering Blog

------------------------------------------------------------------------

# 11. Reliability

## Research

-   Retry
-   Timeout
-   Recovery
-   Resume
-   Checkpoint
-   Idempotency
-   Atomic Write
-   Transactions
-   Exactly Once
-   At Least Once

## Top 5 Sources

1.  Airflow Best Practices
2.  Delta Lake Transactions
3.  Apache Beam Model
4.  Kafka Delivery Semantics
5.  Designing Data-Intensive Applications

------------------------------------------------------------------------

# 12. Production Architecture

## Research

-   Bronze / Silver / Gold
-   Lakehouse
-   Data Mesh (concept)
-   Orchestration
-   Catalog
-   Lineage
-   Monitoring
-   Governance

## Top 5 Sources

1.  Databricks Lakehouse Architecture
2.  Microsoft Fabric Architecture
3.  Google Cloud Data Platform
4.  AWS Modern Data Architecture
5.  Uber Engineering Blog

------------------------------------------------------------------------

# Five Highest ROI References

1.  Fundamentals of Data Engineering ⭐⭐⭐⭐⭐
2.  Designing Data-Intensive Applications ⭐⭐⭐⭐⭐
3.  The Data Warehouse Toolkit ⭐⭐⭐⭐⭐
4.  Official Documentation (Delta, Iceberg, Hudi, Parquet, Polars,
    DuckDB, dbt) ⭐⭐⭐⭐☆
5.  Engineering Blogs (Netflix, Uber, Airbnb, Databricks, MotherDuck)
    ⭐⭐⭐⭐☆

------------------------------------------------------------------------

# Recommended Learning Loop

For each checkpoint:

1.  Read the theory.
2.  Take notes.
3.  Implement the concept.
4.  Compare with production implementations.
5.  Write a design document explaining:
    -   Why this design?
    -   Trade-offs
    -   Alternatives
    -   When to use it
