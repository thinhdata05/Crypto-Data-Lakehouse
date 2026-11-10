---
description: Automatic documentation update reflex, language separation (English for Agent files, Vietnamese for Human docs), single source of truth for DDL, and technical depth preservation
globs: "docs/**, MEMORY.md, README.md, AGENTS.md, .agent/rules/**, sql/ddl/**"
---

# 04. Documentation & Agent Behavior Rules

Standards governing automatic documentation updates, language separation between Agent and Human files, DDL single source of truth, and technical depth preservation.

---

## 1. Strict Language Convention (Agent vs. Human Markdown Files)
- **Agent Operational Files (MUST BE WRITTEN IN ENGLISH)**:
  - [`AGENTS.md`](file:///e:/project_hybrid_lakehouse_crypto/AGENTS.md)
  - [`MEMORY.md`](file:///e:/project_hybrid_lakehouse_crypto/MEMORY.md)
  - [`.agent/rules/01-lakehouse-invariants.md`](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/01-lakehouse-invariants.md)
  - [`.agent/rules/02-financial-domain-rules.md`](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/02-financial-domain-rules.md)
  - [`.agent/rules/03-safety-and-devops.md`](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/03-safety-and-devops.md)
  - [`.agent/rules/04-documentation-rules.md`](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/04-documentation-rules.md)
- **Human Documentation Files (MUST BE WRITTEN IN VIETNAMESE)**:
  - [`README.md`](file:///e:/project_hybrid_lakehouse_crypto/README.md)
  - [`docs/1-architecture/storage-and-warehouse.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/1-architecture/storage-and-warehouse.md)
  - [`docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md)
  - [`docs/3-specifications/phase1-migration-spec.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/3-specifications/phase1-migration-spec.md)
  - [`docs/3-specifications/phase2-cloud-setup-spec.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/3-specifications/phase2-cloud-setup-spec.md)
  - [`docs/4-runbooks/catalog-partition-migration.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/4-runbooks/catalog-partition-migration.md)
  - [`docs/4-runbooks/operations-and-maintenance.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/4-runbooks/operations-and-maintenance.md)
  - [`docs/5-data-contracts/symbol-and-currency-contract.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/5-data-contracts/symbol-and-currency-contract.md)
- **Zero Legacy Policy**:
  - Never leave obsolete references to `s3://warehouse/`, `silver.crypto_bars`, `silver.curated_crypto_bars`, `bronze.source_manifest`, `silver.pipeline_audit`, or `gold.curated_crypto_bars` in active documentation.

---

## 2. Automatic Documentation Reflex
Whenever the AI Agent modifies code, schemas, pipelines, or fixes a bug, the Agent **MUST automatically update**:
1. [`MEMORY.md`](file:///e:/project_hybrid_lakehouse_crypto/MEMORY.md) (in English):
   - Latest row counts, file counts, symbol counts, test counts, and audit statuses.
2. Functional Vietnamese documentation under [`docs/`](file:///e:/project_hybrid_lakehouse_crypto/docs/) and [`README.md`](file:///e:/project_hybrid_lakehouse_crypto/README.md):
   - **Bug & Incident Post-Mortems**: Log Symptom, Root Cause Analysis (RCA), Resolution, and Prevention in [`docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md).
   - **Performance Optimizations**: Log Before vs. After metrics (Peak RAM, Rows/sec, Wall-clock time, File count, VHDX size) in [`docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md) and [`README.md`](file:///e:/project_hybrid_lakehouse_crypto/README.md).

---

## 3. Single Source of Truth for DDL & SQL Views
- Local Iceberg DDL is defined in [`sql/ddl/01_create_iceberg_tables.sql`](file:///e:/project_hybrid_lakehouse_crypto/sql/ddl/01_create_iceberg_tables.sql).
- Cloud Iceberg reference DDL and Gold analytical views are specified in [`docs/3-specifications/phase2-cloud-setup-spec.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/3-specifications/phase2-cloud-setup-spec.md).

---

## 4. Preservation of Technical Depth
- When updating or rewriting documentation, **always preserve**:
  - Mathematical formulas in KaTeX (JVM OOM memory pressure equations, Granger causality, Hasbrouck Information Share, normalized spread formulas).
  - Real empirical benchmark tables (RAM usage, throughput in rows/s, file counts, VHDX compaction metrics).
