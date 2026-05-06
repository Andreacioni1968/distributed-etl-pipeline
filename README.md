# High-Performance Distributed ETL Orchestrator

An asynchronous, hardware-aware distributed task execution engine built for high-throughput ETL pipelines with zero-data-loss guarantees.

## 🏗 Architecture & System Design

Standard task schedulers often fall short when dealing with dynamic hardware constraints and unpredictable network latencies across distributed nodes. This custom orchestrator was engineered to manage hybrid compute environments, separating I/O-bound ingestion tasks from CPU-bound analytical processing.

### Key Engineering Features:

* **Hardware-Aware Throttling (Thermal Gatekeeper):** Implements a dynamic monitoring daemon that reads compute node temperatures (via `/sys/class/thermal`). It automatically throttles parallel execution or gracefully degrades non-essential long-running tasks to prevent hardware thermal throttling and system crashes under sustained loads.
* **Self-Healing & Idempotency:** Full state management via PostgreSQL. Tasks transition strictly through a state machine (`pending` -> `running` -> `completed`/`failed`). A dedicated "zombie killer" subroutine actively monitors for orphaned tasks, automatically terminating stalled remote processes via SSH and redistributing the workloads to ensure eventual consistency.
* **Logical Concurrency Control:** Features a custom `exclusive_class` distributed locking mechanism to prevent race conditions and I/O collisions when multiple heavy analytical pipelines attempt to write to the same columnar partitions simultaneously.
* **Polyglot Persistence Integration:** Engineered to orchestrate workloads across diverse storage engines. Operational state and messaging queues are handled transactionally in PostgreSQL, while output data is strictly consolidated into Apache Parquet format and ClickHouse for highly optimized OLAP queries.

## 📁 Repository Structure

This repository extracts the core engine logic from the production environment into clean, standalone modules:

* `core_orchestrator.py`: The primary asynchronous loop managing the thread pools, task dispatching, and database connection pools (with `autocommit` optimizations to prevent idle-in-transaction blocking).
* `resilience_manager.py`: Contains the strict idempotency logic, distributed lock management, and the automated recovery/zombie-killer subroutines.
* `hardware_throttle.py`: The standalone thermal monitoring and dynamic resource allocation logic.

---
*Note: This repository contains the abstracted architectural patterns of the engine. Specific business logic, proprietary data transformations, and production infrastructure configurations have been intentionally removed for security compliance.*
