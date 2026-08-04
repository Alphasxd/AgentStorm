# ADR 0003: PostgreSQL and object storage before analytical databases

Status: Accepted

PostgreSQL owns run metadata, idempotency, queue leases, permits, aggregates, and comparisons. S3-
compatible storage receives the canonical compressed shard before database finalization. This
supports current consistency and query needs without adding ClickHouse. An analytical database is
considered only when measured event volume and query latency justify the operational cost.
