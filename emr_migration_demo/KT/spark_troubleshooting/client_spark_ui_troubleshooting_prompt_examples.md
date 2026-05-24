# Client Spark UI Troubleshooting Prompt Examples

## Purpose

This file stores worked examples for prompts in:

```text
KT/spark_troubleshooting/client_spark_ui_troubleshooting_plan.md
```

Use this file to understand what a good prompt result should look like before using the prompts in a client environment.

The examples are organized by dataset scale and prompt type.

## Small Dataset Examples

### Physical Plan To Code Mapping Prompt

Use this example after identifying:

```text
SQL Query 8
Job 22
Stage 38
count at BrbfJob.scala:80
```

Input evidence:

```text
Stage 38:
  tasks: 200
  shuffle read: 398.4 MiB / 1,500,000 records
  shuffle write: 9.8 KiB / 200 records
  median shuffle read: 2 MiB
  max shuffle read: 3.1 MiB
  median duration: 4 s
  max duration: 20 s
  max GC: 0.6 s
  executor split: 100 tasks / 100 tasks

SQL Query 8:
  duration: 1.9 min
  jobs: 17, 18, 19, 20, 21, 22
  action: count at BrbfJob.scala:80

Physical plan signals:
  HashAggregate count(1)
  Exchange SinglePartition
  InMemoryTableScan
  InMemoryRelation StorageLevel(disk, memory, deserialized)
  BroadcastHashJoin for small dimensions
  ShuffledHashJoin for eligible_user_data and feature_log
  Exchange hashpartitioning(..., 200)
  UDF projections before joins
```

Example output:

#### 1. Action-Line Mapping

Confirmed action line:

```text
count at BrbfJob.scala:80
```

Code action:

```scala
printMetric("joined_row_count", joined.count())
```

Interpretation:

```text
This count action triggered SQL Query 8 and Job 22 / Stage 38.
The action line is confirmed, but the expensive work comes from materializing the upstream joined DataFrame, not from count() alone.
```

#### 2. Operator Fingerprints From Physical Plan

Useful fingerprints:

```text
Action/operator:
  HashAggregate count(1)
  Exchange SinglePartition
  InMemoryTableScan
  InMemoryRelation persisted with StorageLevel(disk, memory, deserialized)

Major joins:
  BroadcastHashJoin on bid_request_id
  BroadcastHashJoin on contextual_id
  BroadcastHashJoin on advertiser_id
  BroadcastHashJoin on advertiser_id, campaign_id
  ShuffledHashJoin on bid_request_id, user_id_hash
  ShuffledHashJoin on user_id_hash, contextual_id
  BroadcastHashJoin on user_id_hash / sib_user_id_hash

Shuffle clues:
  Exchange hashpartitioning(bid_request_id, 200)
  Exchange hashpartitioning(bid_request_id, user_id_hash, 200)
  Exchange hashpartitioning(user_id_hash, contextual_id, 200)
  Exchange hashpartitioning(feature_user_id_hash, feature_contextual_id, 200)
  Exchange hashpartitioning(user_id_hash, 200)

UDF clues:
  UDF(bid_request_id) AS bid_request_hash
  UDF(normalized_user_id) AS user_id_hash
  UDF(contextual_id cast string) AS contextual_join_key
  UDF(bid_timestamp) AS rounded_bid_time
```

#### 3. GitLab Search Terms

Entry point:

```text
BrbfJob
joined_row_count
count(
```

Datasets and paths:

```text
bids
impressions_feedback
contextual
advertiser
koa_settings
eligible_user_data
feature_log
sib
```

Join keys:

```text
bid_request_id
user_id_hash
contextual_id
feature_user_id_hash
feature_contextual_id
advertiser_id
campaign_id
sib_user_id_hash
```

Operators:

```text
join(
broadcast(
dropDuplicates
persist
StorageLevel.MEMORY_AND_DISK
spark.sql.shuffle.partitions
```

UDF and preparation:

```text
normalized_user_id
bid_request_hash
contextual_join_key
rounded_bid_time
```

#### 4. Likely Code Areas

| Spark evidence | Code clue / search term | Likely source area | Confidence |
|---|---|---|---|
| `count at BrbfJob.scala:80` | `joined_row_count`, `joined.count()` | `BrbfJob.scala:80` | confirmed |
| `InMemoryRelation`, `StorageLevel(disk, memory...)` | `persist(StorageLevel.MEMORY_AND_DISK)` | `joined.persist(...)` | confirmed |
| Broadcast joins on contextual / advertiser / koa | `broadcast(`, `advertiser_id`, `campaign_id` | joined enrichment block | high |
| Shuffled join on `bid_request_id`, `user_id_hash` | `eligibleUserData`, `bid_request_id`, `user_id_hash` | eligible user join | high |
| Shuffled join on `user_id_hash`, `contextual_id` | `featureLog`, `feature_user_id_hash`, `feature_contextual_id` | feature log join | high |
| 200-partition Exchanges | `spark.sql.shuffle.partitions`, `repartition`, join keys | runtime config and shuffle joins | high |
| UDF projections before joins | `Udfs.sha256String`, `roundTimestampToMinutes` | bid preparation block | high |

#### 5. Stage-To-Operator Hypothesis

Stage 38 metrics:

```text
200 tasks
398.4 MiB shuffle read / 1,500,000 records
9.8 KiB shuffle write / 200 records
median shuffle read: 2 MiB
max shuffle read: 3.1 MiB
median duration: 4 s
max duration: 20 s
low GC
balanced executors
```

Likely mapping:

```text
Stage 38 is part of the joined.count() action over the persisted joined DataFrame.
It appears tied to reading or materializing 200 partitions of the joined result and doing partial count aggregation.
```

Strongest code-level source:

```text
joined.count() at BrbfJob.scala:80
```

Upstream transformations creating the 200-partition shape:

```text
bids
  .join(...)
  .join(eligibleUserData, Seq("bid_request_id", "user_id_hash"), "left")
  .join(featureLog, user_id_hash/contextual_id condition, "left")
  .join(sib, ...)
  .persist(...)
```

#### 6. What To Verify In Code

Check:

```text
Is spark.sql.shuffle.partitions fixed at 200?
Is joined persisted before count?
Are there repeated count actions after joined.count()?
Are eligibleUserData and featureLog joined without broadcast?
Are join keys exactly bid_request_id/user_id_hash and user_id_hash/contextual_id?
Are UDFs applied before joins?
Is any manual repartition/coalesce used around joined?
```

Likely answers for the demo:

```text
spark.sql.shuffle.partitions = 200: yes
joined.persist(MEMORY_AND_DISK): yes
repeated joined counts: yes, several match-count metrics after line 80
manual repartition in joined block: no obvious manual repartition there
shuffle joins caused by join keys and fixed shuffle partitions: likely
```

#### 7. Databricks / Photon Opportunity Signals

| Code area / operator | Current evidence | Follow-up category | Confirmed / inferred |
|---|---|---|---|
| `spark.sql.shuffle.partitions=200` | Stage 38 has 200 tiny tasks | AQE partition coalescing | confirmed |
| `eligibleUserData` join | `ShuffledHashJoin`, 200-partition Exchange | AQE join strategy / shuffle optimization | confirmed |
| `featureLog` join | `ShuffledHashJoin`, 200-partition Exchange | AQE join strategy / shuffle optimization | confirmed |
| UDF projections | `UDF(...)` before joins | less Photon-friendly code / UDF rewrite review | confirmed |
| Broadcast dimension joins | multiple `BroadcastHashJoin` | already using broadcast; Photon may still accelerate native execution | confirmed |
| persisted joined DataFrame | `InMemoryRelation`, `StorageLevel(disk, memory...)` | cache/persist review | confirmed |
| repeated metric counts | multiple `joined.filter(...).count()` after line 80 | repeated action / cache value review | inferred from code |

#### 8. Conservative Current-State Conclusion

```text
The expensive Stage 38 belongs to SQL Query 8, triggered by count at BrbfJob.scala:80.
The physical plan points to a count over an InMemoryRelation for the persisted joined DataFrame.
The joined DataFrame is built from several broadcast joins plus larger 200-partition shuffled joins to eligible_user_data and feature_log.

Stage 38 itself is balanced, low-GC, and low-spill, with 200 small tasks reading about 2 MiB median shuffle data each.
This supports the current bottleneck classification of too many small shuffle partitions / task-wave overhead, likely influenced by fixed spark.sql.shuffle.partitions=200 and the upstream joined DataFrame shape.

Search GitLab for joined_row_count, joined.count(), eligible_user_data, feature_log, user_id_hash, contextual_id, and spark.sql.shuffle.partitions to confirm the exact source block.
```

## Medium Dataset Examples

Pending.
