# Spark Troubleshooting Cheat Sheet

Use this as the quick mental model. Go to the detailed guides only when the current step needs more depth.

Detailed references:

```text
EMR Spark Troubleshooting Guide: emr_spark_troubleshooting_guide.md
Client Spark UI Troubleshooting Plan: client_spark_ui_troubleshooting_plan.md
```

## The Core Loop

```text
1. Find the workload
2. Confirm runtime config
3. Find the longest job/query
4. Find the longest stages
5. Classify the bottleneck
6. Map it back to code/operators
7. Translate to Databricks opportunities
```

## 1. Find The Workload

Start from Airflow or the scheduler:

```text
Airflow DAG/task
-> EMR step or Spark submit
-> YARN application id
-> Spark History application
```

Goal:

```text
Know exactly which Spark app you are analyzing.
```

## 2. Confirm Runtime Config

In Spark UI:

```text
Environment -> Spark Properties
SQL query details if Environment is missing values
```

Capture first:

```text
spark.sql.adaptive.enabled
spark.sql.adaptive.skewJoin.enabled
spark.sql.shuffle.partitions
spark.sql.autoBroadcastJoinThreshold
executor cores/memory
dynamic allocation
```

Mental model:

```text
AQE off -> static plan, Databricks AQE may help.
Skew handling off -> skew evidence matters more.
Fixed shuffle partitions -> look for too many tiny tasks or too few heavy tasks.
Broadcast missing for small tables -> possible AQE/stats/broadcast opportunity.
```

## 3. Find The Longest Job Or Query

Use:

```text
Jobs tab
SQL tab
```

Goal:

```text
Find where the app spends most wall-clock time.
```

Focus rule:

```text
one job/query >= 40%-50% runtime -> start there
no single dominant job -> inspect top 2-3
top 2-3 explain 70%-80% runtime -> first investigation set
many repeated small jobs -> check repeated-pattern overhead
```

Use SQL physical plan twice:

```text
early -> map operators and Exchanges
after stage metrics -> map bottleneck back to operator/code
```

## 4. Find The Longest Stages

Use:

```text
Stages tab
SQL query linked stages
```

Capture for top stages:

```text
duration
tasks
shuffle read/write
input/output
GC time
memory spill
disk spill
executor balance
```

Goal:

```text
Find the stage that explains the job duration.
```

Focus rule:

```text
one stage >= 40%-50% job/query runtime -> start there
no single dominant stage -> inspect top 2-3
short repeated stages can still matter if they repeat, fail, or spill
```

Quick decision order:

```text
1. max shuffle read vs median shuffle read
2. max duration vs median duration
3. spill / GC / peak execution memory
4. scheduler delay / shuffle fetch wait
5. executor balance
```

## 5. Classify The Bottleneck

### Skew

```text
median task is reasonable
max task is much larger
max shuffle read is much larger than median
stage waits near the end
```

Rule of thumb:

```text
max < 2x-3x median -> usually okay
max 3x-5x median -> watch closely
max > 5x-10x median -> likely skew
```

### Too Many Small Partitions

```text
many tasks
small shuffle read per task
balanced executors
low spill
stage slow because tasks run in many waves
```

Example:

```text
200 tasks
median shuffle read 2 MiB
balanced 100/100 tasks across executors
AQE disabled
```

Likely Databricks angle:

```text
AQE partition coalescing
```

### Too Few Heavy Partitions

```text
few tasks
large shuffle read per task
long task duration
spill or high GC
```

Likely angles:

```text
increase partitions
AQE skew handling
better layout
executor sizing
```

### Memory Pressure

```text
high GC time
memory spill
disk spill
high peak execution memory
slow tasks spill more than normal tasks
```

Rule of thumb:

```text
GC < 5%-10% of task time -> usually okay
GC 10%-20% -> watch closely
GC > 20%-30% with spill -> likely memory pressure
GC > 50%, large disk spill, OOMs -> severe memory pressure
```

Likely angles:

```text
reduce shuffle size
tune joins/aggregates
increase executor memory
AQE/skew handling
Photon for supported operators
```

### Expensive Shuffle/Join

```text
large shuffle read/write
Exchange before join
SortMergeJoin or ShuffledHashJoin
small table not broadcast
```

Likely angles:

```text
broadcast tuning
table statistics
AQE join strategy
Delta layout
```

### Expensive Sort/Write

```text
range partitioning
Sort
WriteFiles
large output
many files or slow output stage
```

Likely angles:

```text
avoid unnecessary global sort
optimize file size
Delta layout
write partitioning
```

## 6. Map Stage Back To Code

Use:

```text
job description, such as count at BrbfJob.scala:80
associated SQL query id
SQL physical plan
stage DAG / RDD graph
driver logs and printed metrics
source code around the action
```

Goal:

```text
Know which code path triggered the bottleneck: join, aggregate, repartition, sort, write, or UDF projection.
```

## 7. Databricks Translation

Use source evidence first, then map to Databricks:

```text
AQE partition coalescing -> too many small shuffle partitions
AQE skew handling -> skewed shuffle partitions
AQE join strategy / broadcast -> small table shuffle joins
Photon -> scans, native projections, joins, aggregates, sorts, writes
Delta stats/layout -> better pruning, stats, file sizing, join planning
UDF rewrite -> make logic optimizer/Photon friendly
```

Be careful:

```text
Photon does not magically speed up opaque UDF-heavy logic.
AQE does not fix every skew problem.
Databricks baseline is not the same as source-platform baseline.
```

## Quick Prompt Order

Use prompts from `client_spark_ui_troubleshooting_plan.md` in this order:

```text
1. Investigation Prompt
2. Runtime Configuration Prompt
3. Airflow Count Discovery Prompt
4. Physical Plan Bottleneck Prompt
5. Spark Stage Size Prompt
6. Bottleneck Stage Metrics Prompt
7. Databricks Count Baseline Prompt
```

## Current Demo Checkpoint

As of the Step 12 small run:

```text
Stage 38 was not skewed.
Stage 38 was not executor-imbalanced.
Stage 38 looked like too many small shuffle partitions for available parallelism.
Likely Databricks angle: AQE partition coalescing.
```
