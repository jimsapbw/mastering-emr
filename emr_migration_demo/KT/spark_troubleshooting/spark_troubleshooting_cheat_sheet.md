# Spark Troubleshooting Cheat Sheet

Use this as the quick mental model. Go to the detailed guides only when the current step needs more depth.

Detailed references:

```text
EMR Spark Troubleshooting Guide: emr_spark_troubleshooting_guide.md
Client Spark UI Troubleshooting Plan: client_spark_ui_troubleshooting_plan.md
Prompt Outcome Examples: client_spark_ui_troubleshooting_prompt_examples.md
```

## Index

| Section | Use When |
|---|---|
| [The Core Loop](#the-core-loop) | Remembering the overall Spark troubleshooting sequence. |
| [1. Find The Workload](#1-find-the-workload) | Tracing Airflow/EMR/Spark History application identity. |
| [2. Confirm Runtime Config](#2-confirm-runtime-config) | Capturing AQE, shuffle partitions, broadcast threshold, and executor settings. |
| [3. Find The Longest Job Or Query](#3-find-the-longest-job-or-query) | Choosing the first expensive query/job to inspect. |
| [4. Find The Longest Stages](#4-find-the-longest-stages) | Selecting and triaging bottleneck stages. |
| [5. Classify The Bottleneck](#5-classify-the-bottleneck) | Distinguishing skew, memory pressure, partition sizing, joins, sorts, and writes. |
| [6. Map Stage Back To Code](#6-map-stage-back-to-code) | Connecting stage/operator evidence to source action and transformation code. |
| [7. Databricks Translation](#7-databricks-translation) | Mapping evidence to AQE, Photon, Delta, stats, layout, and UDF opportunities. |
| [Quick Prompt Order](#quick-prompt-order) | Choosing which client troubleshooting prompt to run next. |
| [Current Demo Checkpoint](#current-demo-checkpoint) | Recalling the current small-run Stage 38 conclusion. |
| [Current Medium Checkpoint](#current-medium-checkpoint) | Recalling the MCBP medium disk-pressure state. |
| [Current Light-Medium Checkpoint](#current-light-medium-checkpoint) | Recalling the preferred 3x demo-load conclusion. |

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
-> command-runner.jar / spark-submit args
-> application JAR or Python file
-> --class / main class / entry point
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
active executor cores / approximate task slots
estimated task waves
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

### Too Many Small Partitions

```text
many tasks
small shuffle read per task
many task waves relative to available task slots
expected wave time explains much of stage duration
balanced executors
low spill
stage slow because tasks run in many waves
```

Mental model:

```text
Spark usually runs about 1 task per executor core.
active executor cores ~= concurrent task slots
task waves = stage tasks / concurrent task slots
expected wave time = task waves * median task duration
```

Example:

```text
200 tasks
12 active executor cores / task slots
200 / 12 = about 17 task waves
median shuffle read 2 MiB
max shuffle read 3.1 MiB
median task duration 4 s
expected wave time about 17 * 4 s = 68 s
observed stage duration about 96 s
balanced executors
AQE disabled
```

Interpretation:

```text
If tasks are tiny and healthy, and observed stage time is close to task waves * median task duration,
partition count is a strong suspect.
```

Likely Databricks angle:

```text
AQE partition coalescing
```

### Too Few Heavy Partitions

```text
few tasks
few task waves
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

### Balanced Partitions

```text
task waves are reasonable for available slots
median shuffle read is large enough to avoid tiny-task overhead
max is less than about 2x-3x median
GC and spill are low
executors are balanced
stage duration is close to wave estimate
```

For large workloads, healthy shuffle tasks are often tens to hundreds of MiB, but do not use one universal target. Judge by waves, bytes, duration, GC, spill, and skew together.

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
1. Airflow DAG Entry-Point Prompt
2. Runtime Configuration Prompt
3. Airflow Count Discovery Prompt
4. Physical Plan Bottleneck Prompt
5. Spark Stage Size Prompt
6. Bottleneck Stage Metrics Prompt
7. Stage Operator And Code Mapping Prompt
8. Use the matching source-side follow-up prompt before leaving the source analysis:
     Skew Follow-Up Prompt
     Memory Pressure Follow-Up Prompt
     Small Shuffle Partitions Follow-Up Prompt
     Feature-Log Join Cardinality Follow-Up Prompt, when a join fanout or grain issue is the best current classification
9. Use the matching Databricks-side prompt when moving from source findings to Databricks validation:
     Explain Insertion Prompt, to place explain in code or notebook
     Databricks Explain Plan Interpretation Prompt, after adding explain to the baseline
     Databricks Genie Explain Prompt Builder, if you want a focused Genie question about the explain plan
     Databricks Count Baseline Prompt, if source-side counts or join sizes are missing
     Databricks Baseline Run Interpretation Prompt, after the baseline run completes
     Databricks Genie Baseline Prompt Builder, if you want Genie to inspect baseline evidence
```

## Current Demo Checkpoint

As of the Step 12 small run:

```text
Stage 38 was not skewed.
Stage 38 was not executor-imbalanced.
Stage 38 used 200 tasks over 12 task slots, or about 17 task waves.
Median task duration was about 4 s, so expected wave time was about 68 s.
Observed stage duration was about 96 s.
Stage 38 looked like too many small shuffle partitions for available parallelism.
Likely Databricks angle: AQE partition coalescing.
```

## Current Medium Checkpoint

As of MCBP medium Step 3:

```text
Medium data exists and is validated under:
  s3://aigithub-emr-2026/emr-migration-demo-medium/

Medium Steps 1 and 2 completed:
  FeatureLogConverter
  EligibleUserDataLogConverter

Medium Step 3 attempt 1 failed:
  step id: s-0661117QH89RUU39ZQ1
  app id: application_1779541486316_0010
  action: count at BrbfJob.scala:80
  failed stage: Stage 37.0
  root cause: No space left on device

Medium Step 3 attempt 2 failed:
  step id: s-09085642ZQTGXNML2TKJ
  app id: application_1779541486316_0011
  controlled change: cluster scale-out plus --executor-cores 2
  observed before failure: Job 20 reached Stage 38 with about 11.8 GiB input over 200 tasks

Current decision:
  stop tuning blindly on this cluster
  create a restore checkpoint
  create a new cluster with larger worker local/EBS disk
  rerun only medium Step 3 first

New cluster target:
  EMR 7.13.0 / Spark 3.5.6-amzn-2 if possible
  1 primary node
  r8g.xlarge core nodes
  4 core nodes
  0 task nodes first
  On-Demand for first baseline rerun
  worker disk gp3, 300 GiB, 3000 IOPS, 125 MiB/s throughput, 1 volume per instance
  managed scaling enabled
  max cluster size 6, max core nodes 4, max On-Demand 6
  Spark event log enabled

Medium Step 3 first-rerun submit overrides:
  --conf spark.sql.shuffle.partitions=400
  --conf spark.executor.cores=2
  --conf spark.sql.adaptive.enabled=true
  --conf spark.sql.adaptive.coalescePartitions.enabled=true
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
  --conf spark.shuffle.compress=true
  --conf spark.shuffle.spill.compress=true

Important:
  verify AWS credentials and Maven availability before rerun work
  rebuild and upload the JAR first
  BrbfJob submit-time --conf overrides now work because in-code settings are defaults only
```

## Current Light-Medium Checkpoint

As of the completed light-medium run on cluster `j-3O78ZN9EMO9W2`:

```text
dataset prefix:
  s3://aigithub-emr-2026/emr-migration-demo-light-medium/

scale:
  3x small / lightmedium

BRBF Step 3:
  app id: application_1779641349593_0009
  step id: s-06984631PV9G98A51OTN
  runtime: about 12 minutes

dominant query:
  Query 8 / count at BrbfJob.scala:80
  duration: 7.5 minutes

dominant stage:
  Stage 63 / Job 38
  duration: 7.1 minutes
  tasks: 400
  shuffle read: 1101.9 MiB / 4,000,000 records
  median task duration: 95 ms
  p75 task duration: 21 s
  max task duration: 1.5 min

main finding:
  feature-log ShuffledHashJoin expands the joined path from about 3M rows to
  602.4M rows.

classification:
  feature-log join cardinality/fanout with Stage 63 task-duration stragglers
  not primarily source scan size
  not proven memory pressure or spill from the pasted metrics

code mapping:
  BrbfJob.scala:80 -> joined.count()
  BrbfJob.scala:51-78 -> joined DataFrame construction and persist
  BrbfJob.scala:57-62 -> featureLog join on user_id_hash/contextual_id
  FeatureLogConverter.scala:38 -> deduplicates by feature_event_id only

Databricks recommendation:
  if fanout is accidental, fix join grain / pre-aggregate / deduplicate / add a time window
  if fanout is intentional, optimize Delta layout/statistics, AQE/skew handling,
  Photon execution, and repeated actions over the expanded joined shape

Photon estimate with code unchanged:
  plan for about 1.2x-2x end-to-end
  treat 2x-3x as upside
  do not promise 5x because Photon cannot remove the 602.4M-row fanout

native expression follow-up:
  sha256String -> likely native sha2(...)
  roundTimestampToMinutes -> likely native timestamp math
  stableUuidFromString -> test carefully before rewriting
```
