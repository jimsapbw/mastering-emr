# Client Spark UI Troubleshooting Plan

## Purpose

Use this plan when starting a migration or performance assessment in a client environment.

The goal is to collect enough Spark runtime evidence to understand the current workload before proposing Databricks, Photon, Delta, or code-level optimizations.

## Preferred Access

Ask for access to the current platform's Spark observability surfaces:

```text
Live Spark UI
Spark History Server
YARN, Kubernetes, or cluster application history
Driver logs
Executor logs
Orchestration logs, such as Airflow
Input and output storage locations
Table metadata and file layout details
```

Live Spark UI is useful while a job is running. Spark History Server is usually more important for assessment work because completed applications can be reviewed after the run.

## First Step: Trace The Airflow Task

When starting from a client Airflow DAG repository, first identify the task that launches or references the Spark or EMR workload.

Find:

```text
Airflow DAG id
Airflow task id
operator type
AWS account and region
EMR cluster id, cluster name, or cluster creation config
whether the cluster is long-running or transient
Spark submit command or EMR step definition
JAR, Python file, or main class
input paths
output paths
runtime arguments
Airflow connection id
IAM role or instance profile
retry and timeout behavior
```

Search the DAG repo for:

```text
EmrAddStepsOperator
EmrCreateJobFlowOperator
EmrServerlessStartJobOperator
EmrContainerOperator
SparkSubmitOperator
BashOperator
PythonOperator
DatabricksSubmitRunOperator
job_flow_id
cluster_id
emr
add_steps
spark-submit
command-runner.jar
--class
s3://
main_class
steps
```

Classify the launch pattern:

```text
Existing EMR cluster:
  Find job_flow_id, cluster_id, cluster name, Airflow variable, or connection lookup.

Transient EMR cluster:
  Inspect EmrCreateJobFlowOperator and the job flow overrides.

EMR Serverless:
  Find application_id, execution role, job driver, and configuration overrides.

EMR on EKS:
  Find virtual_cluster_id, execution role, release label, and job driver.

Direct Spark submit:
  Find master URL, deploy mode, app resource, class, packages, and arguments.
```

Then build the trace:

```text
Airflow DAG run
-> Airflow task instance
-> EMR step id or Spark submission
-> YARN application id
-> Spark History Server application
-> Spark jobs
-> Spark stages
-> SQL physical operators
```

This trace is the backbone of the performance investigation.

## Second Step: Confirm Runtime Configuration

After identifying the Airflow task and EMR or Spark target, confirm the runtime settings from Spark UI or Spark History Server:

```text
Spark UI -> Environment
```

Capture the settings that directly affect the performance interpretation:

```text
spark.sql.adaptive.enabled
spark.sql.adaptive.skewJoin.enabled
spark.sql.shuffle.partitions
spark.sql.autoBroadcastJoinThreshold
```

Also capture supporting context:

```text
spark.default.parallelism
spark.serializer
spark.sql.parquet.compression.codec
executor memory
executor cores
driver memory
dynamic allocation settings
```

Why this matters:

```text
AQE disabled:
  The current workload is using a more static Spark plan. Databricks AQE may provide an immediate optimization path.

Skew join handling disabled:
  Skew seen in stage/task metrics is not being automatically corrected by Spark.

Fixed shuffle partitions:
  A single global shuffle partition value may be too high for some stages and too low for others.

Broadcast threshold:
  Explains why small dimensions may use BroadcastHashJoin while larger joins use SortMergeJoin.
```

Why investigate broadcast behavior:

```text
If the current Spark plan does not broadcast genuinely small dimension tables, Databricks may have an optimization opportunity through AQE, better statistics, or explicit broadcast tuning.

Source-plan evidence:
  small table scan
  SortMergeJoin or ShuffledHashJoin
  Exchange on both join sides
  high shuffle read/write in the join stage

Databricks target evidence:
  BroadcastExchange
  BroadcastHashJoin
  fewer Exchange stages
  lower shuffle read/write
  shorter join stage
```

Build a join and table-size inventory:

```text
For each important join, capture:
  left dataset
  right dataset
  left row count
  right row count
  left input size
  right input size
  join keys
  join type
  physical join operator
  broadcasted side, if any
  Exchange on each side
  related stage shuffle read/write
  related stage spill
```

Why this matters:

```text
Small table + shuffle join:
  possible Databricks AQE/statistics/broadcast opportunity.

Large table broadcasted:
  possible memory pressure risk.

Missing stats:
  optimizer may choose conservative joins.

Broadcast already working:
  Databricks benefit may come more from Photon, AQE partition coalescing, skew handling, Delta layout, or UDF removal.
```

Examples to look for in Spark History Server:

```text
Too few shuffle partitions:
  200 tasks, large shuffle read per task, long task duration, spill.

Too many shuffle partitions:
  thousands of tiny tasks, little data per task, scheduler overhead.

Skew:
  median task duration is small, but max task duration is much larger.
  one task reads far more shuffle data than the others.
```

Migration framing:

```text
Current Spark or EMR baseline:
  fixed shuffle partition count, AQE disabled or limited, manual skew handling.

Databricks target:
  AQE can coalesce small shuffle partitions, split skewed shuffle partitions, and sometimes change join strategy at runtime.
```

## Recommended Source Of Truth

For the current production platform, use:

```text
Spark History Server:
  jobs, stages, SQL plans, shuffle, spill, skew, executor behavior

YARN or cluster application history:
  application id, lifecycle, status, container links, logs

Orchestration logs:
  dependency order, retries, schedules, end-to-end DAG runtime
```

YARN history and Spark History Server are related but not equivalent. YARN confirms the application lifecycle. Spark History Server explains the Spark execution internals.

## If Current Spark UI Access Is Not Available

If the client cannot provide current-platform Spark UI or Spark History Server access at first, run the workload on Databricks as a non-optimized baseline.

This is still useful for understanding workload shape:

```text
longest jobs
longest stages
shuffle read and write
memory and disk spill
task duration skew
input and output sizes
join strategies
physical operators
executor pressure
```

However, treat this as a Databricks baseline, not as a precise source-system baseline.

Databricks differs from EMR or other Spark platforms in:

```text
runtime libraries
default Spark settings
cluster sizing
scheduler behavior
cloud storage integration
AQE behavior
I/O optimizations
Photon availability
```

## Baseline Sequence

Use three comparison layers:

```text
1. Source-platform baseline
   Current production or current EMR/Spark behavior.

2. Databricks non-optimized baseline
   Same code and same data shape, with minimal migration changes.

3. Databricks optimized target
   Delta, AQE, Photon, layout tuning, join tuning, and code improvements.
```

This keeps the analysis honest:

```text
Source platform:
  What is slow today?

Databricks baseline:
  How does the migrated workload behave before optimization?

Databricks optimized:
  Which improvements come from platform features and code changes?
```

## Databricks Non-Optimized Baseline

When first moving code to Databricks for analysis, keep the first run conservative:

```text
use the same input data
preserve the same row counts
keep job logic as close as practical
avoid early rewrites unless required for compatibility
record Spark UI evidence
disable Photon if the goal is a Spark-to-Spark comparison
document any unavoidable config differences
```

The purpose is not to make the job fast yet. The purpose is to understand where the migrated workload spends time.

## Databricks Optimized Target

After the baseline is understood, run an optimized pass:

```text
enable Photon where appropriate
enable and review AQE behavior
replace UDFs with native expressions where practical
review broadcast and shuffle join strategies
use Delta tables when appropriate
tune file sizes and table layout
remove unnecessary repartitions
reduce avoidable sorts
handle skew with platform features before custom salting
```

## Operator Evidence To Collect

In Spark UI or Spark History Server, inspect the SQL tab and physical plans.

Current Spark operators to capture:

```text
FileScan parquet
Project
Filter
Exchange
BroadcastExchange
BroadcastHashJoin
SortMergeJoin
HashAggregate
SortAggregate
Sort
Union
WriteFiles
ScalaUDF
PythonUDF
```

The most important operator for shuffle diagnosis is:

```text
Exchange
```

`Exchange` usually means a shuffle was introduced by:

```text
join
groupBy
distinct
dropDuplicates
repartition
sort
countDistinct
```

## Photon Mapping

EMR and open-source Spark UIs do not show Photon operators. Photon is Databricks-specific.

The useful migration move is to identify Spark operators that may become Photon-friendly later:

```text
Parquet or Delta scans
native projections
filters
hash joins
aggregations
sorts
some writes
```

Less friendly areas:

```text
Scala UDF-heavy projections
Python UDF-heavy projections
opaque row-by-row transformations
manual salting that creates extra shuffles
manual repartitions that fight the optimizer
unnecessary global sorts
```

A common migration story:

```text
Current Spark baseline:
  FileScan -> UDF Project -> Exchange -> Join -> Exchange -> Aggregate -> Sort -> Write

Databricks target:
  Delta/Parquet scan -> native expressions -> AQE joins/skew handling -> Photon operators -> optimized write
```

## Troubleshooting Checklist

For each important application, collect:

```text
application id
job or DAG name
cluster shape
input scale
total runtime
longest Spark job
top three longest stages
shuffle read and write
memory spill
disk spill
task duration skew
executor imbalance
join strategies
physical operators
final output row count
```

Then classify the bottleneck:

```text
input scan
UDF projection
join shuffle
manual repartition
aggregation
countDistinct
sort
write
skew
memory spill
executor imbalance
orchestration overhead
```

## Client Conversation Framing

Use careful language when current-platform Spark UI is unavailable:

```text
We can begin with a Databricks baseline to understand workload shape, but the current production Spark History Server remains the best source for current-state performance evidence.
```

Use careful language when comparing runtimes:

```text
The first Databricks run is a migration baseline, not a final optimized target. Runtime differences may reflect cluster shape, Spark defaults, I/O behavior, and platform differences.
```

## Investigation Prompt

Use this prompt when reviewing the Airflow DAG repo:

```text
I am investigating an Airflow-managed Spark/EMR workload for migration and performance analysis.

Please inspect this DAG repository and identify the first complete execution path from Airflow to Spark:

1. Find the DAG and task that launches the workload.
2. Identify the operator type and whether it uses EMR steps, transient EMR cluster creation, EMR Serverless, EMR on EKS, direct spark-submit, or Databricks.
3. Extract the cluster or application target, including cluster id/name lookup, AWS region, Airflow connection, IAM role, and whether the cluster is long-running or created per run.
4. Extract the Spark workload details: main class or script, JAR or Python path, deploy mode, Spark configs, input paths, output paths, runtime arguments, retries, and timeout settings.
5. Show how to map one Airflow task run to the Spark observability chain:
   Airflow task instance -> EMR step id or Spark submission -> YARN application id -> Spark History Server application -> jobs/stages/SQL operators.
6. List any missing access or metadata needed to complete the trace.
7. Do not optimize yet. Only document the current-state execution path and evidence to collect.
```

## Runtime Configuration Prompt

Use this prompt after identifying the Spark application in the client environment:

```text
I am investigating a Spark application for migration and performance analysis.

Please help me confirm the runtime configuration from Spark UI or Spark History Server.

Application context:
- Spark application name:
- Spark application id:
- Airflow DAG id:
- Airflow task id:
- EMR step id or Spark submission id:
- Cluster id/name:

Tasks:

1. Open Spark History Server or live Spark UI for the target application.
2. Go to Environment -> Spark Properties and search for:
   - spark.sql.adaptive.enabled
   - spark.sql.adaptive.skewJoin.enabled
   - spark.sql.shuffle.partitions
   - spark.default.parallelism
   - spark.sql.autoBroadcastJoinThreshold
   - spark.serializer
   - spark.sql.parquet.compression.codec
   - executor memory
   - executor cores
   - driver memory
   - dynamic allocation settings
3. If key SQL settings are not visible in Environment, open the SQL tab.
4. Click the longest SQL/DataFrame query and check query details for runtime configs and the physical plan.
5. Check whether the physical plan contains AdaptiveSparkPlan.
6. In the Stages tab, confirm whether shuffle-heavy stages have task counts that match spark.sql.shuffle.partitions.
7. Record whether settings were observed directly, inferred from SQL plan behavior, or inferred from stage task counts.

Return the findings in this table:

Setting                                      Value                  Evidence source
spark.sql.adaptive.enabled                   ?                      Environment / SQL plan / not found
spark.sql.adaptive.skewJoin.enabled          ?                      Environment / SQL query details / not found
spark.sql.shuffle.partitions                 ?                      Environment / SQL query details / stage task count
spark.default.parallelism                    ?                      Environment / SQL query details / not found
spark.sql.autoBroadcastJoinThreshold         ?                      Environment / SQL query details / default unknown
spark.serializer                             ?                      Environment / not found
spark.sql.parquet.compression.codec          ?                      Environment / not found
executor memory                              ?                      Environment
executor cores                               ?                      Environment
driver memory                                ?                      Environment
dynamic allocation                           ?                      Environment

Then interpret the result:

- If AQE is disabled, note that Databricks AQE may be an optimization path.
- If skew join handling is disabled, note that Databricks skew handling may help if stage metrics show skew.
- If shuffle partitions are fixed, compare the configured number with shuffle stage task counts.
- If tasks are heavy and spilling, note possible under-partitioning or skew.
- If tasks are tiny and numerous, note possible over-partitioning or scheduler overhead.
- If median task duration is small but max task duration is much larger, note likely skew.

Do not optimize yet. Only document the current runtime configuration and how it affects interpretation of Spark UI bottlenecks.
```

## Airflow Count Discovery Prompt

Use this prompt when you have access to the client Airflow DAG repository and task logs:

```text
I am investigating table sizes and row counts for an Airflow-managed Spark/EMR workload.

Please inspect the Airflow DAG code and available task logs to find current-state count and size evidence.

Context:
- DAG id:
- Task id:
- Execution date/run id:
- Spark application or EMR step, if known:
- Input tables/paths, if known:
- Output tables/paths, if known:

Tasks:

1. Identify tasks that launch the Spark job and tasks that validate or audit it.
2. Search DAG code and task logs for:
   - count
   - row_count
   - input_count
   - output_count
   - records
   - written
   - validation
   - audit
   - reconcile
   - dq
   - source_count
   - target_count
3. Extract any table-level or partition-level counts.
4. Extract exact input and output paths or table names, including partition filters.
5. Identify whether counts are produced before the Spark job, after the Spark job, or by a separate validation step.
6. Map each count to the join/table-size inventory:
   - dataset name
   - path/table
   - partition/date/hour
   - row count
   - source of count
   - confidence level
7. List missing counts and the safest next way to get them.

Return a table:

Dataset         Path/table         Partition         Row count         Count source         Confidence         Missing follow-up
?               ?                  ?                 ?                 Airflow log/DAG      high/medium/low    ?

Do not run new queries. Only use DAG code, task definitions, and existing logs unless explicitly approved.
```

## Physical Plan Bottleneck Prompt

Use this prompt after finding the longest SQL/DataFrame query in Spark UI or Spark History Server and copying its physical plan:

```text
I am analyzing the physical plan for the longest Spark SQL/DataFrame query in a client workload.

Please review the physical plan and identify likely bottlenecks, evidence to validate in Spark UI, and Databricks optimization opportunities.

Context:
- Current platform:
- Spark application name:
- Spark application id:
- Airflow DAG/task:
- Input scale:
- Cluster shape:
- Runtime settings, if already known:
  - spark.sql.adaptive.enabled =
  - spark.sql.adaptive.skewJoin.enabled =
  - spark.sql.shuffle.partitions =
  - spark.default.parallelism =
  - spark.sql.autoBroadcastJoinThreshold =
  - spark.serializer =
  - spark.sql.parquet.compression.codec =
  - executor cores/memory =
  - driver memory =
  - dynamic allocation =

Physical plan:
<paste physical plan here>

Please produce:

1. Runtime configuration capture
   If runtime settings are provided, interpret them.
   If they are not provided, tell me exactly where to find them:
   - Environment -> Spark Properties
   - SQL query details
   - physical plan clues
   - stage task counts
   Capture:
   - spark.sql.adaptive.enabled
   - spark.sql.adaptive.skewJoin.enabled
   - spark.sql.shuffle.partitions
   - spark.default.parallelism
   - spark.sql.autoBroadcastJoinThreshold
   - spark.serializer
   - spark.sql.parquet.compression.codec
   - executor cores/memory
   - driver memory
   - dynamic allocation

2. Operator inventory
   List the major operators in execution order:
   - FileScan
   - Project
   - Filter
   - UDF / ScalaUDF / PythonUDF
   - Exchange
   - BroadcastExchange
   - BroadcastHashJoin
   - ShuffledHashJoin
   - SortMergeJoin
   - HashAggregate
   - SortAggregate
   - Union
   - Sort
   - WriteFiles

3. Shuffle map
   Identify every Exchange and explain what caused it:
   - join
   - groupBy
   - countDistinct
   - dropDuplicates / deduplicate
   - repartition
   - sort / orderBy
   - write preparation

4. Join and table-size inventory
   Build a table with one row per important join.
   For each important join, capture:
   - left dataset
   - right dataset
   - left row count, if known
   - right row count, if known
   - left input size, if visible
   - right input size, if visible
   - size evidence source for each side
   - join keys
   - join type
   - physical join operator
   - broadcasted side, if any
   - Exchange on left side
   - Exchange on right side
   - related stage shuffle read/write
   - related stage spill
   If a row count or input size is not available from the physical plan, mark it as pending and tell me exactly where to get it:
   - Spark SQL scan metrics
   - Spark stage input metrics
   - Spark stage shuffle metrics
   - table statistics
   - storage/file listing
   - validation job counts
   - source query counts
   Use this table format:

   Join                         Left rows/size        Right rows/size       Size evidence source       Operator              Broadcast?       Next size check
   <left> -> <right>             ?                     ?                     pending / known            ?                     ?                ?

   Then classify:
   - broadcast is working as expected
   - possible broadcast/AQE/statistics opportunity
   - possible memory risk from broadcasting too much data
   - stats or table metadata missing

5. Bottleneck hypotheses
   For each expensive-looking operator or Exchange, state what Spark UI evidence should confirm or reject it:
   - long stage duration
   - high shuffle read/write
   - memory spill
   - disk spill
   - skewed task durations
   - high GC time
   - tiny-task scheduler overhead
   - large output write time

6. Stage evidence checklist
   Tell me exactly which Spark UI tabs to inspect next:
   - Jobs
   - Stages
   - Stage detail task distribution
   - SQL tab linked stages
   - Executors
   And what metrics to capture from each.

7. Databricks optimization path
   For each bottleneck, propose the Databricks-side improvement category:
   - AQE partition coalescing
   - AQE skew join handling
   - join strategy changes
   - broadcast tuning
   - removing manual repartitioning
   - replacing UDFs with native expressions
   - Delta table layout or file size tuning
   - ZORDER / clustering / liquid clustering where appropriate
   - reducing unnecessary sort
   - optimizing countDistinct or aggregation strategy

8. Photon mapping
   Identify operators that may benefit from Photon:
   - scans
   - filters
   - projections using native expressions
   - joins
   - aggregations
   - sorts
   - writes
   Also identify operators or code patterns that are less Photon-friendly:
   - Scala UDF
   - Python UDF
   - opaque row-by-row transformations
   - manual salting/repartitioning that forces extra shuffles

9. Prioritized findings
   Rank bottlenecks by likely impact:
   - high
   - medium
   - low
   For each, give the current evidence, missing evidence, and recommended next investigation step.

10. Migration story
   Summarize the current-state Spark execution pattern and the Databricks target-state pattern in plain language for a client-facing migration narrative.

Important:
- Do not claim Photon will accelerate unsupported UDF logic.
- Do not assume AQE fixes all skew; tie claims to stage/task evidence.
- Distinguish confirmed evidence from hypotheses.
- Keep source-platform baseline and Databricks optimized target separate.
```

## Spark Stage Size Prompt

Use this prompt when you have Spark History Server access and need to infer table or join-side sizes from SQL and stage metrics:

```text
I am investigating table sizes and join-side sizes from Spark History Server.

Please help me use SQL and Stage metrics to populate a join/table-size inventory.

Context:
- Spark application name:
- Spark application id:
- Longest SQL query id/name:
- Physical plan, if available:
- Known input tables/paths:

Tasks:

1. Open the SQL tab and identify FileScan nodes for each input dataset.
2. Capture any SQL metrics shown for scans, broadcasts, joins, aggregates, and writes:
   - number of output rows
   - data size
   - files read
   - scan time
   - broadcast size
3. Open the stages linked to the longest SQL query.
4. For each join-related stage, capture:
   - stage id
   - duration
   - number of tasks
   - input records
   - input size
   - shuffle read records
   - shuffle read size
   - shuffle write records
   - shuffle write size
   - memory spill
   - disk spill
5. Map stage evidence back to joins in the physical plan:
   - BroadcastHashJoin
   - BroadcastExchange
   - ShuffledHashJoin
   - SortMergeJoin
   - Exchange nodes
6. Mark any table size or join-side size that cannot be determined from Spark UI as pending.
7. Recommend the next safest source for missing sizes:
   - Airflow logs
   - S3/object storage listing
   - Glue/Hive table stats
   - Athena/source SQL count
   - EMR notebook/spark-shell
   - Databricks notebook baseline

Return a table:

Join                  Left size evidence       Right size evidence       Operator              Stage evidence              Missing size follow-up
?                     ?                        ?                         ?                     ?                           ?

Distinguish actual metrics from estimates. Do not infer exact row counts from bytes unless clearly labeled as an estimate.
```

## Bottleneck Stage Metrics Prompt

Use this prompt after identifying a long-running stage and copying its Summary Metrics plus Aggregated Metrics by Executor:

```text
I am analyzing a suspected bottleneck Spark stage from Spark UI / Spark History Server.

Please classify the bottleneck using the stage Summary Metrics and Aggregated Metrics by Executor.

Context:
- Spark application name:
- Spark application id:
- SQL query id/name:
- Job id:
- Stage id:
- Stage description:
- Total stage duration:
- Number of tasks:
- Runtime settings, if known:
  - spark.sql.adaptive.enabled =
  - spark.sql.adaptive.skewJoin.enabled =
  - spark.sql.shuffle.partitions =
  - executor cores/memory =
  - dynamic allocation =
- Physical plan operator or Exchange related to this stage, if known:

Summary Metrics:
<paste Summary Metrics table here>

Aggregated Metrics by Executor:
<paste Aggregated Metrics by Executor table here>

Please produce:

1. Stage classification
   Classify the stage as one or more of:
   - data skew
   - too many small shuffle partitions
   - too few/heavy shuffle partitions
   - memory pressure / spill
   - GC pressure
   - executor imbalance
   - scheduler/task-wave overhead
   - expensive join
   - expensive aggregation/countDistinct
   - expensive sort
   - expensive write

2. Evidence table
   Summarize the evidence:
   - median task duration
   - max task duration
   - max/median duration ratio
   - median shuffle read
   - max shuffle read
   - max/median shuffle ratio
   - memory spill
   - disk spill
   - GC time
   - executor task distribution
   - executor shuffle distribution

3. Skew decision
   Decide whether this stage is skewed.
   Use directional thresholds:
   - healthy: max is less than about 2x-3x median
   - watch closely: max is about 3x-5x median
   - likely skew: max is greater than about 5x-10x median
   Explain the decision.

4. Partitioning decision
   Decide whether this looks like:
   - too many small partitions
   - too few large partitions
   - appropriate partitioning
   - cannot tell yet
   Consider number of tasks, shuffle read per task, stage duration, and available executor task slots.

5. Executor balance decision
   Compare executors:
   - tasks per executor
   - total task time
   - shuffle read per executor
   - spill per executor
   - failed/killed tasks
   Decide whether one executor or host is causing the bottleneck.

6. Databricks optimization mapping
   Map the finding to possible Databricks improvements:
   - AQE partition coalescing
   - AQE skew join handling
   - Photon joins/aggregations/sorts/scans
   - better table statistics
   - Delta layout/file-size tuning
   - broadcast tuning
   - reducing manual repartitioning
   - increasing/decreasing shuffle partitions
   - executor sizing changes

7. Next investigation step
   Tell me exactly what to inspect next:
   - another stage
   - task table sorted by duration
   - SQL physical plan operator
   - Executors tab
   - storage/table counts
   - Airflow logs

Important:
- Distinguish confirmed evidence from hypotheses.
- Do not call a stage skewed just because it is long-running.
- Do not call a stage over-partitioned unless per-task data is small and task-wave/scheduler overhead is plausible.
- Do not claim Databricks will improve it without explaining which feature maps to the evidence.
```

## Databricks Count Baseline Prompt

Use this prompt when source-side counts are unavailable or when creating the Databricks migration baseline:

```text
I am using Databricks to calculate table counts and join-key cardinalities for a Spark migration baseline.

Please create a conservative notebook plan that reads the same source data and captures table-size evidence without changing the workload logic.

Context:
- Source paths or tables:
- Partition filters:
- Join keys:
- Current source platform:
- Databricks cluster/runtime:
- Whether Photon is enabled:

Tasks:

1. Read each source table/path with the same partition filters used by the production job.
2. For each dataset, calculate:
   - row count
   - input file count
   - approximate data size if available
   - approximate distinct count for join keys
   - null count for join keys
3. For each important join, produce:
   - left row count
   - right row count
   - left join-key distinct count
   - right join-key distinct count
   - expected small side
   - candidate broadcast side
4. Label this as Databricks migration-baseline evidence, not source-platform runtime evidence.
5. Recommend which counts should later be validated against source Airflow logs, Spark History metrics, or audit tables.

Example output table:

Dataset         Partition/filter       Rows       Files       Approx bytes       Join key        Distinct keys       Null keys
?               ?                      ?          ?           ?                  ?               ?                   ?

Join            Left rows       Right rows       Candidate broadcast side       Reason       Needs source validation?
?               ?               ?                ?                              ?            yes/no

Do not claim performance improvement from these counts alone. Use them to support join strategy and broadcast/AQE investigation.
```
