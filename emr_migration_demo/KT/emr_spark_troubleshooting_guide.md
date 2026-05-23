# EMR Spark Troubleshooting Guide

## Purpose

This guide explains how to troubleshoot the EMR baseline jobs and collect evidence for the migration problem statement.

The goal is to identify where the pipeline spends time, which Spark operators are expensive, and which parts may improve later on Databricks.

## Main Tooling

Use the Spark UI while the application is running, or Spark History Server after the application finishes.

Important Spark UI tabs:

```text
Jobs
Stages
SQL
Executors
Environment
```

## Live UI Vs History UI

Use the live Spark UI only while the Spark application is still running.

Current demo cluster details:

```text
Cluster name: itv-github-dev-cluster
Cluster ID: j-37DIRU3WHU1C5
Region: us-east-1
EMR version: emr-7.13.0
Spark version: 3.5.6
Primary node public DNS: ec2-100-55-172-52.compute-1.amazonaws.com
Log destination: s3://aws-logs-210570462212-us-east-1/elasticmapreduce/
CloudWatch log group: /aws/emr/j-37DIRU3WHU1C5
```

In the AWS EMR console:

```text
EMR Console
-> Clusters
-> target cluster
-> Application user interfaces
```

If a Spark application is running, EMR may show a direct Spark UI link.

If the console only shows YARN ResourceManager and Spark History Server, open:

```text
YARN ResourceManager
-> Applications
-> running Spark application
-> ApplicationMaster or Tracking UI
```

That link usually opens the live Spark UI while the application is active.

If the YARN application shows:

```text
Tracking UI = Unassigned
```

refresh the YARN page a few times. This means YARN knows about the running application, but the live Spark UI link has not been assigned or displayed yet.

For direct access to the live Spark UI, try the Spark driver UI ports on the primary node:

```text
http://ec2-100-55-172-52.compute-1.amazonaws.com:4040
http://ec2-100-55-172-52.compute-1.amazonaws.com:4041
http://ec2-100-55-172-52.compute-1.amazonaws.com:4042
```

Spark usually starts with port `4040`. If that port is already in use by another Spark application or shell, the next application may use `4041`, `4042`, and so on.

After the Spark application finishes:

```text
Live Spark UI is no longer available.
Use Spark History Server instead.
```

Post-run troubleshooting tools:

```text
EMR step history
YARN completed application history
Spark History Server
YARN logs
S3 output validation
```

Useful commands after a run completes:

```bash
aws emr list-steps \
  --region us-east-1 \
  --cluster-id <cluster-id> \
  --query 'Steps[*].[Id,Name,Status.State,Status.Timeline.StartDateTime,Status.Timeline.EndDateTime]' \
  --output table
```

```bash
yarn application -list -appStates FINISHED,FAILED,KILLED
```

```bash
yarn logs -applicationId <application_id> | less
```

## Troubleshooting Workflow

### 1. Confirm Runtime Configuration

Go to:

```text
Spark UI -> Environment
```

Confirm key baseline settings such as:

Important for this migration analysis:

```text
spark.sql.adaptive.enabled
spark.sql.adaptive.skewJoin.enabled
spark.sql.shuffle.partitions
spark.sql.autoBroadcastJoinThreshold
```

Nice to have for supporting context:

```text
spark.default.parallelism
spark.serializer
spark.sql.parquet.compression.codec
```

For the EMR baseline, we expect AQE to be disabled or not relied on when running the final before-DAG workload.

Why the important settings matter:

```text
spark.sql.adaptive.enabled
  If false, Spark is using a more static plan. This gives Databricks an immediate optimization opening with AQE.

spark.sql.adaptive.skewJoin.enabled
  If false, Spark is not automatically splitting skewed shuffle partitions. Skew symptoms become stronger migration evidence.

spark.sql.shuffle.partitions
  Controls default SQL shuffle parallelism for joins, groupBy, distinct, dropDuplicates, repartition, sort, and countDistinct.

spark.sql.autoBroadcastJoinThreshold
  Helps explain why Spark chose BroadcastHashJoin versus SortMergeJoin.
```

Broadcast join investigation:

```text
If small dimension tables are joined with large fact tables but the physical plan shows SortMergeJoin or ShuffledHashJoin, Spark may be doing unnecessary shuffle work.

In Databricks, AQE plus table statistics may allow BroadcastHashJoin for safe small-side joins, reducing Exchange stages and shuffle read/write.

If spark.sql.autoBroadcastJoinThreshold is not visible, use the physical plan as the source of truth:
  BroadcastHashJoin / BroadcastExchange means Spark broadcasted a join side.
  SortMergeJoin or ShuffledHashJoin with Exchanges on both sides may indicate a broadcast opportunity.
```

How to interpret shuffle partition symptoms:

```text
Too few shuffle partitions:
  fewer, heavier tasks
  large shuffle read per task
  long task durations
  memory or disk spill

Too many shuffle partitions:
  many tiny tasks
  small shuffle read per task
  scheduler overhead
  possible small output files

Skew:
  most tasks finish quickly
  a few tasks take much longer
  max task duration is far above median task duration
  one or a few tasks read much more shuffle data than the rest
```

`spark.default.parallelism` is still useful to record, but for DataFrame-heavy Spark SQL workloads, `spark.sql.shuffle.partitions` is usually the more important first setting.

How to find these settings:

```text
1. Open Spark History Server.
2. Open the target Spark application.
3. Go to Environment -> Spark Properties.
4. Search for the setting name.
5. If a setting is not shown in Environment, open the SQL tab.
6. Click the longest SQL/DataFrame query.
7. Look for runtime SQL configs and physical plan details in the query page.
```

In this demo, some settings are set inside `BrbfJob.scala` after the Spark session starts:

```scala
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.default.parallelism", "200")
```

These may not always appear in the Environment tab. If they are missing there, verify them from the SQL query details and stage behavior.

Helpful cross-checks:

```text
AQE disabled:
  SQL physical plan should not show AdaptiveSparkPlan.

AQE enabled:
  SQL physical plan often shows AdaptiveSparkPlan.

shuffle.partitions = 200:
  shuffle-heavy SQL stages often show around 200 tasks.

skewJoin disabled:
  skewed task distributions are not being automatically split by Spark AQE skew handling.
```

Capture the result in this format:

```text
Setting                                      Value / Evidence
spark.sql.adaptive.enabled                   false
spark.sql.adaptive.skewJoin.enabled          false
spark.sql.shuffle.partitions                 200
spark.default.parallelism                    200
spark.sql.autoBroadcastJoinThreshold         not shown / default / configured value
Evidence source                              SQL query details, Environment tab, or stage task count
```

### 2. Build Join And Table Size Inventory

Before diagnosing join bottlenecks, build an inventory of the important joins and the approximate size of each side.

Capture:

```text
join name or code location
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
Exchange on left side
Exchange on right side
shuffle read/write in related stage
spill in related stage
```

Useful sources:

```text
Spark SQL physical plan:
  FileScan nodes
  BroadcastExchange
  BroadcastHashJoin
  ShuffledHashJoin
  SortMergeJoin
  Exchange nodes

Spark UI Stages:
  input size
  shuffle read/write
  task count
  memory spill
  disk spill

Storage or table metadata:
  S3 file sizes
  table statistics
  partition counts
  row counts from validation jobs
```

How to get table counts and sizes:

Preferred current-state sources:

```text
Airflow DAG and task logs:
  Look for validation tasks, audit writes, printed row counts, source counts, output counts, and reconciliation checks.

Spark History Server SQL metrics:
  Look for FileScan metrics, output rows, scan size, broadcast size, and records read from the actual run.

Spark History Server stage metrics:
  Look for input records, input size, shuffle read records, shuffle read size, output records, and output size.
```

Useful source-system options if available:

```text
S3 or object storage listing:
  Get total bytes and file counts for exact input partitions.

Glue/Hive metastore:
  Get table locations, partition lists, and any available table or partition statistics.

Athena or source SQL:
  Run count queries and approximate distinct counts for join keys, if approved.

EMR notebook, spark-shell, or attached source cluster:
  Read the exact partition and run count or distinct-count checks, if approved.
```

Fallback migration-baseline option:

```text
Databricks notebook:
  Read the same source paths and calculate counts, file sizes, and join-key cardinalities.
  Treat this as migration-baseline evidence, not the original source-platform runtime evidence.
```

Example checks:

```bash
aws s3 ls s3://bucket/path/to/table/partition/ --recursive --summarize
```

```sql
SELECT count(*) FROM db.table WHERE ds = '<partition>';
SELECT approx_distinct(join_key) FROM db.table WHERE ds = '<partition>';
```

```scala
val df = spark.read.parquet("s3://bucket/path/to/table/partition/")
df.count()
df.select("join_key").distinct().count()
df.inputFiles.length
```

Interpretation:

```text
Small right side + BroadcastHashJoin:
  broadcast is happening as expected.

Small right side + SortMergeJoin/ShuffledHashJoin + Exchanges:
  possible broadcast/AQE/statistics opportunity.

Large side broadcasted:
  possible executor memory pressure risk.

Missing table statistics:
  optimizer may choose conservative shuffle joins.

UDF/filter before join:
  optimizer may have poor size estimates or reduced visibility.
```

For the migration story, this inventory helps separate:

```text
joins that already behave well on EMR
joins that may benefit from Databricks AQE and better stats
joins that may benefit from Photon execution
joins that need code or layout changes
```

### 3. Find The Longest Job

Go to:

```text
Spark UI -> Jobs
```

Look for:

```text
longest duration
failed or retried jobs
jobs with many stages
jobs triggered by count/write/show actions
```

For our converters, `count()` and `write` actions create Spark jobs.

For the main BRBF job, the final write will likely trigger the heaviest work.

### 4. Find Expensive Stages

Go to:

```text
Spark UI -> Stages
```

Capture:

```text
stage duration
number of tasks
shuffle read
shuffle write
memory spill
disk spill
input size
output size
task time variance
failed/retried tasks
```

Important symptoms:

```text
Large shuffle read/write -> expensive Exchange/repartition/join/aggregate/sort
High disk spill -> memory pressure or large shuffle/aggregate
Few very slow tasks -> skew
Many tiny tasks -> too many small partitions/files
Very large tasks -> under-partitioning or skew
```

This is where we apply the `spark.sql.shuffle.partitions` check from Step 1:

```text
too few partitions -> heavy tasks, high shuffle per task, long duration, spill
too many partitions -> many tiny tasks, low data per task, scheduler overhead
skew -> median task is reasonable, but max task duration or max shuffle read is much larger
```

Second-pass metrics:

Start with the basic metrics first:

```text
duration
GC time
shuffle read size / records
shuffle write size / records
memory spill
disk spill
executor balance
```

If the bottleneck is still unclear, enable additional task metrics:

```text
Scheduler Delay:
  useful when tasks are tiny but the stage is slow; supports scheduler/task-wave overhead.

Shuffle Read Fetch Wait Time:
  useful when shuffle read is high; shows time spent waiting for shuffle blocks.

Shuffle Remote Reads:
  useful with fetch wait time; high remote reads can explain slow shuffle stages.

Shuffle Write Time:
  useful when a stage writes a large shuffle.

Peak Execution Memory:
  useful for joins, aggregations, memory pressure, and spill investigation.

Task Deserialization Time:
  useful if task startup or large serialized closures are suspected.

Result Serialization Time / Getting Result Time:
  useful when collecting large results to the driver; usually lower priority for write/count jobs.

OnHeap / OffHeap / Direct / Mapped memory metrics:
  useful for deeper memory troubleshooting when GC, spill, or native memory pressure is visible.
```

For top bottleneck stages, a practical expanded capture set is:

```text
Duration
Scheduler Delay
GC Time
Shuffle Read Size / Records
Shuffle Read Fetch Wait Time
Shuffle Remote Reads
Shuffle Write Size / Records
Shuffle Write Time
Memory Spill
Disk Spill
Peak Execution Memory
```

### 5. Diagnose Skew

In the Stages tab, open a long-running stage and inspect task durations.

Skew often looks like:

```text
Most tasks finish quickly.
A few tasks run much longer.
The whole stage waits on the slowest tasks.
```

Evidence to capture:

```text
max task duration
median task duration
shuffle read per task
records read per task
spill per task
```

First skew check:

```text
Compare median shuffle read per task to max shuffle read per task.
Compare median task duration to max task duration.
```

If max is close to median, the stage is probably not skewed.

If max is many times larger than median, the stage may be skewed.

Actual demo example from Step 12, Job 22, Stage 38:

```text
median task duration: 4 s
max task duration:    20 s
median shuffle read:  2 MiB
max shuffle read:     3.1 MiB
executor split:       100 tasks / 100 tasks
```

Interpretation:

```text
Not obvious data skew.
Shuffle read is balanced.
Executors are balanced.
The bottleneck is more likely many small shuffle partitions running in waves.
```

Hypothetical skew example:

```text
median task duration: 4 s
max task duration:    8 min
median shuffle read:  2 MiB
max shuffle read:     800 MiB
```

Interpretation:

```text
Likely data skew.
A few partitions are much larger than the rest.
The stage waits for the slowest tasks.
Investigate join keys, hot keys, skewed groups, AQE skew handling, salting, or data layout.
```

Practical skew benchmarks:

```text
Healthy or acceptable:
  max shuffle read is less than about 2x-3x median
  max task duration is less than about 2x-3x median
  no meaningful spill

Watch closely:
  max shuffle read is about 3x-5x median
  max task duration is about 3x-5x median
  some spill or executor imbalance appears

Likely skew:
  max shuffle read is greater than about 5x-10x median
  max task duration is greater than about 5x-10x median
  a few tasks dominate stage runtime
  slow tasks read far more records or spill more than the rest

Severe skew:
  median task reads MiB-scale data, max task reads GiB-scale data
  median task runs seconds, max task runs minutes or hours
  stage progress stalls near the end
```

For large client workloads with hundreds of GB or TBs:

```text
Do not judge skew by absolute size alone.
Judge skew by comparing max vs median and by looking at spill and task duration.

Example not necessarily skew at large scale:
  median shuffle read: 800 MiB
  max shuffle read:    1.4 GiB
  tasks are similarly long
  little or no spill

Example likely skew at large scale:
  median shuffle read: 800 MiB
  max shuffle read:    40 GiB
  median duration:     2 min
  max duration:        45 min
  high disk spill on slow tasks
```

Use these as directional thresholds, not hard rules. Cluster size, executor memory, file format, compression, join type, and storage performance all affect what is acceptable.

This is especially important for the main BRBF job after high-frequency users and hot contextual keys are joined.

### 6. Identify Operators

Go to:

```text
Spark UI -> SQL
```

Click the SQL query or DataFrame action.

Look at the physical plan and operators.

Common operators:

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
BatchEvalPython
ScalaUDF-related projection
```

The most important operator for shuffle diagnosis is:

```text
Exchange
```

`Exchange` means Spark reshuffled data. It usually appears around:

```text
repartition
joins
groupBy/aggregations
sort/orderBy
distinct/dropDuplicates
```

### 7. Use Explain Plans From Code Or Shell

Before running a job, use:

```scala
df.explain("formatted")
```

or:

```scala
df.explain(true)
```

Use this to see:

```text
logical plan
optimized logical plan
physical plan
join strategy
Exchange nodes
Sort nodes
Aggregate nodes
scan columns
filters
UDF projections
```

This is useful when linking code changes to physical operators.

## What To Capture For The EMR Baseline

For each important run, capture:

```text
application id
step name
input scale
cluster size
Spark config
total runtime
runtime by EMR step
longest Spark job
longest Spark stage
shuffle read/write
memory spill
disk spill
task duration variance
join strategy
final output row count
```

For screenshots, prioritize:

```text
Spark UI Jobs tab
Spark UI Stages tab for the slowest stage
Spark UI SQL tab physical plan
Spark UI Executors tab
Spark UI Environment tab
```

## Step-Specific Notes

### Step 08: FeatureLogConverter

Expected visible operators:

```text
FileScan parquet
Project
Scala UDF projections
HashAggregate or Deduplicate-related aggregate
Exchange from dropDuplicates
Exchange from repartition
Write parquet
```

Expected baseline symptoms:

```text
UDF-derived columns
deduplication shuffle
manual repartition shuffle
Parquet write
```

Since the tiny dataset has only 50,000 rows, symptoms may be small.

### Step 09: EligibleUserDataLogConverter

Expected visible operators:

```text
FileScan parquet
Project
Scala UDF projections
HashAggregate or Deduplicate-related aggregate
Exchange from dropDuplicates
Exchange from repartition
Write parquet
```

Expected baseline symptoms:

```text
UDF-derived user and bid hashes
eligibility/frequency projections
deduplication shuffle
manual repartition by user_id_hash and frequency_bracket
Parquet write
```

Since the tiny dataset has only 20,000 rows, symptoms may be small.

### Step 10: BrbfJob

This is where the strongest baseline evidence should appear.

Expected visible operators:

```text
multiple FileScan parquet operators
Project with early derived columns
BroadcastHashJoin for small dimensions
SortMergeJoin or hash joins for larger joins
Exchange around joins
Exchange around manual salting/repartitioning
HashAggregate
Union
Sort
Write parquet
```

Expected baseline symptoms:

```text
wide rows before joins
large shuffle read/write
expensive join stages
manual salting branch complexity
high/low frequency branch split
union pressure
sort pressure
skewed task durations
spill risk
```

## Mapping To Databricks And Photon

EMR Spark UI will not show Photon operators because Photon is Databricks-specific.

However, EMR Spark UI does show the Spark operators that may later map to Photon-accelerated execution on Databricks.

Likely Photon-friendly operators:

```text
Parquet/Delta scans
Filter
Project
Hash joins
Aggregations
Sorts
Window-style SQL operations
```

Less Photon-friendly or poor optimizer visibility:

```text
custom Scala UDF-heavy logic
opaque per-row transformations
manual skew-handling code that forces extra shuffles
```

Migration comparison idea:

```text
EMR baseline:
  FileScan + UDF Project + Exchange + Join + Aggregate + Sort + Parquet write

Databricks target:
  Native expressions + AQE + Photon-friendly joins/aggregates/scans + Delta layout
```

## Key Questions To Answer During Analysis

Use the Spark UI to answer:

```text
Which job took the longest?
Which stage took the longest?
Which operators created Exchanges?
How much shuffle read/write happened?
Did any stage spill to disk?
Were task durations skewed?
Which joins were broadcast joins vs sort-merge joins?
Did UDF projections appear before major joins?
Did manual repartitioning create extra shuffle stages?
Did the final sort or union create visible pressure?
```

These answers become the evidence for why the Databricks migration can improve the workload.
