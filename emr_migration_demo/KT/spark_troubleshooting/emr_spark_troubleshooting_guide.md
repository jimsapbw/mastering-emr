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
Cluster ID: j-3S62AU5IR98MM
Region: us-east-1
EMR version: emr-7.13.0
Spark version: 3.5.6
Primary node public DNS: ec2-100-55-172-52.compute-1.amazonaws.com
Log destination: s3://aws-logs-210570462212-us-east-1/elasticmapreduce/
CloudWatch log group: /aws/emr/j-3S62AU5IR98MM
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

Focus guidance:

```text
If one job or SQL query takes 40%-50% or more of total app runtime, start there.
If no single job dominates, inspect the top 2-3 jobs or SQL queries by duration.
If the top 2-3 jobs together explain 70%-80% of runtime, they are the first investigation set.
```

Also watch for repeated patterns:

```text
many repeated small jobs
many repeated scans
many repeated shuffles
many repeated retries
```

Dominant duration finds the biggest bottleneck. Repeated patterns reveal death by a thousand cuts.

For our converters, `count()` and `write` actions create Spark jobs.

For the main BRBF job, the final write will likely trigger the heaviest work.

Use the SQL physical plan twice:

```text
First look:
  After finding the longest job/query, open the SQL tab and physical plan.
  Use it as a map of operators: scan, project/UDF, join, Exchange, aggregate, sort, write.

Second look:
  After finding a bottleneck stage, return to the physical plan.
  Map the stage back to the operator or Exchange that caused it.
```

The physical plan gives the map. Stage metrics give the evidence. Returning to the physical plan connects the evidence back to the code path.

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

Focus guidance:

```text
If one stage takes 40%-50% or more of the job/query runtime, start there.
If no single stage dominates, inspect the top 2-3 stages by duration.
If the top 2-3 stages explain 70%-80% of the job/query runtime, they are the first stage investigation set.
```

Do not ignore a short stage if it repeats many times, fails, spills heavily, or reveals the same shuffle pattern across the app.

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

Stage diagnosis matrix:

```text
High max shuffle read + high max duration:
  Likely skew.
  One or a few tasks got much more data and took much longer.

High max shuffle read + normal duration:
  Not necessarily a bottleneck.
  A task read more data, but it did not hurt runtime much.

Normal max shuffle read + high max duration:
  Not data skew.
  Investigate GC, spill, scheduler delay, shuffle fetch wait, executor imbalance, or straggler behavior.

High max shuffle read + high spill + high duration:
  Skew plus memory pressure.
  A large partition is too big and spills, making the slow task worse.

Normal shuffle read + high spill + high duration:
  Memory pressure, not skew.
  Data is balanced, but each task's operation is memory-heavy.

Normal shuffle read + normal spill + high duration:
  Look at scheduler delay, shuffle fetch wait, CPU/operator cost, or code path.

Small shuffle read per task + many tasks + long stage:
  Too many small partitions or task-wave overhead.

Large shuffle read per task + spill + long tasks:
  Too few/heavy partitions or large shuffle workload.

One executor has much higher task time, shuffle, or spill:
  Executor imbalance, host issue, or skew concentrated on one executor.

All executors balanced but stage is slow:
  Workload design, partitioning, or operator cost is likely the bottleneck.
```

Practical order:

```text
1. Compare max shuffle read vs median shuffle read.
2. Compare max duration vs median duration.
3. Check memory spill, disk spill, GC time, and peak execution memory.
4. Check scheduler delay and shuffle fetch wait.
5. Check executor balance.
```

### 4.1 Diagnose Skew

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

### 4.2 Diagnose Memory Pressure

Memory pressure means tasks are struggling to keep working data in memory.

Common symptoms:

```text
high GC time
memory spill
disk spill
high peak execution memory
slow tasks spill more than normal tasks
executors show uneven spill or GC
```

First memory-pressure check:

```text
Compare GC time to task duration.
Check memory spill and disk spill.
Check peak execution memory if available.
Compare spill and GC across executors.
```

Actual demo example from Step 12, Job 22, Stage 38:

```text
median task duration: 4 s
max task duration:    20 s
max GC time:          0.6 s
memory spill:         not shown in summary
disk spill:           not shown in summary
executor balance:     100 tasks / 100 tasks
```

Interpretation:

```text
Not obvious memory pressure.
GC is low relative to task duration.
No spill was visible in the captured summary.
Executors were balanced.
The bottleneck is more likely task-wave overhead from many small shuffle partitions.
```

Hypothetical high-GC memory-pressure example:

```text
median task duration: 2 min
max task duration:    8 min
median GC time:       45 s
max GC time:          4 min
memory spill:         20 GiB total
disk spill:           120 GiB total
peak execution memory near executor limit
```

Interpretation:

```text
Likely memory pressure.
Tasks spend a large share of runtime in garbage collection.
Spark spills data to memory and disk because joins or aggregations do not fit comfortably.
Investigate join size, aggregation cardinality, shuffle size, executor memory, partition sizing, and skew.
```

Practical GC benchmarks:

```text
Healthy or acceptable:
  GC time is less than about 5%-10% of task runtime
  little or no spill
  peak execution memory is comfortably below executor memory

Watch closely:
  GC time is about 10%-20% of task runtime
  some memory spill appears
  disk spill appears on a few tasks

Likely memory pressure:
  GC time is greater than about 20%-30% of task runtime
  meaningful memory spill and disk spill
  slow tasks spill much more than normal tasks
  stage time tracks spill-heavy tasks

Severe memory pressure:
  GC time is greater than about 50% of task runtime
  large disk spill
  executor lost / OOM / container killed
  tasks repeatedly retry or fail
```

For large client workloads with hundreds of GB or TBs:

```text
Do not judge memory pressure by spill alone.
Some spill can be normal in very large joins or aggregations.
Judge by the relationship between spill, GC, task duration, and failures.

Example not necessarily severe:
  total disk spill: 50 GiB
  task duration is balanced
  GC is low
  no retries or executor failures

Example likely memory pressure:
  total disk spill: 2 TiB
  GC is 30%-60% of task time
  slowest tasks spill much more than median
  executor failures or OOM messages appear
```

Common causes:

```text
large shuffle joins
high-cardinality aggregations
countDistinct
too few shuffle partitions
skewed partitions
large broadcast side
wide rows kept before joins
persist/cache pressure
```

Databricks optimization angles:

```text
AQE partition coalescing or skew handling
better join strategy
better table statistics
Delta layout and file sizing
Photon for supported joins and aggregations
rewriting UDF-heavy logic to native expressions
executor sizing changes
reducing unnecessary cache/persist pressure
```

### 4.3 Diagnose Partition Sizing And Task Waves

Partition sizing problems often appear after skew and memory pressure have been ruled out.

The question is:

```text
Is this stage slow because each task is too heavy, or because Spark has too many small tasks to march through?
```

First partition-sizing check:

```text
1. Get task count from the Stage page.
2. Get total active cores from the Executors tab.
3. Treat total active cores as approximate concurrent task slots.
4. Estimate task waves:
     task waves = number of tasks / approximate task slots
5. Estimate stage time from waves:
     expected stage time = task waves * median task duration
6. Compare expected stage time to observed stage duration.
```

Spark normally runs about one task per executor core. In Spark History Server:

```text
Executors tab -> Summary -> Active or Total -> Cores
```

Use active executor cores for the application as the practical task-slot estimate.

Actual demo example from Step 12, Job 22, Stage 38:

```text
stage duration:             1.6 min, about 96 s
number of tasks:            200
active executor cores:      12
approximate task slots:     12
estimated task waves:       200 / 12 = 16.7, about 17 waves
median task duration:       4 s
expected wave time:         17 * 4 s = 68 s, about 1.1 min
median shuffle read:        2 MiB
max shuffle read:           3.1 MiB
total shuffle read:         398.4 MiB
max GC time:                0.6 s
memory spill:               not shown in summary
disk spill:                 not shown in summary
shuffle fetch wait:         low
scheduler delay:            low
executor balance:           balanced
```

Interpretation:

```text
Likely too many small shuffle partitions for this cluster and data size.

The median-task wave estimate explains much of the observed stage duration.
The remaining time can come from slower waves, task startup overhead, final stragglers,
write/commit overhead, and normal Spark scheduling overhead.

This is not obvious skew because max shuffle read is close to median.
This is not obvious memory pressure because GC and spill are low.
This is not a clear fetch or scheduler-delay issue because those metrics are low.
```

Hypothetical too-many-small-partitions example:

```text
task slots:             12
shuffle partitions:     2,000
task waves:             167
median shuffle read:    4 MiB
max shuffle read:       8 MiB
median task duration:   2 s
max task duration:      12 s
spill:                  none
GC:                     low
executor balance:       even
observed stage time:    several minutes
```

Interpretation:

```text
Likely over-partitioned.
Tasks are small and healthy, but there are so many waves that stage wall-clock time is high.
Reduce shuffle partitions for this scale, remove unnecessary repartitions, or rely on AQE partition coalescing where available.
```

Hypothetical too-few-heavy-partitions example:

```text
task slots:             12
shuffle partitions:     12
task waves:             1
median shuffle read:    3 GiB
max shuffle read:       6 GiB
median task duration:   12 min
max task duration:      25 min
memory spill:           high
disk spill:             high
GC:                     elevated
executor balance:       roughly even
```

Interpretation:

```text
Likely under-partitioned or using heavy partitions.
The stage does not have many waves, but each task carries too much data.
Increase partitions, reduce row width earlier, check join/aggregate size, and inspect memory pressure.
```

Hypothetical balanced-partitions example:

```text
task slots:             12
shuffle partitions:     96
task waves:             8
median shuffle read:    128 MiB
max shuffle read:       220 MiB
median task duration:   35 s
max task duration:      75 s
spill:                  little or none
GC:                     low
executor balance:       even
observed stage time:    close to wave estimate
```

Interpretation:

```text
Likely reasonable partition sizing.
Tasks are large enough to amortize scheduling overhead but not so large that they spill heavily.
If the stage is still slow, investigate operator cost, join strategy, sort/write cost, storage reads, or repeated actions.
```

Practical partition-sizing benchmarks:

```text
Healthy or acceptable:
  median shuffle read is often tens to hundreds of MiB for large shuffle stages
  max shuffle read is less than about 2x-3x median
  GC is less than about 5%-10% of task runtime
  little or no spill
  stage duration is reasonably explained by task waves and task duration

Watch closely:
  median shuffle read is low MiB-scale with many waves
  or median shuffle read is hundreds of MiB to low GiB with some spill
  max is about 3x-5x median
  stage time starts tracking either wave count or spill-heavy tasks

Likely too many small partitions:
  median shuffle read is KiB to low-MiB scale, often below about 16-32 MiB
  task count is high relative to executor cores
  estimated task waves are high
  median task duration is short
  GC, spill, scheduler delay, and fetch wait are low
  executors are balanced
  observed stage duration is largely explained by many waves of small tasks

Likely too few heavy partitions:
  median task reads hundreds of MiB to GiB-scale data
  task durations are long
  GC and spill are meaningful
  peak execution memory is high
  stage duration tracks heavy task runtime rather than wave count
```

For large client workloads with hundreds of GB or TBs:

```text
Do not use one universal target partition size.
Judge partition sizing by task waves, per-task bytes, duration, GC, spill, and skew together.

Example not necessarily over-partitioned at large scale:
  tasks:                4,000
  task slots:           500
  task waves:           8
  median shuffle read:  128 MiB
  max shuffle read:     250 MiB
  GC/spill:             low
  stage time:           close to wave estimate

Example likely over-partitioned at large scale:
  tasks:                40,000
  task slots:           500
  task waves:           80
  median shuffle read:  2 MiB
  max shuffle read:     5 MiB
  GC/spill:             low
  stage time:           dominated by many short task waves

Example likely under-partitioned at large scale:
  tasks:                500
  task slots:           500
  task waves:           1
  median shuffle read:  8 GiB
  max shuffle read:     20 GiB
  GC/spill:             high
  stage time:           dominated by heavy spill-prone tasks
```

Use the wave estimate as a reasoning tool, not a precise SLA.
If observed stage duration is close to:

```text
task waves * median task duration
```

and task data is tiny with low GC/spill, partition count is a strong suspect.

If observed duration is much higher than the wave estimate, inspect:

```text
max task duration
GC time
memory spill and disk spill
shuffle fetch wait
scheduler delay
executor imbalance
write commit time
operator cost
```

### 5. Identify Operators

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

Use the physical plan after stage diagnosis, not only before it.

The practical mapping flow is:

```text
1. Find the longest job or SQL query.
2. Find the bottleneck stage inside that job/query.
3. Return to the associated SQL physical plan.
4. Look for the operator boundary that explains the stage:
     Exchange
     HashAggregate / SortAggregate
     BroadcastHashJoin / ShuffledHashJoin / SortMergeJoin
     Sort
     WriteFiles
5. Map that operator back to the code action or transformation.
```

For the Step 12 small BRBF run, use this current mapping target:

```text
Job 22
-> SQL Query 8
-> Stage 38
-> physical plan operator around the 200-partition Exchange / aggregate path
-> BrbfJob.scala action shown in the Spark UI job description
```

The goal is not just to say "Stage 38 is slow." The goal is to say:

```text
Stage 38 is slow because this specific operator/code path creates work with these symptoms.
```

Examples:

```text
Stage metrics show skew
  -> find the join/aggregate Exchange and keys that created skew.

Stage metrics show high GC/spill
  -> find the join, aggregate, sort, cache, or wide-row operator creating memory pressure.

Stage metrics show many small tasks and low spill
  -> find the Exchange using too many shuffle partitions for the cluster's available task slots.
```

### 6. Use Explain Plans From Code Or Shell

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

## Completion Gate Before Scaling Up

Before moving from one dataset size to a larger dataset size, finish one clean analysis loop.

Use this gate for any scale transition, such as tiny to small, small to medium, or medium to a larger stress run.

Confirm:

```text
1. Runtime configuration is captured.
2. Longest job/query is identified.
3. Bottleneck stage is identified.
4. Stage metrics are classified:
     skew or not skew
     memory pressure or not memory pressure
     too many small partitions or too few heavy partitions
     executor imbalance or balanced executors
     scheduler/fetch wait issue or not
5. Bottleneck stage is mapped back to SQL physical operator and code path.
6. Final output is validated after the run.
7. The run conclusion is written in one short evidence summary.
```

Run conclusion format:

```text
Run:
  application id:
  EMR step id:
  input scale:

Runtime config:
  AQE:
  skew join handling:
  shuffle partitions:
  executor cores/memory:

Main bottleneck:
  job/query:
  stage:
  duration:
  share of job/query runtime:

Evidence:
  shuffle read median/max:
  duration median/max:
  GC median/max:
  spill:
  scheduler delay:
  shuffle fetch wait:
  executor balance:

Classification:
  skew:
  memory pressure:
  partition sizing:
  join/operator pressure:

Likely source-platform issue:
  ...

Databricks migration angle:
  AQE:
  Photon:
  Delta/statistics/layout:
  code/UDF changes:
```

Current Step 12 small-run example:

```text
Stage 38 does not show obvious skew, memory pressure, scheduler delay, shuffle fetch wait, or executor imbalance.
The likely issue is many small shuffle tasks from static 200 shuffle partitions on a small cluster.
Databricks opportunity: AQE partition coalescing may reduce task waves; Photon may help supported joins/aggregates/sorts/scans but does not directly remove task scheduling overhead.
```

Only after the current-size loop is complete should the analysis move to a larger dataset.

For the larger run, reuse the same workflow and intentionally look for stronger evidence:

```text
larger shuffle read/write
larger task duration variance
memory spill and disk spill
higher GC percentage
hot keys or skewed aggregates
joins that should or should not broadcast
operators that map to AQE, Photon, Delta layout, or code changes
```

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
