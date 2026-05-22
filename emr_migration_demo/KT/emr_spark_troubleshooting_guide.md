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

```text
spark.sql.adaptive.enabled
spark.sql.adaptive.skewJoin.enabled
spark.sql.shuffle.partitions
spark.default.parallelism
spark.sql.autoBroadcastJoinThreshold
spark.serializer
spark.sql.parquet.compression.codec
```

For the EMR baseline, we expect AQE to be disabled or not relied on when running the final before-DAG workload.

### 2. Find The Longest Job

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

### 3. Find Expensive Stages

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

### 4. Diagnose Skew

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

This is especially important for the main BRBF job after high-frequency users and hot contextual keys are joined.

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
