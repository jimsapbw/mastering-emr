# Client Spark UI Troubleshooting Prompt Examples

## Purpose

This file stores worked examples for prompts in:

```text
KT/spark_troubleshooting/client_spark_ui_troubleshooting_plan.md
```

Use this file to understand what a good prompt result should look like before using the prompts in a client environment.

The examples are organized by dataset scale and prompt type.

## Index

| Section | Use When |
|---|---|
| [Small Dataset Examples](#small-dataset-examples) | Reviewing worked prompt outputs for the small demo dataset. |
| [Airflow DAG Entry-Point Prompt](#airflow-dag-entry-point-prompt) | Seeing the expected output for Airflow-to-Spark entry-point discovery. |
| [Stage Operator And Code Mapping Prompt](#stage-operator-and-code-mapping-prompt) | Seeing the expected Section 5 stage/operator/code mapping output. |
| [Medium Dataset Examples](#medium-dataset-examples) | Placeholder for future medium-scale worked examples. |

## Small Dataset Examples

### Airflow DAG Entry-Point Prompt

Use this example before the demo has a real Airflow DAG file.

This is based on:

```text
KT/airflow_orchestration_plan.md
KT/11_package_upload_run_emr_steps.md
KT/12_scale_data_and_tune_runtime.md
spark/src/main/scala/com/demo/emr/
```

Because the actual Airflow DAG is not implemented yet, treat this as a planned-DAG / EMR-step-definition example. Update this example after the real DAG exists.

Example output:

#### Execution Path

```text
Airflow DAG/task
-> EmrAddStepsOperator, planned
-> existing EMR cluster or configured EMR cluster id
-> EMR step using command-runner.jar
-> spark-submit
-> Scala main class from --class
-> Spark History Server application
-> jobs/stages/SQL operators
```

Planned demo pipeline:

```text
FeatureLogConverter
-> EligibleUserDataLogConverter
-> BrbfJob
```

#### Task Inventory

| DAG id | Task id | Operator | Launch pattern | Cluster/app target | Spark entry point | Key args | Logs/count hints | Missing follow-up |
|---|---|---|---|---|---|---|---|---|
| pending | `submit_feature_log_converter` | `EmrAddStepsOperator`, planned | EMR step on existing or selected EMR cluster | cluster id/name from Airflow config or variable | `com.demo.emr.FeatureLogConverter` | `--bucket`, `--base-prefix`, `--run-date`, `--hour`, `--late-hour`, `--output-mode` | converter logs, EMR step logs, Spark History app | actual DAG file, cluster lookup variable, step sensor task |
| pending | `submit_eligible_user_data_converter` | `EmrAddStepsOperator`, planned | EMR step | same EMR cluster | `com.demo.emr.EligibleUserDataLogConverter` | same runtime args | converter logs, EMR step logs, Spark History app | actual DAG dependency/sensor details |
| pending | `submit_brbf_job` | `EmrAddStepsOperator`, planned | EMR step | same EMR cluster | `com.demo.emr.BrbfJob` | same runtime args | `brbf_job.*` printed metrics from driver logs; Spark History app | actual DAG file and step-to-application lookup |

#### Scala Entry-Point Clues

For Scala jobs, the important Airflow/EMR clue is:

```text
--class com.demo.emr.FeatureLogConverter
--class com.demo.emr.EligibleUserDataLogConverter
--class com.demo.emr.BrbfJob
```

Those map to source files:

```text
spark/src/main/scala/com/demo/emr/FeatureLogConverter.scala
spark/src/main/scala/com/demo/emr/EligibleUserDataLogConverter.scala
spark/src/main/scala/com/demo/emr/BrbfJob.scala
```

#### Spark Submit Shape

Example BRBF step:

```text
spark-submit
--deploy-mode client
--class com.demo.emr.BrbfJob
s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
--bucket aigithub-emr-2026
--base-prefix emr-migration-demo-small
--run-date 2026-05-21
--hour 10
--late-hour 11
--output-mode overwrite
```

#### Search Terms Used

For a client GitLab repo, useful searches are:

```text
EmrAddStepsOperator
EmrStepSensor
job_flow_id
cluster_id
cluster_name
command-runner.jar
spark-submit
--class
main_class
s3://
BrbfJob
FeatureLogConverter
EligibleUserDataLogConverter
```

For this demo, useful searches are:

```text
com.demo.emr.FeatureLogConverter
com.demo.emr.EligibleUserDataLogConverter
com.demo.emr.BrbfJob
emr-migration-demo-0.1.0.jar
emr-migration-demo-small
```

#### Where Counts / Audit Evidence May Come From

For this demo, the strongest count evidence is in Spark driver logs because `BrbfJob` prints metrics such as:

```text
brbf_job.source_bids_count
brbf_job.source_impressions_count
brbf_job.joined_row_count
brbf_job.joined_impression_match_count
brbf_job.joined_feature_log_match_count
brbf_job.final_output_count
```

In a real Airflow DAG, look for:

```text
validation tasks after each EMR step
Airflow task logs that capture driver stdout
EMR step logs
CloudWatch logs
Spark History application id
audit/reconciliation table updates
```

#### Missing Access / Metadata

Because this is not a real Airflow DAG yet, these remain pending:

```text
actual DAG id
actual task ids
Airflow connection id
cluster lookup variable or hard-coded cluster id
EMR step sensor tasks
how Airflow records EMR step ids
how Airflow links EMR step id to YARN/Spark app id
actual task logs
retry/timeout/SLA settings
validation/audit task definitions
```

#### Conservative Conclusion

```text
The demo does not yet have an implemented Airflow DAG, but the planned orchestration pattern is clear:
Airflow should submit three EMR steps with EmrAddStepsOperator, each using command-runner.jar and spark-submit with a Scala --class entry point.

The most important code-entry clues are the --class values, especially com.demo.emr.BrbfJob for the expensive Spark History investigation.
Once an actual DAG exists, the next step is to map one Airflow task instance to its EMR step id, then to the Spark History application.
```

### Stage Operator And Code Mapping Prompt

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

#### 9. Executive Databricks / Photon Summary

```text
Main finding:
  Stage 38 is not slow because of obvious skew or memory pressure.
  It is slow because Spark created 200 small shuffle tasks and processed them in many waves.

AQE partition coalescing:
  Our finding: Stage 38 had 200 tiny shuffle tasks from Exchange hashpartitioning(..., 200), tied to the joined DataFrame path and shuffled joins around eligible_user_data and feature_log.
  What it can fix: AQE can coalesce those small shuffle partitions at runtime so Spark runs fewer task waves.

AQE join strategy:
  Our finding: eligible_user_data and feature_log used ShuffledHashJoin, while smaller dimensions used BroadcastHashJoin.
  What it can fix: If table stats show a join side is small enough, Databricks may switch some shuffle joins to broadcast joins.

Photon:
  Our finding: the plan uses native operators like FileScan, BroadcastHashJoin, ShuffledHashJoin, HashAggregate, Sort, and WriteFiles.
  What it can improve: Photon can accelerate supported native scans, joins, aggregations, sorts, and writes.

UDF review:
  Our finding: Scala UDF projections appear before joins.
  What to watch: Photon is less helpful for opaque UDF logic, so native Spark SQL rewrites may expose more optimization.

Delta/statistics/layout:
  Our finding: join decisions depend on table sizes and stats.
  What it can improve: Better stats and layout can improve pruning, join planning, and shuffle behavior.
```

#### 10. Completion-Gate Conclusion

```text
Completion gate:
  pass

Reason:
  The small dataset source-side investigation has completed one clean loop:
  runtime configuration was captured, Query 8 / Job 22 / Stage 38 was identified,
  skew and memory pressure were ruled out as primary causes, partition/task-wave evidence
  explains the slow stage, and the stage was mapped back to the joined DataFrame path
  triggered by joined.count().

  The Databricks translation is also documented:
  AQE partition coalescing is the clearest opportunity for this small run,
  AQE join strategy depends on table-size evidence,
  Photon may help native operators,
  and UDF projections should be reviewed later.

Still missing before scaling:
  none for the small-source troubleshooting loop.
  Databricks explain and baseline runs are useful next, but they are part of the Databricks validation path,
  not blockers for moving the EMR source investigation to medium scale.

Next recommended move:
  move to the medium dataset to look for more realistic skew, memory pressure, spill, and larger shuffle behavior.
```

## Medium Dataset Examples

### Medium Step 3 Disk-Pressure Example

Use this as the expected style when a medium or production-sized source run fails before a full Spark History analysis is possible.

Prompt context:

```text
I am analyzing a medium Spark run that failed during the main BRBF job.

Known context:
- medium prefix: s3://aigithub-emr-2026/emr-migration-demo-medium/
- medium Steps 1 and 2 completed and wrote converted data to S3
- Step 3 action: count at BrbfJob.scala:80
- attempt 1: step s-0661117QH89RUU39ZQ1, app application_1779541486316_0010, Job 20 / Stage 37.0
- attempt 1 root cause: java.io.IOException: No space left on device
- attempt 2: step s-09085642ZQTGXNML2TKJ, app application_1779541486316_0011
- attempt 2 controlled change: EMR scale-out plus --executor-cores 2
- attempt 2 live UI reached Job 20 / Stage 38 with about 11.8 GiB input over 200 tasks before failing

Please classify the failure conservatively and tell me the next controlled action.
```

Expected answer shape:

```text
Main finding:
  Medium Step 3 is failing from local disk/shuffle pressure during the joined.count() path, not from a proven code correctness issue.

Evidence:
  Attempt 1 failed in Job 20 / Stage 37.0 with java.io.IOException: No space left on device.
  Attempt 2 progressed farther after scale-out and lower executor concurrency, reaching Job 20 / Stage 38 with about 11.8 GiB input over 200 tasks, but still failed.
  The controlled change helped progress but did not make the workload reliable on the current cluster.

Classification:
  skew: unknown from completed history; do not claim primary skew without task median/max evidence.
  memory pressure: possible but not proven as primary from the pasted evidence alone.
  local disk pressure: confirmed by the No space left on device root cause.
  partition sizing: still relevant because the stage uses 200 tasks, but partition tuning should not hide the disk-capacity finding.
  infrastructure limit: likely, because the cluster instance groups showed no larger worker EBS allocation.

Next controlled action:
  Stop tuning blindly on the current cluster.
  Create a restore checkpoint.
  Create a new EMR cluster with larger worker local/EBS disk from the start.
  Rerun only medium Step 3 first, because medium Steps 1 and 2 already completed on S3.

If the rerun completes:
  Validate final output.
  Run the full Spark UI prompt sequence against the completed application.

If the rerun fails:
  Preserve the first root-cause error, failed stage, task failure text, and executor loss evidence before making another change.
```
