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
| [Light-Medium Dataset Examples](#light-medium-dataset-examples) | Reviewing runtime configuration for the preferred 3x demo load. |
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

| DAG id | Task id | Operator | Launch pattern | Cluster/app target | Submit wrapper | App artifact | Spark entry point | Key args | Logs/count hints | Missing follow-up |
|---|---|---|---|---|---|---|---|---|---|---|
| pending | `submit_feature_log_converter` | `EmrAddStepsOperator`, planned | EMR step on existing or selected EMR cluster | cluster id/name from Airflow config or variable | `command-runner.jar` -> `spark-submit` | `emr-migration-demo-0.1.0.jar` | `com.demo.emr.FeatureLogConverter` | `--bucket`, `--base-prefix`, `--run-date`, `--hour`, `--late-hour`, `--output-mode` | converter logs, EMR step logs, Spark History app | actual DAG file, cluster lookup variable, step sensor task |
| pending | `submit_eligible_user_data_converter` | `EmrAddStepsOperator`, planned | EMR step | same EMR cluster | `command-runner.jar` -> `spark-submit` | `emr-migration-demo-0.1.0.jar` | `com.demo.emr.EligibleUserDataLogConverter` | same runtime args | converter logs, EMR step logs, Spark History app | actual DAG dependency/sensor details |
| pending | `submit_brbf_job` | `EmrAddStepsOperator`, planned | EMR step | same EMR cluster | `command-runner.jar` -> `spark-submit` | `emr-migration-demo-0.1.0.jar` | `com.demo.emr.BrbfJob` | same runtime args | `brbf_job.*` printed metrics from driver logs; Spark History app | actual DAG file and step-to-application lookup |

#### Scala Entry-Point Clues

For Scala jobs, the important Airflow/EMR clue is:

```text
command-runner.jar
spark-submit
--class com.demo.emr.FeatureLogConverter
--class com.demo.emr.EligibleUserDataLogConverter
--class com.demo.emr.BrbfJob
s3://.../emr-migration-demo-0.1.0.jar
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

## Light-Medium Dataset Examples

### Airflow DAG Entry-Point Prompt

Use this example for the current demo when Airflow is not implemented yet, but
the EMR step pattern and Spark code entry points are known.

Prompt context:

```text
I am tracing the Airflow-to-Spark entry point for the light-medium BRBF demo.

There is no real Airflow DAG yet. Treat this as a planned-DAG / EMR-step
definition exercise based on the current codebase and completed EMR run.

Known context:
- dataset prefix: s3://aigithub-emr-2026/emr-migration-demo-light-medium/
- cluster id: j-3O78ZN9EMO9W2
- BRBF Spark app id: application_1779641349593_0009
- BRBF EMR step id: s-06984631PV9G98A51OTN
- BRBF app name: emr-migration-brbf-before-dag
- BRBF Step 3 runtime: about 12 minutes
- JAR: s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar

Please identify the likely DAG tasks, Spark entry points, submit pattern,
runtime arguments, and the missing metadata that a real client Airflow DAG
would need to provide.
```

Example output:

#### Execution Path

```text
Airflow DAG run, planned
-> submit_feature_log_converter, planned task
-> EMR step using command-runner.jar
-> spark-submit --class com.demo.emr.FeatureLogConverter
-> Spark application for converter
-> converted/feature_log output in S3

Airflow DAG run, planned
-> submit_eligible_user_data_converter, planned task
-> EMR step using command-runner.jar
-> spark-submit --class com.demo.emr.EligibleUserDataLogConverter
-> Spark application for converter
-> converted/eligible_user_data output in S3

Airflow DAG run, planned
-> submit_brbf_before_dag, planned task
-> EMR step s-06984631PV9G98A51OTN
-> spark-submit --class com.demo.emr.BrbfJob
-> Spark app application_1779641349593_0009
-> final/brbf output in S3
```

#### Task Inventory

| DAG id | Task id | Operator | Launch pattern | Cluster/app target | Submit wrapper | App artifact | Spark entry point | Key args | Evidence | Missing follow-up |
|---|---|---|---|---|---|---|---|---|---|---|
| `emr_migration_brbf_light_medium`, proposed | `submit_feature_log_converter`, proposed | `EmrAddStepsOperator`, proposed | EMR step on existing cluster | `j-3O78ZN9EMO9W2` | `command-runner.jar` -> `spark-submit` | `s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar` | `com.demo.emr.FeatureLogConverter` | `--bucket aigithub-emr-2026`, `--base-prefix emr-migration-demo-light-medium`, `--run-date 2026-05-21`, `--hour 10`, `--late-hour 11`, `--output-mode overwrite` | Step definition in KT runbook | actual DAG file, actual step id, Spark app id |
| `emr_migration_brbf_light_medium`, proposed | `submit_eligible_user_data_converter`, proposed | `EmrAddStepsOperator`, proposed | EMR step on existing cluster | `j-3O78ZN9EMO9W2` | `command-runner.jar` -> `spark-submit` | `s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar` | `com.demo.emr.EligibleUserDataLogConverter` | same dataset/runtime args | Step definition in KT runbook | actual DAG file, actual step id, Spark app id |
| `emr_migration_brbf_light_medium`, proposed | `submit_brbf_before_dag`, proposed | `EmrAddStepsOperator`, proposed | EMR step on existing cluster | step `s-06984631PV9G98A51OTN`, app `application_1779641349593_0009` | `command-runner.jar` -> `spark-submit` | `s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar` | `com.demo.emr.BrbfJob` | same dataset/runtime args plus AQE/shuffle submit confs | Spark UI / Environment / completed run | actual Airflow task log and step sensor metadata |

#### Spark Submit Shape

The planned BRBF Airflow task should submit this shape:

```text
spark-submit
--deploy-mode client
--conf spark.sql.shuffle.partitions=400
--conf spark.executor.cores=2
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
--conf spark.shuffle.compress=true
--conf spark.shuffle.spill.compress=true
--class com.demo.emr.BrbfJob
s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
--bucket aigithub-emr-2026
--base-prefix emr-migration-demo-light-medium
--run-date 2026-05-21
--hour 10
--late-hour 11
--output-mode overwrite
```

#### Code Entry Points

Search terms:

```text
FeatureLogConverter
EligibleUserDataLogConverter
BrbfJob
emr-migration-demo-light-medium
emr-migration-demo-0.1.0.jar
spark.sql.shuffle.partitions
```

Expected source files:

```text
spark/src/main/scala/com/demo/emr/FeatureLogConverter.scala
spark/src/main/scala/com/demo/emr/EligibleUserDataLogConverter.scala
spark/src/main/scala/com/demo/emr/BrbfJob.scala
KT/12_scale_data_and_tune_runtime.md
```

#### Counts / Audit Evidence To Capture

For the current demo, use driver logs and validation scripts:

```text
raw data validation counts for emr-migration-demo-light-medium
feature_log_converter.raw_feature_log_count
feature_log_converter.converted_feature_log_count
eligible_user_data_converter.raw_matched_user_data_count
eligible_user_data_converter.converted_eligible_user_data_count
brbf_job.source_bids_count
brbf_job.source_impressions_count
brbf_job.source_feature_log_count
brbf_job.source_eligible_user_data_count
brbf_job.joined_row_count
brbf_job.final_output_count
```

For a real client DAG, ask for:

```text
Airflow task logs
EMR step ids for all three tasks
Spark application ids for all three tasks
EMR step sensor states
retry/timeout/SLA settings
validation or reconciliation task definitions
where row counts are stored or published
```

#### Conservative Conclusion

```text
There is no implemented Airflow DAG yet, so DAG id and task ids are proposed,
not confirmed.

The planned orchestration is clear enough for the demo:
three Airflow tasks should submit three EMR steps to cluster j-3O78ZN9EMO9W2
using command-runner.jar and spark-submit. The important Spark code entry point
for the performance walkthrough is com.demo.emr.BrbfJob, confirmed by EMR step
s-06984631PV9G98A51OTN and Spark app application_1779641349593_0009.

In a client environment, the next action would be to map the real Airflow task
instance to the EMR step id and Spark application id, then use Spark History
Server for runtime configuration, SQL plan, stage metrics, and bottleneck
classification.
```

### Runtime Configuration Prompt

Use this after identifying the preferred 3x demo BRBF application:

```text
Application name: emr-migration-brbf-before-dag
Application id: application_1779641349593_0009
EMR cluster: j-3O78ZN9EMO9W2
EMR step id: s-06984631PV9G98A51OTN
Dataset prefix: s3://aigithub-emr-2026/emr-migration-demo-light-medium/
Observed BRBF Step 3 runtime: about 12 minutes
```

Input evidence:

```text
Environment -> Spark Properties:
  spark.sql.adaptive.enabled=true
  spark.sql.adaptive.coalescePartitions.enabled=true
  spark.sql.shuffle.partitions=400
  spark.default.parallelism=200
  spark.serializer=org.apache.spark.serializer.KryoSerializer
  spark.executor.cores=2
  spark.executor.memory=18971M
  spark.driver.memory=2048M
  spark.dynamicAllocation.enabled=true
  spark.shuffle.compress=true
  spark.shuffle.spill.compress=true

Physical plan:
  AdaptiveSparkPlan isFinalPlan=true
  Exchange hashpartitioning(..., 400)
  AQEShuffleRead coalesced
  BroadcastHashJoin for smaller lookup joins
  ShuffledHashJoin for eligible_user_data and feature_log paths
```

Example output:

#### Runtime Config Findings

| Setting | Value | Evidence source |
|---|---:|---|
| `spark.sql.adaptive.enabled` | `true` | Environment and `AdaptiveSparkPlan isFinalPlan=true` |
| `spark.sql.adaptive.skewJoin.enabled` | not found | Not present in pasted Environment settings |
| `spark.sql.shuffle.partitions` | `400` | Environment and `Exchange hashpartitioning(..., 400)` |
| `spark.default.parallelism` | `200` | Environment |
| `spark.sql.autoBroadcastJoinThreshold` | not found | Environment value not pasted; broadcast behavior observed in plan |
| `spark.serializer` | `org.apache.spark.serializer.KryoSerializer` | Environment |
| `spark.sql.parquet.compression.codec` | not found | Environment value not pasted |
| executor memory | `18971M` | Environment and Resource Profile |
| executor cores | `2` | Environment and Resource Profile |
| driver memory | `2048M` | Environment |
| dynamic allocation | `true` | Environment |

#### Interpretation

```text
AQE is enabled and active. The physical plan contains AdaptiveSparkPlan
isFinalPlan=true and shows AQEShuffleRead coalesced, so runtime partition
coalescing is actually being used.

The configured shuffle partition count is 400. The plan confirms 400-partition
hash exchanges on the main shuffled join paths, including bid_request_id,
bid_request_id/user_id_hash, user_id_hash/contextual_id, and SIB aggregation.

The plan uses BroadcastHashJoin for smaller lookup datasets and ShuffledHashJoin
for larger eligible_user_data and feature_log joins. This means the runtime
configuration is already applying the intended submit-time tuning: Kryo,
AQE, AQE coalescing, shuffle compression, spill compression, 400 shuffle
partitions, and 2 executor cores.

The runtime config by itself does not prove skew or memory pressure. To classify
the bottleneck, inspect the SQL tab and stage metrics for task duration spread,
spill, shuffle read/write size, failed tasks, and executor balance.
```

#### Conservative Conclusion

```text
The light-medium 3x app has the expected tuned runtime configuration.
AQE is enabled and visibly active, shuffle partitioning is configured at 400,
and executor concurrency is capped at 2 cores per executor.

For this cluster, light-medium is the preferred demo load because BRBF Step 3
completed in about 12 minutes: long enough to show Spark UI behavior, short
enough for a client walkthrough.

Do not optimize further from configuration alone. Continue with SQL query,
stage, task, spill, and skew prompts before making any tuning recommendation.
```

### Physical Plan Bottleneck Prompt

Use this after opening Query 8 for the preferred 3x light-medium BRBF run.

Prompt context:

```text
Application name: emr-migration-brbf-before-dag
Application id: application_1779641349593_0009
EMR cluster: j-3O78ZN9EMO9W2
EMR step id: s-06984631PV9G98A51OTN
Dataset prefix: s3://aigithub-emr-2026/emr-migration-demo-light-medium/
BRBF Step 3 runtime: about 12 minutes

SQL query:
  Query 8
  Submitted: 2026/05/24 21:14:33
  Duration: 7.5 min
  Succeeded jobs: 27-40
  Description: count at BrbfJob.scala:80
```

Input evidence:

```text
Runtime:
  spark.sql.adaptive.enabled=true
  spark.sql.adaptive.coalescePartitions.enabled=true
  spark.sql.shuffle.partitions=400
  spark.default.parallelism=200
  spark.executor.cores=2
  spark.executor.memory=18971M
  spark.serializer=org.apache.spark.serializer.KryoSerializer

Physical plan / SQL metrics:
  AdaptiveSparkPlan isFinalPlan=true
  AQE re-optimization applied
  AQEShuffleRead coalesced
  Exchange hashpartitioning(..., 400)
  InMemoryRelation computed rows: 602,400,000
  InMemoryTableScan output rows: 602,400,000
  final count aggregate output rows: 1

Major scan metrics:
  bids scan: 3,000,000 rows, 257.3 MiB, 128 files
  impressions same-hour scan: 1,785,000 rows, 27.8 MiB, 128 files
  impressions late-hour scan: 315,000 rows, 6.4 MiB, 128 files

Major join metrics:
  ShuffledHashJoin on impressions path:
    output rows: 3,000,000
    build side rows: 2,100,000
    build side data size: about 2.2 GiB
    AQE coalesced partitions: 34
  ShuffledHashJoin on eligible_user_data path:
    output rows: 3,000,000
    build side rows: 500,000
    build side data size: about 2.6 GiB
    AQE coalesced partitions: 40
  ShuffledHashJoin on feature_log path:
    output rows: 602,400,000
    build side rows: 1,000,000
    build side data size: about 25.0 GiB
    unique hash keys: 800,200
  BroadcastHashJoin lookup joins:
    contextual, advertiser, koa_settings, sib
```

Example output:

#### 1. Runtime Configuration Capture

```text
AQE is enabled and active. Query 8 uses AdaptiveSparkPlan isFinalPlan=true and
AQEShuffleRead coalesced. The configured shuffle partition count is 400, and
the plan confirms 400-partition Exchange operators before AQE coalescing.

The key runtime settings are the expected tuned values for this demo run:
400 shuffle partitions, 2 executor cores, Kryo serializer, AQE enabled, and
AQE partition coalescing enabled.
```

#### 2. Query / Stage Focus Check

```text
Query 8 is the correct first focus because it ran for 7.5 minutes out of the
about 12-minute BRBF Step 3 run. That is more than 50% of the application
runtime.

Top stages to inspect next:
  Stage 63: feature-log shuffled hash join build/read path
  Stage 70: final count over InMemoryTableScan / InMemoryRelation
  Stage 56 and Stage 49: upstream 3,000,000-row shuffle join paths
  Stage 42 and Stage 43: scan and impression aggregate/shuffle paths
```

#### 3. Operator Inventory

```text
FileScan parquet:
  bids, impressions same-hour, impressions late-hour, contextual, advertiser,
  koa_settings, eligible_user_data, feature_log, sib

Project / UDF preparation:
  bid_request_hash, user_id_hash, contextual_join_key, rounded_bid_time,
  bid_price_bucket, event_quality_score, campaign_grouping_key

Filter:
  isnotnull filters on join keys

Union:
  same-hour and late-hour impressions feedback

SortAggregate / Sort:
  impression deduplication by bid_request_id
  SIB deduplication by user_id_hash

Exchange:
  hashpartitioning(..., 400) for major shuffle joins and aggregations

BroadcastHashJoin:
  contextual lookup
  advertiser lookup
  koa_settings lookup
  SIB lookup after deduplication

ShuffledHashJoin:
  bids -> impressions_feedback
  joined bids -> eligible_user_data
  joined bids -> feature_log

InMemoryRelation / InMemoryTableScan:
  persisted joined DataFrame, computed/read at 602,400,000 rows

HashAggregate / Exchange SinglePartition:
  final count aggregation for joined.count()
```

#### 4. Shuffle Map

| Exchange / Path | Cause | Evidence | Initial partitions | AQE coalescing |
|---|---|---:|---:|---:|
| `bid_request_id` | impression feedback dedup/join | 2.1M records, 63.0 MiB shuffle bytes written | 400 | 34 |
| `bid_request_id` from bids | bids to impression join | 3.0M records, 830.1 MiB shuffle bytes written | 400 | 34 |
| `bid_request_id,user_id_hash` | eligible user join | 3.0M records, 1092.4 MiB shuffle bytes written | 400 | 40 |
| `user_id_hash,contextual_id` | feature log join | 3.0M records, 1021.2 MiB shuffle bytes written | 400 | not enough stage detail pasted |
| `feature_user_id_hash,feature_contextual_id` | feature log join build side | 1.0M build rows, about 25.0 GiB build side footprint | 400 | not enough stage detail pasted |
| `SinglePartition` | final count aggregation | 400 records to 1 output row | 1 | not applicable |

#### 5. Join And Table-Size Inventory

| Join | Left rows / size | Right rows / size | Size evidence source | Operator | Broadcast? | Next size check |
|---|---:|---:|---|---|---|---|
| bids -> impressions_feedback | 3,000,000 / 257.3 MiB scan | 2,100,000 rows / about 2.2 GiB build side | SQL scan and join metrics | `ShuffledHashJoin` / initial `SortMergeJoin` | no | Stage 42/49 task spread and spill |
| joined -> contextual | 3,000,000 | 500,000 raw rows | validation count and plan | `BroadcastHashJoin` | right side | Broadcast size and executor memory |
| joined -> advertiser | 3,000,000 | 15,000 raw rows | validation count and plan | `BroadcastHashJoin` | right side | Broadcast size |
| joined -> koa_settings | 3,000,000 | 15,000 raw rows | validation count and plan | `BroadcastHashJoin` | right side | Broadcast size |
| joined -> eligible_user_data | 3,000,000 | 500,000 rows / about 2.6 GiB build side | validation count and join metrics | `ShuffledHashJoin` | no | Stage 56 task spread and spill |
| joined -> feature_log | 3,000,000 before join | 1,000,000 rows / about 25.0 GiB build side | validation count and join metrics | `ShuffledHashJoin` | no | Stage 63 task spread, spill, and key skew |
| joined -> SIB | 602,400,000 after feature join | 250,000 raw rows before dedup | validation count and plan | `BroadcastHashJoin` | right side | Confirm SIB broadcast size |

#### 6. Bottleneck Hypotheses

| Hypothesis | Evidence | Confidence | What to verify next |
|---|---|---|---|
| Feature-log join causes row explosion | joined output jumps to 602,400,000 rows; feature-log ShuffledHashJoin build side is about 25.0 GiB | high | Stage 63 duration, task max/median ratio, spill, shuffle read distribution |
| Query cost is dominated by joined.count() materializing cached joined data | Query 8 is `count at BrbfJob.scala:80`; InMemoryRelation computed rows and InMemoryTableScan output rows are both 602,400,000 | high | Stage 70 task duration and spill for count over cached data |
| AQE coalescing is active and helping, but cannot remove data explosion | AQEShuffleRead coalesced appears; 400 exchanges coalesced to 34/40 partitions for some paths | high | Compare initial partitions to actual task counts in stages |
| Small lookup joins are not the bottleneck | BroadcastHashJoin output remains 3,000,000 until feature-log path; small scans are fast | medium-high | Broadcast metrics and executor memory |
| File scans are not the bottleneck | scan times are seconds, file sizes are small, metadata time is 0 ms | high | Stage scan duration and input read time |
| Memory pressure is possible but not proven from this paste | 25.0 GiB feature-log build side footprint is large; pasted sort spill is 0.0 B where shown | medium | Stage 63 memory spill, disk spill, GC time, executor lost/failure counts |
| Skew is possible around feature-log keys | 800,200 unique hash keys for 1,000,000 values, but 602.4M output rows implies fanout on matching keys | medium | Stage 63 median/max task duration and shuffle read skew |

#### 7. Stage Evidence Checklist

Capture these from Spark UI next:

```text
SQL tab:
  Query 8 stage list with duration, task count, input, shuffle read, shuffle write

Stages tab:
  Stage 63 detail:
    task duration min/median/max
    shuffle read min/median/max
    memory spill and disk spill
    GC time
    failed tasks
    executor balance

  Stage 70 detail:
    task duration min/median/max
    input/read from cache
    spill
    GC time
    final aggregation timing

  Stage 56 and Stage 49 detail:
    task duration spread
    shuffle read/write
    spill
    skew indicators

Executors tab:
  active executors
  failed tasks
  total task time
  shuffle read/write by executor
  spill by executor if visible
```

#### 8. Databricks Optimization Path

| Bottleneck area | Current evidence | Databricks-side category |
|---|---|---|
| Feature-log join fanout | 3.0M rows becomes 602.4M rows; 25.0 GiB build side footprint | data model/cardinality review, join-key validation, pre-aggregation/dedup strategy |
| Shuffled joins | multiple `Exchange hashpartitioning(..., 400)` and `ShuffledHashJoin` operators | AQE join strategy, AQE skew join handling, table stats |
| Shuffle partition sizing | 400 configured, AQE coalescing active | AQE partition coalescing already helps; compare Databricks AQE behavior |
| Native operators | FileScan, joins, aggregates, sorts | Photon acceleration for supported operators |
| UDF projections before joins | UDF-derived hash/time/join columns in plan | replace with native expressions where practical |
| Cached joined DataFrame | InMemoryRelation computed at 602.4M rows | cache/persist review; reduce repeated actions or cache narrower data |

#### Conservative Conclusion

```text
Query 8 is the dominant BRBF query for the light-medium demo run. The main
physical-plan bottleneck is not raw file scanning or small lookup broadcast
joins. The expensive shape is the joined DataFrame path, especially the
feature-log ShuffledHashJoin, where the joined output expands to 602.4M rows.

AQE is enabled and active, and it is coalescing shuffle reads. That helps
partition overhead, but it does not solve the underlying row explosion and
large shuffled join footprint.

The next prompt should use stage metrics for Stage 63 and Stage 70 to classify
whether the feature-log path is primarily skew, spill/memory pressure, or
large-but-balanced shuffle work.
```

### Spark Stage Size Prompt

Use this after collecting Query 8 SQL metrics and the top stage list.

Prompt context:

```text
Application name: emr-migration-brbf-before-dag
Application id: application_1779641349593_0009
Longest query: Query 8, count at BrbfJob.scala:80
Query duration: 7.5 min
Dataset prefix: s3://aigithub-emr-2026/emr-migration-demo-light-medium/
Known scale: lightmedium / 3x small
```

Input evidence:

```text
Validation / scan counts:
  bids: 3,000,000 rows, 257.3 MiB scan, 128 files
  impressions_feedback same-hour: 1,785,000 rows, 27.8 MiB scan, 128 files
  impressions_feedback late-hour: 315,000 rows, 6.4 MiB scan, 128 files
  impressions_feedback total: 2,100,000 rows
  contextual: 500,000 rows
  matched_user_data / eligible_user_data source: 500,000 rows
  advertiser: 15,000 rows
  koa_settings: 15,000 rows
  feature_log: 1,000,000 rows
  sib: 250,000 rows

Query 8 SQL metrics:
  InMemoryRelation computed rows: 602,400,000
  InMemoryTableScan output rows: 602,400,000
  feature-log ShuffledHashJoin output rows: 602,400,000
  eligible-user ShuffledHashJoin output rows: 3,000,000
  impression ShuffledHashJoin output rows: 3,000,000

Join-side size metrics:
  impression join build side: about 2.2 GiB, 2,100,000 rows
  eligible-user join build side: about 2.6 GiB, 500,000 rows
  feature-log join build side: about 25.0 GiB, 1,000,000 rows, 800,200 unique hash keys

Stage metrics:
  Stage 63: 7.1 min, 400 tasks, shuffle read 1101.9 MiB / 4,000,000 records
  Stage 207: 2.0 min, 400 tasks, input 67.6 GiB, shuffle write 186.2 MiB
  Stage 206: 1.8 min, 400 tasks, input 67.6 GiB, shuffle write 5.6 GiB
  Stage 176: 1.4 min, 400 tasks, input 67.6 GiB, shuffle write 78.9 MiB
```

Example output:

#### Join / Table-Size Inventory

| Join / path | Left size evidence | Right size evidence | Operator | Stage evidence | Missing size follow-up |
|---|---:|---:|---|---|---|
| bids scan | 3,000,000 rows / 257.3 MiB / 128 files | not applicable | `FileScan parquet` | Stage 42 scan metrics | none |
| impressions scan and union | 1,785,000 same-hour + 315,000 late-hour = 2,100,000 rows; 34.2 MiB scanned | not applicable | `FileScan`, `Filter`, `Union`, `SortAggregate` | Stage 43, 400-partition exchange, 63.0 MiB shuffle bytes written | none |
| bids -> impressions_feedback | bids side 3,000,000 rows | build side 2,100,000 rows / about 2.2 GiB | `ShuffledHashJoin` | Stage 42/49; AQE coalesced to 34 partitions | task spread and spill for Stage 49 |
| joined -> contextual | joined rows before lookup: 3,000,000 | contextual raw count 500,000 | `BroadcastHashJoin` | broadcast shown in plan | broadcast size from SQL metric if needed |
| joined -> advertiser | joined rows before lookup: 3,000,000 | advertiser raw count 15,000 | `BroadcastHashJoin` | broadcast shown in plan | broadcast size from SQL metric if needed |
| joined -> koa_settings | joined rows before lookup: 3,000,000 | koa_settings raw count 15,000 | `BroadcastHashJoin` | broadcast shown in plan | broadcast size from SQL metric if needed |
| joined -> eligible_user_data | left side 3,000,000 rows | build side 500,000 rows / about 2.6 GiB | `ShuffledHashJoin` | Stage 56; 3.0M shuffle records, 1092.4 MiB shuffle bytes written, AQE coalesced to 40 partitions | Stage 56 spill and task spread |
| joined -> feature_log | left side about 3,000,000 rows before join | build side 1,000,000 rows / about 25.0 GiB / 800,200 unique hash keys | `ShuffledHashJoin` | Stage 63; 7.1 min, 1.1 GiB shuffle read / 4.0M records | key cardinality and duplicate counts by `user_id_hash,contextual_id`; Stage 63 spill |
| joined -> SIB | left side after feature join: 602,400,000 rows | SIB raw count 250,000 before dedup | `BroadcastHashJoin` | broadcast shown in plan | post-dedup SIB row count and broadcast size |
| persisted joined DataFrame | 602,400,000 computed rows | not applicable | `InMemoryRelation`, `InMemoryTableScan` | Stage 63 creates/materializes; Stage 70 count reads; Stages 176/206/207 read 67.6 GiB shape | cache size / storage tab if needed |
| final output write | 602,400,000-row joined shape feeds write path | not applicable | parquet write path | Stages 206/207 read 67.6 GiB; shuffle writes 5.6 GiB and 186.2 MiB | output file count and final S3 size |

#### Size Interpretation

```text
The scan sizes are modest. Bids scan reads 257.3 MiB, and impression scans read
about 34.2 MiB total. File scan size is not the root bottleneck.

The important size jump happens in the feature-log join path. The joined data
stays around 3,000,000 rows through impressions, contextual, advertiser,
koa_settings, and eligible_user_data. The feature-log ShuffledHashJoin expands
the result to 602,400,000 rows.

That row expansion explains why later stages read 67.6 GiB even though the raw
input scans are much smaller. Stages 176, 206, and 207 are downstream costs
over the large materialized joined/output shape.
```

#### Missing Size Follow-Ups

```text
Highest priority:
  feature_log duplicate counts by user_id_hash, contextual_id
  joined side duplicate counts by user_id_hash, contextual_id before feature_log join
  Stage 63 memory spill and disk spill
  final output S3 file count and total bytes

Useful but secondary:
  broadcast sizes for contextual, advertiser, koa_settings, and SIB
  cache/storage size for the persisted joined DataFrame
  Stage 206/207 output file count and per-file size distribution
```

#### Conservative Conclusion

```text
The Spark stage-size evidence confirms the bottleneck is not source scan size.
The dominant size event is the feature-log join fanout: a roughly 3M-row joined
path becomes 602.4M rows after the feature-log ShuffledHashJoin.

Stage 63 is the primary compute bottleneck tied to that join. Stages 176, 206,
and 207 are downstream effects because they read the 67.6 GiB materialized
joined/output shape.

The next safest investigation is key-cardinality analysis for the feature-log
join keys, not blind Spark tuning.
```

### Bottleneck Stage Metrics Prompt

Use this after opening Stage 63 from Query 8.

Prompt context:

```text
Application name: emr-migration-brbf-before-dag
Application id: application_1779641349593_0009
SQL query: Query 8, count at BrbfJob.scala:80
Query duration: 7.5 min
Stage: 63 attempt 0
Associated job: 38
Operator path: feature-log ShuffledHashJoin build/read path
```

Input evidence:

```text
Stage 63:
  completed tasks: 400
  total task time: 1.2 h
  shuffle read: 1101.9 MiB / 4,000,000 records
  locality: Node local 281, Rack local 119
  failed tasks: 0
  killed tasks: 0

Task duration:
  min: 68 ms
  p25: 78 ms
  median: 95 ms
  p75: 21 s
  max: 1.5 min

GC time:
  min: 0 ms
  p25: 0 ms
  median: 0 ms
  p75: 0.2 s
  max: 2 s

Shuffle read per task:
  min: 2.4 MiB / 7,763 records
  p25: 2.4 MiB / 7,978 records
  median: 2.5 MiB / 8,111 records
  p75: 3.0 MiB / 11,991 records
  max: 5.1 MiB / 24,047 records

Longest tasks:
  task 531: 1.5 min, 2 s GC, 72 MiB peak execution memory, 5.1 MiB / 23,972 records
  task 302: 1.4 min, 1 s GC, 72 MiB peak execution memory, 5.1 MiB / 24,047 records
  task 279: 1.1 min, 0.8 s GC, 72 MiB peak execution memory, 4.4 MiB / 20,099 records
  task 606: 1.1 min, 0.4 s GC, 72 MiB peak execution memory, 4.5 MiB / 20,077 records

Executor distribution:
  executor task counts range from 44 to 89
  executor task time ranges from 8.5 min to 14 min
  failed tasks: 0 on every executor

Top application stages:
  Stage 63, count at BrbfJob.scala:80:
    duration: 7.1 min
    tasks: 400/400
    shuffle read: 1101.9 MiB
  Stage 207, parquet at DatasetIO.scala:59:
    duration: 2.0 min
    tasks: 400/400
    input: 67.6 GiB
    shuffle write: 186.2 MiB
  Stage 206, parquet at DatasetIO.scala:59:
    duration: 1.8 min
    tasks: 400/400
    input: 67.6 GiB
    shuffle write: 5.6 GiB
  Stage 176, count at BrbfJob.scala:101:
    duration: 1.4 min
    tasks: 400/400
    input: 67.6 GiB
    shuffle write: 78.9 MiB
```

Example output:

#### Stage Classification

| Category | Classification | Evidence |
|---|---|---|
| Skew / stragglers | likely task-duration skew | median task duration is 95 ms, p75 is 21 s, max is 1.5 min |
| Shuffle byte skew | mild to moderate, not enough alone | median shuffle read is 2.5 MiB, max is 5.1 MiB |
| Record-count skew | moderate | median records read is 8,111, max is 24,047 |
| Memory pressure | not primary from this evidence | peak execution memory on longest tasks is 72 MiB; GC max is 2 s |
| Spill pressure | not proven | spill metrics were not pasted for Stage 63 |
| Executor imbalance | mild, not primary | executor task counts vary 44-89, but no failures/exclusions |
| Failure instability | not present | 0 failed tasks and 0 killed tasks |

#### App-Level Stage Ranking

```text
Stage 63 is the primary bottleneck stage by duration: 7.1 minutes.
It is tied to Query 8 / count at BrbfJob.scala:80 and the feature-log
ShuffledHashJoin path.

Stages 206 and 207 are secondary write-path stages. They are shorter than
Stage 63, but still important because they each read 67.6 GiB and are tied to
parquet at DatasetIO.scala:59. This means the row explosion from the joined
DataFrame also makes the final write path heavier.

Stage 176 is a secondary count/action stage at BrbfJob.scala:101. It reads the
same 67.6 GiB materialized shape and is probably downstream of the large joined
DataFrame rather than the root cause.
```

#### Interpretation

```text
Stage 63 is not slow because every task is heavy. Most tasks are extremely
short: median duration is only 95 ms. The stage is slow because a subset of
tasks are stragglers: p75 jumps to 21 seconds and the maximum task duration is
1.5 minutes.

Shuffle read size is not wildly skewed by bytes. The median task reads 2.5 MiB
and the max reads 5.1 MiB, about 2x. Record count skew is stronger but still
not extreme by itself: median 8,111 records versus max 24,047, about 3x.

The likely issue is task-duration skew caused by uneven join work or hash-map
behavior in the feature-log ShuffledHashJoin, rather than raw shuffle bytes
alone. This fits the physical-plan finding that the feature-log path expands
the joined DataFrame to 602.4M rows.

Memory pressure is not the primary conclusion from this paste. GC is low
relative to the longest task durations, peak execution memory on the longest
tasks is only 72 MiB, and there are no failed tasks. Spill still needs to be
checked explicitly before ruling it out.
```

#### What To Capture Next

```text
For Stage 63:
  memory spill min/median/max
  disk spill min/median/max
  task deserialization time
  result serialization time
  scheduler delay if visible
  input records by task if Spark UI exposes it

For the feature-log join:
  top user_id_hash/contextual_id key counts
  duplicate counts per join key on both sides
  feature_log rows per user_id_hash/contextual_id

For Stage 70:
  confirm whether final count over 602.4M cached rows has similar stragglers
  or is mostly a consequence of the Stage 63 materialization.
```

#### Databricks / Photon Opportunity Signals

```text
AQE partition coalescing:
  Already active. It helps reduce partition overhead but does not remove
  feature-log join fanout.

AQE skew join handling:
  Potentially relevant if key-level skew is confirmed from Stage 63 or source
  key counts. The task-duration spread is large enough to justify checking.

Join cardinality / data modeling:
  Strongest opportunity. The feature-log join expands the joined result to
  602.4M rows, so validate whether the join should be many-to-many or whether
  feature_log should be deduplicated, windowed, pre-aggregated, or constrained
  before the join.

Photon:
  Likely helpful for native joins, aggregations, scans, and sorts, but Photon
  will not by itself fix an unintended join fanout.

UDF review:
  UDF-derived join/prep columns appear before joins. Review later for native
  expression rewrites, but do not treat UDFs as the primary bottleneck from
  Stage 63 alone.
```

#### Conservative Conclusion

```text
Stage 63 confirms that the feature-log join path has a straggler pattern.
The stage reads about 1.1 GiB across 400 tasks with no failures, low GC, and
only mild-to-moderate shuffle byte skew, but task duration is highly uneven:
median 95 ms, p75 21 seconds, max 1.5 minutes.

The app-level stage ranking confirms Stage 63 is the largest single stage.
Stages 206, 207, and 176 are secondary downstream costs over the 67.6 GiB
materialized joined/output shape.

The best current classification is feature-log join fanout with task-duration
skew/stragglers. Memory pressure and disk spill are not proven from the pasted
metrics and should not be claimed as primary until spill metrics are captured.

The next investigation should validate feature-log join key cardinality and
capture Stage 63 spill metrics plus Stage 70 metrics for the final count over
the 602.4M-row cached joined DataFrame.
```

### Stage Operator And Code Mapping Prompt

Use this after Query 8, Stage 63, and the source code are available.

Prompt context:

```text
Application name: emr-migration-brbf-before-dag
Application id: application_1779641349593_0009
SQL query: Query 8
Query description: count at BrbfJob.scala:80
Query duration: 7.5 min
Primary stage: Stage 63, 7.1 min
Dataset: emr-migration-demo-light-medium
```

Input evidence:

```text
Spark UI:
  Query 8 is triggered by count at BrbfJob.scala:80.
  Stage 63 is the longest stage at 7.1 minutes.
  Stage 63 reads 1101.9 MiB / 4,000,000 shuffle records across 400 tasks.
  Stage 63 task duration skew: median 95 ms, p75 21 s, max 1.5 min.
  Query 8 InMemoryRelation / InMemoryTableScan has 602,400,000 rows.
  Feature-log ShuffledHashJoin output rows: 602,400,000.
  Feature-log build side: 1,000,000 rows, about 25.0 GiB, 800,200 unique hash keys.

Source code:
  spark/src/main/scala/com/demo/emr/BrbfJob.scala
  spark/src/main/scala/com/demo/emr/FeatureLogConverter.scala
  spark/src/main/scala/com/demo/emr/common/DatasetIO.scala
```

Example output:

#### 1. Action-Line Mapping

Confirmed action line:

```scala
printMetric("joined_row_count", joined.count())
```

Source location:

```text
spark/src/main/scala/com/demo/emr/BrbfJob.scala:80
```

Interpretation:

```text
Query 8 and Stage 63 are triggered by the first action on the persisted joined
DataFrame. The count itself is simple, but it forces Spark to materialize the
entire upstream joined DataFrame.
```

#### 2. Joined DataFrame Construction

Source location:

```text
spark/src/main/scala/com/demo/emr/BrbfJob.scala:51-78
```

Relevant code shape:

```scala
val joined = bids
  .join(impressions, Seq("bid_request_id"), "left")
  .join(contextual, Seq("contextual_id"), "left")
  .join(broadcast(advertiser), Seq("advertiser_id"), "left")
  .join(broadcast(koaSettings), Seq("advertiser_id", "campaign_id"), "left")
  .join(eligibleUserData, Seq("bid_request_id", "user_id_hash"), "left")
  .join(
    featureLog,
    bids("user_id_hash") === featureLog("feature_user_id_hash") &&
      bids("contextual_id") === featureLog("feature_contextual_id"),
    "left"
  )
  .join(sib, bids("user_id_hash") === sib("sib_user_id_hash"), "left")
  .persist(StorageLevel.MEMORY_AND_DISK)
```

Mapping:

| Spark UI evidence | Code location | Interpretation |
|---|---|---|
| `count at BrbfJob.scala:80` | `BrbfJob.scala:80` | `joined.count()` materializes the joined DataFrame |
| `InMemoryRelation`, `InMemoryTableScan`, 602.4M rows | `BrbfJob.scala:78` | `joined.persist(StorageLevel.MEMORY_AND_DISK)` caches the expanded joined result |
| `ShuffledHashJoin` on `bid_request_id,user_id_hash` | `BrbfJob.scala:56` | eligible user data left join |
| `ShuffledHashJoin` on `user_id_hash,contextual_id` | `BrbfJob.scala:57-62` | feature-log left join; primary fanout point |
| `BroadcastHashJoin` for advertiser / KOA | `BrbfJob.scala:54-55` | explicit `broadcast(...)` hints |
| `BroadcastHashJoin` for contextual/SIB | `BrbfJob.scala:53`, `BrbfJob.scala:63` | planner chose broadcast for smaller lookup-style joins |
| 400-partition exchanges | submit-time config and join keys | `spark.sql.shuffle.partitions=400`; AQE later coalesces some reads |

#### 3. Feature-Log Code Mapping

Feature-log prep in BRBF:

```text
spark/src/main/scala/com/demo/emr/BrbfJob.scala:220-229
```

Relevant code:

```scala
private def prepareFeatureLog(raw: DataFrame): DataFrame = {
  raw.select(
    col("feature_event_id"),
    col("user_id_hash").as("feature_user_id_hash"),
    col("contextual_id").as("feature_contextual_id"),
    col("feature_name"),
    col("feature_value"),
    col("feature_value_bucket"),
    col("rounded_event_time").as("feature_rounded_event_time")
  )
}
```

Feature-log converter source:

```text
spark/src/main/scala/com/demo/emr/FeatureLogConverter.scala:24-66
```

Relevant signals:

```text
FeatureLogConverter deduplicates by feature_event_id, not by the BRBF join key.
The converted output is repartitioned by user_id_hash and contextual_id.
Therefore many feature_event_id rows can still share the same
user_id_hash/contextual_id join key and fan out during the BRBF join.
```

#### 4. Downstream Count And Write Mapping

Source locations:

```text
spark/src/main/scala/com/demo/emr/BrbfJob.scala:88-101
spark/src/main/scala/com/demo/emr/BrbfJob.scala:103
spark/src/main/scala/com/demo/emr/common/DatasetIO.scala:37-59
```

Mapping:

| Spark UI evidence | Code location | Interpretation |
|---|---|---|
| Stage 176, `count at BrbfJob.scala:101`, input 67.6 GiB | `BrbfJob.scala:101` | `finalOutput.count()` over downstream aggregate/union result |
| Stages 206/207, `parquet at DatasetIO.scala:59`, input 67.6 GiB | `BrbfJob.scala:103`, `DatasetIO.scala:37-59` | final parquet write path |
| High/low aggregation branches | `BrbfJob.scala:88-99`, `BrbfJob.scala:243-296` | downstream aggregation over the already-expanded joined DataFrame |

#### 5. Stage-To-Code Hypothesis

```text
Stage 63:
  Primary source action:
    BrbfJob.scala:80, joined.count()

  Primary upstream code path:
    BrbfJob.scala:51-78, joined DataFrame construction and persistence

  Most important operator-to-code mapping:
    BrbfJob.scala:57-62, featureLog join on
    user_id_hash == feature_user_id_hash and
    contextual_id == feature_contextual_id

  Reason:
    Spark UI shows the feature-log ShuffledHashJoin expands the joined result
    to 602.4M rows, and Stage 63 is the longest stage with strong task-duration
    straggler behavior.
```

#### 6. What To Verify In Code Or With A Follow-Up Query

```text
Check feature-log join cardinality:
  count rows per feature_user_id_hash, feature_contextual_id in featureLog
  count rows per user_id_hash, contextual_id in bids/joined before featureLog
  compare expected business cardinality with observed many-to-many fanout

Check whether featureLog should be:
  deduplicated by user_id_hash/contextual_id
  windowed by rounded_event_time
  pre-aggregated before BRBF
  filtered to relevant feature_name / feature_value_bucket
  joined with an additional time/window predicate

Check whether repeated actions after line 80 are intentional:
  BrbfJob.scala:81-86 repeats counts against joined
  BrbfJob.scala:101 counts finalOutput before write
```

#### 7. Databricks / Photon Opportunity Signals

| Code area | Current evidence | Databricks-side category |
|---|---|---|
| `BrbfJob.scala:57-62` feature-log join | 602.4M output rows; Stage 63 stragglers | join cardinality fix, AQE skew handling if key skew confirmed |
| `FeatureLogConverter.scala:38` dedup by `feature_event_id` only | join key may still be many-to-many | pre-aggregation/dedup by business join key |
| `BrbfJob.scala:78` persist joined | caches 602.4M-row shape | cache/persist review, cache narrower or later if possible |
| `BrbfJob.scala:81-86` repeated joined counts | multiple actions over large cached shape | metric/audit count strategy review |
| `BrbfJob.scala:243-296` aggregations | downstream of row explosion | Photon/native aggregate acceleration, but after cardinality validation |
| `DatasetIO.scala:59` parquet write | downstream 67.6 GiB shape | Delta layout/file-size tuning later |

#### Conservative Conclusion

```text
Stage 63 maps to Query 8, triggered by joined.count() at BrbfJob.scala:80.
The expensive work is not the count operator itself; it is materializing the
persisted joined DataFrame built at BrbfJob.scala:51-78.

The strongest code-level bottleneck candidate is the feature-log left join at
BrbfJob.scala:57-62. Spark UI shows this join expands the joined DataFrame to
602.4M rows, and Stage 63 shows task-duration stragglers while processing that
path.

The next engineering investigation should focus on feature-log join key
cardinality and business semantics before proposing Spark tuning. The relevant
source areas are BrbfJob.scala:57-62, BrbfJob.scala:220-229, and
FeatureLogConverter.scala:24-66.
```

### Feature-Log Join Cardinality Follow-Up Prompt

Use this after mapping Stage 63 to the feature-log join code path.

Prompt context:

```text
Application name: emr-migration-brbf-before-dag
Application id: application_1779641349593_0009
SQL query: Query 8, count at BrbfJob.scala:80
Primary stage: Stage 63
Primary code path: BrbfJob.scala:57-62
Dataset: emr-migration-demo-light-medium

Known evidence:
  Stage 63 is the longest stage at 7.1 minutes.
  Stage 63 has task-duration stragglers: median 95 ms, p75 21 s, max 1.5 min.
  Query 8 materializes 602,400,000 joined rows.
  Feature-log ShuffledHashJoin output rows: 602,400,000.
  Feature-log build side: 1,000,000 rows, about 25.0 GiB, 800,200 unique hash keys.

Code evidence:
  BrbfJob.scala:57-62 joins featureLog on user_id_hash and contextual_id.
  BrbfJob.scala:80 triggers joined.count().
  FeatureLogConverter.scala:38 deduplicates by feature_event_id only.
  FeatureLogConverter.scala:59 repartitions converted feature_log by user_id_hash and contextual_id.
  scripts/generate_mock_data.py creates skewed user_id and contextual_id values for both bids and feature_log.
```

Example output:

#### 1. Fanout Hypothesis

```text
The feature-log join is very likely a many-to-many fanout point.

The BRBF join key is:
  bids.user_id_hash == featureLog.feature_user_id_hash
  bids.contextual_id == featureLog.feature_contextual_id

FeatureLogConverter deduplicates only by feature_event_id, not by
user_id_hash/contextual_id. Therefore multiple feature events can still match
the same bid-side key.

The mock data generator also creates skewed user_id and contextual_id patterns
for both bids and feature_log. That means hot user/contextual combinations can
exist on both sides of the join.

Spark UI confirms the result: the path grows from about 3M rows before
feature_log to 602.4M joined rows after the feature-log ShuffledHashJoin.
```

#### 2. Safe Cardinality Checks

Run these in an EMR notebook, `spark-shell`, or a Databricks notebook against
the same light-medium S3 prefix.

Check feature-log duplicate keys:

```scala
val featureLog = spark.read.parquet(
  "s3://aigithub-emr-2026/emr-migration-demo-light-medium/converted/feature_log/year=2026/month=05/day=21/hour=10/"
)

val featureKeyCounts = featureLog
  .groupBy($"user_id_hash", $"contextual_id")
  .count()
  .withColumnRenamed("count", "feature_rows_per_key")

featureKeyCounts
  .orderBy(desc("feature_rows_per_key"))
  .show(20, false)

featureKeyCounts
  .agg(
    count(lit(1)).as("feature_join_keys"),
    sum($"feature_rows_per_key").as("feature_rows"),
    avg($"feature_rows_per_key").as("avg_feature_rows_per_key"),
    expr("percentile_approx(feature_rows_per_key, 0.50)").as("p50"),
    expr("percentile_approx(feature_rows_per_key, 0.75)").as("p75"),
    expr("percentile_approx(feature_rows_per_key, 0.95)").as("p95"),
    max($"feature_rows_per_key").as("max_feature_rows_per_key")
  )
  .show(false)
```

Check bid-side duplicate keys before the feature-log join:

```scala
val bids = spark.read.parquet(
  "s3://aigithub-emr-2026/emr-migration-demo-light-medium/raw/bids/year=2026/month=05/day=21/hour=10/"
)

val bidKeyCounts = bids
  .groupBy($"user_id_hash", $"contextual_id")
  .count()
  .withColumnRenamed("count", "bid_rows_per_key")

bidKeyCounts
  .orderBy(desc("bid_rows_per_key"))
  .show(20, false)

bidKeyCounts
  .agg(
    count(lit(1)).as("bid_join_keys"),
    sum($"bid_rows_per_key").as("bid_rows"),
    avg($"bid_rows_per_key").as("avg_bid_rows_per_key"),
    expr("percentile_approx(bid_rows_per_key, 0.50)").as("p50"),
    expr("percentile_approx(bid_rows_per_key, 0.75)").as("p75"),
    expr("percentile_approx(bid_rows_per_key, 0.95)").as("p95"),
    max($"bid_rows_per_key").as("max_bid_rows_per_key")
  )
  .show(false)
```

Estimate fanout by key without materializing all joined rows:

```scala
val fanoutByKey = bidKeyCounts
  .join(featureKeyCounts, Seq("user_id_hash", "contextual_id"), "inner")
  .withColumn("estimated_join_rows", $"bid_rows_per_key" * $"feature_rows_per_key")

fanoutByKey
  .orderBy(desc("estimated_join_rows"))
  .show(20, false)

fanoutByKey
  .agg(
    count(lit(1)).as("matching_keys"),
    sum($"estimated_join_rows").as("estimated_feature_join_rows"),
    avg($"estimated_join_rows").as("avg_join_rows_per_matching_key"),
    expr("percentile_approx(estimated_join_rows, 0.50)").as("p50"),
    expr("percentile_approx(estimated_join_rows, 0.75)").as("p75"),
    expr("percentile_approx(estimated_join_rows, 0.95)").as("p95"),
    max($"estimated_join_rows").as("max_join_rows_for_one_key")
  )
  .show(false)
```

#### 3. Interpretation Rules

| Result | Interpretation | Next action |
|---|---|---|
| `estimated_feature_join_rows` is near `602,400,000` | fanout is explained by key cardinality | focus on business semantics and join design |
| a small number of keys dominate `estimated_join_rows` | key skew plus fanout | investigate AQE skew join and key-specific data treatment |
| many keys have moderate fanout | broad many-to-many relationship | pre-aggregate, deduplicate, filter, or window feature_log before join |
| feature side has many rows per `user_id_hash/contextual_id` | feature_log grain is finer than BRBF join grain | decide whether to use latest feature, aggregate features, or add time predicate |
| bid side has many rows per `user_id_hash/contextual_id` | bid-side hot keys contribute to fanout | check whether high-frequency branch/salting should happen before feature join |
| fanout estimate is far below 602.4M | missing predicate or analysis mismatch | inspect the actual joined side before feature_log, not raw bids |

#### 4. Databricks Signals

```text
AQE skew join:
  Relevant only if the fanoutByKey result shows a small set of dominant keys.
  Stage 63 task duration spread justifies checking, but AQE skew handling is not
  the first conclusion until key dominance is measured.

Photon:
  Likely accelerates native join/aggregate/scan/write operators, but it does
  not remove a 602.4M-row fanout if the join semantics produce it.

Delta layout and statistics:
  Useful for scan pruning, join planning, and file sizing. They may improve
  planning around feature_log, but they do not fix many-to-many cardinality.

Code/data model:
  Highest-value path if fanout is confirmed. Candidate changes include
  deduplicating feature_log by user_id_hash/contextual_id, pre-aggregating
  feature rows, filtering to relevant feature_name values, joining only the
  latest/windowed feature, or adding a time-window predicate.
```

#### Current Conclusion

```text
Based on Spark UI and code evidence, the current best classification is
feature-log join cardinality/fanout with Stage 63 stragglers.

This is not yet a Spark tuning recommendation. The next safe action is to run
key-cardinality checks for user_id_hash/contextual_id on both sides of the
feature-log join and compare the estimated fanout with the observed 602.4M
joined rows.

If fanout is expected by business semantics, Databricks/Photon can help
accelerate the workload. If fanout is accidental, the primary fix is code or
data-model semantics before infrastructure tuning.
```

### Final Databricks / Photon Recommendation

Use this as the executive summary after the light-medium source analysis is complete.

```text
Primary recommendation:
  Do not treat the source finding as a pure Spark tuning issue.
  First classify whether the feature-log fanout is intentional.

If fanout is accidental:
  Correct the feature_log join grain before tuning:
    deduplicate by user_id_hash/contextual_id if that matches business logic
    pre-aggregate feature_log
    filter to relevant feature_name / feature_value_bucket
    add a time/window predicate
    join only latest/windowed features

If fanout is intentional:
  Keep the many-to-many join and optimize the execution:
    store prepared inputs as Delta
    collect statistics
    cluster/layout feature_log around join keys and time
    keep AQE and skew handling enabled
    use Photon for native scans, joins, aggregates, sorts, and writes
    reduce repeated count/filter actions over the expanded joined DataFrame
    tune final output file layout

Photon expectation with code unchanged:
  planning estimate: about 1.2x-2x end-to-end
  upside estimate: 2x-3x if native joins/aggregations/writes dominate
  do not promise 5x because Photon cannot remove 602.4M-row fanout

Native expression follow-up:
  Udfs.sha256String -> likely native sha2(...)
  Udfs.roundTimestampToMinutes -> likely native timestamp math
  Udfs.stableUuidFromString -> test carefully before rewriting

Final source conclusion:
  The preferred 3x light-medium demo load is fully analyzed. The strongest
  current-state finding is feature-log join cardinality/fanout with Stage 63
  task-duration stragglers. The next migration step is either key-cardinality
  validation or Databricks baseline/explain validation, not another blind EMR
  tuning attempt.
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
  Verify AWS credentials and Maven availability.
  Rebuild and upload the JAR so BrbfJob submit-time --conf overrides are active.
  Create a new EMR cluster with larger worker local/EBS disk from the start.
  Rerun only medium Step 3 first, because medium Steps 1 and 2 already completed on S3.

If the rerun completes:
  Validate final output.
  Run the full Spark UI prompt sequence against the completed application.

If the rerun fails:
  Preserve the first root-cause error, failed stage, task failure text, and executor loss evidence before making another change.
```
