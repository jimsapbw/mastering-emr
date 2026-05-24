# Resume Plan

Use this file to resume the EMR-side migration demo after a break, cluster termination, or new Codex session.

## Current Goal

Build an EMR-side Spark demo that reproduces the BRBF-style **before DAG**:

```text
Read sources
Compute columns early
Impression join
Advertiser join
Feature-log join
SIB join
Manual salting
Union + sort
Output to S3
```

Databricks migration planning is intentionally deferred until the EMR baseline is built and measured.

Master runbook:

```text
emr_migration_demo/KT/emr_migration_demo_master_runbook.md
```

## Completed So Far

### 1. S3 Prefixes

Created the demo layout under:

```text
s3://aigithub-emr-2026/emr-migration-demo/
```

Main folders:

```text
raw/
converted/
final/
validation/
artifacts/
```

Raw datasets:

```text
bids
impressions_feedback
contextual
matched_user_data
advertiser
koa_settings
feature_log
sib
```

Reference:

```text
emr_migration_demo/KT/01_create_s3_prefixes.md
```

### 2. Mock Data Generator

Created:

```text
emr_migration_demo/scripts/generate_mock_data.py
```

Ran the `tiny` scale preset and wrote Snappy Parquet to S3.

Tiny row counts:

```text
bids: 100,000
impressions_feedback: 70,000
contextual: 20,000
matched_user_data: 20,000
advertiser: 1,000
koa_settings: 1,000
feature_log: 50,000
sib: 20,000
```

Reference:

```text
emr_migration_demo/KT/02_mock_dataset_generation.md
```

### 3. Maven Setup

Installed Maven:

```text
Apache Maven 3.8.4
```

Reference:

```text
emr_migration_demo/KT/03_maven_setup.md
```

### 4. Dataset Contract And Validation

Created:

```text
emr_migration_demo/scripts/validate_mock_data.py
emr_migration_demo/KT/04_dataset_contract_and_validation.md
```

Validation passed on the tiny dataset.

Important observed results:

```text
counts.bids=100000
counts.impressions_feedback_total=70000
late_feedback.same_hour_rows=59500
late_feedback.late_n_plus_1_rows=10500
```

All planned joins have non-zero key overlap.

Reference:

```text
emr_migration_demo/KT/04_dataset_contract_and_validation.md
```

### 5. GitHub Backup

Backed up the demo scaffold to:

```text
https://github.com/jimsapbw/mastering-emr/tree/main/emr_migration_demo
```

Commit:

```text
eed4b8c Add EMR migration demo scaffold
```

Reference:

```text
emr_migration_demo/KT/05_git_backup.md
```

Latest GitHub backup:

```text
Repository: https://github.com/jimsapbw/mastering-emr/tree/main/emr_migration_demo
Branch: main
Commit: a81e5c1 Add Spark troubleshooting cheat sheet and follow-up prompts
```

May 24 troubleshooting docs restore checkpoint:

```text
Repository: https://github.com/jimsapbw/mastering-emr/tree/main/emr_migration_demo
Branch: main
Commit: 3fd1f9e Organize Spark troubleshooting docs
Organized Spark troubleshooting docs under KT/spark_troubleshooting/
Added client Spark UI prompt outcome examples
Added Airflow DAG Entry-Point Prompt
Added Physical Plan To Code Mapping Prompt
Updated cheat sheet prompt sequence
Updated KT links to the new troubleshooting folder
```

This latest commit includes:

```text
Step 08 FeatureLogConverter
Step 09 EligibleUserDataLogConverter
Step 10 BrbfJob
KT docs for steps 08 and 09
KT docs for Step 10 implementation and validation
EMR baseline problem statement
EMR Spark troubleshooting guide
Master runbook
Airflow orchestration plan
Databricks post-migration plan
Databricks Photon best practices
Future migration stories
Updated resume plan and KT index
Client Spark UI troubleshooting plan
Spark troubleshooting cheat sheet
Stage-level skew, memory pressure, and partitioning prompts
```

The commit was created from:

```text
/mnt/tmp/mastering-emr-codex-backup
```

and pushed with GitHub HTTPS/PAT authentication.

Local patch backup:

```text
emr_migration_demo/KT/0001-Add-EMR-converter-steps-and-KT-docs.patch
```

### 6. Scala Maven Spark Project

Created the Scala/Maven project:

```text
emr_migration_demo/spark/pom.xml
emr_migration_demo/spark/src/main/scala/com/demo/emr/RawDataCountApp.scala
```

Verified:

```text
Maven build: PASS
RawDataCountApp S3 read/count run: PASS
```

Reference:

```text
emr_migration_demo/KT/06_scala_maven_spark_project.md
```

### 7. Shared Scala Utilities

Created shared utilities:

```text
common/AppConfig.scala
common/ArgsParser.scala
common/S3Paths.scala
common/SparkSessionFactory.scala
common/DatasetIO.scala
common/Udfs.scala
```

Refactored `RawDataCountApp` to use the helpers and verified the same tiny S3 counts.

Reference:

```text
emr_migration_demo/KT/07_shared_scala_utilities.md
```

### 8. Feature Log Converter

Created the first EMR pipeline entry point:

```text
emr_migration_demo/spark/src/main/scala/com/demo/emr/FeatureLogConverter.scala
```

Verified:

```text
Maven build: PASS
FeatureLogConverter tiny S3 run: PASS
```

Observed:

```text
feature_log_converter.raw_feature_log_count=50000
feature_log_converter.converted_feature_log_count=50000
feature_log_converter.distinct_user_hash_count=40200
feature_log_converter.distinct_contextual_count=13035
```

Reference:

```text
emr_migration_demo/KT/08_feature_log_converter.md
```

### 9. Eligible User Data Converter

Created the second EMR pipeline entry point:

```text
emr_migration_demo/spark/src/main/scala/com/demo/emr/EligibleUserDataLogConverter.scala
```

Verified:

```text
Maven build: PASS
EligibleUserDataLogConverter tiny S3 run: PASS
```

Observed:

```text
eligible_user_data_converter.raw_matched_user_data_count=20000
eligible_user_data_converter.converted_eligible_user_data_count=20000
eligible_user_data_converter.eligible_user_count=5000
eligible_user_data_converter.high_frequency_count=4000
eligible_user_data_converter.low_frequency_count=16000
```

Reference:

```text
emr_migration_demo/KT/09_eligible_user_data_converter.md
```

### 10. BRBF Job

Created the main EMR baseline entry point:

```text
emr_migration_demo/spark/src/main/scala/com/demo/emr/BrbfJob.scala
```

Verified:

```text
Maven build: PASS
BrbfJob tiny S3 run: PASS
```

Observed:

```text
brbf_job.source_bids_count=100000
brbf_job.source_impressions_count=70000
brbf_job.source_contextual_count=20000
brbf_job.source_advertiser_count=1000
brbf_job.source_koa_settings_count=1000
brbf_job.source_eligible_user_data_count=20000
brbf_job.source_feature_log_count=50000
brbf_job.source_sib_count=16200
brbf_job.joined_row_count=1080000
brbf_job.high_frequency_joined_count=1000000
brbf_job.low_frequency_joined_count=80000
brbf_job.final_output_count=91883
```

Reference:

```text
emr_migration_demo/KT/10_brbf_job.md
emr_migration_demo/KT/10_brbf_job_validation.md
```

### 11. Step 10 Manual Validation Progress

Created:

```text
emr_migration_demo/KT/10_brbf_job_validation.md
```

Validated manually in `spark-shell`:

```text
raw bids read correctly
prepareBids created early derived columns
supporting datasets were prepared
sample bids joined to impressions
sample bids joined to contextual
sample bids joined to advertiser
sample bids joined to KOA settings
sample bids joined to eligible_user_data
sample bids joined to feature_log
sample bids joined to SIB
5 sample bids expanded to 5000 joined rows
post-join columns were created
high/low aggregation example was documented
```

Current pause point:

```text
Resume with KT/10_brbf_job_validation.md.
Continue from Validation 7: Final Output.
Then proceed to Step 11: package/upload JAR and run EMR steps.
```

## Important Commands

Create S3 prefixes:

```bash
bash emr_migration_demo/scripts/create_s3_prefixes.sh aigithub-emr-2026
```

Generate tiny mock data:

```bash
spark-submit \
  --master local[*] \
  emr_migration_demo/scripts/generate_mock_data.py \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11 \
  --scale tiny \
  --partitions 32 \
  --mode overwrite
```

Validate mock data:

```bash
spark-submit \
  --master local[*] \
  emr_migration_demo/scripts/validate_mock_data.py \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11
```

Back up local demo files to S3:

```bash
aws s3 sync emr_migration_demo s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo/ --region us-east-1
```

Restore local demo files from S3:

```bash
aws s3 sync s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo/ emr_migration_demo/ --region us-east-1
```

S3 code backup location:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo/
```

Step 11 zip backup:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo_2026-05-22_step11_complete.zip
```

Step 12 zip backup:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo_2026-05-22_step12_small_run_complete.zip
```

Latest Step 12 troubleshooting playbook zip backup:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo_2026-05-23_spark_troubleshooting_playbook_a81e5c1.zip
```

May 24 restore checkpoint backup:

```text
Troubleshooting docs commit: 3fd1f9e Organize Spark troubleshooting docs
Latest restore checkpoint commit subject: Add Databricks troubleshooting prompt flow
Local MCBP medium disk checkpoint zip created:
  /mnt/tmp/emr_migration_demo_2026-05-24_mcbp_medium_disk_checkpoint.zip
S3 zip backup: pending because this shell has no AWS credentials
S3 zip target:
  s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo_2026-05-24_mcbp_medium_disk_checkpoint.zip
S3 explorable folder backup: complete
S3 explorable folder:
  s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo/
GitHub local commit created in /mnt/tmp/mastering-emr-git-sync:
  Add medium disk checkpoint and rerun config
GitHub push: pending because this shell cannot prompt for GitHub HTTPS credentials
```

May 24 Spark troubleshooting prompt checkpoint:

```text
Troubleshooting docs now live under KT/spark_troubleshooting/.
Each main troubleshooting doc has an index/table of contents, except README.

Small-run source analysis through Guide Step 5 is documented:
  Stage 38 -> Job 22 -> SQL Query 8 -> count at BrbfJob.scala:80
  Stage 38 is likely dominated by too many small shuffle partitions/task waves.
  The source-side follow-up prompts now cover skew, memory pressure, and small shuffle partitions.

Step 6 explain-plan guidance is documented:
  add df.explain("formatted") immediately before the expensive action or write.
  For the demo, add joined.explain("formatted") before joined.count().
  Use Databricks explain output as expected-plan evidence, not runtime proof.

Client Spark UI prompt sequence is aligned across the plan and cheat sheet:
  1. source-side investigation through Stage Operator And Code Mapping
  2. matching source-side follow-up prompt, if needed
  3. Databricks-side validation prompts

New Databricks-side prompts added:
  Explain Insertion Prompt
  Databricks Explain Plan Interpretation Prompt
  Databricks Genie Explain Prompt Builder
  Databricks Count Baseline Prompt
  Databricks Baseline Run Interpretation Prompt
  Databricks Genie Baseline Prompt Builder
```

Latest explorable S3 folder backup:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo/
```

Download zip backup:

```bash
aws s3 cp \
  s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo_2026-05-22_step12_small_run_complete.zip \
  /mnt/tmp/emr_migration_demo_2026-05-22_step12_small_run_complete.zip \
  --region us-east-1
```

Unzip backup:

```bash
mkdir -p /mnt/tmp/emr_migration_demo_restore
unzip /mnt/tmp/emr_migration_demo_2026-05-22_step12_small_run_complete.zip -d /mnt/tmp/emr_migration_demo_restore
```

GitHub code backup location:

```text
https://github.com/jimsapbw/mastering-emr/tree/main/emr_migration_demo
```

Install Maven on a new cluster:

```bash
sudo dnf install -y maven
```

## Next Recommended Steps

### Step 11: Package And Run Tiny End-To-End

Build:

```bash
cd emr_migration_demo/spark
mvn -Dmaven.repo.local=/mnt/tmp/m2/repository clean package
```

Upload JAR:

```bash
aws s3 cp target/*.jar s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/ --region us-east-1
```

Run the 3 steps on tiny data as EMR steps and validate output.

Current Step 11 status:

```text
JAR build: PASS
JAR contents verified: PASS
JAR upload to S3: PASS
EMR Step 1 FeatureLogConverter: COMPLETED
EMR Step 2 EligibleUserDataLogConverter: COMPLETED
EMR Step 3 BrbfJob: COMPLETED
Final output count validation: PASS
Final branch count validation: PASS
```

Current cluster ID:

```text
j-37DIRU3WHU1C5
```

Reference:

```text
emr_migration_demo/KT/11_package_upload_run_emr_steps.md
```

Observed Step 11 results:

```text
Step 1 ID: s-05260282LTDLOYKA0X5W
Step 2 ID: s-05079433OEGPED8V5I0K
Step 3 ID: s-021230414IK4ECF2EB4X
finalBrbf.count() = 91883
low_frequency_hash = 72500
high_frequency_salted = 19383
```

### Step 12: Scale Data

After the tiny end-to-end pipeline works, regenerate data with `small`:

```text
bids: 1,000,000
impressions_feedback: 700,000
feature_log: 500,000
contextual: 200,000
matched_user_data: 200,000
sib: 100,000
advertiser: 10,000
koa_settings: 10,000
```

Only scale further after the EMR baseline works correctly.

Current Step 12 status:

```text
small S3 prefix layout: PASS
small data generation: PASS
small raw data validation: PASS
small EMR Step 1 FeatureLogConverter: COMPLETED on new cluster
small EMR Step 2 EligibleUserDataLogConverter: COMPLETED on new cluster
small EMR Step 3 BrbfJob: COMPLETED on new cluster
small final output validation: PASS from prior small run; re-check if needed
small Spark troubleshooting walkthrough: IN PROGRESS
medium controlled baseline plan: IN PROGRESS
```

Small data prefix:

```text
s3://aigithub-emr-2026/emr-migration-demo-small/
```

Original small EMR step IDs:

```text
Step 1 ID: s-068112428LZKPF5DOUMU
Step 2 ID: s-0730235RZAH2SJX8RGR
Step 3 ID: s-024954934CXKODACJN4N
```

New troubleshooting cluster:

```text
Cluster name: itv-github-dev-cluster
Cluster ID: j-3S62AU5IR98MM
EMR release: emr-7.13.0
Spark: 3.5.6
Primary DNS: ec2-100-55-172-52.compute-1.amazonaws.com
Step concurrency: 1
Status during troubleshooting: Waiting
```

Small Step 3 application used for Spark History Server analysis:

```text
Application ID: application_1779541486316_0003
Application name: emr-migration-brbf-before-dag
EMR step ID: s-10425111SB3XON8UMCDV
Cluster ID: j-3S62AU5IR98MM
State: FINISHED/SUCCEEDED
```

Runtime configuration confirmed for the small Step 3 troubleshooting run:

```text
spark.sql.adaptive.enabled=false
spark.sql.adaptive.skewJoin.enabled=false
spark.sql.shuffle.partitions=200
spark.default.parallelism=200
spark.dynamicAllocation.enabled=true
spark.executor.cores=4
spark.executor.memory=18971M
spark.driver.memory=2048M
```

The explicit `spark.sql.autoBroadcastJoinThreshold` value was not visible in Spark Properties, which is normal when the default is used. Broadcast behavior was still confirmed from the SQL physical plan because multiple `BroadcastHashJoin` and `BroadcastExchange` operators appeared.

Current Spark History Server learning checkpoint:

```text
Longest job inspected: Job 22
Job duration: 1.9 min
Associated SQL Query: 8
Longest stage: Stage 38
Stage 38 duration: 1.6 min
Stage 38 share of Job 22: roughly 84%
Stage 38 task count: 200
Stage 38 shuffle read: 398.4 MiB / 1,500,000 records
Stage 38 shuffle write: 9.8 KiB / 200 records
```

Stage 38 diagnosis so far:

```text
Not obvious skew:
  median shuffle read = 2 MiB
  max shuffle read = 3.1 MiB

Not obvious memory pressure:
  median task duration = 4 s
  median GC time = 50 ms
  max task duration = 20 s
  max GC time = 0.6 s
  peak execution memory = 68 MiB
  executor memory from Environment = 18971M

Not obvious shuffle fetch/network issue:
  median shuffle fetch wait = 1 ms
  max shuffle fetch wait = 14 ms

Not obvious scheduler delay:
  median scheduler delay = 3 ms
  max scheduler delay = 19 ms

Executor balance looked healthy:
  Executor 1 = 100 tasks, 199.2 MiB shuffle read, 6.5 min task time
  Executor 2 = 100 tasks, 199.2 MiB shuffle read, 6.5 min task time

Likely current hypothesis:
  too many small shuffle partitions / task-wave overhead for available parallelism.
  Spark History Executors summary showed 12 active executor cores / approximate task slots.
  With 200 shuffle tasks, Spark needs about 17 task waves.
  Median task duration was about 4 s, so expected wave time was about 68 s.
  Observed Stage 38 duration was about 96 s.
```

Important physical plan findings from the longest query:

```text
BroadcastHashJoin and BroadcastExchange are present for small joins.
ShuffledHashJoin is present for larger joins.
Many Exchange nodes use 200 partitions.
The plan includes UDF Project nodes before joins.
The plan includes HashAggregate and count(distinct bid_request_id).
The plan ends with Union, range partitioning, Sort, and WriteFiles to Parquet snappy.
```

Medium Controlled Baseline Plan, MCBP, current status:

```text
MCBP #1 medium scale definition: PASS
  bids: 10,000,000
  impressions_feedback: 7,000,000
  contextual: 1,000,000
  matched_user_data: 1,000,000
  advertiser: 25,000
  koa_settings: 25,000
  feature_log: 3,000,000
  sib: 500,000

MCBP #2 medium S3 prefix layout: PASS
  s3://aigithub-emr-2026/emr-migration-demo-medium/

MCBP #3 medium raw data generation: PASS
  total raw objects observed: 1168
  total raw size observed: about 1.31 GiB

MCBP #4 medium raw data validation: PASS
  counts.bids=10000000
  counts.impressions_feedback_total=7000000
  counts.feature_log=3000000
  counts.matched_user_data=1000000
  counts.sib=500000
  top advertiser ids each have about 900000 bid rows
  top contextual ids each have about 100000 bid rows

MCBP #5 medium Step 1 FeatureLogConverter: COMPLETED
  step id: s-05377242F3A87LPS5JUZ

MCBP #6 medium Step 2 EligibleUserDataLogConverter: COMPLETED
  step id: s-04508423OSLJSZBQW90K

MCBP #7 medium Step 3 BrbfJob attempt 1: FAILED
  step id: s-0661117QH89RUU39ZQ1
  application id: application_1779541486316_0010
  action: count at BrbfJob.scala:80
  job id: 20
  failed stage: Stage 37.0
  root cause: java.io.IOException: No space left on device
  interpretation: executor local disk exhausted during shuffle/intermediate processing

MCBP #7 medium Step 3 BrbfJob attempt 2: FAILED
  step id: s-09085642ZQTGXNML2TKJ
  application id: application_1779541486316_0011
  controlled change: EMR managed scaling max raised, cluster scaled to 6 YARN nodes, rerun with --executor-cores 2
  live UI observed Job 20 count at BrbfJob.scala:80 running more than 15 minutes
  live UI observed Stage 38 active with 200 tasks and about 11.8 GiB input
  failed tasks increased before final failure
  interpretation: scale-out helped the run progress farther but did not make medium Step 3 reliable on this cluster
```

Current MCBP decision:

```text
Do not keep tuning blindly on this cluster.
Create a restore checkpoint.
Create a new EMR cluster with larger worker local/EBS disk from the start.
Rerun only medium Step 3 first because medium Steps 1 and 2 already completed on S3.

If a new cluster is deferred, keep the two failed medium attempts as the medium disk-pressure example.
```

Resume action as of 5:00 P.M. EST on 2026-05-23:

```text
Continue Step 12 Spark troubleshooting, not pipeline execution.

1. Memory pressure:
   Continue learning how to diagnose GC, memory spill, disk spill, and peak execution memory.
   Keep real Stage 38 example, hypothetical bad examples, and large-data benchmarks in the guide.

2. Small shuffle partitions:
   Explain how we concluded Stage 38 likely has too many small shuffle partitions.
   Use examples and task-wave math so the reasoning is easy to repeat.

3. DAG visualization:
   Decide when DAG Visualization is useful versus optional.
   Treat SQL physical plan and stage metrics as primary evidence unless the DAG view adds clarity.

4. Code/operator mapping:
   Learn how to map Job -> Stage -> SQL Query -> physical operator -> Scala code path.
   For this run, connect Stage 38 / SQL Query 8 back to the relevant Exchange, aggregate, join, or write operator.

5. Small-load Databricks/Photon conclusion:
   After code/operator mapping, classify each relevant operator for Databricks:
     AQE partition coalescing
     AQE skew handling
     AQE join strategy / broadcast
     Photon-friendly scans, projections, joins, aggregates, sorts, writes
     less Photon-friendly Scala UDF projections
   The small-load analysis is complete only after this conclusion is written.
```

Planned progression after the small-load troubleshooting pass:

```text
1. Finish the small-load troubleshooting journey first.
   Do not jump to a bigger dataset until the small run is fully understood:
     runtime configuration
     longest job/query
     bottleneck stage
     skew check
     memory/spill check
     partition/task-wave check
     DAG visualization decision
     physical operator and code-path mapping
     small-load Databricks/Photon conclusion

2. Then move to a medium-load run.
   Goal: collect a more realistic troubleshooting example where skew, memory pressure, or both may become visible.
   Use the same Spark UI workflow and prompts, but expect larger shuffle sizes, longer task durations, and more meaningful spill/GC evidence.

   Important distinction:
     Small load completes the analysis pattern and small-load Photon story.
     Medium load is still needed for stronger production-like evidence about skew, memory pressure, spill, and realistic Photon/AQE benefit.

3. If medium data still does not naturally show skew or memory pressure, create a practical stress scenario.
   Preferred options:
     increase data scale
     increase hot-key concentration
     reduce available executor memory only for a controlled test
     increase shuffle/aggregation pressure

4. Keep the client-facing goal in mind.
   The final workflow should let us paste runtime settings, physical plan, stage metrics, executor metrics, and optional table counts into prompts and quickly classify the likely bottleneck.
```

Reference:

```text
emr_migration_demo/KT/12_scale_data_and_tune_runtime.md
emr_migration_demo/KT/spark_troubleshooting/emr_spark_troubleshooting_guide.md
```

## Resume Prompt

Use this prompt in a future Codex session:

```text
Read emr_migration_demo/KT/00_resume_plan.md, KT/12_scale_data_and_tune_runtime.md, KT/spark_troubleshooting/emr_spark_troubleshooting_guide.md, KT/spark_troubleshooting/client_spark_ui_troubleshooting_plan.md, KT/spark_troubleshooting/client_spark_ui_troubleshooting_prompt_examples.md, and KT/spark_troubleshooting/spark_troubleshooting_cheat_sheet.md.

Resume Step 12 from the MCBP medium controlled baseline checkpoint.

Small-load source troubleshooting is complete:
- small BRBF app: application_1779541486316_0003
- small finding: Stage 38 / Job 22 / SQL Query 8 / count at BrbfJob.scala:80
- conclusion: too many small shuffle partitions/task waves, not primary skew or memory pressure
- Stage Operator And Code Mapping Prompt now includes the completion-gate conclusion

Medium data and converted inputs are already prepared:
- medium prefix: s3://aigithub-emr-2026/emr-migration-demo-medium/
- MCBP #1-#4 complete: scale confirmed, S3 layout created, data generated, raw data validated
- MCBP #5 complete: FeatureLogConverter, step s-05377242F3A87LPS5JUZ
- MCBP #6 complete: EligibleUserDataLogConverter, step s-04508423OSLJSZBQW90K

Medium Step 3 has failed twice on cluster j-3S62AU5IR98MM:
- attempt 1: step s-0661117QH89RUU39ZQ1, app application_1779541486316_0010, Job 20 / Stage 37.0, root cause java.io.IOException: No space left on device
- attempt 2: step s-09085642ZQTGXNML2TKJ, app application_1779541486316_0011, controlled change was EMR scale-out plus --executor-cores 2; live UI reached Job 20 / Stage 38 with about 11.8 GiB input over 200 tasks before failing

Current decision:
- stop tuning blindly on the current cluster
- create a restore checkpoint before infrastructure changes
- create a new EMR cluster with larger worker local/EBS disk from the start
- rerun only medium Step 3 first, because medium Steps 1 and 2 already wrote converted data to S3

New-cluster target:
- same EMR release if possible: EMR 7.13.0 / Spark 3.5.6-amzn-2
- 1 primary node
- core instance type: r8g.xlarge
- 4 core nodes
- 0 task nodes for first rerun
- Spot disabled; use On-Demand for the first baseline rerun
- core worker EBS: gp3, 300 GiB, 3000 IOPS, 125 MiB/s throughput, 1 volume per instance
- managed scaling enabled with max cluster size 6, max core nodes 4, max On-Demand 6
- Spark event log enabled

Before rerunning Step 3:
- rebuild and upload the JAR because BrbfJob now allows submit-time --conf overrides by setting its baseline values only as defaults
- do not rerun medium Steps 1 or 2 first
- keep the live Spark UI tunnel ready for Job 20 / Stage 37 or 38

Medium Step 3 first-rerun submit overrides:
- --conf spark.sql.shuffle.partitions=400
- --conf spark.executor.cores=2
- --conf spark.sql.adaptive.enabled=true
- --conf spark.sql.adaptive.coalescePartitions.enabled=true
- --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
- --conf spark.shuffle.compress=true
- --conf spark.shuffle.spill.compress=true

Keep updating the resume plan, EMR troubleshooting guide, client Spark UI plan, prompt examples, and cheat sheet so the workflow remains copy/paste friendly for client Spark UI investigations.
```
