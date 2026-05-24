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
small Spark troubleshooting walkthrough: COMPLETE
medium controlled baseline plan: BLOCKED at Step 3 until larger-disk cluster rerun
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

Current handoff checkpoint as of 2026-05-24:

```text
Primary objective:
  Resume Step 12 at the MCBP medium Step 3 disk-pressure checkpoint.

Verified locally:
  BrbfJob.scala uses setDefaultSparkConf(...), so submit-time --conf values can override
  the baseline AQE, skew handling, shuffle partition, and default parallelism settings.

Still pending before rerun:
  rebuild JAR
  upload rebuilt JAR to S3
  create or select a larger-disk replacement EMR cluster
  rerun only medium Step 3

Do not rerun:
  medium raw data generation
  medium Step 1 FeatureLogConverter
  medium Step 2 EligibleUserDataLogConverter
```

Active replacement EMR cluster for environment prep:

```text
Cluster name: itv-github-dev-cluster
Cluster ID: j-3O78ZN9EMO9W2
Region: us-east-1
EMR release: emr-7.13.0
Spark version: 3.5.6
Status when recorded: Waiting
Capacity: 1 Primary, 4 Core, 0 Task
Primary public DNS: ec2-100-55-172-52.compute-1.amazonaws.com
Core instance type: r8g.xlarge
Core purchasing: On-Demand
Core EBS: 300 GiB, 1 volume per core node
Managed scaling: enabled
Minimum cluster size: 4 instances
Maximum cluster size: 6 instances
Maximum core nodes: 4 instances
Maximum On-Demand instances: 6 instances
S3 logs: s3://aws-logs-210570462212-us-east-1/elasticmapreduce/
CloudWatch log group: /aws/emr/j-3O78ZN9EMO9W2
Persistent UIs: Spark History Server, YARN Timeline Server, Tez UI
Idle auto-termination: 12 hours
```

Environment prep scope approved:

```text
1. Update KT docs with the new cluster ID and configuration.
2. Verify AWS credentials from the shell.
3. Verify access to the new cluster with aws emr describe-cluster.
4. Verify Maven on the primary node, install if needed.
5. Rebuild the JAR from current source.
6. Upload the rebuilt JAR to S3.

Stop before rerunning medium Step 3.
```

Environment prep status as of 2026-05-24:

```text
Step 1 docs update: COMPLETE
Step 2 AWS credentials check: COMPLETE
  account: 210570462212
  role: arn:aws:sts::210570462212:assumed-role/AmazonEMR-InstanceProfile-20260502T160809/i-05120b102340638e8
Step 3 cluster describe: COMPLETE
  cluster id: j-3O78ZN9EMO9W2
  name: itv-github-dev-cluster
  state: WAITING
  release: emr-7.13.0
  primary public DNS: ec2-100-55-172-52.compute-1.amazonaws.com
Step 4 local Maven readiness: COMPLETE
  Maven installed locally: Apache Maven 3.8.4
Step 5 local JAR rebuild: COMPLETE
  Build command:
    cd spark
    mvn -Dmaven.repo.local=/mnt/tmp/m2/repository clean package
  Result: BUILD SUCCESS
  JAR:
    spark/target/emr-migration-demo-0.1.0.jar
  Verified classes:
    com.demo.emr.FeatureLogConverter
    com.demo.emr.EligibleUserDataLogConverter
    com.demo.emr.BrbfJob
Step 6 S3 JAR upload: COMPLETE
  uploaded:
    s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
  S3 listing:
    2026-05-24 19:00:43 53147 emr-migration-demo-0.1.0.jar

Environment prep is complete.
Stop before rerunning medium Step 3 unless explicitly approved.
```

Medium rerun submission on replacement cluster:

```text
Submitted on: 2026-05-24
Cluster ID: j-3O78ZN9EMO9W2
Submission shape: full medium 3-step sequence

Note:
  The prior controlled plan preferred rerunning only medium Step 3 because
  medium Steps 1 and 2 had already completed on S3. For this rerun, all
  three medium steps were submitted in order.

Current step snapshot:
  Step 1 FeatureLogConverter:
    step id: s-068154131WPULWDW4XNP
    application id: application_1779641349593_0001
    state: COMPLETED
    start time: 2026-05-24T19:02:43.480000+00:00
    end time: 2026-05-24T19:03:31.597000+00:00
    console time: May 24, 2026 at 15:02 to 15:02
    runtime: 48 seconds
  Step 2 EligibleUserDataLogConverter:
    step id: s-00846262IMCXFAJXETUF
    application id: application_1779641349593_0002
    state: COMPLETED
    start time: 2026-05-24T19:03:36.602000+00:00
    end time: 2026-05-24T19:04:12.651000+00:00
    console time: May 24, 2026 at 15:02 to 15:03
    runtime: 36 seconds
  Step 3 BrbfJob disk baseline:
    step id: s-08787702FEIHIIC8N85H
    application id: application_1779641349593_0003
    state: RUNNING
    start time: 2026-05-24T19:04:17.656000+00:00
    Spark app start time: 2026-05-24T19:04:20.222GMT
    Spark History Server app state when checked: running / completed=false

Step 3 still uses the planned first-rerun overrides:
  --conf spark.sql.shuffle.partitions=400
  --conf spark.executor.cores=2
  --conf spark.sql.adaptive.enabled=true
  --conf spark.sql.adaptive.coalescePartitions.enabled=true
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
  --conf spark.shuffle.compress=true
  --conf spark.shuffle.spill.compress=true

Live Spark UI checkpoint:
  Spark History URL through tunnel:
    http://localhost:18080/history/application_1779641349593_0003/jobs/
  Active job:
    Job 37
    description: count at BrbfJob.scala:80
    submitted: 2026-05-24T19:06:16.341GMT
    duration when user copied UI: about 6.8 min
    stages: 0/7 succeeded
    tasks when user copied UI: 83/512, 13 running
  API snapshot shortly after:
    status: RUNNING
    stage ids: 60, 56, 57, 61, 58, 55, 59
    tasks: 96/512 completed, 13 active
    failed tasks: 0
    active stages: 1
  Follow-up API snapshot:
    tasks: 109/512 completed, 13 active
    failed tasks: 0
  Stage view checkpoint:
    active stages: 1
    pending stages: 6
    completed stages: 37
    skipped stages: 18
    active stage: Stage 61
    description: count at BrbfJob.scala:80
    submitted: 2026-05-24 19:06:16
    duration when copied: 8.3 min
    tasks when copied: 109/196, 13 running
    input shown in UI: 2010.9 MiB
    shuffle write shown in UI: 117.2 GiB
    pending stages for Job 37: 60, 59, 58, 57, 56, 55
  Stage 61 API checkpoint:
    status: ACTIVE
    tasks: 128/196 complete, 13 active
    failed tasks: 0
    memory spill: 3,487,983,206,400 bytes, about 3.2 TiB
    disk spill: 150,354,839,245 bytes, about 140 GiB
    peak execution memory: 520,812,365,684 bytes, about 485 GiB aggregate
    shuffle read: 2,487,355,609 bytes, about 2.3 GiB
    shuffle read records: 8,435,363
    shuffle write: 151,271,936,539 bytes, about 141 GiB
    shuffle write records: 3,575,298,561
    GC time: 54,951 ms
    shuffle fetch wait: 11,013 ms

Live Executors checkpoint:
  summary:
    active executors including driver: 7
    dead executors: 0
    storage memory: 0.0 B / 59.3 GiB
    disk used: 0.0 B
    total executor cores: 12
    active tasks: 13
    failed tasks: 0
    complete tasks: 566
    total tasks: 579
    task time: 1.6 h
    GC time: 55 s
    input: 1.3 GiB
    shuffle read: 8.3 GiB
    shuffle write: 117.8 GiB
  executor balance:
    executor 1: 121 complete, 2 active, 0 failed, 22.6 GiB shuffle write
    executor 2: 141 complete, 2 active, 0 failed, 21.3 GiB shuffle write
    executor 3: 133 complete, 3 active, 0 failed, 20.5 GiB shuffle write
    executor 4: 140 complete, 2 active, 0 failed, 20.3 GiB shuffle write
    executor 5: 17 complete, 2 active, 0 failed, 16.7 GiB shuffle write
    executor 6: 14 complete, 2 active, 0 failed, 16.5 GiB shuffle write
  interpretation:
    No dead executors and no failed tasks are visible in this snapshot.
    The executor table's Disk Used column reflects storage/disk persistence, not
    total task spill. Stage 61 task metrics show substantial memory and disk spill,
    but the larger-disk replacement cluster is still progressing without executor loss.
  Job 37 completion checkpoint:
    completion time: 2026-05-24T19:21:48.775GMT
    status: SUCCEEDED
    completed tasks: 196
    skipped tasks: 316
    failed task attempts: 2
    killed tasks: 0
    completed stages: 1
    skipped stages: 6
    failed stages: 0
    interpretation: the spill-heavy joined.count() path recovered from 2 failed
      task attempts and completed without failed stages. Step 3 continued running
      after Job 37, so the pipeline moved past the previous failure checkpoint.
  Job 38 runtime estimate checkpoint:
    checked at: 2026-05-24T19:42:55Z
    status: RUNNING
    active stage: Stage 70
    active stage tasks: 115/400 complete, 9 active
    job tasks: 115/921 complete
    failed task attempts: 2
    killed tasks: 0
    failed stages: 0
    active stage elapsed: about 21 minutes
    rough estimate at current stage rate: Stage 70 alone may need about 50 more minutes,
      plus later pending stages and remaining Step 3 actions. This is slow but still
      progressing; the main warning signs remain failed stages, executor loss, or
      rapidly increasing failed task attempts.
  Job 38 risk checkpoint:
    status: RUNNING
    job tasks: 402/921 task attempts complete
    completed task indices: 273
    failed task attempts: 19
    active tasks: 11
    completed stages: 1
    failed stage attempts: 2
    killed tasks: 0
    active stage: Stage 70 attempt 2
    active stage attempt progress: 64/120 complete, 11 active
    failure seen in prior Stage 70 attempt:
      FetchFailedException
      No route to host: ip-172-31-82-135.ec2.internal:7337
    interpretation:
      Full medium is no longer just slow; it is showing shuffle fetch instability.
      It remains alive, but risk is elevated and this scale is too heavy for the
      planned 20-minute demo target.
  Cancel attempt:
    command attempted:
      aws emr cancel-steps --region us-east-1 --cluster-id j-3O78ZN9EMO9W2 --step-ids s-08787702FEIHIIC8N85H
    result:
      AccessDeniedException
    reason:
      AmazonEMR-InstanceProfile-20260502T160809 does not have elasticmapreduce:CancelSteps
    current state after failed cancel:
      EMR step still RUNNING
      Spark Job 38 still RUNNING
    safe next options:
      cancel from AWS console or an IAM principal with elasticmapreduce:CancelSteps
      or explicitly approve a YARN application kill from the cluster if console cancel is not available
  Console cancel result:
    state: CANCELLED
    state change reason: Cancelled by user
    end time: 2026-05-24T20:46:26.039000+00:00
    final Step 3 outcome:
      intentionally cancelled after collecting enough evidence that full medium
      is too slow/unstable for the 20-minute demo target
    final Spark evidence before cancel:
      Job 37 succeeded and proved the larger-disk cluster got past the first
      old no-space failure checkpoint.
      Job 38 was still running with 19 failed task attempts, 2 failed stage
      attempts, and FetchFailedException / No route to host evidence.
    next planned action:
      create smalllevel2 scale, likely 1.5x small, under
      s3://aigithub-emr-2026/emr-migration-demo-smalllevel2/
      The immediate target is about 10 minutes, not the earlier 20-minute
      light-medium target.
```

Future smalllevel2 demo-scale plan:

```text
Why:
  The full medium run is useful as a stress/evidence run, but it is too heavy
  for an interactive client demo target. The next practical demo scale should
  preserve the same BRBF workload shape while targeting about 10 minutes
  end-to-end on the replacement cluster/config.

When:
  Do this after the medium Step 3 cancellation and evidence are documented.

Target:
  smalllevel2 runtime goal: about 10 minutes end-to-end
  cluster/config: keep the same replacement-cluster shape and Step 3 runtime overrides

Known scale anchors:
  small = 1x
    bids: 1,000,000
    impressions_feedback: 700,000
    contextual: 200,000
    matched_user_data: 200,000
    advertiser: 10,000
    koa_settings: 10,000
    feature_log: 500,000
    sib: 100,000
  medium = 10x bids and much heavier joined/shuffle behavior
    bids: 10,000,000
    impressions_feedback: 7,000,000
    contextual: 1,000,000
    matched_user_data: 1,000,000
    advertiser: 25,000
    koa_settings: 25,000
    feature_log: 3,000,000
    sib: 500,000

Observed medium evidence:
  Step 3 on the larger-disk replacement cluster was intentionally cancelled
  after about 102 minutes. Job 37 completed and proved the disk fix got past
  the old no-space checkpoint. Job 38 was still running and showed elevated
  risk: failed task attempts, failed stage attempts, and shuffle fetch
  instability.

Initial scale hypothesis:
  Because medium did not complete and showed non-linear shuffle/spill behavior,
  start at about 1.5x small before jumping higher.

Recommended smalllevel2 row counts:
  bids: 1,500,000
  impressions_feedback: 1,050,000
  contextual: 300,000
  matched_user_data: 300,000
  advertiser: 12,000
  koa_settings: 12,000
  feature_log: 750,000
  sib: 150,000

Implementation note:
  scripts/generate_mock_data.py now includes the smalllevel2 preset.
  KT/12_scale_data_and_tune_runtime.md includes copy/paste commands for
  creating the S3 layout, generating raw data, validating raw data, and
  submitting the three EMR steps on cluster j-3O78ZN9EMO9W2.

Observed smalllevel2 result:
  BRBF Step 3 completed in about 4 minutes on the replacement cluster.
  Interpretation: 1.5x small is stable and useful as a safe demo baseline, but
  it undershoots the desired 10-minute BRBF troubleshooting target.

Next sizing recommendation:
  Skip the 2x rung unless a very cautious intermediate test is needed.
  Use the 3x light-medium candidate next for a better 10-minute calibration.
  This became the selected next run for the replacement cluster.

Candidate 3x light-medium row counts:
  bids: 3,000,000
  impressions_feedback: 2,100,000
  contextual: 500,000
  matched_user_data: 500,000
  advertiser: 15,000
  koa_settings: 15,000
  feature_log: 1,000,000
  sib: 250,000

Implementation note:
  scripts/generate_mock_data.py now includes the lightmedium preset.
  Use --scale lightmedium with base prefix:
    s3://aigithub-emr-2026/emr-migration-demo-light-medium/
  KT/12_scale_data_and_tune_runtime.md includes copy/paste commands for
  creating, generating, validating, and submitting the light-medium run.

Observed light-medium result:
  BRBF Step 3 completed in about 12 minutes on the replacement cluster.
  Interpretation: 3x light-medium is the preferred demo load for this cluster.
  It is long enough to show useful Spark UI behavior and short enough for a
  client walkthrough.

Light-medium Spark UI analysis result:
  app id: application_1779641349593_0009
  app name: emr-migration-brbf-before-dag
  EMR step id: s-06984631PV9G98A51OTN
  dominant query: Query 8 / count at BrbfJob.scala:80
  query duration: 7.5 minutes
  dominant stage: Stage 63 / Job 38
  stage duration: 7.1 minutes
  stage metrics:
    400 tasks
    shuffle read: 1101.9 MiB / 4,000,000 records
    median task duration: 95 ms
    p75 task duration: 21 s
    max task duration: 1.5 min
    failed tasks: 0
  physical-plan finding:
    feature-log ShuffledHashJoin expands the joined path from about
    3,000,000 rows to 602,400,000 rows
  code mapping:
    BrbfJob.scala:80 triggers joined.count()
    BrbfJob.scala:51-78 builds and persists joined
    BrbfJob.scala:57-62 is the featureLog join on user_id_hash/contextual_id
    FeatureLogConverter.scala:38 deduplicates by feature_event_id, not by
    user_id_hash/contextual_id
  classification:
    feature-log join cardinality/fanout with Stage 63 task-duration stragglers
    not primarily source scan size
    not proven memory pressure or spill from the pasted evidence
  Databricks recommendation:
    first validate whether feature-log fanout is business-expected
    if accidental, fix join grain / pre-aggregate / deduplicate / add time window
    if intentional, keep fanout but use Delta layout, AQE/skew handling, Photon,
    and reduce repeated actions over the expanded joined DataFrame
  Photon expectation if code is kept as-is:
    planning estimate: about 1.2x-2x end-to-end improvement
    possible upside: 2x-3x if native joins/aggregates/writes dominate
    do not promise 5x because Photon does not remove 602.4M-row fanout
  UDF rewrite note:
    sha256String can likely become native sha2(...)
    roundTimestampToMinutes can likely become native timestamp math
    stableUuidFromString needs correctness testing before rewrite
    UDF rewrite alone is secondary to fanout and likely modest compared with
    cardinality/layout changes
  worked examples updated:
    KT/spark_troubleshooting/client_spark_ui_troubleshooting_prompt_examples.md
    now includes light-medium examples for Airflow entry point, runtime config,
    physical plan bottleneck, Spark stage size, bottleneck stage metrics,
    stage/operator/code mapping, and feature-log cardinality follow-up.

Scale-selection rule:
  If a completed run exists, estimate:
    estimated_target_scale = completed_scale * (10 minutes / observed_runtime_minutes)
  If only the cancelled medium run is available, treat medium as an upper
  bound, not a linear sizing datapoint. The cancelled 10x run exceeded 100
  minutes and was unstable, so the next practical demo scale should stay close
  to small.

Caution:
  Do not rely only on linear scaling. The BRBF joined/shuffle path expands
  nonlinearly because of joins, high-frequency rows, aggregation, spill, and
  sort/write behavior. The 1.5x run completed safely in about 4 minutes, and
  the 3x run completed in about 12 minutes. Keep full medium as the upper-risk
  guardrail.

Proposed future prefix:
  s3://aigithub-emr-2026/emr-migration-demo-smalllevel2/

Next proposed prefix:
  s3://aigithub-emr-2026/emr-migration-demo-light-medium/
```

Current resume checkpoint:

```text
The EMR-side light-medium analysis loop is complete.

Do not submit another EMR tuning run before syncing the repo unless there is a
new explicit experiment.

Next planned action:
  sync the updated code and KT docs to the Git repository.

After git sync, optional next work:
  run the feature-log key-cardinality checks from the prompt examples
  or begin the Databricks baseline/explain-plan path.
```

Resume environment prep after AWS credentials are available:

```bash
aws sts get-caller-identity --region us-east-1

aws emr describe-cluster \
  --region us-east-1 \
  --cluster-id j-3O78ZN9EMO9W2 \
  --query 'Cluster.[Id,Name,Status.State,ReleaseLabel,MasterPublicDnsName]' \
  --output table

aws s3 cp \
  spark/target/emr-migration-demo-0.1.0.jar \
  s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar \
  --region us-east-1
```

Preflight checks for the next shell:

```bash
aws sts get-caller-identity --region us-east-1
mvn -version
```

If Maven is missing on the active machine or new EMR primary:

```bash
sudo dnf install -y maven
```

Then rebuild and upload from the repo root:

```bash
cd spark
mvn -Dmaven.repo.local=/mnt/tmp/m2/repository clean package
aws s3 cp target/*.jar s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/ --region us-east-1
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

Resume Step 12 after the completed light-medium Spark UI analysis checkpoint.

Small-load source troubleshooting is complete:
- small BRBF app: application_1779541486316_0003
- small finding: Stage 38 / Job 22 / SQL Query 8 / count at BrbfJob.scala:80
- conclusion: too many small shuffle partitions/task waves, not primary skew or memory pressure
- Stage Operator And Code Mapping Prompt now includes the completion-gate conclusion

Medium stress evidence is complete enough for now:
- medium prefix: s3://aigithub-emr-2026/emr-migration-demo-medium/
- old cluster j-3S62AU5IR98MM failed medium Step 3 twice with disk/shuffle pressure
- replacement cluster j-3O78ZN9EMO9W2 got past the old no-space checkpoint but full medium was intentionally cancelled after about 102 minutes
- conclusion: full medium is useful stress evidence, but too heavy/unstable for the client walkthrough target

Calibrated demo load is complete:
- smalllevel2 / 1.5x completed BRBF Step 3 in about 4 minutes
- lightmedium / 3x completed BRBF Step 3 in about 12 minutes
- preferred demo prefix: s3://aigithub-emr-2026/emr-migration-demo-light-medium/
- preferred demo app: application_1779641349593_0009
- preferred demo step: s-06984631PV9G98A51OTN
- cluster: j-3O78ZN9EMO9W2

Light-medium Spark UI analysis is complete:
- dominant query: Query 8 / count at BrbfJob.scala:80, duration 7.5 minutes
- dominant stage: Stage 63 / Job 38, duration 7.1 minutes
- Stage 63: 400 tasks, 1101.9 MiB / 4,000,000 shuffle records, median task 95 ms, p75 21 s, max 1.5 min
- physical-plan finding: feature-log ShuffledHashJoin expands about 3M rows to 602.4M rows
- code mapping: BrbfJob.scala:80 -> joined.count(); BrbfJob.scala:57-62 -> featureLog join on user_id_hash/contextual_id
- feature-log converter note: FeatureLogConverter.scala:38 deduplicates by feature_event_id, not by user_id_hash/contextual_id
- classification: feature-log join cardinality/fanout with Stage 63 task-duration stragglers
- not primary source scan size; not proven memory pressure/spill from pasted metrics

Databricks recommendation:
- validate whether feature-log fanout is business-expected
- if accidental: fix join grain, pre-aggregate, deduplicate, filter, or add a time/window predicate
- if intentional: optimize Delta layout/statistics, AQE/skew handling, Photon, and repeated actions over the expanded DataFrame
- Photon with code unchanged: plan for about 1.2x-2x end-to-end; treat 2x-3x as upside; do not promise 5x because Photon cannot remove 602.4M-row fanout
- native expression follow-up: sha256String -> sha2(...), roundTimestampToMinutes -> native timestamp math, stableUuidFromString needs correctness tests

Docs updated:
- KT/12_scale_data_and_tune_runtime.md
- KT/spark_troubleshooting/emr_spark_troubleshooting_guide.md
- KT/spark_troubleshooting/client_spark_ui_troubleshooting_plan.md
- KT/spark_troubleshooting/client_spark_ui_troubleshooting_prompt_examples.md
- KT/spark_troubleshooting/spark_troubleshooting_cheat_sheet.md

Next planned action:
- sync the updated code and KT docs to the Git repository
- do not run more EMR tuning before git sync unless a new explicit experiment is requested
```
