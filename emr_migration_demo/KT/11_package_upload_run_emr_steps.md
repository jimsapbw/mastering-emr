# Step 11: Package, Upload JAR, And Run EMR Steps

## Purpose

Run the three Spark Scala entry points as EMR steps using one shared JAR:

```text
com.demo.emr.FeatureLogConverter
com.demo.emr.EligibleUserDataLogConverter
com.demo.emr.BrbfJob
```

This is the first move from local/manual `spark-submit` testing toward an EMR pipeline shape.

## Current EMR Cluster

Detected from:

```text
/mnt/var/lib/info/job-flow.json
```

Cluster ID:

```text
j-37DIRU3WHU1C5
```

Cluster shape at time of validation:

```text
Primary: r8g.xlarge x 1
Core:    r8g.xlarge x 2
Task:    requested count 0
```

## Build JAR

Command:

```bash
cd /home/hadoop/emr_migration_demo/spark
mvn -Dmaven.repo.local=/mnt/tmp/m2/repository clean package
```

Observed:

```text
BUILD SUCCESS
```

Built artifact:

```text
/home/hadoop/emr_migration_demo/spark/target/emr-migration-demo-0.1.0.jar
```

JAR contents verified:

```text
com/demo/emr/FeatureLogConverter.class
com/demo/emr/EligibleUserDataLogConverter.class
com/demo/emr/BrbfJob.class
```

## Upload JAR To S3

Target artifact path:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
```

Command:

```bash
aws s3 cp \
  /home/hadoop/emr_migration_demo/spark/target/emr-migration-demo-0.1.0.jar \
  s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar \
  --region us-east-1
```

Initial blocker:

```text
AWS CLI credentials are not configured on this node.
```

Observed error:

```text
Unable to locate credentials
```

`hadoop fs` upload was also blocked by missing AWS credentials:

```text
No AWS Credentials provided
```

This was resolved by allowing the tool command to run with access to the EMR instance role credentials.

Check credentials with:

```bash
aws configure list
aws sts get-caller-identity --region us-east-1
```

EMR control-plane permissions also had to be added to the EMR instance profile role:

```text
AmazonEMR-InstanceProfile-20260502T160809
```

Required actions:

```text
elasticmapreduce:DescribeCluster
elasticmapreduce:ListInstanceGroups
elasticmapreduce:ListBootstrapActions
elasticmapreduce:AddJobFlowSteps
elasticmapreduce:ListSteps
elasticmapreduce:DescribeStep
```

Cluster readiness check passed:

```text
Cluster state: WAITING
Message: Cluster ready to run steps.
```

Upload result:

```text
upload: spark/target/emr-migration-demo-0.1.0.jar to s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
```

S3 verification:

```text
2026-05-22 19:15:01      52921 emr-migration-demo-0.1.0.jar
```

## Submit EMR Steps

After the JAR exists in S3, add the three Spark steps.

### Step 1: FeatureLogConverter

```bash
aws emr add-steps \
  --region us-east-1 \
  --cluster-id j-37DIRU3WHU1C5 \
  --steps '[
    {
      "Name": "emr-migration-demo-step-1-feature-log-converter",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "client",
        "--class", "com.demo.emr.FeatureLogConverter",
        "s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar",
        "--bucket", "aigithub-emr-2026",
        "--base-prefix", "emr-migration-demo",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    }
  ]'
```

Observed:

```text
Step ID: s-05260282LTDLOYKA0X5W
Status: COMPLETED
```

### Step 2: EligibleUserDataLogConverter

```bash
aws emr add-steps \
  --region us-east-1 \
  --cluster-id j-37DIRU3WHU1C5 \
  --steps '[
    {
      "Name": "emr-migration-demo-step-2-eligible-user-data-converter",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "client",
        "--class", "com.demo.emr.EligibleUserDataLogConverter",
        "s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar",
        "--bucket", "aigithub-emr-2026",
        "--base-prefix", "emr-migration-demo",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    }
  ]'
```

Observed:

```text
Step ID: s-05079433OEGPED8V5I0K
Status: COMPLETED
```

### Step 3: BrbfJob

```bash
aws emr add-steps \
  --region us-east-1 \
  --cluster-id j-37DIRU3WHU1C5 \
  --steps '[
    {
      "Name": "emr-migration-demo-step-3-brbf-job",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "client",
        "--class", "com.demo.emr.BrbfJob",
        "s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar",
        "--bucket", "aigithub-emr-2026",
        "--base-prefix", "emr-migration-demo",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    }
  ]'
```

Observed:

```text
Step ID: s-021230414IK4ECF2EB4X
Status: COMPLETED
```

## Monitor Steps

List steps:

```bash
aws emr list-steps \
  --region us-east-1 \
  --cluster-id j-37DIRU3WHU1C5 \
  --query 'Steps[*].[Id,Name,Status.State,Status.FailureDetails.Message]' \
  --output table
```

Describe a step:

```bash
aws emr describe-step \
  --region us-east-1 \
  --cluster-id j-37DIRU3WHU1C5 \
  --step-id <step-id>
```

## Expected Tiny-Run Validation

Step 1 expected:

```text
feature_log_converter.raw_feature_log_count=50000
feature_log_converter.converted_feature_log_count=50000
feature_log_converter.distinct_user_hash_count=40200
feature_log_converter.distinct_contextual_count=13035
```

Step 2 expected:

```text
eligible_user_data_converter.raw_matched_user_data_count=20000
eligible_user_data_converter.converted_eligible_user_data_count=20000
eligible_user_data_converter.eligible_user_count=5000
eligible_user_data_converter.high_frequency_count=4000
eligible_user_data_converter.low_frequency_count=16000
```

Step 3 expected:

```text
brbf_job.source_bids_count=100000
brbf_job.source_impressions_count=70000
brbf_job.source_eligible_user_data_count=20000
brbf_job.source_feature_log_count=50000
brbf_job.source_sib_count=16200
brbf_job.joined_row_count=1080000
brbf_job.high_frequency_joined_count=1000000
brbf_job.low_frequency_joined_count=80000
brbf_job.final_output_count=91883
```

Final output path:

```text
s3://aigithub-emr-2026/emr-migration-demo/final/brbf/year=2026/month=05/day=21/hour=10/
```

Observed final validation:

```text
finalBrbf.count() = 91883
```

Observed branch counts:

```text
+---------------------+-----+
|branch               |count|
+---------------------+-----+
|low_frequency_hash   |72500|
|high_frequency_salted|19383|
+---------------------+-----+
```

## Current Status

Completed:

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

Step 11 conclusion:

```text
PASS
```
