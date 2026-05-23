# EMR Migration Demo KT

This folder documents the EMR-side demo design and the commands executed as we build the scenario step by step.

## Step Index

0. [Resume Plan](./00_resume_plan.md)
1. [Create S3 Prefixes](./01_create_s3_prefixes.md)
2. [Mock Dataset Generation](./02_mock_dataset_generation.md)
3. [Maven Setup](./03_maven_setup.md)
4. [Dataset Contract And Validation](./04_dataset_contract_and_validation.md)
5. [GitHub Backup](./05_git_backup.md)
6. [Scala Maven Spark Project](./06_scala_maven_spark_project.md)
7. [Shared Scala Utilities](./07_shared_scala_utilities.md)
8. [Feature Log Converter](./08_feature_log_converter.md)
9. [Eligible User Data Converter](./09_eligible_user_data_converter.md)
10. [BRBF Job](./10_brbf_job.md)
11. [BRBF Job Validation](./10_brbf_job_validation.md)
12. [Package Upload Run EMR Steps](./11_package_upload_run_emr_steps.md)
13. [Scale Data And Tune Runtime](./12_scale_data_and_tune_runtime.md)

## References

- [EMR Migration Demo Master Runbook](./emr_migration_demo_master_runbook.md)
- [Resume Plan](./00_resume_plan.md)
- [Simple Scala App To Shared Utility App](./reference_simple_vs_utility_scala_app.md)
- [EMR Baseline Problem Statement](./emr_baseline_problem_statement.md)
- [EMR Spark Troubleshooting Guide](./emr_spark_troubleshooting_guide.md)
- [Client Spark UI Troubleshooting Plan](./client_spark_ui_troubleshooting_plan.md)
- [Future Migration Stories](./future_migration_stories.md)
- [Airflow Orchestration Plan](./airflow_orchestration_plan.md)
- [Databricks Post-Migration Plan](./databricks_post_migration_plan.md)
- [Databricks Photon Best Practices](./databricks_photon_best_practices.md)

## Which Document To Use When

Use this section as the working index.

### Resume Or Rebuild The Demo

Start here after a break, cluster termination, or new Codex session:

- [Resume Plan](./00_resume_plan.md)
- [EMR Migration Demo Master Runbook](./emr_migration_demo_master_runbook.md)

Use these to recreate the base environment:

- [Create S3 Prefixes](./01_create_s3_prefixes.md)
- [Mock Dataset Generation](./02_mock_dataset_generation.md)
- [Maven Setup](./03_maven_setup.md)
- [Dataset Contract And Validation](./04_dataset_contract_and_validation.md)
- [GitHub Backup](./05_git_backup.md)

### Understand The Code Build-Up

Use these when explaining how the Scala/Spark project evolved:

- [Scala Maven Spark Project](./06_scala_maven_spark_project.md)
- [Shared Scala Utilities](./07_shared_scala_utilities.md)
- [Simple Scala App To Shared Utility App](./reference_simple_vs_utility_scala_app.md)

### Understand Each Pipeline Step

Use these for implementation details and validation notes:

- [Feature Log Converter](./08_feature_log_converter.md)
- [Eligible User Data Converter](./09_eligible_user_data_converter.md)
- [BRBF Job](./10_brbf_job.md)
- [BRBF Job Validation](./10_brbf_job_validation.md)

### Run On EMR

Use these when packaging, submitting, monitoring, or scaling the EMR workload:

- [Package Upload Run EMR Steps](./11_package_upload_run_emr_steps.md)
- [Scale Data And Tune Runtime](./12_scale_data_and_tune_runtime.md)

### Troubleshoot Performance

Use these when reviewing Spark History Server, YARN, stages, SQL operators, skew, shuffle, or spills:

- [EMR Spark Troubleshooting Guide](./emr_spark_troubleshooting_guide.md)
- [Client Spark UI Troubleshooting Plan](./client_spark_ui_troubleshooting_plan.md)

### Prepare The Migration Story

Use these when framing the client problem, Airflow orchestration, Databricks target state, Photon, and future backlog:

- [EMR Baseline Problem Statement](./emr_baseline_problem_statement.md)
- [Airflow Orchestration Plan](./airflow_orchestration_plan.md)
- [Databricks Post-Migration Plan](./databricks_post_migration_plan.md)
- [Databricks Photon Best Practices](./databricks_photon_best_practices.md)
- [Future Migration Stories](./future_migration_stories.md)
