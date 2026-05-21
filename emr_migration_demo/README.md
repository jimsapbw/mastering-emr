# EMR Migration Demo

This folder contains the EMR-side assets for the BRBF-style migration demo.

## S3 Layout

Bucket:

```text
s3://aigithub-emr-2026/emr-migration-demo/
```

Initial run partition:

```text
year=2026/month=05/day=21/hour=10
```

Raw source prefixes:

```text
raw/bids/
raw/impressions_feedback/
raw/contextual/
raw/matched_user_data/
raw/advertiser/
raw/koa_settings/
raw/feature_log/
raw/sib/
```

Pipeline prefixes:

```text
converted/feature_log/
converted/eligible_user_data/
final/brbf/
validation/
artifacts/jars/
artifacts/configs/
artifacts/logs/
```

## Create Prefixes

Authenticate AWS CLI first, then run:

```bash
bash emr_migration_demo/scripts/create_s3_prefixes.sh aigithub-emr-2026
```

Optional overrides:

```bash
AWS_REGION=us-east-1 RUN_HOUR=10 LATE_HOUR=11 bash emr_migration_demo/scripts/create_s3_prefixes.sh aigithub-emr-2026
```
