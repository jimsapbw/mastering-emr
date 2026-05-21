# Step 01: Create S3 Prefixes

## Purpose

Create the S3 folder/prefix structure for the EMR-side BRBF-style migration demo.

The prefixes provide a stable layout for:

- raw mock datasets
- converted/intermediate outputs
- final BRBF output
- validation results
- JARs, configs, and logs

## Bucket

```text
s3://aigithub-emr-2026
```

## Demo Base Prefix

```text
s3://aigithub-emr-2026/emr-migration-demo/
```

## Script

```text
emr_migration_demo/scripts/create_s3_prefixes.sh
```

## Command Executed

```bash
bash emr_migration_demo/scripts/create_s3_prefixes.sh aigithub-emr-2026
```

The script creates zero-byte S3 prefix marker objects. S3 prefixes are virtual, but these markers make the folder structure visible in the AWS console and with `aws s3 ls`.

## Top-Level Verification

Command:

```bash
aws s3 ls s3://aigithub-emr-2026/emr-migration-demo/ --region us-east-1
```

Observed output:

```text
                           PRE artifacts/
                           PRE converted/
                           PRE final/
                           PRE raw/
                           PRE validation/
```

## Raw Dataset Verification

Command:

```bash
aws s3 ls s3://aigithub-emr-2026/emr-migration-demo/raw/ --region us-east-1
```

Observed output:

```text
                           PRE advertiser/
                           PRE bids/
                           PRE contextual/
                           PRE feature_log/
                           PRE impressions_feedback/
                           PRE koa_settings/
                           PRE matched_user_data/
                           PRE sib/
```

## Partition Verification

Command:

```bash
aws s3 ls s3://aigithub-emr-2026/emr-migration-demo/raw/bids/year=2026/month=05/day=21/hour=10/ --region us-east-1
```

Observed output:

```text
2026-05-21 19:26:38          0
```

The zero-byte object is expected. It is the marker object for the partition prefix.

## Created Layout

```text
s3://aigithub-emr-2026/emr-migration-demo/
  raw/
    bids/year=2026/month=05/day=21/hour=10/
    impressions_feedback/year=2026/month=05/day=21/hour=10/
    impressions_feedback/year=2026/month=05/day=21/hour=11/
    contextual/year=2026/month=05/day=21/hour=10/
    matched_user_data/year=2026/month=05/day=21/hour=10/
    advertiser/year=2026/month=05/day=21/
    koa_settings/year=2026/month=05/day=21/
    feature_log/year=2026/month=05/day=21/hour=10/
    sib/year=2026/month=05/day=21/

  converted/
    feature_log/year=2026/month=05/day=21/hour=10/
    eligible_user_data/year=2026/month=05/day=21/hour=10/

  final/
    brbf/year=2026/month=05/day=21/hour=10/

  validation/
    run_date=2026-05-21/hour=10/

  artifacts/
    jars/
    configs/
    logs/
```

## Notes

- The run hour is `10`.
- The late feedback hour is `11`, used to simulate `N+1` impression/bid feedback availability.
- Actual Parquet data files have not been generated yet.
- Next planned step: document the dataset contracts and join keys.
