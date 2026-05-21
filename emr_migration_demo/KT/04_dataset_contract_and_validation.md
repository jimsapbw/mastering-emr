# Step 04: Dataset Contract And Validation

## Purpose

Define the mock dataset contract and validate that the raw Parquet data in S3 is usable for the BRBF-style EMR read operation.

This step confirms:

- dataset row counts
- key overlap for planned joins
- same-hour and `N+1` late feedback availability
- skew indicators for the later before-DAG workload

## Raw Base

```text
s3://aigithub-emr-2026/emr-migration-demo/raw/
```

## Dataset Contract

### bids

Partition:

```text
year/month/day/hour
```

Columns:

```text
bid_request_id
user_id
user_id_hash
advertiser_id
campaign_id
contextual_id
bid_timestamp
bid_price
device_type
geo_id
daily_user_freq
frequency_bracket
is_test_bid
year
month
day
hour
```

### impressions_feedback

Partition:

```text
year/month/day/hour
```

Columns:

```text
impression_id
bid_request_id
feedback_timestamp
is_billable
clearing_price
feedback_type
year
month
day
hour
```

### contextual

Partition:

```text
year/month/day/hour
```

Columns:

```text
contextual_id
domain
content_category
is_hot_context
context_score
year
month
day
hour
```

### matched_user_data

Partition:

```text
year/month/day/hour
```

Columns:

```text
bid_request_id
user_id
user_id_hash
segment_id
is_eligible
eligibility_score
year
month
day
hour
```

### advertiser

Partition:

```text
year/month/day
```

Columns:

```text
advertiser_id
advertiser_name
vertical
priority_tier
is_active
year
month
day
```

### koa_settings

Partition:

```text
year/month/day
```

Columns:

```text
advertiser_id
campaign_id
koa_group
bid_modifier
policy_mode
year
month
day
```

### feature_log

Partition:

```text
year/month/day/hour
```

Columns:

```text
feature_event_id
user_id
user_id_hash
contextual_id
feature_name
feature_value
event_timestamp
year
month
day
hour
```

### sib

Partition:

```text
year/month/day
```

Columns:

```text
user_id
user_id_hash
sib_group
sib_score
sib_source
year
month
day
```

## Planned Joins

```text
bids.bid_request_id            -> impressions_feedback.bid_request_id
bids.bid_request_id            -> matched_user_data.bid_request_id
bids.advertiser_id             -> advertiser.advertiser_id
bids.advertiser_id/campaign_id -> koa_settings.advertiser_id/campaign_id
bids.contextual_id             -> contextual.contextual_id
bids.user_id_hash              -> feature_log.user_id_hash
bids.user_id_hash              -> sib.user_id_hash
```

## Validation Script

```text
emr_migration_demo/scripts/validate_mock_data.py
```

## Command

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

## Expected Checks

The script prints:

```text
counts.*
overlap.*
late_feedback.*
skew.*
```

The key result is that all planned joins have non-zero overlap and the late feedback hour has data.

## Observed Tiny-Scale Results

Command completed successfully against the `tiny` mock dataset.

Dataset counts:

```text
counts.bids=100000
counts.impressions_feedback_same_hour=59500
counts.impressions_feedback_late_hour=10500
counts.impressions_feedback_total=70000
counts.contextual=20000
counts.matched_user_data=20000
counts.advertiser=1000
counts.koa_settings=1000
counts.feature_log=50000
counts.sib=20000
```

Join key overlap:

```text
overlap.bids_to_impressions_bid_request_id=70000
overlap.bids_to_matched_user_data_bid_request_id=20000
overlap.bids_to_advertiser_advertiser_id=555
overlap.bids_to_contextual_contextual_id=13035
overlap.bids_to_feature_log_user_id_hash=40200
overlap.bids_to_sib_user_id_hash=16200
overlap.bids_to_koa_settings_advertiser_campaign=555
```

Late feedback:

```text
late_feedback.same_hour_rows=59500
late_feedback.late_n_plus_1_rows=10500
```

Skew indicators:

```text
skew.frequency_bracket_high=20000
skew.frequency_bracket_low=80000
skew.top_advertiser_1=advertiser_id=0,rows=9000
skew.top_advertiser_2=advertiser_id=4,rows=9000
skew.top_advertiser_3=advertiser_id=1,rows=9000
skew.top_advertiser_4=advertiser_id=2,rows=9000
skew.top_advertiser_5=advertiser_id=3,rows=9000
skew.top_contextual_1=contextual_id=0,rows=1000
skew.top_contextual_2=contextual_id=4,rows=1000
skew.top_contextual_3=contextual_id=1,rows=1000
skew.top_contextual_4=contextual_id=2,rows=1000
skew.top_contextual_5=contextual_id=3,rows=1000
```

Validation conclusion:

```text
PASS
```

All planned joins have non-zero key overlap, and the late feedback hour contains data.
