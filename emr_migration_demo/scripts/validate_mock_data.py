#!/usr/bin/env python3
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def parse_args():
    parser = argparse.ArgumentParser(description="Validate BRBF-style mock datasets.")
    parser.add_argument("--bucket", default="aigithub-emr-2026")
    parser.add_argument("--base-prefix", default="emr-migration-demo")
    parser.add_argument("--run-date", default="2026-05-21")
    parser.add_argument("--hour", default="10")
    parser.add_argument("--late-hour", default="11")
    return parser.parse_args()


def partition_parts(run_date):
    year, month, day = run_date.split("-")
    return year, month, day


def raw_path(args, dataset, has_hour=True, hour=None):
    year, month, day = partition_parts(args.run_date)
    path = (
        f"s3://{args.bucket}/{args.base_prefix}/raw/{dataset}/"
        f"year={year}/month={month}/day={day}"
    )
    if has_hour:
        path = f"{path}/hour={hour or args.hour}"
    return f"{path}/"


def read_parquet(spark, path):
    return spark.read.parquet(path)


def count_distinct_overlap(left, right, left_col, right_col):
    return (
        left.select(f.col(left_col).alias("join_key"))
        .distinct()
        .join(right.select(f.col(right_col).alias("join_key")).distinct(), "join_key", "inner")
        .count()
    )


def print_metric(section, name, value):
    print(f"{section}.{name}={value}")


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("emr-migration-validate-mock-data").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    bids = read_parquet(spark, raw_path(args, "bids"))
    impressions_same = read_parquet(
        spark, raw_path(args, "impressions_feedback", hour=args.hour)
    )
    impressions_late = read_parquet(
        spark, raw_path(args, "impressions_feedback", hour=args.late_hour)
    )
    contextual = read_parquet(spark, raw_path(args, "contextual"))
    matched_user_data = read_parquet(spark, raw_path(args, "matched_user_data"))
    advertiser = read_parquet(spark, raw_path(args, "advertiser", has_hour=False))
    koa_settings = read_parquet(spark, raw_path(args, "koa_settings", has_hour=False))
    feature_log = read_parquet(spark, raw_path(args, "feature_log"))
    sib = read_parquet(spark, raw_path(args, "sib", has_hour=False))

    impressions_all = impressions_same.unionByName(impressions_late)

    print("== DATASET COUNTS ==")
    print_metric("counts", "bids", bids.count())
    print_metric("counts", "impressions_feedback_same_hour", impressions_same.count())
    print_metric("counts", "impressions_feedback_late_hour", impressions_late.count())
    print_metric("counts", "impressions_feedback_total", impressions_all.count())
    print_metric("counts", "contextual", contextual.count())
    print_metric("counts", "matched_user_data", matched_user_data.count())
    print_metric("counts", "advertiser", advertiser.count())
    print_metric("counts", "koa_settings", koa_settings.count())
    print_metric("counts", "feature_log", feature_log.count())
    print_metric("counts", "sib", sib.count())

    print("== JOIN KEY OVERLAP ==")
    print_metric(
        "overlap",
        "bids_to_impressions_bid_request_id",
        count_distinct_overlap(bids, impressions_all, "bid_request_id", "bid_request_id"),
    )
    print_metric(
        "overlap",
        "bids_to_matched_user_data_bid_request_id",
        count_distinct_overlap(
            bids, matched_user_data, "bid_request_id", "bid_request_id"
        ),
    )
    print_metric(
        "overlap",
        "bids_to_advertiser_advertiser_id",
        count_distinct_overlap(bids, advertiser, "advertiser_id", "advertiser_id"),
    )
    print_metric(
        "overlap",
        "bids_to_contextual_contextual_id",
        count_distinct_overlap(bids, contextual, "contextual_id", "contextual_id"),
    )
    print_metric(
        "overlap",
        "bids_to_feature_log_user_id_hash",
        count_distinct_overlap(bids, feature_log, "user_id_hash", "user_id_hash"),
    )
    print_metric(
        "overlap",
        "bids_to_sib_user_id_hash",
        count_distinct_overlap(bids, sib, "user_id_hash", "user_id_hash"),
    )
    print_metric(
        "overlap",
        "bids_to_koa_settings_advertiser_campaign",
        bids.select("advertiser_id", "campaign_id")
        .distinct()
        .join(
            koa_settings.select("advertiser_id", "campaign_id").distinct(),
            ["advertiser_id", "campaign_id"],
            "inner",
        )
        .count(),
    )

    print("== LATE FEEDBACK ==")
    print_metric(
        "late_feedback",
        "same_hour_rows",
        impressions_same.filter(f.col("feedback_type") == "same_hour").count(),
    )
    print_metric(
        "late_feedback",
        "late_n_plus_1_rows",
        impressions_late.filter(f.col("feedback_type") == "late_n_plus_1").count(),
    )

    print("== SKEW INDICATORS ==")
    frequency_counts = bids.groupBy("frequency_bracket").count().collect()
    for row in frequency_counts:
        print_metric("skew", f"frequency_bracket_{row['frequency_bracket']}", row["count"])

    top_advertisers = (
        bids.groupBy("advertiser_id")
        .count()
        .orderBy(f.desc("count"))
        .limit(5)
        .collect()
    )
    for idx, row in enumerate(top_advertisers, start=1):
        print_metric(
            "skew",
            f"top_advertiser_{idx}",
            f"advertiser_id={row['advertiser_id']},rows={row['count']}",
        )

    top_contexts = (
        bids.groupBy("contextual_id")
        .count()
        .orderBy(f.desc("count"))
        .limit(5)
        .collect()
    )
    for idx, row in enumerate(top_contexts, start=1):
        print_metric(
            "skew",
            f"top_contextual_{idx}",
            f"contextual_id={row['contextual_id']},rows={row['count']}",
        )

    spark.stop()


if __name__ == "__main__":
    main()
