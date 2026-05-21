#!/usr/bin/env python3
import argparse
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


@dataclass(frozen=True)
class Scale:
    bids: int
    impressions: int
    contextual: int
    matched_user_data: int
    advertiser: int
    koa_settings: int
    feature_log: int
    sib: int


SCALES = {
    "tiny": Scale(
        bids=100_000,
        impressions=70_000,
        contextual=20_000,
        matched_user_data=20_000,
        advertiser=1_000,
        koa_settings=1_000,
        feature_log=50_000,
        sib=20_000,
    ),
    "small": Scale(
        bids=1_000_000,
        impressions=700_000,
        contextual=200_000,
        matched_user_data=200_000,
        advertiser=10_000,
        koa_settings=10_000,
        feature_log=500_000,
        sib=100_000,
    ),
    "medium": Scale(
        bids=10_000_000,
        impressions=7_000_000,
        contextual=1_000_000,
        matched_user_data=1_000_000,
        advertiser=25_000,
        koa_settings=25_000,
        feature_log=3_000_000,
        sib=500_000,
    ),
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate BRBF-style mock Parquet datasets in S3."
    )
    parser.add_argument("--bucket", default="aigithub-emr-2026")
    parser.add_argument("--base-prefix", default="emr-migration-demo")
    parser.add_argument("--run-date", default="2026-05-21")
    parser.add_argument("--hour", default="10")
    parser.add_argument("--late-hour", default="11")
    parser.add_argument("--scale", choices=sorted(SCALES), default="tiny")
    parser.add_argument("--partitions", type=int, default=64)
    parser.add_argument("--mode", choices=["overwrite", "append"], default="overwrite")
    return parser.parse_args()


def partition_parts(run_date):
    year, month, day = run_date.split("-")
    return year, month, day


def s3_path(args, dataset, has_hour=True, hour=None):
    year, month, day = partition_parts(args.run_date)
    path = (
        f"s3://{args.bucket}/{args.base_prefix}/raw/{dataset}/"
        f"year={year}/month={month}/day={day}"
    )
    if has_hour:
        path = f"{path}/hour={hour or args.hour}"
    return f"{path}/"


def with_partitions(df, args, has_hour=True, hour=None):
    year, month, day = partition_parts(args.run_date)
    df = (
        df.withColumn("year", f.lit(year))
        .withColumn("month", f.lit(month))
        .withColumn("day", f.lit(day))
    )
    if has_hour:
        df = df.withColumn("hour", f.lit(hour or args.hour))
    return df


def write_dataset(df, path, mode, partitions):
    (
        df.repartition(partitions)
        .write.mode(mode)
        .option("compression", "snappy")
        .parquet(path)
    )
    print(f"wrote {path}")


def base_range(spark, rows, partitions):
    return spark.range(0, rows, 1, partitions)


def skewed_user_id(id_col):
    return f.when(
        id_col % 100 < 20,
        f.concat(f.lit("hot_user_"), (id_col % 1_000).cast("string")),
    ).otherwise(f.concat(f.lit("user_"), id_col.cast("string")))


def skewed_advertiser_id(id_col, advertiser_count):
    return f.when(
        id_col % 100 < 45,
        (id_col % 5).cast("long"),
    ).otherwise((id_col % advertiser_count).cast("long"))


def skewed_contextual_id(id_col, contextual_count):
    return f.when(
        id_col % 100 < 35,
        (id_col % 100).cast("long"),
    ).otherwise((id_col % contextual_count).cast("long"))


def add_hash(df, source_col, target_col):
    return df.withColumn(target_col, f.sha2(f.col(source_col).cast("string"), 256))


def generate_advertiser(spark, args, scale):
    df = base_range(spark, scale.advertiser, args.partitions).select(
        f.col("id").alias("advertiser_id"),
        f.concat(f.lit("advertiser_"), f.col("id")).alias("advertiser_name"),
        f.concat(f.lit("vertical_"), (f.col("id") % 25)).alias("vertical"),
        f.when(f.col("id") % 10 == 0, f.lit("high")).otherwise(f.lit("standard")).alias(
            "priority_tier"
        ),
        f.when(f.col("id") % 13 == 0, f.lit(False)).otherwise(f.lit(True)).alias(
            "is_active"
        ),
    )
    return with_partitions(df, args, has_hour=False)


def generate_koa_settings(spark, args, scale):
    df = base_range(spark, scale.koa_settings, args.partitions).select(
        (f.col("id") % scale.advertiser).cast("long").alias("advertiser_id"),
        (f.col("id") % 50_000).cast("long").alias("campaign_id"),
        f.concat(f.lit("koa_group_"), (f.col("id") % 20)).alias("koa_group"),
        (f.col("id") % 100 / 100.0).cast("double").alias("bid_modifier"),
        f.when(f.col("id") % 7 == 0, f.lit("strict")).otherwise(f.lit("normal")).alias(
            "policy_mode"
        ),
    )
    return with_partitions(df, args, has_hour=False)


def generate_contextual(spark, args, scale):
    df = base_range(spark, scale.contextual, args.partitions).select(
        f.col("id").alias("contextual_id"),
        f.concat(f.lit("domain_"), (f.col("id") % 50_000), f.lit(".example")).alias(
            "domain"
        ),
        f.concat(f.lit("category_"), (f.col("id") % 200)).alias("content_category"),
        f.when(f.col("id") % 100 < 35, f.lit(True)).otherwise(f.lit(False)).alias(
            "is_hot_context"
        ),
        (f.col("id") % 1000 / 1000.0).cast("double").alias("context_score"),
    )
    return with_partitions(df, args)


def generate_bids(spark, args, scale):
    base = base_range(spark, scale.bids, args.partitions)
    df = base.select(
        f.concat(f.lit("br_"), f.col("id")).alias("bid_request_id"),
        skewed_user_id(f.col("id")).alias("user_id"),
        skewed_advertiser_id(f.col("id"), scale.advertiser).alias("advertiser_id"),
        (f.col("id") % 50_000).cast("long").alias("campaign_id"),
        skewed_contextual_id(f.col("id"), scale.contextual).alias("contextual_id"),
        f.expr(
            f"timestamp('{args.run_date} {args.hour}:00:00') + "
            "INTERVAL 1 SECOND * cast(id % 3600 as int)"
        ).alias("bid_timestamp"),
        (0.2 + (f.col("id") % 500) / 100.0).cast("double").alias("bid_price"),
        f.concat(f.lit("device_"), (f.col("id") % 6)).alias("device_type"),
        f.concat(f.lit("geo_"), (f.col("id") % 250)).alias("geo_id"),
        (f.col("id") % 500).cast("int").alias("daily_user_freq"),
        f.when(f.col("id") % 100 < 20, f.lit("high")).otherwise(f.lit("low")).alias(
            "frequency_bracket"
        ),
        f.when(f.col("id") % 11 == 0, f.lit(True)).otherwise(f.lit(False)).alias(
            "is_test_bid"
        ),
    )
    return with_partitions(add_hash(df, "user_id", "user_id_hash"), args)


def generate_impressions(spark, args, scale, late=False):
    rows = max(1, int(scale.impressions * (0.15 if late else 0.85)))
    offset = int(scale.impressions * 0.85) if late else 0
    base = base_range(spark, rows, args.partitions).withColumn("bid_id", f.col("id") + offset)
    df = base.select(
        f.concat(f.lit("imp_"), f.col("bid_id")).alias("impression_id"),
        f.concat(f.lit("br_"), f.col("bid_id")).alias("bid_request_id"),
        f.expr(
            f"timestamp('{args.run_date} {args.late_hour if late else args.hour}:00:00') + "
            "INTERVAL 1 SECOND * cast(id % 3600 as int)"
        ).alias("feedback_timestamp"),
        f.when(f.col("id") % 20 == 0, f.lit(False)).otherwise(f.lit(True)).alias(
            "is_billable"
        ),
        (0.1 + (f.col("id") % 300) / 100.0).cast("double").alias("clearing_price"),
        f.when(f.lit(late), f.lit("late_n_plus_1")).otherwise(f.lit("same_hour")).alias(
            "feedback_type"
        ),
    )
    return with_partitions(df, args, hour=args.late_hour if late else args.hour)


def generate_matched_user_data(spark, args, scale):
    df = base_range(spark, scale.matched_user_data, args.partitions).select(
        f.concat(f.lit("br_"), f.col("id")).alias("bid_request_id"),
        skewed_user_id(f.col("id")).alias("user_id"),
        f.concat(f.lit("segment_"), (f.col("id") % 500)).alias("segment_id"),
        f.when(f.col("id") % 4 == 0, f.lit(True)).otherwise(f.lit(False)).alias(
            "is_eligible"
        ),
        (f.col("id") % 100).cast("int").alias("eligibility_score"),
    )
    return with_partitions(add_hash(df, "user_id", "user_id_hash"), args)


def generate_feature_log(spark, args, scale):
    df = base_range(spark, scale.feature_log, args.partitions).select(
        f.concat(f.lit("feature_event_"), f.col("id")).alias("feature_event_id"),
        skewed_user_id(f.col("id")).alias("user_id"),
        skewed_contextual_id(f.col("id"), scale.contextual).alias("contextual_id"),
        f.concat(f.lit("feature_"), (f.col("id") % 300)).alias("feature_name"),
        (f.col("id") % 1000 / 1000.0).cast("double").alias("feature_value"),
        f.expr(
            f"timestamp('{args.run_date} {args.hour}:00:00') + "
            "INTERVAL 1 SECOND * cast(id % 3600 as int)"
        ).alias("event_timestamp"),
    )
    return with_partitions(add_hash(df, "user_id", "user_id_hash"), args)


def generate_sib(spark, args, scale):
    df = base_range(spark, scale.sib, args.partitions).select(
        skewed_user_id(f.col("id")).alias("user_id"),
        f.concat(f.lit("sib_group_"), (f.col("id") % 100)).alias("sib_group"),
        (f.col("id") % 1000 / 1000.0).cast("double").alias("sib_score"),
        f.when(f.col("id") % 8 == 0, f.lit("lookback")).otherwise(f.lit("current")).alias(
            "sib_source"
        ),
    )
    return with_partitions(add_hash(df, "user_id", "user_id_hash"), args, has_hour=False)


def main():
    args = parse_args()
    scale = SCALES[args.scale]
    spark = (
        SparkSession.builder.appName(f"emr-migration-mock-data-{args.scale}")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    datasets = [
        ("advertiser", generate_advertiser(spark, args, scale), False, None),
        ("koa_settings", generate_koa_settings(spark, args, scale), False, None),
        ("contextual", generate_contextual(spark, args, scale), True, args.hour),
        ("bids", generate_bids(spark, args, scale), True, args.hour),
        ("impressions_feedback", generate_impressions(spark, args, scale), True, args.hour),
        (
            "impressions_feedback",
            generate_impressions(spark, args, scale, late=True),
            True,
            args.late_hour,
        ),
        (
            "matched_user_data",
            generate_matched_user_data(spark, args, scale),
            True,
            args.hour,
        ),
        ("feature_log", generate_feature_log(spark, args, scale), True, args.hour),
        ("sib", generate_sib(spark, args, scale), False, None),
    ]

    for dataset, df, has_hour, hour in datasets:
        path = s3_path(args, dataset, has_hour=has_hour, hour=hour)
        print(f"writing {dataset} to {path}")
        write_dataset(df, path, args.mode, args.partitions)

    spark.stop()


if __name__ == "__main__":
    main()
