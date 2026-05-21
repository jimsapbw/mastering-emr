#!/usr/bin/env bash
set -euo pipefail

BUCKET="${1:-aigithub-emr-2026}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
BASE_PREFIX="${BASE_PREFIX:-emr-migration-demo}"
RUN_YEAR="${RUN_YEAR:-2026}"
RUN_MONTH="${RUN_MONTH:-05}"
RUN_DAY="${RUN_DAY:-21}"
RUN_HOUR="${RUN_HOUR:-10}"
LATE_HOUR="${LATE_HOUR:-11}"

put_prefix() {
  local prefix="$1"
  aws s3api put-object \
    --bucket "${BUCKET}" \
    --key "${prefix%/}/" \
    --region "${REGION}" \
    >/dev/null
  echo "created s3://${BUCKET}/${prefix%/}/"
}

main() {
  local day_partition="year=${RUN_YEAR}/month=${RUN_MONTH}/day=${RUN_DAY}"
  local hour_partition="${day_partition}/hour=${RUN_HOUR}"
  local late_hour_partition="${day_partition}/hour=${LATE_HOUR}"
  local validation_partition="run_date=${RUN_YEAR}-${RUN_MONTH}-${RUN_DAY}/hour=${RUN_HOUR}"

  put_prefix "${BASE_PREFIX}/raw/bids/${hour_partition}"
  put_prefix "${BASE_PREFIX}/raw/impressions_feedback/${hour_partition}"
  put_prefix "${BASE_PREFIX}/raw/impressions_feedback/${late_hour_partition}"
  put_prefix "${BASE_PREFIX}/raw/contextual/${hour_partition}"
  put_prefix "${BASE_PREFIX}/raw/matched_user_data/${hour_partition}"
  put_prefix "${BASE_PREFIX}/raw/advertiser/${day_partition}"
  put_prefix "${BASE_PREFIX}/raw/koa_settings/${day_partition}"
  put_prefix "${BASE_PREFIX}/raw/feature_log/${hour_partition}"
  put_prefix "${BASE_PREFIX}/raw/sib/${day_partition}"

  put_prefix "${BASE_PREFIX}/converted/feature_log/${hour_partition}"
  put_prefix "${BASE_PREFIX}/converted/eligible_user_data/${hour_partition}"

  put_prefix "${BASE_PREFIX}/final/brbf/${hour_partition}"
  put_prefix "${BASE_PREFIX}/validation/${validation_partition}"
  put_prefix "${BASE_PREFIX}/artifacts/jars"
  put_prefix "${BASE_PREFIX}/artifacts/configs"
  put_prefix "${BASE_PREFIX}/artifacts/logs"
}

main "$@"
