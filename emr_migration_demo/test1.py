from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from datetime import datetime, timedelta
from dags.audauto.utils import emr_cluster_factory

from ttd.slack.slack_groups import AUDAUTO, DATPERF

executionIntervalInDays = 1


# parameters
def logical_date(format):
    # we use replace because format causes issues with {}
    return '{{ logical_date.add(days=$interval$).strftime("$format$") }}'.replace("$interval$", str(executionIntervalInDays)).replace(
        "$format$", format
    )


# generic settings list we'll add to each step
java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096"), ('date', logical_date("%Y-%m-%d"))]

# generic spark settings list we'll add to each step.
spark_options_list = [("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC")]

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-audauto-assembly.jar"

# The top-level dag
fpcs_dag = TtdDag(
    teams_allowed_to_access=[DATPERF.team.jira_team, AUDAUTO.team.jira_team],
    dag_id="perf-automation-fpcs",
    start_date=datetime(2025, 5, 20, 15, 0),
    schedule_interval=timedelta(days=executionIntervalInDays),
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=False,
)
dag = fpcs_dag.airflow_dag

fpcs_cluster_generate_campaign_scores = emr_cluster_factory.create_emr_cluster_r7g(
    "FirstPartyGenerateCampaignScores", 135, core_ebs_per_x=64
)
fpcs_cluster_first_party_cooking_scoring_model = emr_cluster_factory.create_emr_cluster_r7g(
    "FirstPartyCookieScoringModel", 270, core_ebs_per_x=48
)
fpcs_cluster_cookie_scoring_to_advertiser_data = emr_cluster_factory.create_emr_cluster_r7g(
    "FirstPartyCookieScoringToAdvertiserData", 20, core_ebs_per_x=66
)

fpcs_perf_calc_cluster = emr_cluster_factory.create_emr_cluster_r7g("FirstPartyCookieScoringPerfCalculation", 120, core_ebs_per_x=64)

campaign_scores_step = EmrJobTask(
    name="GenerateCampaignScores",
    class_name="model.fpcookiescoring.GenerateCampaignScores",
    timeout_timedelta=timedelta(hours=8),
    additional_args_option_pairs_list=spark_options_list
    + [
        ("conf", "spark.executor.memory=180g"),
        ("conf", "spark.driver.memory=180g"),
        ("conf", "spark.driver.cores=30"),
        ("conf", "spark.driver.maxResultSize=0"),
        ("conf", "spark.driver.memoryOverhead=56g"),
        ("conf", "spark.executor.cores=30"),
        ("conf", "spark.executor.memoryOverhead=56g"),
        ("conf", "spark.sql.shuffle.partitions=12000"),
        ("conf", "spark.dynamicAllocation.enabled=true"),
        ("conf", "spark.network.timeout=14400s"),
        ("conf", "spark.executor.heartbeatInterval=600s"),
    ],
    eldorado_config_option_pairs_list=java_settings_list
    + [
        ('useNewSources', 'true'),
        ('trainingWindowDays', '14'),
        ('labelWindowDays', '1'),
        ('testWindowDays', '2'),
        ('openlineage.enable', 'false'),
    ],
    executable_path=jar_path,
)

fpcs_model_step = EmrJobTask(
    name="FirstPartyCookieScoringModel",
    class_name="model.fpcookiescoring.FirstPartyCookieScoringModel",
    additional_args_option_pairs_list=spark_options_list
    + [
        ("conf", "spark.executor.memory=180g"),
        ("conf", "spark.driver.memory=180g"),
        ("conf", "spark.driver.cores=30"),
        ("conf", "spark.driver.maxResultSize=0"),
        ("conf", "spark.driver.memoryOverhead=56g"),
        ("conf", "spark.executor.cores=30"),
        ("conf", "spark.executor.memoryOverhead=56g"),
        ("conf", "spark.sql.shuffle.partitions=6000"),
        ("conf", "spark.dynamicAllocation.enabled=true"),
    ],
    eldorado_config_option_pairs_list=java_settings_list
    + [
        ('useNewSources', 'true'),
        ('trainingWindowDays', '14'),
        ('labelWindowDays', '1'),
        ('testWindowDays', '2'),
        ('openlineage.enable', 'false'),
    ],
    executable_path=jar_path,
)

cs_to_advertiser_data_step = EmrJobTask(
    name="CookieScoringToAdvertiserData",
    class_name="model.fpcookiescoring.CookieScoringToAdvertiserData",
    additional_args_option_pairs_list=spark_options_list + [("conf", "spark.driver.memory=40g")],
    eldorado_config_option_pairs_list=java_settings_list + [('usersPerFile', '20000'), ('openlineage.enable', 'false')],
    executable_path=jar_path,
)

fpcs_perf_step = EmrJobTask(
    name="FpcsPerformance",
    class_name="model.fpcookiescoring.FpcsPerformance",
    additional_args_option_pairs_list=spark_options_list
    + [
        ("conf", "spark.executor.memory=100g"),
        ("conf", "spark.driver.memory=100g"),
        ("conf", "spark.driver.cores=15"),
        ("conf", "spark.driver.maxResultSize=0"),
        ("conf", "spark.driver.memoryOverhead=18g"),
        ("conf", "spark.executor.cores=15"),
        ("conf", "spark.executor.memoryOverhead=18g"),
        ("conf", "spark.sql.shuffle.partitions=18000"),
        ("conf", "spark.dynamicAllocation.enabled=true"),
    ],
    eldorado_config_option_pairs_list=java_settings_list + [('LookbackWindowDays', '3'), ('openlineage.enable', 'false')],
    executable_path=jar_path,
)

fpcs_cluster_generate_campaign_scores.add_sequential_body_task(campaign_scores_step)
fpcs_cluster_first_party_cooking_scoring_model.add_sequential_body_task(fpcs_model_step)
fpcs_cluster_cookie_scoring_to_advertiser_data.add_sequential_body_task(cs_to_advertiser_data_step)

fpcs_perf_calc_cluster.add_sequential_body_task(fpcs_perf_step)

# setup step dependencies for the model
(
    fpcs_dag
    >> fpcs_cluster_generate_campaign_scores
    >> fpcs_cluster_first_party_cooking_scoring_model
    >> fpcs_cluster_cookie_scoring_to_advertiser_data
    >> fpcs_perf_calc_cluster
)

final_dag_check = FinalDagStatusCheckOperator(dag=dag)
fpcs_perf_calc_cluster.last_airflow_op() >> final_dag_check
