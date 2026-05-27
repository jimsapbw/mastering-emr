DAG Sequence 
You were right: the DAG is in the root folder’s airflow-dags, not inside eldorado.
DAG file:
perf-automation-fpcs.py (line 29)
DAG id:
perf-automation-fpcs
It runs daily, starting from 2025-05-20 15:00.
It has 4 main EMR job tasks:
1.	GenerateCampaignScores
Class: model.fpcookiescoring.GenerateCampaignScores
2.	FirstPartyCookieScoringModel
Class: model.fpcookiescoring.FirstPartyCookieScoringModel
3.	CookieScoringToAdvertiserData
Class: model.fpcookiescoring.CookieScoringToAdvertiserData
4.	FpcsPerformance
Class: model.fpcookiescoring.FpcsPerformance
They run sequentially:
GenerateCampaignScores
 -> FirstPartyCookieScoringModel
 -> CookieScoringToAdvertiserData
 -> FpcsPerformance
 -> final_dag_check
So: 4 business/Spark tasks, plus 1 final DAG status check.

Code Migration sequence 
Yes. For a step-by-step Databricks migration, follow the DAG order and migrate one Spark task at a time. I’d use this file order.
0. Start With DAG + Shared Model Context
Read these first so you understand orchestration and shared datasets:
1.	perf-automation-fpcs.py (line 29)
Airflow DAG: task order, cluster sizes, Spark configs, job args.
2.	DataSources.scala (line 1)
Defines the input/output datasets shared between all FPCS jobs.
3.	fpcookiescoring.scala (line 1)
Package-level helpers/config, if used by the jobs.
Task 1: GenerateCampaignScores
Migrate this first because downstream jobs depend on its outputs.
Main file:
1.	GenerateCampaignScores.scala (line 21)
Supporting files:
2.	DataSources.scala (line 6)
Outputs to preserve:
CampaignScoreDataSet
AdGroupSelectedUsersDataSet
DAG task:
GenerateCampaignScores
class: model.fpcookiescoring.GenerateCampaignScores
This should be your first Databricks job/task.
Task 2: FirstPartyCookieScoringModel
Migrate second. It consumes outputs from Task 1.
Main file:
1.	FirstPartyCookieScoringModel.scala (line 16)
Supporting files:
2.	DataSources.scala (line 15)
Inputs from Task 1:
CampaignScoreDataSet
AdGroupSelectedUsersDataSet
Output to preserve:
FirstPartyCookieScoringResultDataSet
models/firstpartycookiescoring/fpcs/v=1
DAG task:
FirstPartyCookieScoringModel
class: model.fpcookiescoring.FirstPartyCookieScoringModel
Task 3: CookieScoringToAdvertiserData
Migrate third. This is the export/log-formatting step connected to your active formatter file.
Main file:
1.	CookieScoringToAdvertiserData.scala (line 48)
Important shared formatter:
2.	TargetingDataImportLogFormatter.scala (line 43)
Supporting dataset file:
3.	DataSources.scala (line 38)
Input from Task 2:
FirstPartyCookieScoringResultDataSet
Output to preserve:
s3://thetradedesk-useast-logs-2/advertiserdataimport/collected/yyyy/MM/dd/HH/1/
DAG task:
CookieScoringToAdvertiserData
class: model.fpcookiescoring.CookieScoringToAdvertiserData
This task needs extra care in Databricks because it writes log files, renames them, samples file size, and copies to final S3 layout.
Task 4: FpcsPerformance
Migrate last among business tasks. It is downstream reporting/evaluation.
Main file:
1.	FpcsPerformance.scala (line 15)
Supporting file:
2.	DataSources.scala (line 53)
Input:
FirstPartyCookieScoringResultDataSet
BidFeedbackDataSetV5
AttributedEventDataSet
AttributedEventResultDataSet
Output to preserve:
PerfCompDataSet
models/firstpartycookiescoring/perfquery/v=1
DAG task:
FpcsPerformance
class: model.fpcookiescoring.FpcsPerformance
Recommended Migration Order
0. perf-automation-fpcs.py
0. DataSources.scala

1. GenerateCampaignScores.scala
2. FirstPartyCookieScoringModel.scala
3. CookieScoringToAdvertiserData.scala
   + TargetingDataImportLogFormatter.scala
4. FpcsPerformance.scala
5. final_dag_check equivalent in Databricks workflow
For Databricks, I’d validate after each task by comparing the output dataset/path with the EMR version for the same date. The key is not to migrate all four at once. Task 1 output compatibility is the foundation for everything else.

Output to preserve (S3 locations)
When I say “outputs to preserve”, I mean:
Your Databricks version of GenerateCampaignScores must write the same output datasets, in the same schema and expected location, because Task 2 depends on them.
In DataSources.scala, these are the two outputs:
case class CampaignScoreDataSet() extends SimpleS3DataSet[CampaignScoreRecord](
  dataSetType = GeneratedDataSet,
  rootPath = S3Roots.IDENTITY_ROOT,
  rootFolderPath = "models/firstpartycookiescoring/temp/campaignscores")
and:
case class AdGroupSelectedUsersDataSet() extends SimpleS3DataSet[AdGroupSelectedUsersRecord](
  dataSetType = GeneratedDataSet,
  rootPath = S3Roots.IDENTITY_ROOT,
  rootFolderPath = "models/firstpartycookiescoring/temp/adgroupselectedusers")
So preserve output means Databricks should still produce:
models/firstpartycookiescoring/temp/campaignscores
models/firstpartycookiescoring/temp/adgroupselectedusers
with the same schemas.
CampaignScoreDataSet schema:
case class CampaignScoreRecord(
  advertiserid: String,
  CampaignId: String,
  tdid: String,
  dcs: Seq[Int],
  relevantscore_CPA: Double,
  relevantscore_ROAS: Double
)
AdGroupSelectedUsersDataSet schema:
case class AdGroupSelectedUsersRecord(
  AdvertiserId: String,
  AdGroupId: String,
  TrackingTagId: String,
  TDID: String
)
Why this matters:
FirstPartyCookieScoringModel reads them later:
val campaignScores = CampaignScoreDataSet().read
and:
val adGroupSelectedUsers = AdGroupSelectedUsersDataSet().read
So if Task 1 in Databricks writes somewhere else, changes column names, changes types, or writes a different format, Task 2 will break or produce different results.
In simple words:
GenerateCampaignScores
  writes CampaignScoreDataSet
  writes AdGroupSelectedUsersDataSet

FirstPartyCookieScoringModel
  reads those exact datasets
Databricks Migration Step by Step
Yes, but tiny correction: for Task 1 baseline, GenerateCampaignScores.scala and DataSources.scala are the core files, but you also need this helper file:
fpcookiescoring.scala (line 1)
Because GenerateCampaignScores.scala calls:
loadEventTrackerData(...)
loadConversionTrackerData(...)
Those functions live in the package object fpcookiescoring.scala.
So for Task 1, the practical file set is:
GenerateCampaignScores.scala
DataSources.scala
fpcookiescoring.scala
DebugDataSources.scala is only needed if you run with:
generateDebugTables=true
For a first baseline, keep that false.
Best Baseline Approach
For the first Databricks run, I would not rewrite the job into notebooks yet. First prove the existing Scala class can run on Databricks.
Use the same class:
model.fpcookiescoring.GenerateCampaignScores
and same jar if possible:
s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-audauto-assembly.jar
That proves Databricks can execute the existing job before you start refactoring.
Step By Step
1. Create Databricks job
Create a Databricks job with one task:
Task name: GenerateCampaignScores
Type: Spark JAR
Main class: model.fpcookiescoring.GenerateCampaignScores
Jar: eldorado-audauto-assembly.jar
Pass the same configs from Airflow:
date=YYYY-MM-DD
useNewSources=true
trainingWindowDays=14
labelWindowDays=1
testWindowDays=2
openlineage.enable=false
generateDebugTables=false
Use a small safe historical date first.
2. Match Spark config
From the DAG, Task 1 uses:
spark.executor.memory=180g
spark.driver.memory=180g
spark.driver.cores=30
spark.driver.maxResultSize=0
spark.driver.memoryOverhead=56g
spark.executor.cores=30
spark.executor.memoryOverhead=56g
spark.sql.shuffle.partitions=12000
spark.dynamicAllocation.enabled=true
spark.network.timeout=14400s
spark.executor.heartbeatInterval=600s
For an initial smoke test, you can start smaller, but for a real baseline comparison, keep these as close as possible.
3. Make sure Databricks has access
The job reads many internal datasets:
CampaignDataSet
AdGroupDataSet
AdGroupAudienceRetargetingSettingsDataSet
AdGroupAudienceRetargetingPixelsDataSet
CampaignConversionReportingColumnDataSet
BidRequestDataSetV5
ConversionTrackerVerticaLoadDataSetV4
EventTrackerVerticaLoadDataSetV4
So the Databricks cluster needs:
S3 read/write permissions
access to the same source data roots
the internal TTD Spark dataset libraries
the audauto assembly jar
If these libraries are not available, copying only the two Scala files into Databricks will not compile.
4. Preserve outputs
Task 1 must write these two outputs:
CampaignScoreDataSet
AdGroupSelectedUsersDataSet
From DataSources.scala (line 15), those paths are:
models/firstpartycookiescoring/temp/campaignscores
models/firstpartycookiescoring/temp/adgroupselectedusers
For a safe Databricks baseline, I would temporarily redirect these to a sandbox path, for example:
models/firstpartycookiescoring/databricks_test/campaignscores
models/firstpartycookiescoring/databricks_test/adgroupselectedusers
That avoids overwriting production EMR output.
Basic Code Changes If You Port Source Files
If you are not running the jar and instead copying source into Databricks, expect these basic changes:
1.	Replace or adapt TTDConfig.config
Current:
val predictionDate = config.getDate("date", default = currentDateUTC())
Databricks notebook/job style would use widgets or args instead.
2.	Replace cacheToHDFS(...) if needed
Databricks may not behave like EMR HDFS. These calls:
.cacheToHDFS("usersRaw")
may need to become:
.persist()
or checkpoint/write to a Databricks/S3 temp path.
3.	Make output paths sandboxed
Change DataSources.scala output folders for test runs so you do not overwrite EMR production outputs.
4.	Disable Prometheus for first run if it fails
This code creates metrics:
new PrometheusClient(...)
If Databricks cannot push to the same Prometheus endpoint, temporarily no-op or guard metrics for baseline.
5.	Keep package names the same
Do not change:
package model.fpcookiescoring
because the DAG/job class expects:
model.fpcookiescoring.GenerateCampaignScores
Success Criteria
For Task 1 baseline, Databricks is successful when it produces:
CampaignScoreDataSet
AdGroupSelectedUsersDataSet
Then compare against EMR for the same date:
schema matches
row counts are close/exact
sample advertiser IDs match
sample TDIDs match
null patterns look similar
relevantscore_CPA / relevantscore_ROAS ranges look sane
In plain terms: first make Databricks run the same GenerateCampaignScores class, writing the same two datasets to a test path. Only after that should you move to Task 2.
Pending access
-	Prometheus endpoint (if not available), 
-	S3 bucket access for that job 
-	Debug datasource=true (test debug)


