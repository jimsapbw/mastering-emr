package model.fpcookiescoring

import com.thetradedesk.spark.datasets.core.SchemaPolicy.MergeAllFilesSchema
import com.thetradedesk.spark.datasets.core._

case class CampaignScoreRecord(
    advertiserid: String,
    CampaignId: String,
    tdid: String,
    dcs: Seq[Int],
    relevantscore_CPA: Double,
    relevantscore_ROAS: Double
)

case class CampaignScoreDataSet() extends SimpleS3DataSet[CampaignScoreRecord](
  dataSetType = GeneratedDataSet,
  rootPath = S3Roots.IDENTITY_ROOT,
  rootFolderPath = "models/firstpartycookiescoring/temp/campaignscores")

case class AdGroupSelectedUsersRecord(
    AdvertiserId: String,
    AdGroupId: String,
    TrackingTagId: String,
    TDID: String
)

case class AdGroupSelectedUsersDataSet() extends SimpleS3DataSet[AdGroupSelectedUsersRecord](
  dataSetType = GeneratedDataSet,
  rootPath = S3Roots.IDENTITY_ROOT,
  rootFolderPath = "models/firstpartycookiescoring/temp/adgroupselectedusers")

case class TargetedBidRecord(
    TargetingDataName: String,
    TargetingDataId: Long,
    Bid: Double
)

case class FirstPartyCookieScoringResultRecord(
    TDID: String,
    AdvertiserId: String,
    TargetedBids: Seq[TargetedBidRecord],
    Datacenters: Seq[Int],
    IsTestId: Boolean
)

case class FirstPartyCookieScoringResultDataSet() extends DatePartitionedS3DataSet[FirstPartyCookieScoringResultRecord](
  dataSetType = GeneratedDataSet,
  s3RootPath = S3Roots.IDENTITY_ROOT,
  rootFolderPath = "models/firstpartycookiescoring/fpcs/v=1",
  schemaPolicy = MergeAllFilesSchema
)

case class PerfCompRecord(
    AdvertiserId: String,
    CampaignId: String,
    AdGroupId: String,
    TestImpressions: Long,
    TestSpend: Double,
    TestConversions: Long,
    TestCPA: Double,
    ControlImpressions: Long,
    ControlSpend: Double,
    ControlConversions: Long,
    ControlCPA: Double,
    Improvement: Double,
    Date: Long
)

case class PerfCompDataSet() extends DatePartitionedS3DataSet[PerfCompRecord](
  dataSetType = GeneratedDataSet,
  s3RootPath = S3Roots.IDENTITY_ROOT,
  rootFolderPath = "models/firstpartycookiescoring/perfquery/v=1"
)
