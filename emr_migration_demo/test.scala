package model.fpcookiescoring

import java.sql.Timestamp
import java.time.LocalDate
import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources._
import com.thetradedesk.spark.datasets.sources.datalake.BidRequestDataSetV5
import com.thetradedesk.spark.sql.SQLFunctions.{hllCount, hllMerge, hllUpdate}
import com.thetradedesk.spark.util.RichConfig.currentDateUTC
import com.thetradedesk.spark.util.RichDataFrame._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object GenerateCampaignScores extends Logger {
  // metrics
  val prometheus = new PrometheusClient("FirstPartyCookieScoring", "CampaignScoring")
  val advertiserCountMetricGauge = prometheus.createGauge("fpcs_advertiser_count", "Total number of advertisers processed")
  val jobDurationGauge = prometheus.createGauge("fpcs_run_time_seconds", "Job execution time in seconds")
  val excludedAdvertisersGauge = prometheus
    .createGauge("fpcs_excluded_advertisers", "Advertisers excluded because they don't have enough qualified conversions", "advertiserId")

  def main(args: Array[String]): Unit = {
    val reportingColumnDataSet = CampaignConversionReportingColumnDataSet()

    /** Command Line Inputs * */
    val predictionDate: LocalDate = config.getDate("date", default = currentDateUTC())
    val trainingLookbackWindowDays: Int = config.getInt("trainingWindowDays", default = 4)
    val labelLookbackWindowDays: Int = config.getInt("labelWindowDays", default = 1)
    val bidRequestLookbackWindowDays: Int = config.getInt("bidRequestLookbackWindowDays", default = 2)
    val generateDebugTables: Boolean = config.getBoolean("generateDebugTables", default = false)
    val cacheCampaignScore2: Boolean = config.getBoolean("cacheCampaignScore2", default = false)
    val debugBasePath: String = config
      .getString("debugBasePath", default = "s3://ttd-identity/datapipeline/test/models/firstpartycookiescoring/debug")
    val smallTableWriteParallelism: Int = config.getInt("smallTableWriteParallelism", default = 200)


    /** Read/calculate inputs * */
    //val testingRangeStart = predictionDate.minusDays(testingLookbackWindowDays)
    val labelRangeEnd = predictionDate
    val trainingRangeStart = predictionDate.minusDays(labelLookbackWindowDays).minusDays(trainingLookbackWindowDays)
    val bidRequestRangeStart = predictionDate.minusDays(labelLookbackWindowDays).minusDays(bidRequestLookbackWindowDays)

    val jobDurationGaugeTimer = jobDurationGauge.startTimer()

    val campaigns = CampaignDataSet()
      .readLatestPartition()
      .select('CampaignId, 'AdvertiserId, 'CustomCPATypeId, 'CustomROASTypeId)

    val adgroups = AdGroupDataSet()
      .readLatestPartition()
      .select('AdGroupId, 'CampaignId)

    val enabledAdGroups = AdGroupAudienceRetargetingSettingsDataSet()
      .readLatestPartition()
      .filter('isEnabled === true)
      .select('AdGroupId)


    // this dataframe stores the AdvertiserId and CampaignId that with retargeting setting = true
    // in the lastest partition (1 day)
    val enabledCampaignsAndAdvertisers = enabledAdGroups
      .join(broadcast(adgroups), Seq("AdGroupId"), "inner")
      .join(broadcast(campaigns), Seq("CampaignId"), "inner")
      .select('AdvertiserId, 'CampaignId)
      .cache()

    val enabledAdvertisers = enabledCampaignsAndAdvertisers.select('AdvertiserId).distinct()

    val enabledCampaigns = enabledCampaignsAndAdvertisers.select('CampaignId).distinct()

    advertiserCountMetricGauge.set(enabledAdvertisers.count())

    val rtg1 =
      loadEventTrackerData(trainingRangeStart, labelRangeEnd)
        .select('logentrytime, 'advertiserid, 'trackingtagid, 'tdid, 'ipaddress, 'normalizedreferrerurl, 'country, 'region, 'metro, 'city,
          'bidderdatacenterid)
        .withColumn("pixelfiretime", to_timestamp('logentrytime))
        .join(broadcast(enabledAdvertisers), Seq("advertiserid"), "inner") // retain only events corresponding to advertisers we care about

    val rtg2 =
      loadConversionTrackerData(trainingRangeStart, labelRangeEnd)
        .select('logentrytime, 'advertiserid, 'trackingtagid, 'tdid, 'ipaddress, 'normalizedreferrerurl, 'country, 'region, 'metro, 'city,
          'bidderdatacenterid)
        .withColumn("pixelfiretime", to_timestamp('logentrytime))
        .join(broadcast(enabledAdvertisers), Seq("advertiserid"), "inner") // retain only events corresponding to advertisers we care about

    // combine two tables
    // this table union the events and conversions related to the advertisers that have ever turn on retargeting in
    // the latest partition (today)
    // columns: logentrytime, advertiserid, trackingtagid, tdid, ipaddress, normalizedreferrerurl,
    // country, region, metro, city, bidderdatacenterid, pixelfiretime = timestamp(logentrytime)
    val events = rtg1.union(rtg2)

    // While we have events, quickly grab a list of users for each tracking tag id our advertisers have specified as interesting to them,
    // and save it off for the FirstPartyCookieScoringModel job

    // table stores
    // this table includes the pixels that the advertisers interested and would like to retargeting, these should be
    // the subset of the retargeting on trackingtagid level
    val trackingtagids = AdGroupAudienceRetargetingPixelsDataSet()
      .readLatestPartition()
      .select('AdvertiserId as "advertiserid", 'AdGroupId, 'TrackingTagId as "trackingtagid")

    // filter
    // narrow down to the advertiser, adgroup and trackingtag level that the advertiser enabled retargeting and interested in the specific
    // pixels -- those audiences
    // the reason why we use distinct here is: events includes multiple row of the same information here in different timestamps
    val adGroupSelectedUsers = events
      .join(broadcast(trackingtagids), Seq("advertiserid", "trackingtagid"), "inner")
      .select('advertiserid as "AdvertiserId", 'AdGroupId, 'trackingtagid as "TrackingTagId", 'tdid as "TDID")
      .distinct()
      .as[AdGroupSelectedUsersRecord]

    AdGroupSelectedUsersDataSet().write(adGroupSelectedUsers)

    // campaign custom setting
    // compare to the previous version, we add some new columns here: CustomROASWeight, CustomROASClickWeight, CustomROASViewthroughWeight
    // this reporting includes ROAS weight and trackingtag id
    // reportingColumnId means which row of that pixel on the UI
    // in order to keep the basic logic simple and similar to the CPA setting, we ignore the view revenue and click revenue (otherwise
    // we need to include attribution table into our pipeline) and simply use the max value of CustomROAS Weights as the Weight for ROAS
    // the reason why we have roas and cpa weight available at the same campaign: it is ok for the user to set them at the same time
    // however, when we use the retargeting model, it will use the adgroup level roigoal to decide which one to use
    val custom = reportingColumnDataSet
      .readLatestPartition()
      .select('CampaignId, 'AdvertiserId, 'ReportingColumnId, 'TrackingTagId, 'IncludeInCustomCPA, 'Weight,
        'IncludeInCustomROAS, 'CustomROASWeight, 'CustomROASClickWeight, 'CustomROASViewthroughWeight)

    // custom: reportingColumn is a kind of table that stores the definition of the columns in the reporting
    // campaign: this table stores the information of setting. For some reasons, we have the same campaiginid in these two tables
    // even only one store the right settings info, and the other one store the other settings with null value. Thus, we need to
    // join them together; BTW, campaigns includes CPA weight without trackingtag id
    // IncludeInCustomROAS saved in ReportingColumn - true/false
    // CustomROASTypeId (saved in CampaignDataSet):
    // 0: not use any roas weight
    // 1: only use CustomROASWeight it is the Conversion credit on the UI
    // 2: use both CustomROASClickWeight and CustomROASViewthroughWeight
    // 3: use all three weightsweight
    // cp: the retargetting enabled campaigns with available CPA weight or ROAS weight for each trackingtagid

    val cp = campaigns
      .join(broadcast(custom), Seq("CampaignId", "AdvertiserId"), "inner")
      .join(broadcast(enabledCampaigns), Seq("CampaignId"), "inner")
      .withColumn("CPA_weight",
        // campaigns without custom cpa setting
        // take only first reporting column
        when('CustomCPATypeId === 0 && 'ReportingColumnId === 1, lit(1.0))
          // ...or take all reporting columns that are included in custom cpa setting
          .when('CustomCPATypeId > 0 && 'IncludeInCustomCPA === true, coalesce('Weight.cast("double"), lit(1.0)))
          .otherwise(lit(null)))
      // placeholder need to check cp's effect on the roas weight
      .withColumn("ROAS_weight",
        // campaigns without custom roas setting
        // take only first reporting column
        when('CustomROASTypeId === 0 && 'ReportingColumnId === 1, lit(1.0))
          .when('CustomROASTypeId === 1 && 'IncludeInCustomROAS === true, coalesce('CustomROASWeight.cast("double"), lit(1.0)))
          // when max value is roas weight
          .when('CustomROASTypeId > 1 && 'IncludeInCustomROAS === true &&
                'CustomROASWeight.cast("double") >= 'CustomROASClickWeight.cast("double")
                && 'CustomROASWeight.cast("double") >= 'CustomROASViewthroughWeight.cast("double"),
            coalesce('CustomROASWeight.cast("double"), lit(1.0)))
          // when max value is roas click weight
          .when('CustomROASTypeId > 1 && 'IncludeInCustomROAS === true &&
                'CustomROASClickWeight.cast("double") >= 'CustomROASWeight.cast("double")
                && 'CustomROASClickWeight.cast("double") >= 'CustomROASViewthroughWeight.cast("double"),
            coalesce('CustomROASClickWeight.cast("double"), lit(1.0)))
          // when max value is roas view through weight
          .when('CustomROASTypeId > 1 && 'IncludeInCustomROAS === true &&
                'CustomROASViewthroughWeight.cast("double") >= 'CustomROASWeight.cast("double")
                && 'CustomROASViewthroughWeight.cast("double") >= 'CustomROASClickWeight.cast("double"),
            coalesce('CustomROASViewthroughWeight.cast("double"), lit(1.0)))
          .otherwise(lit(null)))
      .filter(!('CPA_weight.isNull && 'ROAS_weight.isNull))
      .select('CampaignId, 'AdvertiserId, 'TrackingTagId, 'CPA_weight, 'ROAS_weight, 'ReportingColumnId)
      .withColumn("conversiontag", 'TrackingTagId)


    // conversions: conversion data for the trackingtag with enabled retargetting settings and available CPA weight
    // read in conversion data
    // need to join with trackingtag table once it's in place
    // take x days more conversion data in case attribution window is longer than a day
    // ConversionTracker does not have campaignid info that's why we use drop duplicates in the userRaw
    // the other reason is that the info we have in the events is not enough to distinguish the audiences --
    // in the future, we can add more info into here to make the model more stronger and may be we can build uo the model without
    // drop duplicates which means we will have more dataset
    val conversions = loadConversionTrackerData(trainingRangeStart, labelRangeEnd)
      .join(broadcast(cp), Seq("advertiserid", "trackingtagid"), "inner")
      .select('advertiserid, 'trackingtagid, 'tdid, 'logentrytime, 'MonetaryValue.cast("double"))
      .withColumn("conversiontime", to_timestamp('logentrytime))
      .join(broadcast(enabledAdvertisers), Seq("advertiserid"), "inner") // retain only events corresponding to advertisers we care about

    if (generateDebugTables) {
      conversions.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "conversions"))
    }

    // get the first conversion time from the conversions table
    val firstConversions = conversions
      .groupBy("tdid", "trackingtagid", "advertiserid")
      .agg(min("conversiontime") as "firstconversion", avg("MonetaryValue") as "MonetaryValue_")
      .withColumnRenamed("trackingtagid", "conversiontag")

    // use firstConversions to add the first conversion time info to events that
    // for each tdid under each advertiserid (regardless trackingtagid) and ignore the converted tracktagid record
    // only one record for per date per advertiserid and tdid and conversiontag
    // kind of adding label by using converted trackingtag
    // join events table with conversions
    // note: firstConversions table is still small enough that we can broadcast it. This might change in the future
    // as we enable more and more adgroups. If the job fails because of broadcast size, this is the first place
    // we should look.
    // kind like building up relationships between first time conversion and past events
    spark.sparkContext.setJobDescription("usersRaw")
    val usersRaw = events
      .join(firstConversions, Seq("advertiserid", "tdid"), "left")
      .filter((($"pixelfiretime" < $"firstconversion") && ($"trackingtagid" =!= $"conversiontag")) || $"firstconversion".isNull)
      .select($"advertiserid", $"tdid", $"logentrytime", $"normalizedreferrerurl", $"country", $"region", $"metro", $"city",
        $"pixelfiretime", $"conversiontag", $"firstconversion", $"bidderdatacenterid", $"MonetaryValue_")
      .dropDuplicates("advertiserid", "tdid", "pixelfiretime",
        "conversiontag") // very important, as we will see multiple events or pixel fires on events and we only want one tdid per advertiser on specific date.
      .cacheToHDFS("usersRaw")

    if (generateDebugTables) {
      usersRaw.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "usersRaw"))
    }

    // tdids and data center info
    spark.sparkContext.setJobDescription("tdidsAndDcs")
    val tdidsAndDcs = usersRaw
      .select('tdid, 'bidderdatacenterid.cast("Int"))
      .groupBy('tdid)
      .agg(collect_set('bidderdatacenterid) as 'dcs)
      .select('tdid, 'dcs)
      .cacheToHDFS("tdidsAndDcs")

    if (generateDebugTables) {
      tdidsAndDcs.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "tdidsAndDcs"))
    }

    // get first and last pixel hits for users
    // need to change value for "currenttime" to midnight of the last date of conversions
    val timestamp_diff = udf((startTime: Timestamp, endTime: Timestamp) => {
      endTime.getTime - startTime.getTime
    })

    // records the time difference info on advertiserid, conversiontag and tdid perspective
    // one conversiontag(includes null) one row
    spark.sparkContext.setJobDescription("firstLastPixel0")
    val firstLastPixel0 = usersRaw
      // let's fight this skew hllMerge
      // create 10 bins
      .withColumn("bin", (rand * 10).cast(IntegerType))
      .groupBy("bin", "tdid", "conversiontag", "advertiserid", "firstconversion")
      .agg(min("pixelfiretime") as "firstpixelfire", max("pixelfiretime") as "lastpixelfire", hllUpdate("pixelfiretime") as "pixelfires_t",
        sum("MonetaryValue_") as "MonetaryValue_", sum(lit(1)) as "MonetaryCount")
      // second aggregation removes bins and merges HLLs
      .groupBy("tdid", "conversiontag", "advertiserid", "firstconversion")
      .agg(min("firstpixelfire") as "firstpixelfire", max("lastpixelfire") as "lastpixelfire",
        hllMerge($"pixelfires_t") as "pixelfires_hll",
        // calculate the mean of MonetaryValue
        sum("MonetaryValue_") / sum("MonetaryCount") as "MonetaryValue")
      .withColumn("currenttime", lit(Timestamp.valueOf(predictionDate.atStartOfDay())))
      .withColumn("firstlaginmins",
        timestamp_diff('firstpixelfire, when('firstconversion.isNull, 'currenttime).otherwise('firstconversion)) / 1000 / 60)
      .withColumn("lastlaginmins",
        timestamp_diff('lastpixelfire, when('firstconversion.isNull, 'currenttime).otherwise('firstconversion)) / 1000 / 60)
      .withColumn("firstlaginhrs", 'firstlaginmins / 60)
      .withColumn("firstlaginhrsint", 'firstlaginhrs.cast(IntegerType))
      .withColumn("lastlaginhrs", 'lastlaginmins / 60)
      .withColumn("lastlaginhrsint", 'lastlaginhrs.cast(IntegerType))
      .withColumn("pixelfires", hllCount('pixelfires_hll))
      .select("advertiserid", "tdid", "conversiontag", "firstconversion", "pixelfires", "firstlaginhrsint", "lastlaginhrsint",
        "lastpixelfire", "MonetaryValue")
      .filter('pixelfires <= 1000)
      .cacheToHDFS("firstLastPixel0")


    // converters
    val windowSpec = Window.partitionBy('advertiserid)
    spark.sparkContext.setJobDescription("firstLastPixel1")
    val firstLastPixel1 = firstLastPixel0
      .filter('conversiontag.isNotNull)
      .withColumn("total", count(lit(1)).over(windowSpec))
      .where('total >= 100)
      .withColumn("converted", lit(1))
      .withColumn("pixelfiretime", 'lastpixelfire)
      .select("tdid", "advertiserid", "conversiontag", "converted", "pixelfires", "firstlaginhrsint", "lastlaginhrsint", "pixelfiretime",
        "MonetaryValue")
      .cacheToHDFS("firstLastPixel1")

    // figure out which advertisers will be excluded because they don't have enough conversions
    findAdvertisersWithNoData(firstLastPixel1, enabledAdvertisers)
    enabledAdvertisers.unpersist()

    // non-converters
    val firstLastPixel2 = firstLastPixel0
      .filter('conversiontag.isNull)
      .withColumn("converted", lit(0))
      .withColumn("pixelfiretime", 'lastpixelfire)
      .select("tdid", "advertiserid", "converted", "pixelfires", "firstlaginhrsint", "lastlaginhrsint", "pixelfiretime")


    // join usersRaw table to firstLastPixel table
    // only take lastpixelfire (pixelfiretime in firstLastPixel is actually last pixel fire)

    // both usersC and usersN only grab the lastest record of the users info from usersRaw
    // converted users
    spark.sparkContext.setJobDescription("usersC")
    val usersC = usersRaw
      .filter('conversiontag.isNotNull)
      .join(firstLastPixel1, Seq("advertiserid", "tdid", "conversiontag", "pixelfiretime"), "inner")
      .select("advertiserid", "tdid", "conversiontag", "normalizedreferrerurl", "country", "region", "metro", "city", "pixelfires",
        "firstlaginhrsint", "lastlaginhrsint", "converted", "bidderdatacenterid", "MonetaryValue")
      .coalesce(smallTableWriteParallelism)
      .cacheToHDFS("usersC")

    if (generateDebugTables) {
      usersC.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "usersC"))
    }

    // non-converted users
    spark.sparkContext.setJobDescription("usersN")
    val usersN = usersRaw
      .filter('conversiontag.isNull)
      .join(firstLastPixel2, Seq("advertiserid", "tdid", "pixelfiretime"), "inner")
      .select("advertiserid", "tdid", "normalizedreferrerurl", "country", "region", "metro", "city", "pixelfires", "firstlaginhrsint",
        "lastlaginhrsint", "converted", "bidderdatacenterid", "pixelfiretime")
      .cacheToHDFS("usersN")


    if (generateDebugTables) {
      usersN.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "usersN"))
    }

    firstLastPixel0.unpersist()

    // conversion rate for each conversion tag
    // use as baseline conversion rate per campaign

    val crC = usersC
      .groupBy("advertiserid", "conversiontag")
      .agg(countDistinct("tdid") as "converters", avg("MonetaryValue") as "MonetaryValue")

    val crN = usersN
      .groupBy("advertiserid")
      .agg(approx_count_distinct("tdid", 0.05) as "nonconverters")

    // MonetaryValue saved in cr
    spark.sparkContext.setJobDescription("cr")
    val cr = crC
      .join(crN, Seq("advertiserid"), "left")
      .withColumn("totalusers", 'converters + when('nonconverters.isNull, 0).otherwise('nonconverters))
      .withColumn("conversionrate", 'converters / 'totalusers)
      .withColumn("baseline", pow('conversionrate, 6))
      .coalesce(smallTableWriteParallelism)
      .cacheToHDFS("cr")

    if (generateDebugTables) {
      cr.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "cr"))
    }

    // conversion rate for lastpage (normalizedreferrerurl)

    val lastPageC = usersC
      .filter('normalizedreferrerurl.isNotNull)
      .withColumn("lastpage", split('normalizedreferrerurl, ";").getItem(0))
      .groupBy("advertiserid", "conversiontag", "lastpage")
      .agg(countDistinct("tdid") as "converters")

    val lastPageN = usersN
      .filter('normalizedreferrerurl.isNotNull)
      .withColumn("lastpage", split('normalizedreferrerurl, ";").getItem(0))
      .groupBy("advertiserid", "lastpage")
      .agg(approx_count_distinct("tdid", 0.05) as "nonconverters")

    spark.sparkContext.setJobDescription("lastPage")
    val lastPage = lastPageC
      .join(lastPageN, Seq("advertiserid", "lastpage"), "left")
      .withColumn("totalusers", 'converters + when('nonconverters.isNull, 0).otherwise('nonconverters))
      .withColumn("conversionrate", 'converters / 'totalusers)
      .coalesce(smallTableWriteParallelism)
      .cacheToHDFS("lastPage")


    // conversion rate for country

    val countryC = usersC
      .filter('country.isNotNull)
      .groupBy("advertiserid", "conversiontag", "country")
      .agg(countDistinct("tdid") as "converters")

    val countryN = usersN
      .filter('country.isNotNull)
      .groupBy("advertiserid", "country")
      .agg(approx_count_distinct("tdid", 0.05) as "nonconverters")

    spark.sparkContext.setJobDescription("country")
    val country = countryC
      .join(countryN, Seq("advertiserid", "country"), "left")
      .withColumn("totalusers", 'converters + when('nonconverters.isNull, 0).otherwise('nonconverters))
      .withColumn("conversionrate", 'converters / 'totalusers)
      .coalesce(smallTableWriteParallelism)
      .cacheToHDFS("country")


    // conversion rate for region

    val regionC = usersC
      .filter('region.isNotNull)
      .groupBy("advertiserid", "conversiontag", "region")
      .agg(countDistinct("tdid") as "converters")

    val regionN = usersN
      .filter('region.isNotNull)
      .groupBy("advertiserid", "region")
      .agg(approx_count_distinct("tdid", 0.05) as "nonconverters")

    spark.sparkContext.setJobDescription("region")
    val region = regionC
      .join(regionN, Seq("advertiserid", "region"), "left")
      .withColumn("totalusers", 'converters + when('nonconverters.isNull, 0).otherwise('nonconverters))
      .withColumn("conversionrate", 'converters / 'totalusers)
      .coalesce(smallTableWriteParallelism)
      .cacheToHDFS("region")


    // conversion rate for city

    val cityC = usersC
      .filter('city.isNotNull)
      .groupBy("advertiserid", "conversiontag", "city")
      .agg(countDistinct("tdid") as "converters")

    val cityN = usersN
      .filter('city.isNotNull)
      .groupBy("advertiserid", "city")
      .agg(approx_count_distinct("tdid", 0.05) as "nonconverters")

    spark.sparkContext.setJobDescription("city")
    val city = cityC
      .join(cityN, Seq("advertiserid", "city"), "left")
      .withColumn("totalusers", 'converters + when('nonconverters.isNull, 0).otherwise('nonconverters))
      .withColumn("conversionrate", 'converters / 'totalusers)
      .coalesce(smallTableWriteParallelism)
      .cacheToHDFS("city")

    // conversion rate for pixelfires

    val pixelfiresC = usersC
      .filter('pixelfires.isNotNull)
      .groupBy("advertiserid", "conversiontag", "pixelfires")
      .agg(countDistinct("tdid") as "converters")

    spark.sparkContext.setJobDescription("pixelfiresN")
    val pixelfiresN = usersN
      .filter('pixelfires.isNotNull)
      .groupBy("advertiserid", "pixelfires")
      .agg(approx_count_distinct("tdid", 0.05) as "nonconverters")
      .coalesce(smallTableWriteParallelism)
      .cacheToHDFS("pixelfiresN")

    // query 10, job 36, stage 82
    spark.sparkContext.setJobDescription("pixelfires")
    val pixelfires = pixelfiresC
      .join(pixelfiresN, Seq("advertiserid", "pixelfires"), "left")
      .withColumn("totalusers", 'converters + when('nonconverters.isNull, 0).otherwise('nonconverters))
      .withColumn("conversionrate", 'converters / 'totalusers)
      .coalesce(smallTableWriteParallelism)
      .cacheToHDFS("pixelfires")

    // conversion rate for lastlaginhrsint
    val lastlagC = usersC
      .filter('lastlaginhrsint.isNotNull)
      .groupBy("advertiserid", "conversiontag", "lastlaginhrsint")
      .agg(countDistinct("tdid") as "converters")

    val lastlagN = usersN
      .filter('lastlaginhrsint.isNotNull)
      .groupBy("advertiserid", "lastlaginhrsint")
      .agg(approx_count_distinct("tdid", 0.05) as "nonconverters")

    spark.sparkContext.setJobDescription("lastlag")
    val lastlag = lastlagC
      .join(lastlagN, Seq("advertiserid", "lastlaginhrsint"), "left")
      .withColumn("totalusers", 'converters + when('nonconverters.isNull, 0).otherwise('nonconverters))
      .withColumn("conversionrate", 'converters / 'totalusers)
      .coalesce(smallTableWriteParallelism)
      .cacheToHDFS("lastlag")


    // score users who have not converted yet

    //  remove one time wonders
    spark.sparkContext.setJobDescription("bidRequestUsers")
    val bidRequestUsers = BidRequestDataSetV5(defaultCloudProvider)
      .readRange(bidRequestRangeStart.atStartOfDay(), predictionDate.atStartOfDay())
      .filter(($"TDID".isNotNull && $"TDID" =!= "" && $"TDID" =!= lit("00000000-0000-0000-0000-000000000000")) || (
        $"DeviceAdvertisingId".isNotNull && $"DeviceAdvertisingId" =!= "" &&
        $"DeviceAdvertisingId" =!= lit("00000000-0000-0000-0000-000000000000")
        ))
      .select("TDID", "DeviceAdvertisingId")
      .groupBy("TDID", "DeviceAdvertisingId")
      .agg(count(lit(1)) as "Count")
      .cacheToHDFS("bidRequestUsers")
    spark.sparkContext.setJobDescription("recentlyNonConvertedUsers")
    val recentUsersN = usersN
      .groupBy("tdid")
      .agg(max("pixelfiretime") as "lastpixelfire")
      .filter($"lastpixelfire" > lit(bidRequestRangeStart.toString))
      .select("TDID")
      .cacheToHDFS("recentlyNonConvertedUsers")
    // filter non-converters - we are only interested in ones that we have seen in bidding in the past few days
    val usersNVerified = removeOneTimeWonders(usersN, bidRequestUsers, recentUsersN)

    // based on advertisedid and factors (lastpage, country, region, city, pixel and lastlag) to calculate conversion rate
    // left semi join cr here to reduce the data volume, to make the outer join for usersN_scores faster
    spark.sparkContext.setJobDescription("usersN_score1")
    val usersN_score1 = usersNVerified
      .withColumn("lastpage", split('normalizedreferrerurl, ";").getItem(0))
      .join(lastPage, Seq("advertiserid", "lastpage"), "inner")
      .withColumn("lastpagecr", 'conversionrate)
      .select("advertiserid", "tdid", "conversiontag", "lastpage", "lastpagecr")
      .join(broadcast(cr.select('advertiserid, 'conversiontag)), Seq("advertiserid", "conversiontag"), "inner")
      .cacheToHDFS("usersN_score1")

    spark.sparkContext.setJobDescription("usersN_score2")
    val usersN_score2 = usersNVerified
      .join(broadcast(country), Seq("advertiserid", "country"), "inner")
      .withColumn("countrycr", 'conversionrate)
      .select("advertiserid", "tdid", "conversiontag", "country", "countrycr")
      .join(broadcast(cr.select('advertiserid, 'conversiontag)), Seq("advertiserid", "conversiontag"), "inner")
      .cacheToHDFS("usersN_score2")

    spark.sparkContext.setJobDescription("usersN_score3")
    val usersN_score3 = usersNVerified
      .join(region, Seq("advertiserid", "region"), "inner")
      .withColumn("regioncr", 'conversionrate)
      .select("advertiserid", "tdid", "conversiontag", "region", "regioncr")
      .join(broadcast(cr.select('advertiserid, 'conversiontag)), Seq("advertiserid", "conversiontag"), "inner")
      .cacheToHDFS("usersN_score3")

    spark.sparkContext.setJobDescription("usersN_score4")
    val usersN_score4 = usersNVerified
      .join(city, Seq("advertiserid", "city"), "inner")
      .withColumn("citycr", 'conversionrate)
      .select("advertiserid", "tdid", "conversiontag", "city", "citycr")
      .join(broadcast(cr.select('advertiserid, 'conversiontag)), Seq("advertiserid", "conversiontag"), "inner")
      .cacheToHDFS("usersN_score4")

    spark.sparkContext.setJobDescription("usersN_score5")
    val usersN_score5 = usersNVerified
      .join(pixelfires, Seq("advertiserid", "pixelfires"), "inner")
      .withColumn("pixelfirescr", 'conversionrate)
      .select("advertiserid", "tdid", "conversiontag", "pixelfires", "pixelfirescr")
      .join(broadcast(cr.select('advertiserid, 'conversiontag)), Seq("advertiserid", "conversiontag"), "inner")
      .cacheToHDFS("usersN_score5")

    spark.sparkContext.setJobDescription("usersN_score6")
    val usersN_score6 = usersNVerified
      .join(lastlag, Seq("advertiserid", "lastlaginhrsint"), "inner")
      .withColumn("lastlagcr", 'conversionrate)
      .select("advertiserid", "tdid", "conversiontag", "lastlaginhrsint", "lastlagcr")
      .join(broadcast(cr.select('advertiserid, 'conversiontag)), Seq("advertiserid", "conversiontag"), "inner")
      .cacheToHDFS("usersN_score6")

    if (generateDebugTables) {
      usersN_score1.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "usersN_score1"))
      usersN_score2.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "usersN_score2"))
      usersN_score3.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "usersN_score3"))
      usersN_score4.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "usersN_score4"))
      usersN_score5.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "usersN_score5"))
      usersN_score6.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "usersN_score6"))
    }

    // take min & avg scores per advertiserid/conversiontag/feature
    spark.sparkContext.setJobDescription("lastpage_cr")
    val lastpage_cr = lastPage
      .groupBy("advertiserid", "conversiontag")
      .agg(min("conversionrate") as "minlastpagecr", avg("conversionrate") as "avglastpagecr")
      .cacheToHDFS("lastpage_cr")

    val country_cr = country
      .groupBy("advertiserid", "conversiontag")
      .agg(min("conversionrate") as "mincountrycr", avg("conversionrate") as "avgcountrycr")
      .cache()

    val region_cr = region
      .groupBy("advertiserid", "conversiontag")
      .agg(min("conversionrate") as "minregioncr", avg("conversionrate") as "avgregioncr")
      .cache()

    val city_cr = city
      .groupBy("advertiserid", "conversiontag")
      .agg(min("conversionrate") as "mincitycr", avg("conversionrate") as "avgcitycr")
      .cache()

    spark.sparkContext.setJobDescription("pixelfires_cr")
    val pixelfires_cr = pixelfires
      .groupBy("advertiserid", "conversiontag")
      .agg(min("conversionrate") as "minpixelfirescr", avg("conversionrate") as "avgpixelfirescr")
      .cacheToHDFS("pixelfires_cr")

    spark.sparkContext.setJobDescription("lastlag_cr")
    val lastlag_cr = lastlag
      .groupBy("advertiserid", "conversiontag")
      .agg(min("conversionrate") as "minlastlagcr", avg("conversionrate") as "avglastlagcr")
      .cacheToHDFS("lastlag_cr")


    // combine all scores
    spark.sparkContext.setJobDescription("campaignScore1")
    val usersN_scores = usersN_score1
      .join(usersN_score2, Seq("advertiserid", "tdid", "conversiontag"), "outer")
      .join(usersN_score3, Seq("advertiserid", "tdid", "conversiontag"), "outer")
      .join(usersN_score4, Seq("advertiserid", "tdid", "conversiontag"), "outer")
      .join(usersN_score5, Seq("advertiserid", "tdid", "conversiontag"), "outer")
      .join(usersN_score6, Seq("advertiserid", "tdid", "conversiontag"), "outer")
      .join(broadcast(cr), Seq("advertiserid", "conversiontag"), "inner") // to include the base conversion rate
      .join(lastpage_cr, Seq("advertiserid", "conversiontag"), "left")
      .join(broadcast(country_cr), Seq("advertiserid", "conversiontag"), "left")
      .join(broadcast(region_cr), Seq("advertiserid", "conversiontag"), "left")
      .join(broadcast(city_cr), Seq("advertiserid", "conversiontag"), "left")
      .join(pixelfires_cr, Seq("advertiserid", "conversiontag"), "left")
      .join(lastlag_cr, Seq("advertiserid", "conversiontag"), "left")

    if (generateDebugTables) {
      usersN_scores.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "usersN_scores"))
    }

    // calculate score for user
    val usersN_overall = usersN_scores
      // lastpagecr: based on advertiserid, lastpage to calculate
      // acglastpagecr: based on advertiserid, conversiontag
      // conversionrate: advertiserid level; base line
      .withColumn("lastpagescore", coalesce('lastpagecr, 'avglastpagecr, 'conversionrate))
      .withColumn("countryscore", coalesce('countrycr, 'avgcountrycr, 'conversionrate))
      .withColumn("regionscore", coalesce('regioncr, 'avgregioncr, 'conversionrate))
      .withColumn("cityscore", coalesce('citycr, 'avgcitycr, 'conversionrate))
      .withColumn("pixelfiresscore", coalesce('pixelfirescr, 'avgpixelfirescr, 'conversionrate))
      .withColumn("lastlagscore", coalesce('lastlagcr, 'avglastlagcr, 'conversionrate))
      .withColumn("score", $"lastpagescore" * $"countryscore" * $"regionscore" * $"cityscore" * $"pixelfiresscore" * $"lastlagscore")
      .select($"advertiserid", $"conversiontag", $"tdid", $"lastpagescore", $"countryscore", $"regionscore", $"cityscore",
        $"pixelfiresscore", $"lastlagscore", $"score")

    if (generateDebugTables) {
      usersN_overall.coalesce(200).write.parquet(FSUtils.combinePaths(debugBasePath, "usersN_overall"))
    }

    // cr: advertiser and conversiontag level user information with number of converters and non-converters (non-converters info on advertiserid level)
    // cp: campaign and advertiser level, campaign setting information
    val campaignsAndConversionRates = cp
      .join(cr, Seq("advertiserid", "conversiontag"), "inner")
      .cacheToHDFS("campaignsAndConversionRates")

    val campaignScore1 = usersN_overall
      .join(broadcast(campaignsAndConversionRates), Seq("advertiserid", "conversiontag"), "inner")
      .cacheToHDFS("campaignScore1")

    if (generateDebugTables) {
      campaignScore1.coalesce(1000).write.parquet(FSUtils.combinePaths(debugBasePath, "campaignScore1"))
    }

    // add cookie scores based on baseline conversion rate
    spark.sparkContext.setJobDescription("campaignScore3")
    var campaignScore2 = campaignScore1
      .join(tdidsAndDcs, Seq("tdid"), "left") // integrate dcs for each tdid
      .select(
        'advertiserid,
        'CampaignId,
        'conversiontag,
        'tdid,
        'CPA_weight, // cpa weight
        'ROAS_weight, // roas weight
        'score,
        'baseline,
        'dcs,
        'MonetaryValue)
      .withColumn("relevantscore", 'score / 'baseline)
    if (cacheCampaignScore2) {
      campaignScore2 = campaignScore2.cacheToHDFS("campaignScore2")
    }

    val campaignScore3 = campaignScore2
      .withColumn("wtrelevance1", $"CPA_weight" * $"relevantscore")
      .withColumn("wtrelevance2", $"ROAS_weight" * $"MonetaryValue" * $"relevantscore")
      .groupBy("advertiserid", "campaignid", "tdid", "dcs")
      .agg(sum("wtrelevance1") as "sumwtr1", sum("CPA_weight") as "sumwt1",
        sum("wtrelevance2") as "sumwtr2", sum($"ROAS_weight") as "sumwt2")
      // nothing has changed for the user, why bother returning it or using it for test
      // if relevant score is equal to 1, bid override will be set to base bid
      // this is a small portion, less than 1% in my tests, but still
      .filter($"sumwtr1" =!= $"sumwt1" || $"sumwtr2" =!= $"sumwt2")
      .withColumn("relevantscore_CPA", $"sumwtr1" / $"sumwt1")
      .withColumn("relevantscore_ROAS", $"sumwtr2" / $"sumwt2")
      .as[CampaignScoreRecord]

    CampaignScoreDataSet().write(campaignScore3)

    // push metrics
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    // we're done
    spark.stop()
  }

  def findAdvertisersWithNoData(firstConversions: DataFrame, enabledAdvertisers: DataFrame): Unit = {
    val advertisersWithData = firstConversions.select('advertiserId)
      .distinct()

    // find all advertisers excluded because they don't have enough conversions
    spark.sparkContext.setJobDescription("excludedAdvertisersGauge")
    val advertisersNoData = enabledAdvertisers
      .join(broadcast(advertisersWithData.withColumn("exists", lit(1))), Seq("advertiserId"), "left")
      .where('exists.isNull)
      .select('advertiserId)
      .as[String]
      .collect()

    advertisersNoData.foreach(advertiser => excludedAdvertisersGauge.labels(advertiser).set(1))
  }

  private def removeOneTimeWonders(users: DataFrame, bidRequestUsers: DataFrame, recentUsersN: DataFrame): DataFrame = {

    println("users = " + users.count())
    spark.sparkContext.setJobDescription("usersVerified")
    // get all users that we've seen more than once
    val bidRequestTdidCounts = bidRequestUsers
      .filter($"TDID".isNotNull && $"TDID" =!= lit("") && $"TDID" =!= lit("00000000-0000-0000-0000-000000000000"))
      .groupBy("TDID")
      .agg(sum("Count") as "Count")
      .filter($"Count" > lit(1))
      .select("tdid")

    val bidRequestDidCounts = bidRequestUsers
      .filter($"DeviceAdvertisingId".isNotNull && $"DeviceAdvertisingId" =!= lit("") &&
              $"DeviceAdvertisingId" =!= lit("00000000-0000-0000-0000-000000000000")
              && $"DeviceAdvertisingId" =!= $"TDID")
      .groupBy("DeviceAdvertisingId")
      .agg(sum("Count") as "Count")
      .filter($"Count" > lit(1))
      .withColumnRenamed("DeviceAdvertisingId", "TDID")
      .select("tdid")

    // get users that we've seen in bidding last few days
    // combine then with users that we've seen in conversions last few days
    val usersSeenMoreThanOnce = bidRequestTdidCounts
      .union(bidRequestDidCounts)
      .union(recentUsersN)
      // just to get rid of duplicates
      // union does not drop duplication in Spark
      .groupBy("tdid")
      .agg(count(lit(1)) as "Count")
      .select("tdid")
    //.cacheToHDFS("usersSeenMoreThanOnce")
    //println("usersSeenMoreThanOnce = " + usersSeenMoreThanOnce.count())

    // weed out one time wonders for non-converters
    val usersVerified = users
      .join(usersSeenMoreThanOnce, "tdid")
      .cacheToHDFS("usersVerified")
    println("usersVerified= " + usersVerified.count())

    usersVerified
  }
}
