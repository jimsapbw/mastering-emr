package model

import java.time.LocalDate
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.datalake.{ConversionTrackerVerticaLoadDataSetV4, EventTrackerVerticaLoadDataSetV4}
import com.thetradedesk.spark.util.TTDConfig.defaultCloudProvider

package object fpcookiescoring {
  def loadConversionTrackerData(rangeStart: LocalDate, rangeEnd: LocalDate) = {
    ConversionTrackerVerticaLoadDataSetV4(defaultCloudProvider)
      // todo: adjust end date to include current hour if we decide to run the job more often
      .readRange(rangeStart.atStartOfDay(), rangeEnd.atStartOfDay())
      .filter('tdid =!= "00000000-0000-0000-0000-000000000000")
  }

  def loadEventTrackerData(rangeStart: LocalDate, rangeEnd: LocalDate) = {
    EventTrackerVerticaLoadDataSetV4(defaultCloudProvider)
      // todo: adjust end date to include current hour if we decide to run the job more often
      .readRange(rangeStart.atStartOfDay(), rangeEnd.atStartOfDay())
      .filter('tdid =!= "00000000-0000-0000-0000-000000000000")
  }
}
