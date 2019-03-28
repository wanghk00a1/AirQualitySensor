package hk.hku.spark.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

/**
  * Reduce the console logging during execution of Spark jobs.
  *
  */
object LogUtils extends Logging {

  def setLogLevels(sparkContext: SparkContext) {

    sparkContext.setLogLevel(Level.WARN.toString)
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
//     stripMargin 固定对齐,默认| 作为出来连接符
      logInfo(
        """Setting log level to [WARN] for streaming executions.
          |To override add a custom log4j.properties to the classpath.""".stripMargin)
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}