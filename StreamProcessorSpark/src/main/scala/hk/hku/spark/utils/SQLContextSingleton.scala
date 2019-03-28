package hk.hku.spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Lazily instantiated singleton instance of SQLContext.
  */
object SQLContextSingleton {

  @transient
  @volatile private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = SQLContext.getOrCreate(sparkContext)
        }
      }
    }
    instance
  }
}