package hk.hku.spark.process

import java.text.SimpleDateFormat
import java.util.Date

import hk.hku.spark.corenlp.CoreNLPSentimentAnalyzer
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import twitter4j.{GeoLocation, TwitterFactory, TwitterObjectFactory}


/**
  * 预处理Tweet 文本数据
  */
object TweetAqiPreprocess {

  @transient
  lazy val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      // Use KryoSerializer for serializing objects as JavaSerializer is too slow.
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      // For reconstructing the Web UI after the application has finished.
      .set("spark.eventLog.enabled", "true")
      // Reduce the RDD memory usage of Spark and improving GC behavior.
      .set("spark.streaming.unpersist", "true")

    val sc = new SparkContext(conf)

    val inputText = "/tweets/data-bak0621/twitter.log"
    val outputText = "/tweets/preprocess/twitter_preprocess.csv"
    preprocessFromHDFS(sc, inputText, outputText)

  }

  def preprocessFromHDFS(sc: SparkContext, input: String, output: String): Unit = {
    log.info("preprocessFromHDFS start")

    val tweet4City = sc.textFile(input)

    val parsedTweets = tweet4City.map(line => {
      // 解析 twitter 元数据
      TwitterObjectFactory.createStatus(line)
    })

    val computeTweets = parsedTweets.map(status => {
      //    SF,NY,LA,chicago
      var city = "NULL"
      if (status.getGeoLocation != null) {
        city = returnCity(status.getGeoLocation.getLongitude, status.getGeoLocation.getLatitude)
      } else if (status.getPlace != null) {
        var x, y = 0.0
        for (coorList <- status.getPlace.getBoundingBoxCoordinates) {
          for (coor <- coorList) {
            // 经度
            x += coor.getLongitude
            // 纬度
            y += coor.getLatitude
          }
        }
        city = returnCity(x / 4, y / 4)
      }

      // id,text,sentiment,date,place,city,
      (status.getId,
        status.getText,
        CoreNLPSentimentAnalyzer.computeWeightedSentiment(status.getText),
        status.getCreatedAt.getTime,
        status.getPlace.getFullName,
        city
      )
    })

    computeTweets.saveAsTextFile(output)

  }


  final val SF_AREA = (-123.1512, 37.0771, -121.3165, 38.5396)
  final val NY_AREA = (-74.255735, 40.496044, -73.700272, 40.915256)
  final val LA_AREA = (-118.6682, 33.7037, -118.1553, 34.3373)
  final val CHICAGO = (-87.940267, 41.644335, -87.524044, 42.023131)
  final val LONDON = (-0.5104, 51.2868, 0.334, 51.6919)

  /**
    *
    * @param x 经度 longitude
    * @param y 纬度 latitude
    * @return
    */
  def returnCity(x: Double, y: Double): String = {
    if (SF_AREA._1 <= x && x <= SF_AREA._3 && SF_AREA._2 <= y && y <= SF_AREA._4)
      "SF"
    else if (NY_AREA._1 <= x && x <= NY_AREA._3 && NY_AREA._2 <= y && y <= NY_AREA._4)
      "NY"
    else if (LA_AREA._1 <= x && x <= LA_AREA._3 && LA_AREA._2 <= y && y <= LA_AREA._4)
      "LA"
    else if (CHICAGO._1 <= x && x <= CHICAGO._3 && CHICAGO._2 <= y && y <= CHICAGO._4)
      "CHICAGO"
    else if (LONDON._1 <= x && x <= LONDON._3 && LONDON._2 <= y && y <= LONDON._4)
      "LONDON"
    else
      "NULL"
  }
}