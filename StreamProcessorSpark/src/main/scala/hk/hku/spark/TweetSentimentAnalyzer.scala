package hk.hku.spark

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.fasterxml.jackson.databind.ObjectMapper
import hk.hku.spark.corenlp.CoreNLPSentimentAnalyzer
import hk.hku.spark.mllib.MLlibSentimentAnalyzer
import hk.hku.spark.utils._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import twitter4j.{Status, TwitterException, TwitterObjectFactory}
import twitter4j.auth.OAuthAuthorization

/**
  * Analyzes and predicts Twitter Sentiment in [near] real-time using Spark Streaming and Spark MLlib.
  * Uses the Naive Bayes Model created from the Training data and applies it to predict the sentiment of tweets
  * collected in real-time with Spark Streaming, whose batch is set to 20 seconds [configurable].
  * Raw tweets [compressed] and also the gist of predicted tweets are saved to the disk.
  * At the end of the batch, the gist of predicted tweets is published to Redis.
  * Any frontend app can subscribe to this Redis Channel for data visualization.
  */
// spark-submit --class "hk.hku.spark.TweetSentimentAnalyzer" --master local[3] /opt/spark-twitter/7305CloudProject/StreamProcessorSpark/target/StreamProcessorSpark-jar-with-dependencies.jar
object TweetSentimentAnalyzer {

  //  def main(args: Array[String]): Unit = {
  //    val json = "{\"created_at\":\"Fri Mar 22 13:25:59 +0000 2019\",\"id\":1109083804667392005,\"id_str\":\"1109083804667392005\",\"text\":\"RT @RobertKlemko: Was talking to an NFL exec for another story and he went off on this tremendous rant on college football and amateurism t\\u2026\",\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/iphone\\\" rel=\\\"nofollow\\\"\\u003eTwitter for iPhone\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":172152165,\"id_str\":\"172152165\",\"name\":\"Boozer\",\"screen_name\":\"BBrown_DoesIt\",\"location\":\"BX \\u27a1\\ufe0f VA\",\"url\":null,\"description\":\"JMU Football Alum & NSU Grad Student \\u03a9\\u03c8\\u03c6 1\\/2 of @SnackzandSmack\",\"translator_type\":\"none\",\"protected\":false,\"verified\":false,\"followers_count\":2936,\"friends_count\":1553,\"listed_count\":5,\"favourites_count\":124871,\"statuses_count\":33759,\"created_at\":\"Thu Jul 29 01:12:21 +0000 2010\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":true,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"131516\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme9\\/bg.gif\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme9\\/bg.gif\",\"profile_background_tile\":true,\"profile_link_color\":\"009999\",\"profile_sidebar_border_color\":\"EEEEEE\",\"profile_sidebar_fill_color\":\"EFEFEF\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/1096379482326401025\\/wLf6TL1H_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/1096379482326401025\\/wLf6TL1H_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/172152165\\/1455316976\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"retweeted_status\":{\"created_at\":\"Thu Mar 21 19:56:58 +0000 2019\",\"id\":1108819810547191809,\"id_str\":\"1108819810547191809\",\"text\":\"Was talking to an NFL exec for another story and he went off on this tremendous rant on college football and amateu\\u2026 https:\\/\\/t.co\\/5n2ufgF2gR\",\"display_text_range\":[0,140],\"source\":\"\\u003ca href=\\\"https:\\/\\/mobile.twitter.com\\\" rel=\\\"nofollow\\\"\\u003eTwitter Web App\\u003c\\/a\\u003e\",\"truncated\":true,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":218129328,\"id_str\":\"218129328\",\"name\":\"Robert Klemko\",\"screen_name\":\"RobertKlemko\",\"location\":\"Denver, CO\",\"url\":\"https:\\/\\/www.si.com\\/author\\/robert-klemko\",\"description\":\"football writer, Sports Illustrated. co-advisor, Bruce Randolph School student newspaper, The Paw Print. Terrapin. Son of an immigrant.\",\"translator_type\":\"none\",\"protected\":false,\"verified\":true,\"followers_count\":40277,\"friends_count\":3185,\"listed_count\":1285,\"favourites_count\":20761,\"statuses_count\":27811,\"created_at\":\"Sun Nov 21 14:13:37 +0000 2010\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":true,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"000000\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"0084B4\",\"profile_sidebar_border_color\":\"FFFFFF\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/770633665181196288\\/H2NcwtQh_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/770633665181196288\\/H2NcwtQh_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/218129328\\/1526680996\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"is_quote_status\":false,\"extended_tweet\":{\"full_text\":\"Was talking to an NFL exec for another story and he went off on this tremendous rant on college football and amateurism that I have to share. \\n\\n\\\"This whole thing stinks...\\\" https:\\/\\/t.co\\/rQuixmVURo\",\"display_text_range\":[0,172],\"entities\":{\"hashtags\":[],\"urls\":[],\"user_mentions\":[],\"symbols\":[],\"media\":[{\"id\":1108819801906905090,\"id_str\":\"1108819801906905090\",\"indices\":[173,196],\"media_url\":\"http:\\/\\/pbs.twimg.com\\/media\\/D2NRvCyUgAIDxvi.jpg\",\"media_url_https\":\"https:\\/\\/pbs.twimg.com\\/media\\/D2NRvCyUgAIDxvi.jpg\",\"url\":\"https:\\/\\/t.co\\/rQuixmVURo\",\"display_url\":\"pic.twitter.com\\/rQuixmVURo\",\"expanded_url\":\"https:\\/\\/twitter.com\\/RobertKlemko\\/status\\/1108819810547191809\\/photo\\/1\",\"type\":\"photo\",\"sizes\":{\"medium\":{\"w\":1200,\"h\":934,\"resize\":\"fit\"},\"thumb\":{\"w\":150,\"h\":150,\"resize\":\"crop\"},\"small\":{\"w\":680,\"h\":529,\"resize\":\"fit\"},\"large\":{\"w\":1218,\"h\":948,\"resize\":\"fit\"}}}]},\"extended_entities\":{\"media\":[{\"id\":1108819801906905090,\"id_str\":\"1108819801906905090\",\"indices\":[173,196],\"media_url\":\"http:\\/\\/pbs.twimg.com\\/media\\/D2NRvCyUgAIDxvi.jpg\",\"media_url_https\":\"https:\\/\\/pbs.twimg.com\\/media\\/D2NRvCyUgAIDxvi.jpg\",\"url\":\"https:\\/\\/t.co\\/rQuixmVURo\",\"display_url\":\"pic.twitter.com\\/rQuixmVURo\",\"expanded_url\":\"https:\\/\\/twitter.com\\/RobertKlemko\\/status\\/1108819810547191809\\/photo\\/1\",\"type\":\"photo\",\"sizes\":{\"medium\":{\"w\":1200,\"h\":934,\"resize\":\"fit\"},\"thumb\":{\"w\":150,\"h\":150,\"resize\":\"crop\"},\"small\":{\"w\":680,\"h\":529,\"resize\":\"fit\"},\"large\":{\"w\":1218,\"h\":948,\"resize\":\"fit\"}}}]}},\"quote_count\":739,\"reply_count\":284,\"retweet_count\":5215,\"favorite_count\":11684,\"entities\":{\"hashtags\":[],\"urls\":[{\"url\":\"https:\\/\\/t.co\\/5n2ufgF2gR\",\"expanded_url\":\"https:\\/\\/twitter.com\\/i\\/web\\/status\\/1108819810547191809\",\"display_url\":\"twitter.com\\/i\\/web\\/status\\/1\\u2026\",\"indices\":[117,140]}],\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"en\"},\"is_quote_status\":false,\"quote_count\":0,\"reply_count\":0,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"urls\":[],\"user_mentions\":[{\"screen_name\":\"RobertKlemko\",\"name\":\"Robert Klemko\",\"id\":218129328,\"id_str\":\"218129328\",\"indices\":[3,16]}],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\":\"1553261159470\"}"
  //    //      val tweet = new twitter4j.JSONObject(json)
  //    //      println(tweet.getString("created_at"))
  //
  //    val json2 = "{\"created_at\":\"Sat Mar 23 07:22:26 +0000 2019\",\"id\":1109354704180518912,\"id_str\":\"1109354704180518912\",\"text\":\"** Have your say on the future of North Walls rec. **\\n\\nThe council are at the rec gathering opinions at a one-off e\\u2026 https:\\/\\/t.co\\/sUNgIumkrO\",\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/iphone\\\" rel=\\\"nofollow\\\"\\u003eTwitter for iPhone\\u003c\\/a\\u003e\",\"truncated\":true,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":1478103278,\"id_str\":\"1478103278\",\"name\":\"Winchester parkrun\",\"screen_name\":\"wchesterparkrun\",\"location\":\"North Walls Recreation Ground\",\"url\":\"http:\\/\\/www.parkrun.org.uk\\/winchester\",\"description\":\"Free 5km run every Saturday at 9am at\",\"translator_type\":\"none\",\"protected\":false,\"verified\":false,\"followers_count\":951,\"friends_count\":65,\"listed_count\":17,\"favourites_count\":420,\"statuses_count\":704,\"created_at\":\"Sun Jun 02 20:06:20 +0000 2013\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":false,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"C1C944\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"B5803D\",\"profile_sidebar_border_color\":\"FFFFFF\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/938762449297141760\\/OexakSP0_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/938762449297141760\\/OexakSP0_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/1478103278\\/1479767388\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"is_quote_status\":false,\"extended_tweet\":{\"full_text\":\"** Have your say on the future of North Walls rec. **\\n\\nThe council are at the rec gathering opinions at a one-off event this morning. Please do pop in and have your say.\\n\\nSome ideas:\\n\\n- an all-weather path\\n- clean, modern toilets \\n- access to car parking\\n- the Pavilion Project\",\"display_text_range\":[0,277],\"entities\":{\"hashtags\":[],\"urls\":[],\"user_mentions\":[],\"symbols\":[]}},\"quote_count\":0,\"reply_count\":0,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"urls\":[{\"url\":\"https:\\/\\/t.co\\/sUNgIumkrO\",\"expanded_url\":\"https:\\/\\/twitter.com\\/i\\/web\\/status\\/1109354704180518912\",\"display_url\":\"twitter.com\\/i\\/web\\/status\\/1\\u2026\",\"indices\":[117,140]}],\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\":\"1553325746947\"}"
  //    val tmp = TwitterObjectFactory.createStatus(json2)
  //    println(tmp.getLang)
  //    println(tmp.getUser.getLang)
  //  }

  @transient
  lazy val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    //    val ssc = StreamingContext.getActiveOrCreate(createSparkStreamingContext)
    val ssc = createSparkStreamingContext
    val simpleDateFormat = new SimpleDateFormat("EE MMM dd HH:mm:ss ZZ yyyy")

    log.info("TweetSentimentAnalyzer start ")

    // 广播 Kafka Producer 到每一个excutors
    val kafkaProducer: Broadcast[BroadcastKafkaProducer[String, String]] = {
      val kafkaProducerConfig = {
        val prop = new Properties()
        prop.put("group.id", PropertiesLoader.groupIdProducer)
        prop.put("acks", "all")
        prop.put("retries ", "1")
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesLoader.bootstrapServersProducer)
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName) //key的序列化;
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        prop
      }
      ssc.sparkContext.broadcast(broadcastKafkaProducer[String, String](kafkaProducerConfig))
    }

    // Load Naive Bayes Model from the location specified in the config file.
    val naiveBayesModel = NaiveBayesModel.load(ssc.sparkContext, PropertiesLoader.naiveBayesModelPath)
    val stopWordsList = ssc.sparkContext.broadcast(StopWordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))

    /**
      * Predicts the sentiment of the tweet passed.
      * Invokes Stanford Core NLP and MLlib methods for identifying the tweet sentiment.
      *
      * @param status -- twitter4j.Status object.
      * @return tuple with Tweet ID, Tweet Text, Core NLP Polarity, MLlib Polarity, Latitude, Longitude,
      *         Profile Image URL, Tweet Date.
      */
    def predictSentiment(status: String): (String, Int, Int) = {
      val tweetText = replaceNewLines(status)
      
      log.info("tweetText : " + tweetText)

      val (corenlpSentiment, mllibSentiment) =
        (0, MLlibSentimentAnalyzer.computeSentiment(tweetText, stopWordsList, naiveBayesModel))
      //      CoreNLPSentimentAnalyzer.computeWeightedSentiment(tweetText),

      (//status.getId,
       // status.getUser.getScreenName,
        tweetText,
        corenlpSentiment,
        mllibSentiment)
       // status.getGeoLocation.getLatitude,
       // status.getGeoLocation.getLongitude,
       // status.getUser.getOriginalProfileImageURL,
       // simpleDateFormat.format(status.getCreatedAt))
    }


    // 直接读取Twitter API 数据
    //    val oAuth: Some[OAuthAuthorization] = OAuthUtils.bootstrapTwitterOAuth()
    //    val rawTweets = TwitterUtils.createStream(ssc, oAuth)


    // kafka consumer 参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertiesLoader.bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> PropertiesLoader.groupId,
      "auto.offset.reset" -> PropertiesLoader.autoOffsetReset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // topics
    val topics = PropertiesLoader.topics.split(",").toSet

    // 读取 Kafka 数据---json 格式字符串
    val rawTweets = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    // 保存Tweet 元数据
    //    if (PropertiesLoader.saveRawTweets) {
    //      rawTweets.cache()
    //      rawTweets.foreachRDD { rdd =>
    //        if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
    //          saveRawTweetsInJSONFormat(rdd, PropertiesLoader.tweetsRawPath)
    //        }
    //      }
    //    }

    val classifiedTweets = rawTweets
      .filter(line => !line.value().isEmpty)
      //.map(line=>line.value())
      //.filter({
      //  case null =>
      //     false
      //  case _ =>
      //     true
      //})
        .map(line=>predictSentiment(line.value()))

    // 分隔符 was chosen as the probability of this character appearing in tweets is very less.
    val DELIMITER = "¦"
    val tweetsClassifiedPath = PropertiesLoader.tweetsClassifiedPath

    classifiedTweets.foreachRDD { rdd =>
      try {
        if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
          // saveClassifiedTweets(rdd, tweetsClassifiedPath)

          // produce message to kafka
          rdd.foreach(message => {
            log.info("producer msg to kafka : " + message)
            // id, screenName, text, sent1, sent2, lat, long, profileURL, date
            kafkaProducer.value.send(PropertiesLoader.topicProducer, message.productIterator.mkString(DELIMITER))
          })
        } else {
          log.warn("classifiedTweets rdd is null")
        }
      } catch {
        case e: TwitterException =>
          log.error("classifiedTweets TwitterException")
          log.error(e)
        case e: Exception =>
          log.error(e)
      }
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(PropertiesLoader.totalRunTimeInMinutes * 60 * 1000) // auto-kill after processing rawTweets for n mins.
  }

  /**
    * Create StreamingContext.
    * Future extension: enable checkpointing to HDFS [is it really required??].
    *
    * @return StreamingContext
    */
  def createSparkStreamingContext(): StreamingContext = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      // Use KryoSerializer for serializing objects as JavaSerializer is too slow.
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      // For reconstructing the Web UI after the application has finished.
      .set("spark.eventLog.enabled", "true")
      // Reduce the RDD memory usage of Spark and improving GC behavior.
      .set("spark.streaming.unpersist", "true")

    val ssc = new StreamingContext(conf, Durations.seconds(PropertiesLoader.microBatchTimeInSeconds))
    ssc
  }

  /**
    * Saves the classified tweets to the csv file.
    * Uses DataFrames to accomplish this task.
    *
    * @param rdd                  tuple with Tweet ID, Tweet Text, Core NLP Polarity, MLlib Polarity,
    *                             Latitude, Longitude, Profile Image URL, Tweet Date.
    * @param tweetsClassifiedPath Location of saving the data.
    */
  def saveClassifiedTweets(rdd: RDD[(Long, String, String, Int, Int, Double, Double, String, String)], tweetsClassifiedPath: String) = {
    val now = "%tY%<tm%<td%<tH%<tM%<tS" format new Date
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

    import sqlContext.implicits._

    val classifiedTweetsDF = rdd.toDF("ID", "ScreenName", "Text", "CoreNLP", "MLlib", "Latitude", "Longitude", "ProfileURL", "Date")
    classifiedTweetsDF.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      // Compression codec to compress when saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      // Will it be good if DF is partitioned by Sentiment value? Probably does not make sense.
      //.partitionBy("Sentiment")
      // TODO :: Bug in spark-csv package :: Append Mode does not work for CSV: https://github.com/databricks/spark-csv/issues/122
      //.mode(SaveMode.Append)
      .save(tweetsClassifiedPath + now)
  }

  /**
    * Jackson Object Mapper for mapping twitter4j.Status object to a String for saving raw tweet.
    */
  val jacksonObjectMapper: ObjectMapper = new ObjectMapper()

  /**
    * Saves raw tweets received from Twitter Streaming API in
    *
    * @param rdd           -- RDD of Status objects to save.
    * @param tweetsRawPath -- Path of the folder where raw tweets are saved.
    */
  def saveRawTweetsInJSONFormat(rdd: RDD[Status], tweetsRawPath: String): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    val tweet = rdd.map(status => jacksonObjectMapper.writeValueAsString(status))
    val rawTweetsDF = sqlContext.read.json(tweet)
    rawTweetsDF.coalesce(1).write
      .format("org.apache.spark.sql.json")
      // Compression codec to compress when saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      .mode(SaveMode.Append)
      .save(tweetsRawPath)
  }

  /**
    * Removes all new lines from the text passed.
    *
    * @param tweetText -- Complete text of a tweet.
    * @return String without new lines.
    */
  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }

  /**
    * Checks if the tweet Status is in English language.
    * Actually uses profile's language as well as the Twitter ML predicted language to be sure that this tweet is
    * indeed English.
    *
    * @param status twitter4j Status object
    * @return Boolean status of tweet in English or not.
    */
  def isTweetInEnglish(status: Status): Boolean = {
    status.getLang == "en" && status.getUser.getLang == "en"
  }

  /**
    * Checks if the tweet Status has Geo-Coordinates.
    *
    * @param status twitter4j Status object
    * @return Boolean status of presence of Geolocation of the tweet.
    */
  def hasGeoLocation(status: Status): Boolean = {
    null != status.getGeoLocation
  }
}
