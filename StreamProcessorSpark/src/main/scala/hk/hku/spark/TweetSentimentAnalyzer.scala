package hk.hku.spark

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import hk.hku.spark.corenlp.CoreNLPSentimentAnalyzer
import hk.hku.spark.mllib.MLlibSentimentAnalyzer
import hk.hku.spark.utils._
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}
import twitter4j.{Status, TwitterException, TwitterObjectFactory}

/**
  * Spark Streaming 读取 kafka twitter 数据
  * 使用 spark mllib 进行分析
  * 包括 Navie Bayes、 CoreNLP 分析结果
  * 结果数据吐入 kafka
  */
// spark-submit --class "hk.hku.spark.TweetSentimentAnalyzer" --master local[3] StreamProcessorSpark-jar-with-dependencies.jar
object TweetSentimentAnalyzer {

  @transient
  lazy val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    val ssc = createSparkStreamingContext
    val simpleDateFormat = new SimpleDateFormat("EE MMM dd HH:mm:ss ZZ yyyy")

    log.info("MessageSentimentAnalyzer start ")

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
      ssc.sparkContext.broadcast(BroadcastKafkaProducer[String, String](kafkaProducerConfig))
    }

    // Load Naive Bayes Model from the location specified in the config file.
    val naiveBayesModel = NaiveBayesModel.load(ssc.sparkContext, PropertiesLoader.naiveBayesModelPath)
    val stopWordsList = ssc.sparkContext.broadcast(StopWordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))

    /**
      * Predicts the sentiment of the tweet passed.
      * Invokes Stanford Core NLP and MLlib methods for identifying the tweet sentiment.
      *
      * @param status -- twitter4j.Status object.
      * @return tuple with Tweet ID, Screen Name, Tweet Text,
      *         Core NLP Polarity, MLlib Polarity, Latitude, Longitude, Profile Image URL, Tweet Date.
      */
    def predictSentiment(status: Status): (Long, String, String, Int, Int) = {
      val tweetText = status.getText.replaceAll("\n", "")

      val (corenlpSentiment, mllibSentiment) = (
        CoreNLPSentimentAnalyzer.computeWeightedSentiment(tweetText),
        MLlibSentimentAnalyzer.computeSentiment(tweetText, stopWordsList, naiveBayesModel)
      )

      (status.getId,
        status.getUser.getScreenName,
        tweetText,
        corenlpSentiment,
        mllibSentiment)
      //          status.getGeoLocation.getLatitude,
      //          status.getGeoLocation.getLongitude,
      //          status.getUser.getOriginalProfileImageURL)

    }


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

    // 读取 Kafka 数据 (key值是null) : 去除 value 为空的数据
    val rawTweets = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
      .filter(line => !line.value().isEmpty)

    val classifiedTweets = rawTweets.map(line => {
        // 解析 twitter 元数据
        TwitterObjectFactory.createStatus(line.value())
      })
//      .filter(status => {
//        // 去除空数据，去除非英文数据
//        if (status == null || !isTweetInEnglish(status))
//          false
//        else
//          true
//      })
      .map(predictSentiment)

    // 分隔符 was chosen as the probability of this character appearing in tweets is very less.
    val DELIMITER = "¦"

    // 数据格式:id, screenName, text, nlp, mllib, latitude, longitude, profileURL, date
    classifiedTweets.foreachRDD { rdd =>
      try {
        if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
          rdd.foreach(message => {
            // log.info(s"producer msg to kafka ${message.toString()}")
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
    ssc.awaitTermination()
    // auto-kill after processing rawTweets for n mins.
    // ssc.awaitTerminationOrTimeout(PropertiesLoader.totalRunTimeInMinutes * 60 * 1000)
  }

  /**
    * 创建 Spark Streaming Context
    * 暂未启用 Checkpoint
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
