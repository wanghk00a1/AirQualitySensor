package hk.hku.spark.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Exposes all the key-value pairs as properties object using Config object of Typesafe Config project.
  */
object PropertiesLoader {
  private val conf: Config = ConfigFactory.load("application.conf")

  val sentiment140TrainingFilePath = conf.getString("SENTIMENT140_TRAIN_DATA_ABSOLUTE_PATH")
  val sentiment140TestingFilePath = conf.getString("SENTIMENT140_TEST_DATA_ABSOLUTE_PATH")
  val nltkStopWords = conf.getString("NLTK_STOPWORDS_FILE_NAME ")

  val naiveBayesModelPath = conf.getString("NAIVEBAYES_MODEL_ABSOLUTE_PATH")
  val modelAccuracyPath = conf.getString("NAIVEBAYES_MODEL_ACCURACY_ABSOLUTE_PATH ")

  val tweetsRawPath = conf.getString("TWEETS_RAW_ABSOLUTE_PATH")
  val saveRawTweets = conf.getBoolean("SAVE_RAW_TWEETS")

  val tweetsClassifiedPath = conf.getString("TWEETS_CLASSIFIED_ABSOLUTE_PATH")

  val consumerKey = conf.getString("CONSUMER_KEY")
  val consumerSecret = conf.getString("CONSUMER_SECRET")
  val accessToken = conf.getString("ACCESS_TOKEN_KEY")
  val accessTokenSecret = conf.getString("ACCESS_TOKEN_SECRET")

  val microBatchTimeInSeconds = conf.getInt("STREAMING_MICRO_BATCH_TIME_IN_SECONDS")
  val totalRunTimeInMinutes = conf.getInt("TOTAL_RUN_TIME_IN_MINUTES")

  // 读 kafka stream 数据
  val bootstrapServers = conf.getString("BOOTSTRAP_SERVER")
  val groupId = conf.getString("GROUP_ID")
  val autoOffsetReset = conf.getString("AUTO_OFFSET_RESET")
  val topics = conf.getString("KAFKA_TOPICS")

  // 往kafka 吐结果数据
  val bootstrapServersProducer = conf.getString("BOOTSTRAP_SERVER_PRODUCER")
  val groupIdProducer = conf.getString("GROUP_ID_PRODUCER")
  val topicProducer = conf.getString("KAFKA_TOPICS_PRODUCER")

  def main(args: Array[String]): Unit = {
    println(PropertiesLoader.saveRawTweets)
  }
}