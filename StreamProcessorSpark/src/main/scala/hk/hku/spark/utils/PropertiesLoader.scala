package hk.hku.spark.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Exposes all the key-value pairs as properties object using Config object of Typesafe Config project.
  */
object PropertiesLoader {
  private val conf: Config = ConfigFactory.load("application.conf")

  // streaming 周期
  val microBatchTimeInSeconds = conf.getInt("STREAMING_MICRO_BATCH_TIME_IN_SECONDS")
  val totalRunTimeInMinutes = conf.getInt("TOTAL_RUN_TIME_IN_MINUTES")

  // naive bayes 参数
  val sentiment140TrainingFilePath = conf.getString("SENTIMENT140_TRAIN_DATA_ABSOLUTE_PATH")
  val sentiment140TestingFilePath = conf.getString("SENTIMENT140_TEST_DATA_ABSOLUTE_PATH")
  val nltkStopWords = conf.getString("NLTK_STOPWORDS_FILE_NAME ")
  val naiveBayesModelPath = conf.getString("NAIVEBAYES_MODEL_ABSOLUTE_PATH")
  val modelAccuracyPath = conf.getString("NAIVEBAYES_MODEL_ACCURACY_ABSOLUTE_PATH ")

  // core nlp 的模型都在maven依赖里，暂无需要配置的参数

  // consume kafka stream 数据
  val bootstrapServers = conf.getString("BOOTSTRAP_SERVER")
  val groupId = conf.getString("GROUP_ID")
  val autoOffsetReset = conf.getString("AUTO_OFFSET_RESET")
  val topics = conf.getString("KAFKA_TOPIC_CONSUMER")

  // produce kafka 吐结果数据
  val bootstrapServersProducer = conf.getString("BOOTSTRAP_SERVER_PRODUCER")
  val groupIdProducer = conf.getString("GROUP_ID_PRODUCER")
  val topicProducer = conf.getString("KAFKA_TOPICS_PRODUCER")

  def main(args: Array[String]): Unit = {
    println(PropertiesLoader.topicProducer)
  }
}
