package hk.hku.spark.mllib

import hk.hku.spark.utils._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

/**
  * Creates a Model of the training dataset using Spark MLlib's Naive Bayes classifier.
  */
// spark-submit --class "hk.hku.spark.mllib.SparkNaiveBayesModelCreator" --master local[3] /opt/spark-twitter/7305CloudProject/StreamProcessorSpark/target/StreamProcessorSpark-jar-with-dependencies.jar
object SparkNaiveBayesModelCreator {
  val log = LogManager.getRootLogger

  def main(args: Array[String]) {
    val sc = createSparkContext()

    log.setLevel(Level.INFO)
    log.info("SparkNaiveBayesModelCreator start ")
    //    LogUtils.setLogLevels(sc)

    val stopWordsList = sc.broadcast(StopWordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))
    createAndSaveNBModel(sc, stopWordsList)
    validateAccuracyOfNBModel(sc, stopWordsList)
  }

  /**
    * Remove new line characters.
    *
    * @param tweetText -- Complete text of a tweet.
    * @return String with new lines removed.
    */
  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }

  /**
    * Create SparkContext.
    * Future extension: enable checkpointing to HDFS [is it really reqd??].
    *
    * @return SparkContext
    */
  def createSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  /**
    * Creates a Naive Bayes Model of Tweet and its Sentiment from the Sentiment140 file.
    * 构建Bayes模型
    *
    * @param sc            -- Spark Context.
    * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
    */
  def createAndSaveNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    //    使用spark sql 加载文本数据
    val tweetsDF: DataFrame = loadSentiment140File(sc, PropertiesLoader.sentiment140TrainingFilePath)

    log.info("createAndSaveNBModel loadSentiment140File")
    //log.info("-------------------------" + tweetsDF.count())

    val labeledRDD = tweetsDF.select("label", "text").rdd.map {
    //val labeledRDD = tweetsDF.rdd.map {
      //line =>
        //log.info("++++++++" + line)
      //case Row(tweet: String, polarity: Int) =>
      case Row(polarity: Int, tweet: String) =>
        //log.info("++++++++" + tweet + " , " + polarity)
        var stop = new ListBuffer[String]()
        stop += "123123"
        val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweet, stopWordsList.value)
        // 将tweet 过滤后的文本String 序列，打上polarity 标签值
         LabeledPoint(polarity, MLlibSentimentAnalyzer.transformFeatures(tweetInWords))
      //case _  =>
        //log.info("__________")
    }
    //labeledRDD.collect()
    labeledRDD.cache()

    val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledRDD, lambda = 1.0, modelType = "multinomial")
    naiveBayesModel.save(sc, PropertiesLoader.naiveBayesModelPath)
  }

  /**
    * Validates and check the accuracy of the model by comparing the polarity of a tweet from the dataset and compares it with the MLlib predicted polarity.
    * 测试模型准确性
    *
    * @param sc            -- Spark Context.
    * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
    */
  def validateAccuracyOfNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(sc, PropertiesLoader.naiveBayesModelPath)

    val tweetsDF: DataFrame = loadSentiment140File(sc, PropertiesLoader.sentiment140TestingFilePath)
    val actualVsPredictionRDD = tweetsDF.select("label", "text").rdd.map {
      case Row(polarity: Int, tweet: String) =>
        val tweetText = replaceNewLines(tweet)
        val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweetText, stopWordsList.value)
        (polarity.toDouble,
          naiveBayesModel.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords)),
          tweetText)
    }
    actualVsPredictionRDD
      .map(x => {log.info("validateAccuracyOfNBModel : "+x._1+" , "+x._2+" , "+x._3)})
    val accuracy = 100.0 * actualVsPredictionRDD.filter(x => x._1 == x._2).count() / tweetsDF.count()
    /*actualVsPredictionRDD.cache()
    val predictedCorrect = actualVsPredictionRDD.filter(x => x._1 == x._2).count()
    val predictedInCorrect = actualVsPredictionRDD.filter(x => x._1 != x._2).count()
    val accuracy = 100.0 * predictedCorrect.toDouble / (predictedCorrect + predictedInCorrect).toDouble*/
    log.info(f"""\n\t<==******** Prediction accuracy compared to actual: $accuracy%.2f%% ********==>\n""")
    saveAccuracy(sc, actualVsPredictionRDD)
    saveAccuracyRate(sc, accuracy)
  }

  /**
    * Loads the Sentiment140 file from the specified path using SparkContext.
    *
    * @param sc                   -- Spark Context.
    * @param sentiment140FilePath -- Absolute file path of Sentiment140.
    * @return -- Spark DataFrame of the Sentiment file with the tweet text and its polarity.
    */
  def loadSentiment140File(sc: SparkContext, sentiment140FilePath: String): DataFrame = {
    val sqlContext = SQLContextSingleton.getInstance(sc)
    // training data 格式:polarity(情绪标签),id,date,query,user,status(tweet元数据)
    val tweetsDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(sentiment140FilePath)
      .toDF("id", "create_at", "label", "text")
//      .toDF("polarity", "id", "date", "query", "user", "status")


    // Drop the columns we are not interested in.
//    tweetsDF.drop("id").drop("date").drop("query").drop("user")
    tweetsDF.drop("id").drop("create_at")
  }

  /**
    * Saves the accuracy computation of the ML library.
    * The columns are actual polarity as per the dataset, computed polarity with MLlib and the tweet text.
    *
    * @param sc                    -- Spark Context.
    * @param actualVsPredictionRDD -- RDD of polarity of a tweet in dataset and MLlib computed polarity.
    */
  def saveAccuracy(sc: SparkContext, actualVsPredictionRDD: RDD[(Double, Double, String)]): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(sc)
    import sqlContext.implicits._
    val actualVsPredictionDF = actualVsPredictionRDD.toDF("Actual", "Predicted", "Text")
    actualVsPredictionDF.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      // Compression codec to compress while saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      .mode(SaveMode.Append)
      .save(PropertiesLoader.modelAccuracyPath)
  }


  /**
    * 记录validate 的准确性数值
    */
  def saveAccuracyRate(sc: SparkContext, accuracy: Double): Unit = {
    val content = DateTime.now() + "\n<==******** Prediction accuracy compared to actual: " + accuracy + " ********==>\n"
    HDFSUtils.deleteHDFSFile("/tweets_sentiment/accuracy_rate/result.txt")
    HDFSUtils.createNewHDFSFile("/tweets_sentiment/accuracy_rate/result.txt", content)
  }

}
