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

/**
  * Spark MLlib 生成 Naive Bayes 训练模型
  *
  */
// spark-submit --class "hk.hku.spark.mllib.SparkNaiveBayesModelCreator" --master local[3] StreamProcessorSpark-jar-with-dependencies.jar
object SparkNaiveBayesModelCreator {
  val log = LogManager.getRootLogger


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("NaiveBayesModelCreator")
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    val sc = SparkContext.getOrCreate(conf)

    log.setLevel(Level.INFO)
    log.info("SparkNaiveBayesModelCreator start ")

    val stopWordsList = sc.broadcast(StopWordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))

    createAndSaveNBModel(sc, stopWordsList)
    validateAccuracyOfNBModel(sc, stopWordsList)
  }


  /**
    * 读取 HDFS 上 sentiment140TrainingFilePath 路径下的训练文件（自己标注polarity），
    * 训练文件
    * 借助 spark sql 转化为 dataframe 并提取需要的列数据 ，
    *
    * Creates a Naive Bayes Model of Tweet and its Sentiment from the Sentiment140 file.
    * 构建Bayes模型
    */
  def createAndSaveNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    //  加载训练数据
    val tweetsDF: DataFrame = loadSentiment140File(sc, PropertiesLoader.sentiment140TrainingFilePath)

    log.info("createAndSaveNBModel loadSentiment140File")

    val labeledRDD = tweetsDF.select("polarity", "status").rdd.map {
      case Row(polarity: Int, tweet: String) => {
        //      text => {
        val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweet, stopWordsList.value)
        // 将tweet 过滤后的文本String 序列，打上polarity 标签值
        LabeledPoint(polarity, MLlibSentimentAnalyzer.transformFeatures(tweetInWords))
      }
    }
    labeledRDD.cache()

    val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledRDD, lambda = 1.0, modelType = "multinomial")
    naiveBayesModel.save(sc, PropertiesLoader.naiveBayesModelPath)
  }

  /**
    * 测试模型准确性
    */
  def validateAccuracyOfNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    // 从 HDFS 加载训练好的模型
    val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(sc, PropertiesLoader.naiveBayesModelPath)

    val tweetsDF: DataFrame = loadSentiment140File(sc, PropertiesLoader.sentiment140TestingFilePath)
    val actualVsPredictionRDD = tweetsDF.select("polarity", "status").rdd.map {
      case Row(polarity: Int, tweet: String) =>
        val tweetText = tweet.replaceAll("\n", "")
        val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweetText, stopWordsList.value)
        (polarity.toDouble,
          naiveBayesModel.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords)),
          tweetText)
    }

    val accuracy = 100.0 * actualVsPredictionRDD.filter(x => x._1 == x._2).count() / tweetsDF.count()

    log.info(f"""\n\t<==******** Prediction accuracy compared to actual: $accuracy%.2f%% ********==>\n""")

    saveAccuracy(sc, actualVsPredictionRDD)
    saveAccuracyRate(sc, accuracy)
  }

  /**
    * Loads the Sentiment140 file from the specified path using SparkContext.
    * 训练文件格式："polarity", "id", "date", "query", "user", "status"
    *
    * @param sc                   -- Spark Context.
    * @param sentiment140FilePath -- 训练文件绝对路径
    * @return -- Spark DataFrame of the Sentiment file with the tweet text and its polarity(标注的极性).
    */
  def loadSentiment140File(sc: SparkContext, sentiment140FilePath: String): DataFrame = {
    val sqlContext = SQLContextSingleton.getInstance(sc)
    // training data 格式:polarity(情绪标签),id,date,query,user,status(tweet元数据)
    val tweetsDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(sentiment140FilePath)
      .toDF("polarity", "id", "date", "query", "user", "status")

    // Drop the columns we are not interested in.
    tweetsDF.drop("id").drop("date").drop("query").drop("user")
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
    * 记录每次 validate 的准确性数值到 HDFS 上
    */
  def saveAccuracyRate(sc: SparkContext, accuracy: Double): Unit = {
    val content = DateTime.now() + "\n<==******** Prediction accuracy compared to actual: " + accuracy + " ********==>\n"
    HDFSUtils.deleteHDFSFile("/tweets_sentiment/accuracy_rate/result.txt")
    HDFSUtils.createNewHDFSFile("/tweets_sentiment/accuracy_rate/result.txt", content)
  }

}
