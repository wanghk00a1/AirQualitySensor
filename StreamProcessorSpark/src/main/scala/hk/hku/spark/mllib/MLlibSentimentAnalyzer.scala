package hk.hku.spark.mllib

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg

object MLlibSentimentAnalyzer {

  /**
    * Predicts sentiment of the tweet text with Naive Bayes model passed after removing the stop words.
    *
    * @param text          -- Complete text of a tweet.
    * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
    * @param model         -- Naive Bayes Model of the trained data.
    * @return Int Sentiment of the tweet.
    */
  def computeSentiment(text: String, stopWordsList: Broadcast[List[String]], model: NaiveBayesModel): Int = {
    val tweetInWords: Seq[String] = getBarebonesTweetText(text, stopWordsList.value)
    val polarity = model.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords))
    normalizeMLlibSentiment(polarity)
  }

  /**
    * Normalize sentiment for visualization perspective.
    * We are normalizing sentiment as we need to be consistent with the polarity value with Core NLP and for visualization.
    *
    * @param sentiment polarity of the tweet
    * @return normalized to either -1, 0 or 1 based on tweet being negative, neutral and positive.
    *         标准化 polarity(极性) 情感。
    */
  def normalizeMLlibSentiment(sentiment: Double) = {
    sentiment match {
      case x if x == 1 => 1 // negative
      case x if x == 2 => 2 // neutral
      case x if x == 3 => 3 // positive
      case _ => 0 // if cant figure the sentiment, term it as neutral
    }
  }

  /**
    * Strips the extra characters in tweets. And also removes stop words from the tweet text.
    *
    * @param tweetText     -- Complete text of a tweet.
    * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
    * @return Seq[String] after removing additional characters and stop words from the tweet.
    *         过滤去tweet 文本中不需要的words 和stop words，返回Seq[String] 单词序列
    */
  def getBarebonesTweetText(tweetText: String, stopWordsList: List[String]): Seq[String] = {
    //Remove URLs, RT, MT and other redundant chars / strings from the tweets.
    tweetText.toLowerCase()
      .replaceAll("\n", "")
      .replaceAll("rt\\s+", "")
      .replaceAll("\\s+@\\w+", "")
      .replaceAll("@\\w+", "")
      .replaceAll("\\s+#\\w+", "")
      .replaceAll("#\\w+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+", "")
      .split(" ")
      //.filter(_.matches("^[a-zA-Z]+$"))
      //.filter(!stopWordsList.contains(_))
    //.fold("")((a,b) => a.trim + " " + b.trim).trim
  }

  val hashingTF = new HashingTF()

  /**
    * Transforms features to Vectors.
    *
    * @param tweetText -- Complete text of a tweet.
    * @return Vector
    */
  def transformFeatures(tweetText: Seq[String]): linalg.Vector = {
    hashingTF.transform(tweetText)
  }

  def main(args: Array[String]): Unit = {
    val tweetText = "今天 天气 真 好 啊！"
    println(tweetText)

    val result = tweetText.toLowerCase()
      .replaceAll("\n", "")
      .replaceAll("rt\\s+", "")
      .replaceAll("\\s+@\\w+", "")
      .replaceAll("@\\w+", "")
      .replaceAll("\\s+#\\w+", "")
      .replaceAll("#\\w+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+", "")
      .split(" ")
      //.filter(_.matches("^[a-zA-Z]+$"))

    println(result.mkString(","))

  }
}
