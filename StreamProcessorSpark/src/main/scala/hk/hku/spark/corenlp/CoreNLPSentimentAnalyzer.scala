package hk.hku.spark.corenlp

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object CoreNLPSentimentAnalyzer {

  lazy val pipeline = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    new StanfordCoreNLP(props)
  }

  def computeSentiment(text: String): Int = {
    // 取最长的语句，作为整体文本的情绪基调。
    val (_, sentiment) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentiment
  }

  /**
    * Normalize sentiment for visualization perspective.
    * We are normalizing sentiment as we need to be consistent with the polarity value with MLlib and for visualization.
    *
    * @param sentiment polarity of the tweet
    * @return normalized to either -1, 0 or 1 based on tweet being negative, neutral and positive.
    */
  def normalizeCoreNLPSentiment(sentiment: Double): Int = {
    sentiment match {
      case s if s <= 0.0 => 0 // neutral
      case s if s < 2.0 => -1 // negative
      case s if s < 3.0 => 0 // neutral
      case s if s < 5.0 => 1 // positive
      case _ => 0 // if we cant find the sentiment, we will deem it as neutral.
    }
  }

  /**
    * 将tweet 文本扩展为 List[(细分的语句:String, 通过RNN计算并标准化后的情绪值:Int)]
    *
    * @param text
    * @return
    */
  def extractSentiments(text: String): List[(String, Int)] = {
    val textSentiment = ListBuffer.empty[(String, Int)]

    // create blank annotator
    val annotation: Annotation = new Annotation(text)

    // run all Annotator - Tokenizer on this text
    pipeline.annotate(annotation)

    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala.toList
    sentences.foreach(sentence => {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      val value = normalizeCoreNLPSentiment(RNNCoreAnnotations.getPredictedClass(tree))
      textSentiment += ((sentence.toString, value))
    })
    textSentiment.toList

    // 原来的代码逻辑一致，import的包更新了
    //    val annotation: Annotation = pipeline.process(text)
    //    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    //    sentences
    //      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
    //      .map { case (sentence, tree) => (sentence.toString, normalizeCoreNLPSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
    //      .toList
  }

  /**
    * 和extractSentiments 类似，但计算情感权重值方法不同
    * 都先通过Annotation 将text 分成多段语句，计算单个语句的sentiment
    * extractSentiments : 整体情感基调 = 最长的语句的 sentiment
    * computeWeightedSentiment : 整体情感基调 = sum(单个语句的情绪值 * 语句的长度) / sum(单个语句的长度)
    *
    * @param tweet
    * @return
    */
  def computeWeightedSentiment(tweet: String): Int = {

    val annotation = pipeline.process(tweet)
    val sentiments: ListBuffer[Double] = ListBuffer()
    val sizes: ListBuffer[Int] = ListBuffer()

    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala.toList
    sentences.foreach(sentence => {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)

      sentiments += sentiment.toDouble
      sizes += sentence.toString.length
    })

    //    源代码有奇妙的编译bug?
    //    for (sentence: CoreMap <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
    //      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
    //      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
    //
    //      sentiments += sentiment.toDouble
    //      sizes += sentence.toString.length
    //    }

    val weightedSentiment = if (sentiments.isEmpty) {
      -1
    } else {
      val weightedSentiments: ListBuffer[Double] = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
      weightedSentiments.sum / sizes.sum
    }

    normalizeCoreNLPSentiment(weightedSentiment)
  }
}