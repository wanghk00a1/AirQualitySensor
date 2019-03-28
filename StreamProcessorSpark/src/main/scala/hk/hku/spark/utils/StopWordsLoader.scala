package hk.hku.spark.utils

import scala.io.Source

/**
  * Helper class for loading stop words from a file ["NLTK_English_Stopwords_Corpus.txt"] from the classpath.
  */
object StopWordsLoader {

  def loadStopWords(stopWordsFileName: String): List[String] = {
    Source.fromInputStream(getClass.getResourceAsStream("/" + stopWordsFileName)).getLines().toList
  }
}