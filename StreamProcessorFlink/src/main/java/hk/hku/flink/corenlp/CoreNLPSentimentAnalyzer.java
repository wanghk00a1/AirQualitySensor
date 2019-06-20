package hk.hku.flink.corenlp;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * @author: LexKaing
 * @create: 2019-06-13 11:46
 * @description:
 **/
public class CoreNLPSentimentAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(CoreNLPSentimentAnalyzer.class);
    private static CoreNLPSentimentAnalyzer INSTANCE;
    private static String propKey = "annotators";
    private static String propValue = "tokenize, ssplit, pos, lemma, parse, sentiment";

    private static StanfordCoreNLP pipeline;

    private CoreNLPSentimentAnalyzer() {
        Properties props = new Properties();
        props.setProperty(propKey, propValue);
        pipeline = new StanfordCoreNLP(props);
    }


    public static CoreNLPSentimentAnalyzer getInstance(){
        synchronized (CoreNLPSentimentAnalyzer.class)
        {
            if (INSTANCE == null)
            {
                INSTANCE = new CoreNLPSentimentAnalyzer();
            }
            return INSTANCE;
        }
    }


    /**
     * 默认的计算 文本 情绪
     * <p>
     * 整体情感基调 = sum(单个语句的情绪值 * 语句的长度) / sum(语句的长度)
     *
     * @param text
     * @return
     */
    public int computeWeightedSentiment(String text) {
        int sentimentSum = 0;
        int sizeSum = 0;

        Annotation document = pipeline.process(text);
        // these are all the sentences in this document
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            int size = sentence.toString().length();

            // 自定义规则计算文本的总感情值
            sentimentSum += sentiment * size;
            sizeSum += size;
        }

        return sentimentSum == 0 ? -1 : normalizeCoreNLPSentiment(Double.valueOf(sentimentSum) / Double.valueOf(sizeSum));
    }

    private int normalizeCoreNLPSentiment(Double sentiment) {
        if (sentiment <= 0.0) {
            return 0; //neutral
        } else if (sentiment < 2.0) {
            return -1; //negative
        } else if (sentiment < 3.0) {
            return 0; //neutral
        } else if (sentiment < 5.0) {
            return 1; //positive
        } else {
            return 0; //default neutral
        }
    }
}