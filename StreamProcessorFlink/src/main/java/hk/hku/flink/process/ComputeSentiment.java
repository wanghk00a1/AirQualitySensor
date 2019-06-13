package hk.hku.flink.process;

import hk.hku.flink.corenlp.CoreNLPSentimentAnalyzer;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

/**
 * @author: LexKaing
 * @create: 2019-06-13 11:43
 * @description:
 **/
public class ComputeSentiment implements MapFunction<Status, String> {

    private static final Logger logger = LoggerFactory.getLogger(ComputeSentiment.class);

    @Override
    public String map(Status tweet) {
        String text = tweet.getText().replaceAll("\n","");

        //使用nlp 处理
        int corenlpSentiment = CoreNLPSentimentAnalyzer.getInstance().computeWeightedSentiment(text);

        String DELIMITER = "¦";

        // id ¦ text ¦ name ¦ corenlp
        // ¦ city
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer
                .append(tweet.getId()).append(DELIMITER)
                .append(text).append(DELIMITER)
                .append(tweet.getUser().getScreenName()).append(DELIMITER)
                .append(corenlpSentiment).append(DELIMITER);

        return stringBuffer.toString();
    }
}