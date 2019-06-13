package hk.hku.flink.process;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**
 * @author: LexKaing
 * @create: 2019-06-13 11:35
 * @description:
 **/
public class ParseTwitter implements MapFunction<String, Status> {

    private static final Logger logger = LoggerFactory.getLogger(ParseTwitter.class);

    @Override
    public Status map(String value) {
        Status status = null;
        try {
            status = TwitterObjectFactory.createStatus(value);
        } catch (TwitterException e) {
            logger.error("TwitterException : ", e);
        }
        return status;
    }
}