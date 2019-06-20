package hk.hku.flink;

import hk.hku.flink.corenlp.CoreNLPSentimentAnalyzer;
import hk.hku.flink.corenlp.LanguageAnalyzer;
import hk.hku.flink.utils.PropertiesLoader;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.util.*;

import static hk.hku.flink.utils.Constants.COMMA;
import static hk.hku.flink.utils.Constants.DELIMITER;

/**
 * @author: LexKaing
 * @create: 2019-06-12 17:40
 * @description:
 **/
public class TweetFlinkAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(TweetFlinkAnalyzer.class);

    public static void main(String[] args) {
        logger.info("Tweets Flink Analyzer job start");
        TweetFlinkAnalyzer tweetFlinkAnalyzer = new TweetFlinkAnalyzer();
        tweetFlinkAnalyzer.startJob();
    }

    private void startJob() {
        // 创建运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 启用容错 checkpoint every 5000 msecs
        env.enableCheckpointing(5000);

        // 初始化 Consumer 配置
        Properties propConsumer = new Properties();
        propConsumer.setProperty("bootstrap.servers", PropertiesLoader.bootstrapServers);
        propConsumer.setProperty("group.id", PropertiesLoader.groupId);
        propConsumer.setProperty("auto.offset.reset", PropertiesLoader.autoOffsetReset);

        // 初始化 Producer 配置
        Properties propProducer = new Properties();
        propProducer.setProperty("bootstrap.servers", PropertiesLoader.bootstrapServersProducer);
        propProducer.setProperty("group.id", PropertiesLoader.groupIdProducer);

        logger.info("Data Stream init");

        // kafka input data
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>(PropertiesLoader.topicConsumer, new SimpleStringSchema(), propConsumer))
                .name("Kafka Consumer");

        // parse tweets : id¦text¦username¦geo¦language¦weather
        DataStream<String> parsedTwitterStream = stream
                .filter(line -> line.length() > 0)
                .map(line -> {
                    try {
                        Status status = TwitterObjectFactory.createStatus(line);
                        if (status.getText().length() > 0) {
                            String text = status.getText().replaceAll("\n", "");

                            StringBuffer stringBuffer = new StringBuffer();
                            stringBuffer.append(status.getId()).append(DELIMITER)
                                    .append(text).append(DELIMITER)
                                    .append(status.getUser().getScreenName()).append(DELIMITER);

                            // 加上 geo info
                            if (status.getGeoLocation() != null)
                                stringBuffer
                                        .append(status.getGeoLocation().getLatitude()).append(COMMA)
                                        .append(status.getGeoLocation().getLongitude());
                            else
                                stringBuffer.append(status.getPlace().getFullName());

                            // 加上 language
                            String language = LanguageAnalyzer.getInstance().detectLanguage(text);
                            stringBuffer.append(DELIMITER).append(language);

                            // 加上 weather keywords related
                            // Arrays.stream({"1","2"}).filter(word -> text.contains(word)).count();
                            Boolean weatherRelated = false;
                            for (String word : PropertiesLoader.weatherKeywords.split(COMMA)) {
                                if (text.contains(word)) {
                                    weatherRelated = true;
                                    break;
                                }
                            }
                            stringBuffer.append(DELIMITER).append(weatherRelated);

                            return stringBuffer.toString();
                        }
                    } catch (TwitterException e) {
                        logger.error("Twitter Parse Exception : ", e);
                    }
                    return null;
                })
                .name("Parsed Tweets Stream");


        // core nlp analysis sentiment
        DataStream sentimentStream = parsedTwitterStream
                .filter(line -> line != null && line.split(DELIMITER)[4].equals("en"))
                .map(tweet -> {
                    String[] text = tweet.split(DELIMITER);
                    //使用nlp 计算 sentiment
                    int corenlpSentiment = CoreNLPSentimentAnalyzer.getInstance().computeWeightedSentiment(text[1]);

                    // tweet + '¦ corenlp'
                    StringBuffer stringBuffer = new StringBuffer();
                    stringBuffer.append(tweet).append(DELIMITER)
                            .append(corenlpSentiment);

                    logger.info("Sentiment Stream : " + stringBuffer.toString());
                    return stringBuffer.toString();
                })
                .name("Sentiment Stream");


        // output tweets process info
        sentimentStream.addSink(new FlinkKafkaProducer<>(PropertiesLoader.topicProducer, new SimpleStringSchema(), propProducer))
                .name("Tweets Sentiment Result Sink");

        // 3min 统计一次


        try {
            env.execute("COMP7705 Flink Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}