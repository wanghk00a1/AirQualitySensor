package hk.hku.flink;

import hk.hku.flink.utils.PropertiesLoader;
import org.apache.flink.api.common.functions.MapFunction;
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

import java.util.Properties;

/**
 * @author: LexKaing
 * @create: 2019-06-12 17:40
 * @description:
 **/
public class TweetFlinkAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(TweetFlinkAnalyzer.class);

    public static void main(String[] args) {
        TweetFlinkAnalyzer tweetFlinkAnalyzer = new TweetFlinkAnalyzer();
        tweetFlinkAnalyzer.startJob();
    }

    void startJob() {
        //        val stopWordsList = StopWordsLoader.loadStopWords(PropertiesLoader.nltkStopWords)

        // 创建运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 启用容错 checkpoint every 5000 msecs
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // 初始化 Consumer 配置
        Properties propConsumer = new Properties();
        propConsumer.setProperty("bootstrap.servers", PropertiesLoader.bootstrapServers);
        propConsumer.setProperty("group.id", PropertiesLoader.groupId);
        propConsumer.setProperty("auto.offset.reset", PropertiesLoader.autoOffsetReset);
        FlinkKafkaConsumer kafkaConsumer =
                new FlinkKafkaConsumer<String>(PropertiesLoader.topicConsumer, new SimpleStringSchema(), propConsumer);

        // 初始化 Producer 配置
        Properties propProducer = new Properties();
        propProducer.setProperty("bootstrap.servers", PropertiesLoader.bootstrapServersProducer);
        propProducer.setProperty("group.id", PropertiesLoader.groupIdProducer);
        FlinkKafkaProducer kafkaProducer =
                new FlinkKafkaProducer<String>(PropertiesLoader.topicProducer, new SimpleStringSchema(), propProducer);

        logger.info("Data Stream init");

        // get kafka input data
        DataStream<String> stream = env.addSource(kafkaConsumer).name("KafkaConsumer");

        // parse tweepy
        DataStream<Status> tweepyStream = stream.filter(line -> line.length() > 0)
                .map(value -> {
                    Status status = null;
                    try {
                        status = TwitterObjectFactory.createStatus(value);
                    } catch (TwitterException e) {
                        logger.error("TwitterException : ", e);
                    } finally {
                        return status;
                    }
                }).filter(tweet -> tweet != null).name("Tweets Stream");

        // output
        tweepyStream.map(line -> {
            String text = line.getText();
            logger.info(text);
            return text;
        }).addSink(kafkaProducer);

        try {
            env.execute("COMP7705 Flink Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}