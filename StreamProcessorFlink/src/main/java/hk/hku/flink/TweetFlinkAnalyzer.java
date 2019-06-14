package hk.hku.flink;

import hk.hku.flink.process.ComputeSentiment;
import hk.hku.flink.process.ParseTwitter;
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

import java.util.Objects;
import java.util.Properties;

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

    void startJob() {
        //        val stopWordsList = StopWordsLoader.loadStopWords(PropertiesLoader.nltkStopWords)

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

        // parse tweets
        DataStream<Status> tweetsStream = stream
                .filter(line -> line.length() > 0)
                .map(new ParseTwitter())
                .filter(Objects::nonNull)
                .name("Tweets Stream");

        // process tweets
        // 需要加关键词过滤
        DataStream tweetsInfo = tweetsStream
                .map(new ComputeSentiment())
                .name("Sentiment Stream");


        // 记录地区信息
        DataStream geoInfo = tweetsStream
                .map(tweet -> {
                    if (tweet.getGeoLocation() != null) {
                        return tweet.getGeoLocation().getLatitude()
                                + ":" + tweet.getGeoLocation().getLongitude();
                    } else {

                        return tweet.getPlace().getCountry() + "|"
                                + tweet.getPlace().getPlaceType()
                                + "|" + tweet.getPlace().getFullName();
                    }
                })
                .name("Geo Stream");


        // output geo info
        geoInfo.addSink(new FlinkKafkaProducer("flink-geo",new SimpleStringSchema(),propProducer))
                .name("Geo Info Sink");

        geoInfo.print()
                .name("Geo Print");

        // output tweets process info
        tweetsInfo.addSink(new FlinkKafkaProducer<>(PropertiesLoader.topicProducer, new SimpleStringSchema(), propProducer))
                .name("Tweets Sentiment Result Sink");


        try {
            env.execute("COMP7705 Flink Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}