package hk.hku.flink;

import hk.hku.flink.corenlp.CoreNLPSentimentAnalyzer;
import hk.hku.flink.corenlp.LanguageAnalyzer;
import hk.hku.flink.domain.TweetAnalysisEntity;
import hk.hku.flink.utils.PropertiesLoader;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TweetEntity;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.text.DecimalFormat;
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
        // 设置并行度15 台机器
        env.setParallelism(2);

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
                .addSource(new FlinkKafkaConsumer<>(PropertiesLoader.topicConsumer,
                        new SimpleStringSchema(), propConsumer))
                .name("Kafka Consumer");

        // parse tweets
        DataStream<TweetAnalysisEntity> parsedTwitterStream = stream
                .filter(line -> line.length() > 0)
                .map(line -> {
                    try {
                        Status status = TwitterObjectFactory.createStatus(line);
                        String text = status.getText().replaceAll("\n", "");

                        if (text.length() > 0) {
                            TweetAnalysisEntity result = new TweetAnalysisEntity();

                            result.setId(status.getId());
                            result.setText(text);
                            result.setUsername(status.getUser().getScreenName());
                            //if (status.getGeoLocation() != null)
                            result.setGeo(status.getPlace().getFullName());
                            result.setLanguage(LanguageAnalyzer.getInstance().detectLanguage(text));

                            // 加上 weather keywords related
                            // Arrays.stream({"1","2"}).filter(word -> text.contains(word)).count();
                            Boolean weatherRelated = false;
                            for (String word : PropertiesLoader.weatherKeywords.split(COMMA)) {
                                if (text.contains(word)) {
                                    weatherRelated = true;
                                    break;
                                }
                            }
                            result.setWeather(weatherRelated);

                            return result;
                        }
                    } catch (TwitterException e) {
                        logger.error("Twitter Parse Exception : ", e);
                    }
                    return null;
                })
                .name("Parsed Tweets Stream");


        // core nlp analysis sentiment
        DataStream<TweetAnalysisEntity> sentimentStream = parsedTwitterStream
                .filter(tweet -> tweet != null && tweet.getLanguage().equals("en"))
                .map(tweet -> {
                    //使用nlp 计算 sentiment
                    int corenlpSentiment = CoreNLPSentimentAnalyzer.getInstance()
                            .computeWeightedSentiment(tweet.getText());

                    tweet.setSentiment(corenlpSentiment);

                    logger.info("Sentiment Stream : " + tweet.toString());
                    return tweet;
                })
                .name("Sentiment Stream");


        // output tweets process info
        sentimentStream
                .map(tweet -> {
                    StringBuffer stringBuffer = new StringBuffer();
                    stringBuffer.append(tweet.getId()).append(DELIMITER)
                            .append(tweet.getUsername()).append(DELIMITER)
                            .append(tweet.getText()).append(DELIMITER)
                            .append(tweet.getWeather()).append(DELIMITER)
                            .append(tweet.getSentiment());
                    return stringBuffer.toString();
                })
                .addSink(new FlinkKafkaProducer<>(PropertiesLoader.topicProducer,
                        new SimpleStringSchema(), propProducer))
                .name("Tweets Sentiment Result Sink");

        // 根据条次数窗口 统计
        DataStream<String> countByNum = sentimentStream
                .keyBy(tweet -> tweet.getWeather())
                .countWindowAll(30)
                .process(new ProcessAllWindowFunction<TweetAnalysisEntity, String, GlobalWindow>() {
                    @Override
                    public void process(Context context, Iterable<TweetAnalysisEntity> elements,
                                        Collector<String> out) throws Exception {

                        int positive = 0, negative = 0, neutral = 0;
                        for (TweetAnalysisEntity element : elements) {
                            if (element.getSentiment() > 0)
                                positive++;
                            else if (element.getSentiment() < 0)
                                negative++;
                            else
                                neutral++;
                        }
                    }
                })
                .name("count window by weather");

        // 根据时间窗口 统计
//        DataStream<String> countByTime = sentimentStream
//                .keyBy(tweet->tweet.getWeather())
//                .timeWindowAll(Time.seconds(60))
//                .process();

        try {
            env.execute("COMP7705 Flink Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}