package hk.hku.flink;

import hk.hku.flink.corenlp.CoreNLPSentimentAnalyzer;
import hk.hku.flink.corenlp.LanguageAnalyzer;
import hk.hku.flink.domain.TweetAnalysisEntity;
import hk.hku.flink.utils.GeoCity;
import hk.hku.flink.utils.PropertiesLoader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static hk.hku.flink.utils.Constants.COMMA;
import static hk.hku.flink.utils.Constants.DELIMITER;
import static hk.hku.flink.utils.Constants.SPLIT;

/**
 * @author: LexKaing
 * @create: 2019-06-12 17:40
 * @description: flink run -c hk.hku.flink.TweetFlinkAnalyzer StreamProcessorFlink-jar-with-dependencies.jar
 **/
public class TweetFlinkAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(TweetFlinkAnalyzer.class);

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

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
        env.setParallelism(14);

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
                .map(new MapFunction<String, TweetAnalysisEntity>() {
                    @Override
                    public TweetAnalysisEntity map(String line) throws Exception {
                        if (line.length() <= 0)
                            return null;
                        try {
                            Status status = TwitterObjectFactory.createStatus(line);
                            String text = status.getText().replaceAll("\n", "");

                            if (text.length() > 0) {
                                TweetAnalysisEntity result = new TweetAnalysisEntity();

                                result.setId(status.getId());
                                result.setText(text);
                                result.setUsername(status.getUser().getScreenName());

                                String city = "NULL";
                                if (status.getGeoLocation() != null) {
                                    city = GeoCity.geoToCity(status.getGeoLocation().getLongitude(),
                                            status.getGeoLocation().getLatitude());

                                } else if (status.getPlace() != null) {
                                    double longitude = 0.0, latitude = 0.0;
                                    for (GeoLocation[] coorList : status.getPlace().getBoundingBoxCoordinates()) {

                                        for (GeoLocation coor : coorList) {
                                            longitude += coor.getLongitude();
                                            latitude += coor.getLatitude();
                                        }
                                    }
                                    city = GeoCity.geoToCity(longitude / 4, latitude / 4);
                                }
                                result.setGeo(city);

                                result.setLanguage(status.getLang());

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

                                result.setRetweet(status.isRetweeted());
                                result.setHasMedia(status.getMediaEntities().length > 0);

                                return result;
                            }
                        } catch (TwitterException e) {
                            logger.error("Twitter Parse Exception : ", e);
                        }
                        return null;
                    }
                })
                .filter(tweet -> tweet != null && tweet.getLanguage().equals("en")) //&& !tweet.getGeo().equals("NULL")
                .name("Parsed Tweets Stream");


        // core nlp analysis sentiment
        DataStream<TweetAnalysisEntity> sentimentStream = parsedTwitterStream
                .map(tweet -> {
                    //使用nlp 计算 sentiment
                    int corenlpSentiment = CoreNLPSentimentAnalyzer.getInstance()
                            .computeWeightedSentiment(tweet.getText());

                    tweet.setSentiment(corenlpSentiment);

                    logger.info("Sentiment Stream : " + tweet.toString());
                    return tweet;
                })
                .name("Sentiment Stream");


        // 根据时间窗口 统计
        DataStream<String> statistics = sentimentStream
                .keyBy(tweet -> tweet.getGeo())
                .timeWindow(Time.minutes(10), Time.minutes(5))
                .trigger(CountTrigger.of(50))
                .process(new ProcessWindowFunction<TweetAnalysisEntity, String, String, TimeWindow>() {
                    @Override
                    public void process(String city, Context context, Iterable<TweetAnalysisEntity> elements
                            , Collector<String> out) throws Exception {

                        int positive = 0, negative = 0, neutral = 0;

                        for (TweetAnalysisEntity element : elements) {
                            if (element.getSentiment() > 0)
                                positive++;
                            else if (element.getSentiment() < 0)
                                negative++;
                            else
                                neutral++;
                        }

                        out.collect(city + SPLIT + positive + SPLIT + negative
                                + SPLIT + neutral + SPLIT + sdf.format(new Date()));
                    }
                })
                .name("statistics window");

//        statistics.print();

        statistics
                .addSink(new FlinkKafkaProducer<>("flink-geo",
                        new SimpleStringSchema(), propProducer))
                .name("Tweets Sentiment Result Sink");


        // count by time
        DataStream<String> countByTime = sentimentStream
                .keyBy(value -> value.getGeo())
                .timeWindow(Time.minutes(10), Time.minutes(5))
                .process(new ProcessWindowFunction<TweetAnalysisEntity, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<TweetAnalysisEntity> elements,
                                        Collector<String> out) throws Exception {

                        int cnt = 0;
                        for (TweetAnalysisEntity tmp : elements) {
                            cnt++;
                        }

                        out.collect("Count:" + key + SPLIT + cnt + SPLIT + sdf.format(new Date()));
                    }
                })
                .name("count by time");

//        countByTime.print();

//        sentimentStream
//                .map(tweet -> {
//                    StringBuffer stringBuffer = new StringBuffer();
//                    stringBuffer.append(tweet.getId()).append(DELIMITER)
//                            .append(tweet.getUsername()).append(DELIMITER)
//                            .append(tweet.getSentiment()).append(DELIMITER)
//                            .append(tweet.getWeather()).append(DELIMITER)
//                            .append(tweet.getText());
//                    return stringBuffer.toString();
//                })

        countByTime
                .addSink(new FlinkKafkaProducer<>("flink-count",
                        new SimpleStringSchema(), propProducer))
                .name("Tweets Sentiment Result Sink");

        try {
            env.execute("COMP7705 Flink Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}