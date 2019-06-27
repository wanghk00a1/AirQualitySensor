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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
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

        int countTrigger = Integer.valueOf(args[0]);

        logger.info("count trigger : " + countTrigger);

        tweetFlinkAnalyzer.startJob(countTrigger);
    }

    private void startJob(int countTrigger) {
        // 创建运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // 启用容错 checkpoint every 5000 msecs
//        env.enableCheckpointing(5000);
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

        Properties propProducer2 = new Properties();
        propProducer2.setProperty("bootstrap.servers", PropertiesLoader.bootstrapServersProducer);
        propProducer2.setProperty("group.id", "flink-producer");

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

                                if (status.getLang().equals("en")) {
                                    int coreNlpSentiment = CoreNLPSentimentAnalyzer.getInstance()
                                            .computeWeightedSentiment(text);

                                    result.setSentiment(coreNlpSentiment);
                                }


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
                .filter(tweet -> tweet != null && tweet.getLanguage().equals("en"))
                .name("Parsed Tweets Stream");


        // londonStream
        DataStream<TweetAnalysisEntity> londonStream = parsedTwitterStream
                .filter(value -> value.getGeo().toUpperCase().equals("LONDON"))
                .name("LONDON Stream");

        // new yorkStream
        DataStream<TweetAnalysisEntity> nyStream = parsedTwitterStream
                .filter(value -> value.getGeo().toUpperCase().equals("NY"))
                .name("NEW YORK Stream");


        /*
            使用 keyBy(tweet -> tweet.getGeo())
            会意外的很慢，原因未知
            process 和apply 用法类似，略有区别
         */
        londonStream
                .timeWindowAll(Time.seconds(countTrigger))
//                .trigger(CountTrigger.of(countTrigger))
//                .evictor(CountEvictor.of(0))
                .process(new ProcessAllWindowFunction<TweetAnalysisEntity, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<TweetAnalysisEntity> elements
                            , Collector<String> out) {
                        int positive = 0, negative = 0, neutral = 0;
//                        for (TweetAnalysisEntity element : elements) {
//                            if (element.getSentiment() > 0)
//                                positive++;
//                            else if (element.getSentiment() < 0)
//                                negative++;
//                            else
//                                neutral++;
//                        }
                        String cnt = countElement(elements);
                        out.collect("LONDON" + SPLIT + cnt + SPLIT + sdf.format(new Date()));
                    }
                })
                .addSink(new FlinkKafkaProducer<>("flink-geo",
                        new SimpleStringSchema(), propProducer))
                .name("Tweets LONDON");

        nyStream
                .timeWindowAll(Time.seconds(countTrigger))
                .apply(new AllWindowFunction<TweetAnalysisEntity, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TweetAnalysisEntity> values,
                                      Collector<String> out) throws Exception {
                        String cnt = countElement(values);
                        out.collect("NY" + SPLIT + cnt + SPLIT + sdf.format(new Date()));
                    }
                })
                .addSink(new FlinkKafkaProducer<>("flink-geo",
                        new SimpleStringSchema(), propProducer2))
                .name("Tweets NY");

        try {
            env.execute("COMP7705 Flink Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String countElement(Iterable<TweetAnalysisEntity> values) {

        int positive = 0, negative = 0, neutral = 0;
        for (TweetAnalysisEntity element : values) {
            if (element.getSentiment() > 0)
                positive++;
            else if (element.getSentiment() < 0)
                negative++;
            else
                neutral++;
        }

        return positive + SPLIT + negative + SPLIT + neutral;
    }

}