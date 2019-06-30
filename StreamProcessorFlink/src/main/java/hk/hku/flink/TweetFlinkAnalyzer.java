package hk.hku.flink;

import com.google.gson.Gson;
import hk.hku.flink.corenlp.CoreNLPSentimentAnalyzer;
import hk.hku.flink.domain.TweetAnalysisEntity;
import hk.hku.flink.domain.TweetStatisticEntity;
import hk.hku.flink.trigger.CountWithTimeoutTrigger;
import hk.hku.flink.utils.GeoCity;
import hk.hku.flink.utils.PropertiesLoader;
import org.apache.flink.api.common.functions.MapFunction;
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
 * @description: 默认每5分钟 / 每500条吐一次结果
 * flink run -c hk.hku.flink.TweetFlinkAnalyzer StreamProcessorFlink-jar-with-dependencies.jar $timeout $countTrigger
 **/
public class TweetFlinkAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(TweetFlinkAnalyzer.class);

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static Gson gson = new Gson();

    public static void main(String[] args) {
        logger.info("Tweets Flink Analyzer job start");
        TweetFlinkAnalyzer tweetFlinkAnalyzer = new TweetFlinkAnalyzer();

        int timeout = 300;
        int countTrigger = 500;
        try {
            timeout = Integer.valueOf(args[0]);
            countTrigger = Integer.valueOf(args[1]);
        } catch (Exception e) {
            logger.info("main", e);
        }
        logger.info("count trigger : " + countTrigger);

        tweetFlinkAnalyzer.startJob(timeout, countTrigger);
    }

    private static String countElement(String city, Iterable<TweetAnalysisEntity> values) {
        int positive = 0, negative = 0, total = 0;
        int w_positive = 0, w_negative = 0, w_total = 0;
        for (TweetAnalysisEntity element : values) {
            total++;
            if (element.getHasWeather()) {
                w_total++;
                if (element.getSentiment() > 0) {
                    positive++;
                    w_positive++;
                } else if (element.getSentiment() < 0) {
                    w_negative++;
                    negative++;
                }
            } else {
                if (element.getSentiment() > 0) {
                    positive++;
                } else if (element.getSentiment() < 0) {
                    negative++;
                }
            }
        }

        TweetStatisticEntity tmp = new TweetStatisticEntity();
        tmp.setCity(city);
        tmp.setTimestamp((new Date()).getTime());
        tmp.setPositive(positive);
        tmp.setNegative(negative);
        tmp.setTotal(total);
        tmp.setW_positive(w_positive);
        tmp.setW_negative(w_negative);
        tmp.setW_total(w_total);

        return gson.toJson(tmp);
    }

    /**
     * 自定义的窗口，timeout 单位秒
     *
     * @param timeout
     * @param countTrigger
     */
    private void startJob(int timeout, int countTrigger) {
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
        propProducer.setProperty("group.id", "flink-producer1");

        Properties propProducer2 = new Properties();
        propProducer2.setProperty("bootstrap.servers", PropertiesLoader.bootstrapServersProducer);
        propProducer2.setProperty("group.id", "flink-producer2");

        logger.info("Data Stream init");

        // kafka input data
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>(PropertiesLoader.topicConsumer,
                        new SimpleStringSchema(), propConsumer))
                .name("Kafka Consumer");

        // parse tweets
        DataStream<TweetAnalysisEntity> parsedTwitterStream = stream
                .map((MapFunction<String, TweetAnalysisEntity>) line -> {
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
                            result.setCreatetime(status.getCreatedAt().getTime());

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
                            result.setHasWeather(weatherRelated);

                            result.setRetweet(status.isRetweeted());
                            result.setHasMedia(status.getMediaEntities().length > 0);

                            return result;
                        }
                    } catch (TwitterException e) {
                        logger.error("Twitter Parse Exception : ", e);
                    }
                    return null;
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

        // 存储各自城市的tweet 文本分析结果
        londonStream
                .map(value -> gson.toJson(value))
                .addSink(new FlinkKafkaProducer<String>("flink-london", new SimpleStringSchema(), propProducer))
                .name("london tweets");

        nyStream
                .map(value -> gson.toJson(value))
                .addSink(new FlinkKafkaProducer<String>("flink-ny", new SimpleStringSchema(), propProducer))
                .name("new york tweets");

        /*
            使用 keyBy(tweet -> tweet.getGeo()) 会意外的很慢，原因未知.
            process 和apply 用法类似，但更底层的算法,可以自己写定时触发计算的定时器
         */
        londonStream
                .timeWindowAll(Time.seconds(timeout))
                .trigger(new CountWithTimeoutTrigger<>(countTrigger, TimeCharacteristic.ProcessingTime))
//                .evictor(CountEvictor.of(0))
                .process(new ProcessAllWindowFunction<TweetAnalysisEntity, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<TweetAnalysisEntity> elements, Collector<String> out) {
                        String result = countElement("LONDON", elements);
                        out.collect(result);
                    }
                })
                .addSink(new FlinkKafkaProducer<>("flink-london-count", new SimpleStringSchema(), propProducer))
                .name("LONDON statistics");

        nyStream
                .timeWindowAll(Time.seconds(timeout))
                .trigger(new CountWithTimeoutTrigger<>(countTrigger, TimeCharacteristic.ProcessingTime))
                .apply(new AllWindowFunction<TweetAnalysisEntity, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TweetAnalysisEntity> values, Collector<String> out) throws Exception {
                        String result = countElement("NY", values);
                        out.collect(result);
                    }
                })
                .addSink(new FlinkKafkaProducer<>("flink-ny-count", new SimpleStringSchema(), propProducer2))
                .name("NY statistics");

        try {
            env.execute("COMP7705 Flink Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}