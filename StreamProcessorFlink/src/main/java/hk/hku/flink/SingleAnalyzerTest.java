package hk.hku.flink;

import hk.hku.flink.corenlp.CoreNLPSentimentAnalyzer;
import hk.hku.flink.domain.TweetAnalysisEntity;
import hk.hku.flink.utils.GeoCity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import twitter4j.*;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static hk.hku.flink.utils.Constants.COMMA;

/**
 * @author: LexKaing
 * @create: 2019-07-13 14:51
 * @description:
 * 测试集群环境里一台机器的性能，处理2000条数据大概需要的时间。
 * java -classpath ./target/StreamProcessorFlink-jar-with-dependencies.jar hk.hku.flink.SingleAnalyzerTest
 **/
public class SingleAnalyzerTest {

    private final static Logger logger = Logger.getLogger(SingleAnalyzerTest.class);

    private static String weatherWords = "weather,aqi,health,smoke,air,pollution,breathe,lungs,smog,haze,cough";
    public static void main(String[] args) {
        int size = Integer.valueOf(args[0]);

        Properties props = new Properties();
        props.put("bootstrap.servers", "slave01:9092,slave02:9092,slave03:9092");
        props.put("group.id", "single-consumer-test");
        props.put("auto.offset.reset", "latest");  //[latest(default), earliest, none]
        props.put("enable.auto.commit", "true");// 自动commit
        props.put("auto.commit.interval.ms", "1000");// 自动commit的间隔
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        Collection<String> topics = Arrays.asList("alex1");
        consumer.subscribe(topics);

        ConsumerRecords<String, String> consumerRecords;

        logger.info("Single Test collection start with size : " + size);

        List<ConsumerRecord> tweetList = new ArrayList<>();
        while (true) {
            consumerRecords = consumer.poll(Duration.ofSeconds(60));
            for (ConsumerRecord consumerRecord : consumerRecords) {
                tweetList.add(consumerRecord);
            }
            if (tweetList.size() >= size)
                break;
        }

        logger.info("Single Test collection end.");

        logger.info("current total count : " + tweetList.size());

        long start = System.currentTimeMillis();

        List<TweetAnalysisEntity> resultList = tweetList.stream().map(consumerRecord -> {
            String value = consumerRecord.value().toString();

            if (value.length() <= 0)
                return null;
            try {
                Status status = TwitterObjectFactory.createStatus(value);
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
                    for (String word : weatherWords.split(COMMA)) {
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
                .collect(Collectors.toList());

        long end = System.currentTimeMillis();

        logger.info("Single Test process over.");

        logger.info("Duration : " + (end - start));
    }
}