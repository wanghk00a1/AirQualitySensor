package hk.hku.cloud.kafka.service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import hk.hku.cloud.kafka.dao.KafkaDaoImpl;
import hk.hku.cloud.kafka.domain.AqiEntity;
import hk.hku.cloud.kafka.domain.TweetStatisticEntity;
import hk.hku.cloud.ml.RandomTree;
import hk.hku.cloud.utils.KafkaProperties;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import twitter4j.JSONArray;
import twitter4j.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author: LexKaing
 * @create: 2019-04-02 02:22
 * @description:
 **/
@Service
@Transactional
public class KafkaService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaService.class);

    private static Gson gson = new Gson();

    private static volatile boolean consumeKafka = true;

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Autowired
    private SimpMessagingTemplate template;

    @Autowired
    private KafkaDaoImpl kafkaDaoImpl;

    private static List<AqiEntity> parseAqiStr(String res) throws ParseException {
        List<AqiEntity> aqiEntities = new ArrayList<>();
        JSONObject jsonObject = new JSONObject(res);
        String city = null;

        if (jsonObject.getString("id").equals("7McFS9nFSf5TQmwva")) {
            city = "LONDON";
        } else if (jsonObject.getString("id").equals("gXTDkEBCX9BBKe5wc"))
            city = "NY";

        JSONObject measurements = jsonObject.getJSONObject("measurements");
        JSONArray hourlyAqi = measurements.getJSONArray("hourly");

        for (int i = 0; i < hourlyAqi.length(); i++) {
            AqiEntity tmp = new AqiEntity();

            String timestamp = hourlyAqi.getJSONObject(i)
                    .getString("ts")
                    .replaceAll("T", " ")
                    .replaceAll(".000Z", "");
            int AQI = hourlyAqi.getJSONObject(i).getInt("aqi");

            tmp.setCity(city);
            tmp.setTimestamp(String.valueOf(sdf.parse(timestamp).getTime()));
            tmp.setAqi(AQI);
            aqiEntities.add(tmp);
        }
        return aqiEntities;
    }

    public void setConsumeKafka(boolean consumeKafka) {
        this.consumeKafka = consumeKafka;
        logger.info("setConsumeKafka : " + consumeKafka);
    }

    /**
     * 消费 London & NY tweets 数据
     * 发送到前端 /topic/consumeTweets
     */
    @Async
    public void consumeTweets() {
        Properties props = KafkaProperties.getConsumerProperties("web-consumer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费者订阅多个topic
        Collection<String> topics = Arrays.asList("flink-london", "flink-ny");
        consumer.subscribe(topics);

        ConsumerRecords<String, String> consumerRecords;

        logger.info("Consumer Kafka start.");
        while (consumeKafka) {
            // timeout(ms): buffer 中的数据未就绪情况下，等待的最长时间，如果设置为0，立即返回 buffer 中已经就绪的数据
            consumerRecords = consumer.poll(Duration.ofSeconds(1));
//            logger.info("consumerRecords count is : " + consumerRecords.count());

            for (ConsumerRecord consumerRecord : consumerRecords) {
                Object key = consumerRecord.key();
                String value = consumerRecord.value().toString();
                // 发送消息给订阅 "/topic/consumeTweets" 且在线的用户
                if (value.length() > 0)
                    template.convertAndSend("/topic/consumeTweets", value);
            }
        }

        consumer.close();
        // 后端断开连接时,通知一下前端，可以前端做校验---暂时不处理这种情况
        // template.convertAndSend("/topic/close", "closed");
        logger.info("Consumer Kafka End.");
    }

    /**
     * 持续消费 London & NY 的统计数据
     * 解析并存储到 mysql
     */
    @Async
    public void consumeStatistic() {
        Properties props = KafkaProperties.getConsumerProperties("web-consumer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Collection<String> topics = Arrays.asList("flink-count");
        consumer.subscribe(topics);

        ConsumerRecords<String, String> consumerRecords;

        logger.info("Consumer Statistic Lang start.");
        while (true) {
            consumerRecords = consumer.poll(Duration.ofSeconds(60));

            for (ConsumerRecord consumerRecord : consumerRecords) {
                String value = consumerRecord.value().toString();
                if (value.length() > 0) {
                    TweetStatisticEntity tmp = gson.fromJson(value, TweetStatisticEntity.class);
                    tmp.setRandom_tree(RandomTree.getInstance().predictAQI(tmp));
                    kafkaDaoImpl.insertAqi(tmp);
                }
            }
        }
    }

    /**
     * 定时往socket 地址发送一条消息，保证web socket 存活
     */
    @Async
    public void keepSocketAlive() {
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(10);
                String value = "ping-alive";
                template.convertAndSend("/topic/consumeTweets", value);
            } catch (Exception e) {
                logger.error("putTimingMessage exception : ", e);
            }
        }
    }

    /**
     * 根据城市 LONDON / NY 获取对应的AQI数据,JSON 字符串
     */
    public List<TweetStatisticEntity> getPredictAqiDataByCity(String city, int limit) {
        return kafkaDaoImpl.queryPredictAqi(city, limit);
    }

    /**
     * 获取政府AQI数据并存储到mysql
     * 每30分钟执行一次
     */
    @Scheduled(fixedRate = 30 * 60 * 1000, initialDelay = 30 * 1000)
    public void crawlGovernmentAqiData() {
        logger.info("crawl government aqi data : " + sdf.format(new Date()));
        String london = "https://website-api.airvisual.com/v1/cities/7McFS9nFSf5TQmwva" +
                "/measurements?units.temperature=celsius&units.distance=kilometer&AQI=US&language=en";

        String ny = "https://website-api.airvisual.com/v1/cities/gXTDkEBCX9BBKe5wc" +
                "/measurements?units.temperature=celsius&units.distance=kilometer&AQI=US&language=en";

        try {
            HttpClient httpClient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(london);
            HttpResponse response = httpClient.execute(httpGet);
            String res = EntityUtils.toString(response.getEntity());

            kafkaDaoImpl.insertActualAQI(parseAqiStr(res));

            HttpGet httpGet2 = new HttpGet(ny);
            HttpResponse response2 = httpClient.execute(httpGet2);
            String res2 = EntityUtils.toString(response2.getEntity());

            kafkaDaoImpl.insertActualAQI(parseAqiStr(res2));
        } catch (Exception e) {
            logger.error("crawlGovernmentAqiData", e);
        }
    }

    public List<AqiEntity> getActualAqiDataByCity(String city, int limit) {
        return kafkaDaoImpl.queryActualAqi(city, limit);
    }
}