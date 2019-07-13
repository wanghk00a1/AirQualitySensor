package hk.hku.cloud.utils;

import java.util.Properties;

/**
 * @author: LexKaing
 * @create: 2019-06-28 20:00
 * @description:
 **/
public class KafkaProperties {

    private final static String kafka_server = "slave01:9092,slave02:9092,slave03:9092";

    // kafka consumer 配置项
    public static Properties getConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka_server);
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "latest");  //[latest(default), earliest, none]
        props.put("enable.auto.commit", "true");// 自动commit
        props.put("auto.commit.interval.ms", "1000");// 自动commit的间隔
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    // kafka producer 配置项
    public static Properties getProducerProperties(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka_server);
        props.setProperty("group.id", groupId);
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 30000);
        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

}