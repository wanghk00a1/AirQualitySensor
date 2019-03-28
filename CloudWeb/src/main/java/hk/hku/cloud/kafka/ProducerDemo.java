package hk.hku.cloud.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author: LexKaing
 * @create: 2019-03-13 19:32
 * @description:
 **/
public class ProducerDemo {
    private static Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "202.45.128.135:29107");
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 30000);
        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        logger.info("ProducerDemo: start.");

        String topic = "test";
        int msgNum = 10; // 发送的消息数

        for (int i = 0; i < msgNum; i++) {
            String msg = i + " This is test blog.";
            logger.info("msgNum : " + msg);

            producer.send(new ProducerRecord<>(topic, msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if (e != null) {
                                logger.error("null callback ", e);
                            } else {
                                logger.info("The offset of the record we just sent is: " + metadata.offset());
                            }
                        }
                    }
//            (RecordMetadata metadata, Exception e) -> {
//                if (e != null) {
//                    logger.error("null callback ", e);
//                } else {
//                    logger.info("The offset of the record we just sent is: " + metadata.offset());
//                }
//            }
            );

//            producer.flush();

            try {
                Thread.sleep(100);
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }

        //列出topic的相关信息
        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        partitions = producer.partitionsFor(topic);
        for (PartitionInfo p : partitions) {
            logger.info("partition Info : " + p);
        }

//        producer.flush();

        producer.close();
//        producer.close(0, TimeUnit.MILLISECONDS);
        logger.info("ProducerDemo: end.");
    }

}