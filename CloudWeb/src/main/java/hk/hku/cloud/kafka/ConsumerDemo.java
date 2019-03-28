package hk.hku.cloud.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * @author: LexKaing
 * @create: 2019-03-13 16:01
 * @description:
 * 消费 Kafka 数据
 * 本地运行，无法穿越跳板机的限制，服务器运行正常。
 **/
public class ConsumerDemo extends Thread {
    private static Logger LOG = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        //配置项
        Properties props = new Properties();
//        InputStream in = ConsumerDemo.class.getClassLoader().getResourceAsStream("consumer.properties");
//        props.load(in);

        props.put("bootstrap.servers", "202.45.128.135:29107");
        props.put("group.id", "alex-consumer-group");
        // What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server
        // (e.g. because that data has been deleted)
        props.put("auto.offset.reset", "latest");  //[latest(default), earliest, none]
        props.put("enable.auto.commit", "true");// 自动commit
        props.put("auto.commit.interval.ms", "1000");// 自动commit的间隔
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费者订阅多个topic
        Collection<String> topics = Arrays.asList("test");
        consumer.subscribe(topics);

        ConsumerRecords<String, String> consumerRecords;

        LOG.info("ConsumerDemo: start.");

        boolean flag = true;
        while (flag) {
            // 从topic中拉取数据:
            // timeout(ms): buffer 中的数据未就绪情况下，等待的最长时间，如果设置为0，立即返回 buffer 中已经就绪的数据
            consumerRecords = consumer.poll(Duration.ofMillis(1000));
            LOG.info("consumerRecords count is : " + consumerRecords.count());

            // 遍历每一条记录--handle records
            for (ConsumerRecord consumerRecord : consumerRecords) {
                long offset = consumerRecord.offset();
                int partition = consumerRecord.partition();
                Object key = consumerRecord.key();
                Object value = consumerRecord.value();

                System.out.format("%d\t%d\t%s\t%s\n", offset, partition, key, value);

                if (key.toString().equals("stop")) {
                    flag = false;
                    break;
                }
            }
        }

        //Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
        consumer.close();

        LOG.info("ConsumerDemo End.");
    }

}