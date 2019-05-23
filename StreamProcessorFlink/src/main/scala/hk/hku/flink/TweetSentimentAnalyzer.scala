package hk.hku.flink

import java.util.Properties

import hk.hku.flink.utils.{PropertiesLoader, StopWordsLoader}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.slf4j.LoggerFactory

object TweetSentimentAnalyzer {

  @transient
  lazy val log = LoggerFactory.getLogger(TweetSentimentAnalyzer.getClass)

  def main(args: Array[String]): Unit = {
    val stopWordsList = StopWordsLoader.loadStopWords(PropertiesLoader.nltkStopWords)

    // 创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 启用容错 checkpoint every 5000 msecs
    env.enableCheckpointing(5000)

    // kafka consumer
    val propConsumer = new Properties()
    propConsumer.setProperty("bootstrap.servers", PropertiesLoader.bootstrapServers)
    propConsumer.setProperty("group.id", PropertiesLoader.groupId)
    propConsumer.setProperty("auto.offset.reset", PropertiesLoader.autoOffsetReset)

    val kafkaConsumer = new FlinkKafkaConsumer[String](PropertiesLoader.topicConsumer, new SimpleStringSchema(), propConsumer)
    // start from the latest record,配置Kafka分区的起始位置,default : setStartFromGroupOffsets
    // kafkaConsumer.setStartFromLatest()

    // kafka producer
    val propProducer = new Properties()
    propProducer.setProperty("bootstrap.servers", PropertiesLoader.bootstrapServersProducer)
    propProducer.setProperty("group.id", PropertiesLoader.groupIdProducer)

    val kafkaProducer = new FlinkKafkaProducer[String](PropertiesLoader.topicProducer, new SimpleStringSchema(), propProducer)


    // 读取kafka 数据
    val stream :DataStreamSource[String] = env.addSource(kafkaConsumer)

    // compute sentiment

    // 吐出kafka 数据
    stream.addSink(kafkaProducer)

    // execute program
    env.execute("COMP7705 Flink Job")
  }
}
