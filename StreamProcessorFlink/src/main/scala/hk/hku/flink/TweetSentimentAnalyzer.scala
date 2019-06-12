package hk.hku.flink

import java.util.Properties

import hk.hku.flink.utils.{PropertiesLoader, StopWordsLoader}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.{SerializationSchema, SimpleStringSchema}
import org.slf4j.LoggerFactory
import twitter4j.{Status, TwitterException, TwitterObjectFactory}

object TweetSentimentAnalyzer {

  @transient
  lazy val logger = LoggerFactory.getLogger(TweetSentimentAnalyzer.getClass)

  def main(args: Array[String]): Unit = {
    val stopWordsList = StopWordsLoader.loadStopWords(PropertiesLoader.nltkStopWords)

    // 创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 启用容错 checkpoint every 5000 msecs
    env.enableCheckpointing(5000)

    /** 初始化 Consumer 配置 */
    val propConsumer = new Properties()
    propConsumer.setProperty("bootstrap.servers", PropertiesLoader.bootstrapServers)
    propConsumer.setProperty("group.id", PropertiesLoader.groupId)
    propConsumer.setProperty("auto.offset.reset", PropertiesLoader.autoOffsetReset)

    // 获取kafka 元数据
    val kafkaConsumer: FlinkKafkaConsumer[String] =
      new FlinkKafkaConsumer[String](PropertiesLoader.topicConsumer, new SimpleStringSchema(), propConsumer)

    // start from the latest record,配置Kafka分区的起始位置,default : setStartFromGroupOffsets
    // kafkaConsumer.setStartFromLatest()

    // 将 Kafka Consumer 加入到流处理
    val stream: DataStream[String] = env.addSource(kafkaConsumer)

    // 引入watermark
    // watrmark 允许数据延迟毫秒
//    val MaxOutOfOrderness = 86400 * 1000L
//    stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds()) {})


//    stream.map{(head,line) => line.toString}


    /** 初始化 Producer 配置 */
    val propProducer = new Properties()
    propProducer.setProperty("bootstrap.servers", PropertiesLoader.bootstrapServersProducer)
    propProducer.setProperty("group.id", PropertiesLoader.groupIdProducer)

    val kafkaProducer: FlinkKafkaProducer[String] =
      new FlinkKafkaProducer[String](PropertiesLoader.topicProducer, new SimpleStringSchema(), propProducer)

    // compute sentiment

    /** 将Kafka Producer加入到流处理 */
    stream.addSink(kafkaProducer)

    // execute program
    env.execute("COMP7705 Flink Job")
  }

  /**
    * 解析 twitter json 数据
    */
  def praseTwitterText(line: String): Unit = {
    var status: Status = null

    try
      status = TwitterObjectFactory.createStatus(line)
    catch {
      case e: TwitterException =>
        logger.error("TwitterException : ", e)
    } finally {
      // return the parsed tweet, or null
      status
    }
  }

}
