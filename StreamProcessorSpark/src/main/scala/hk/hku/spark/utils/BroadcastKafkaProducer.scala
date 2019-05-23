package hk.hku.spark.utils

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

class BroadcastKafkaProducer[K, V](createproducer: () => KafkaProducer[K, V]) extends Serializable {
  lazy val producer = createproducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] = producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] = producer.send(new ProducerRecord[K, V](topic, value))
}


object BroadcastKafkaProducer {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]): BroadcastKafkaProducer[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook({
        producer.close()
      })
      producer
    }
    new BroadcastKafkaProducer(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): BroadcastKafkaProducer[K, V] = apply(config.toMap)
}