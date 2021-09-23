package Demo

import Demo.CountWords.countWords
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}
import java.util.concurrent.TimeUnit
import java.util.Properties


object KafkaExamples {
//./kafka-console-producer.sh --broker-list localhost:9092 --topic customer.create

  val stopWords = Set("a", "an", "the")
  val window = Time.of(10, TimeUnit.SECONDS)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val kafkaConsumerProperties = Map(
//      "zookeeper.connect" -> "localhost:2181",
//      "group.id" -> "flink",
//      "bootstrap.servers" -> "localhost:9092"
//    )

    val kafkaConsumerProperties = new Properties()
    kafkaConsumerProperties.setProperty("bootstrap.servers", "localhost:9092")
    kafkaConsumerProperties.setProperty("zookeeper.connect", "localhost:2181")
    kafkaConsumerProperties.setProperty("group.id", "tests")

    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "input",
      KafkaStringSchema,
      kafkaConsumerProperties
    )

    val kafkaProducer = new FlinkKafkaProducer[String](
      "localhost:9092",
      "output",
      KafkaStringSchema
    )

    val lines = env.addSource(kafkaConsumer)

    val wordCounts = countWords(lines, stopWords, window)

    wordCounts
      .map(_.toString)
      .addSink(kafkaProducer)

    env.execute("Flink Kafka Example")

  }

  object KafkaStringSchema extends SerializationSchema[String] with DeserializationSchema[String] {

    import org.apache.flink.api.common.typeinfo.TypeInformation
    import org.apache.flink.api.java.typeutils.TypeExtractor

    override def serialize(t: String): Array[Byte] = t.getBytes("UTF-8")

    override def isEndOfStream(t: String): Boolean = false

    override def deserialize(bytes: Array[Byte]): String = new String(bytes, "UTF-8")

    override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  }


}
