package ca.mcit.bigdata.kafka

import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.JavaConverters._

object Kafka02Consumer extends App {

  val topicName = "trips"

  val consumerProperties = new Properties()
  consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProperties.setProperty(GROUP_ID_CONFIG, "group-id-1")
  consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
  consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false")

  val consumer = new KafkaConsumer[String, String](consumerProperties)
  consumer.subscribe(List(topicName).asJava)

  println("| Key | Message | Partition | Offset |")

  while (true) {
    val polledRecords: ConsumerRecords[String, String] =
      consumer.poll(Duration.ofSeconds(1))

      var enrichedList: List[EnrichedTrip] = List()
      if (!polledRecords.isEmpty){
        println(s"Polled ${polledRecords.count()} messages from $topicName")
        val recordIterator = polledRecords.iterator()

        while (recordIterator.hasNext) {
          val record: ConsumerRecord[String, String] = recordIterator.next()
          println(s"| ${record.key()} | ${record.value} | ${record.partition} | ${record.offset()}")
          val dataRecord = record.value().split(",")
          enrichedList = enrichedList ++
            List(EnrichedTrip(TripsRoute(Trips(dataRecord(0), dataRecord(1), dataRecord(2), dataRecord(3),
           if (dataRecord(6) == "1") true else false), Routes(null,null,null)), CalendarDates(null,null, 0)))
        }
        Kafka01Producer.ProduceEnrichedTopic(enrichedList)
      }
    for (x <- enrichedList) {println(x)}
  }
}

