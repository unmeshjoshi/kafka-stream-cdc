package com.streaming.kafka

import java.util.{Properties, UUID}

import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class KafkaProduceConsumeTest extends FunSuite with BeforeAndAfterAll with Matchers with Eventually with KafkaTest {
  case class Member(id: String, name: String)
  case class MemberRole(memberId: String, relId: String, role: String)
  case class Relationship(id: String, name: String)

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()(embeddedKafkaConfig)
  }

  import collection.JavaConverters._
  test("Produce And Consume Kafka Message") {
    val producer = createProducer()
    val topic = "memberTopic"
    produceMessage(producer, topic)

    Thread.sleep(1000)
    val consumer = createConsumer()
    consumer.subscribe(Set(topic).asJava)

    //Need to do following only if we need to seek to specific offset.
    //    val partitions: util.List[PartitionInfo] = consumer.partitionsFor(topic)
    //    consumer.poll(10) //need to
    //    consumer.seek(new TopicPartition(topic, partitions.get(0).partition()), 0)

    var i = 0
    while(i < 100) {
      val records: ConsumerRecords[String, String] = consumer.poll(1000)
      records.iterator().asScala.foreach(record ⇒ {
        println(s"Received Record $record")
      })
      println(s"Received ${records.count()} messages")
      i += records.count()
    }

  }

  def createConsumer() = {
    val props = new Properties()
    props.put("application.id", UUID.randomUUID().toString)
    props.put("bootstrap.servers", bootstrapServers())
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer" + UUID.randomUUID())
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    new KafkaConsumer[String, String](props)
  }

  def createProducer() = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers())
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  def produceMessage(producer: KafkaProducer[String, String], topic: String, noOfMessages: Int = 100): Unit = {
    for (i ← 0 to noOfMessages) {
      val record = new ProducerRecord[String, String](topic, 0, "key" + i, "value")
      val metadata = producer.send(record).get()
      println(s"sent meta(partition=${metadata.partition}, offset=${metadata.offset}) \n")
    }
  }
}
