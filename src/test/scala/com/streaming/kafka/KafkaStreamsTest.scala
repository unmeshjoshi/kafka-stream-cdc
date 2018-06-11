package com.streaming.kafka

import java.io.ByteArrayOutputStream
import java.util
import java.util.concurrent.Future
import java.util.{Properties, UUID}

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.lightbend.kafka.scala.streams.{DefaultSerdes, StreamsBuilderS}
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}
import com.streaming.Networks
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.{Consumed, KafkaStreams}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class KafkaStreamsTest extends FunSuite with BeforeAndAfterAll with Matchers with Eventually with KafkaTest {
  implicit val system = ActorSystem("Test")
  implicit val mat: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  override protected def beforeAll(): Unit = {
    EmbeddedKafka.start()(embeddedKafkaConfig)
  }

  private def startConsumer() = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers())
    props.put("application.id",  UUID.randomUUID().toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    implicit val consumed = Consumed.`with`(DefaultSerdes.stringSerde, DefaultSerdes.byteArraySerde)

    val builder = new StreamsBuilderS
    val memberStream = builder.stream[String, Array[Byte]]("memberTopic")

    val mappedStream = memberStream.map((k, v) ⇒ {
      val value = AvroInputStream.binary[Member](v)
      value.close()
      val member = value.iterator.toList.head
      (new String(k), member)
    })
      .print(Printed.toSysOut())


    builder.build()

    new KafkaStreams(builder.build(), props).start()
  }

  case class Member(id: String, name: String)

  case class MemberRole(memberId: String, relId: String, role: String)

  test("test avro serdes") {
    val member = Member("1", "$")
    val seriliasedMember = avroSerialise(member)
    val deserialisedMember = deserialise(seriliasedMember)

    assert(member == deserialisedMember)
  }


  private def deserialise(ser: Array[Byte]) = {
    AvroInputStream.binary[Member](ser).iterator.toList.head
  }

  private def avroSerialise(member:Member) = {
    val os = new ByteArrayOutputStream()
    val avroStream = AvroOutputStream.binary[Member](os)
    avroStream.write(member)
    avroStream.close()
    os.close()
    assert(os.toByteArray.length > 0)
    os.toByteArray
  }

  test("Produce three table details on three topics") {



    startConsumer()

    val producer: KafkaProducer[String, Array[Byte]] = createProducer
    for (i ← 0 to 10) {

      var member = Member("1", "Member1")

      val os = new ByteArrayOutputStream()
      val avroStream = AvroOutputStream.binary[Member](os)
      avroStream.write(member)
      avroStream.close()
      os.close()
      val array = os.toByteArray


      val data = new ProducerRecord[String, Array[Byte]]("memberTopic", "key", os.toByteArray)
      val value: Future[RecordMetadata] = producer.send(data)
      println(value.get().serializedValueSize())
    }
    Thread.sleep(10000)

  }

  private def createProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers())
    props.put("application.id", UUID.randomUUID().toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new KafkaProducer[String, Array[Byte]](props)
    producer
  }

  case class Relationship(id: String, name: String)
}
