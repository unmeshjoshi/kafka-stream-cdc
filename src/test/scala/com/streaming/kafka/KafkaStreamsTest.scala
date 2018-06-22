package com.streaming.kafka

import java.io.ByteArrayOutputStream
import java.util
import java.util.concurrent.Future
import java.util.{Properties, UUID}

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.lightbend.kafka.scala.streams.{DefaultSerdes, KStreamS, StreamsBuilderS}
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}
import com.streaming.Networks
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{Printed, Produced, Serialized, SessionWindows}
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
    props.put("commit.interval.ms", "1000")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    implicit val consumed = Consumed.`with`(DefaultSerdes.stringSerde, DefaultSerdes.byteArraySerde)
    implicit val produced = Produced.`with`(DefaultSerdes.stringSerde, DefaultSerdes.byteArraySerde)

    val stringSerde: Serde[String] = Serdes.String()
    val byteArraySerde: Serde[Array[Byte]] = Serdes.ByteArray()
    implicit val serialized: Serialized[String, Array[Byte]] = Serialized.`with`(stringSerde, byteArraySerde)

    val builder = new StreamsBuilderS
    val memberStream: KStreamS[String, Array[Byte]] = builder.stream[String, Array[Byte]]("memberTopic")

    val initialValue: () ⇒ Array[Byte] = () ⇒ Array.emptyByteArray
    val aggregateFunction: (String, Array[Byte], Array[Byte]) ⇒ Array[Byte] = (str, arr, lst) ⇒ {
      println(arr)
      lst ++ arr
    }

    val merger: (String, Array[Byte], Array[Byte]) ⇒ Array[Byte] = (str, arr, lst) ⇒ {
      println("merger")
      lst ++ arr
    }

    memberStream.groupByKey.aggregate(initialValue, aggregateFunction).mapValues(array ⇒ {
      val value = AvroInputStream.binary[Member](array)
      println("*************************************************" + value)
      val member = value.iterator.toList.head
      member
      array
    }).toStream.foreach((action, array) ⇒ {
      println(array)
    })

    new KafkaStreams(builder.build(), props).start()
  }

  case class Member(id: String, name: String)

  case class MemberRole(memberId: String, relId: String, role: String)


  private def createProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers())
    props.put("application.id", UUID.randomUUID().toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new KafkaProducer[String, Array[Byte]](props)
    producer
  }

  test("Produce three table details on three topics") {
    val producer: KafkaProducer[String, Array[Byte]] = createProducer
    for (i ← 0 to 100) {

      var member = Member(s"key${i % 2}", s"Member${i}")
      val ba = avroSerialise(member)
      val data = new ProducerRecord[String, Array[Byte]]("memberTopic", s"key${i % 2}", ba)
      val value: Future[RecordMetadata] = producer.send(data)
      println(value.get().serializedValueSize()) //blocking send
    }

    startConsumer()


    Thread.sleep(100000)

  }

  case class Relationship(id: String, name: String)
//
//  test("test avro serdes") {
//    val member = Member("1", "$")
//    val seriliasedMember = avroSerialise(member)
//    val deserialisedMember = deserialise(seriliasedMember)
//
//    assert(member == deserialisedMember)
//  }


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

  private def messages() = {
    var lst:List[Member] = List[Member]()
    for(i ← 0 to 10) {
      val key = s"key${i}"
      var member = Member(s"${i % 2}", s"Member${i}")
      lst = member :: lst
    }
    lst
  }

  private def produceMessages(producer:KafkaProducer[String, Array[Byte]], messages:List[Member]) = {
    messages.foreach(member ⇒ {
      val array = avroSerialise(member)
      val data = new ProducerRecord[String, Array[Byte]]("memberTopic", member.id, array)
      val value: Future[RecordMetadata] = producer.send(data)
      println(value.get().serializedValueSize())
    })
  }
}
