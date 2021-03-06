package com.streaming.kafka

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerMessage, ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import com.streaming.Networks
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.{immutable, mutable}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object AkkaStreamTest {

  implicit class RichFuture[T](val f: Future[T]) extends AnyVal {
    def await: T = Await.result(f, 20.seconds)

    def done: Future[T] = Await.ready(f, 20.seconds)
  }

}

class AkkaStreamTest extends FunSuite with BeforeAndAfterAll with Matchers with Eventually {
  implicit val patience = PatienceConfig(10.seconds, 1.seconds)
  implicit val system = ActorSystem("Test")
  implicit val mat: Materializer = ActorMaterializer()

  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = createKafkaConfig


  def createKafkaConfig: EmbeddedKafkaConfig = {
    val kafkaHost = new Networks().hostname()
    val kafkaPort = 9002

    def defaultBrokerProperties(hostName: String) = {
      val brokers = s"PLAINTEXT://$hostName:$kafkaPort"
      Map("listeners" → brokers, "advertised.listeners" → brokers)
    }

    EmbeddedKafkaConfig(customBrokerProperties = defaultBrokerProperties(kafkaHost))
  }

  def bootstrapServers = {
    val kafkaHost = new Networks().hostname()
    val kafkaPort = 9002

    s"${kafkaHost}:${kafkaPort}"
  }


  val producerDefaults =
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrapServers)

  val consumerDefaults = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withGroupId("test")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withWakeupTimeout(10 seconds)
    .withMaxWakeups(10)

  override protected def beforeAll(): Unit = {
    EmbeddedKafka.start()(embeddedKafkaConfig)
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }
  test("should produce and consume kafka messages") {
    produce

    import scala.concurrent.ExecutionContext.Implicits.global
    //need to implement windowed takewhile as explained here
    //https://softwaremill.com/windowing-data-in-akka-streams/
    var buffer: mutable.Buffer[String] = mutable.Buffer.empty[String]
    Consumer.committableSource(consumerDefaults, Subscriptions.topics("topic1"))
      .via(new AccumulateWhileUnchanged(_.record.key()))
      .mapAsync(1) { messages: immutable.Seq[ConsumerMessage.CommittableMessage[String, String]] ⇒ Future {
        println(s"********************** ${messages}")
        buffer = buffer ++ messages.map(_.record.value())

      }}.runWith(Sink.ignore)
    eventually(buffer.size shouldBe 10)
    println(s"******************************** ${buffer.size}")
  }

  private def produce() = {
    val done = Source(1 to 10)
      .map { elem =>
        println(s"producing ${elem}")
        new ProducerRecord[String, String]("topic1", (elem / 5).toString, elem + "a")
      }
      .runWith(Producer.plainSink(producerDefaults))
  }
}