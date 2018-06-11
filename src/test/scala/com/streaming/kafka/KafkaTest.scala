package com.streaming.kafka
import com.streaming.Networks
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}

trait KafkaTest {

  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = createKafkaConfig

  def kafkaHost = new Networks().hostname()
  def kafkaPort = 9002

  def createKafkaConfig: EmbeddedKafkaConfig = {
    def defaultBrokerProperties(hostName: String) = {
      val brokers = s"PLAINTEXT://$hostName:$kafkaPort"
      Map("listeners" → brokers, "advertised.listeners" → brokers)
    }

    EmbeddedKafkaConfig(customBrokerProperties = defaultBrokerProperties(kafkaHost))
  }

  def bootstrapServers() = {

    s"${kafkaHost}:${kafkaPort}"
  }
}
