import sbt._

object Dependencies {

  val Version = "0.1-SNAPSHOT"
  val KafkaStreaming = Seq(
    Libs.`junit` % Test,
    Libs.`junit-interface` % Test,
    Libs.`mockito-core` % Test,
    Libs.`scalatest` % Test,
    Kafka.`akka-stream-kafka`,
    Kafka.`kafkaStreamsScala`,
    Kafka.`scalatest-embedded-kafka` % Test
  )
}