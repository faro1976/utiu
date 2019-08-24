package it.utiu.tapas.stream

import akka.kafka.ConsumerSettings
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import akka.kafka.ProducerSettings
import akka.kafka.Subscriptions
import akka.kafka.CommitterSettings
import akka.NotUsed
import akka.kafka.scaladsl.Consumer
import akka.Done
import scala.concurrent.duration._
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import akka.actor.ActorSystem
import scala.concurrent.Future
import akka.stream.scaladsl.Sink

class BTCConsumer {
  val topic="test"
  val kafkaBootstrapServers="localhost"
  val groupId = "group1"
  
  val actorSystem = ActorSystem("rob")
  
// configure Kafka consumer (1)
val kafkaConsumerSettings = ConsumerSettings(actorSystem, new IntegerDeserializer, new StringDeserializer)
  .withBootstrapServers(kafkaBootstrapServers)
  .withGroupId(groupId)
  .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  .withStopTimeout(0.seconds)  

Consumer.committableSource(kafkaConsumerSettings, Subscriptions.topics(topic))
.mapAsync(10)(msg =>
  Future.successful(msg.toString(), msg.committableOffset))
  .runWith(Sink.ignore)
 
}  
  
val control: Consumer.DrainingControl[Done] = Consumer
  .committableSource(kafkaConsumerSettings, Subscriptions.topics(topic)) // (5)
  .asSourceWithContext(_.committableOffset) // (6)
  .map(_.record)
  .map { consumerRecord => // (7)
    println(consumerRecord)
//    val movie = consumerRecord.value().parseJson.convertTo[Movie]
//    WriteMessage.createUpsertMessage(movie.id.toString, movie)
  }
//  .via(ElasticsearchFlow.createWithContext(indexName, "_doc")) // (8)
//  .map { writeResult => // (9)
//    writeResult.error.foreach { errorJson =>
//      throw new RuntimeException(s"Elasticsearch update failed ${writeResult.errorReason.getOrElse(errorJson)}")
//    }
//    NotUsed
//  }
//  .asSource // (10)
//  .map {
//    case (_, committableOffset) =>
//      committableOffset
//  }
//  .toMat(Committer.sink(CommitterSettings(actorSystem)))(Keep.both) // (11)
//  .mapMaterializedValue(Consumer.DrainingControl.apply) // (12)
  .run()
}