import org.apache.kafka.clients.consumer.{CommitFailedException, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.spark.streaming.kafka010.KafkaUtils._
import SparkBigData._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.LogManager
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies._

import java.util.Collections
import java.util
import java.util.Properties
import org.apache.log4j.LogManager._

import scala.collection.JavaConverters._
import java.time.Duration

object KafkaStreaming {

  var kafkaConsumerParam: Map[String, Object]=  Map(null, null)

  def getKafkaConsumerParams(kafkaBootstrapServers: String, kafkaConsumerGroupId: String, kafkaConsumerReaderOrder: String,
                     kafkaZookeeper: String, kerberosName: String): Map[String, Object] = {
    kafkaConsumerParam = Map(
      "bootstrap.servers"-> kafkaBootstrapServers,
      "group.id"-> kafkaConsumerGroupId,
      "zookeeper.hosts"-> kafkaZookeeper,
      "auto.offset.reset"-> kafkaConsumerReaderOrder,
      "enable.auto.commit"-> (false:java.lang.Boolean),
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "sasl.kerberos.service.name"-> kerberosName,
      "security.protocol"-> SecurityProtocol.PLAINTEXT
    )
    return kafkaConsumerParam
  }

  def getConsommateurKafka(kafkaBootstrapServers: String, kafkaConsumerGroupId: String,
                          kafkaConsumerReaderOrder: String,
                          kafkaZookeeper: String, kerberosName: String,
                          batchDuration:Int, kafkaTopics: String):InputDStream[ConsumerRecord[String, String]] ={

    val ssc = getSparkStreamingContext(true, batchDuration)
    val parameters = getKafkaConsumerParams("localHost:9092", "DSC",
      "latest", "", "")

    val consommateurKafka = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array("topic1"), parameters)
    )

      return consommateurKafka
  }


  /* kafka consumer developpé avec l'API Cliente Kafka ( Sans utiliser l'API Kafka.Utils) */

  def getKafkaClientConsumerParams(kafkaBootstrapServers: String, kafkaConsumerGroupId:String): Properties ={

    val props = new Properties()
    props.put("bootstrap.servers", "kafkaBootstrapServers")
    props.put("group.id", "kafkaConsumerGroupId")
    props.put("auto.offset.reset", "latest")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.Deserializer")
    props.put("value.deserializer" ,"org.apache.kafka.common.serialization.Deserializer")

    return props

  }

  def getKafkaClientConsumer(kafkaBootstrapServers: String, kafkaConsumerGroupId:String, topic_list:String):KafkaConsumer[String,String] = {

    trace_kafka.info("Instruction d'un consommateur...")
    /*Instanciation du consumer*/
    val consumer = new KafkaConsumer[String, String](
      getKafkaClientConsumerParams(kafkaBootstrapServers, kafkaConsumerGroupId))
    consumer.subscribe(Collections.singletonList(topic_list))

    /*consoomation des données: liste des messages à recupérer*/
    while(true) {
      val messages : ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(30)) /*Temps écoulé après le dernier offset*/
      if (!messages.isEmpty) {
        trace_kafka.info(s"Nombre de messages collectés dans la fenetre : " + messages.count())
        for (message <- messages.asScala) {
          println("Topic: " + message.topic() +
          ", Key: " + message.key() +
          ", Value: " + message.value() +
          ", Offset: " + message.offset() +
          ", Partition:" + message.partition())
        }
        try{
          consumer.commitAsync()
        } catch {
          case ex:CommitFailedException=>
            trace_kafka.error(" Erreur dans le commit des offsets. Kafka n'a pas reçu le jeton de reconnaissance confirmant que nous avons bien reçu les données")
        }


      }

      /*methode 2 : utiliser messages.iterator*/

    /*val messageIterateur = messages.iterator()
      while (messageIterateur.hasNext==true){
        val msg = messageIterateur.next()
        println(msg.topic() + " " + msg.key() + " " + msg.value() + " " + msg.partition())
      }
    */

    }

    consumer.close()
    return consumer

  }

  /* Configuration du Producer Kafka */

  def getKafkaProducerParams(bootstrapServers:String):Properties = {

    val props = new Properties()
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("bootstrap.servers", "bootstrapServers")
    props.put("acks", "all")
    props.put("security.protocol", "SASL_PLAINTEXT")

    return props
  }


  private val trace_kafka = LogManager.getLogger("Console")

  def getProducteurKafka(KafkaBootstrapServers:String, topic_name:String, message:String):KafkaProducer[String, String] = {

    trace_kafka.info(s"Instanciation d'une instance du producer kafka aux serveurs  ${KafkaBootstrapServers}")
    val producerParam = getKafkaProducerParams(KafkaBootstrapServers)
    val producer_kafka = new KafkaProducer[String, String](producerParam)


    trace_kafka.info(s"message à publier dans le topic " + {topic_name} + ": " + {message})
    val record_publish = new ProducerRecord[String, String](topic_name, message)

    try{
      trace_kafka.info("Publication du message")
      producer_kafka.send(record_publish)

    } catch{
      case ex:Exception =>
        trace_kafka.error(s"Erreur de la publication message dans Kafka ${ex.printStackTrace()}")
        trace_kafka.info(s"La liste des paramétres pour la connexion du producer est :" + {getKafkaProducerParams(KafkaBootstrapServers)})
    } finally{
      println("N'oubliez pas de fermer ce Producer à la fin de son utilisation")
      //producer_kafka.close()
    }

    return producer_kafka

  }


}
