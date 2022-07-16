import KafkaStreaming._
import SparkBigData._
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

object ConsommationStreaming {

  val bootstrapServers : String = ""
  val consumerGroupId : String = ""
  val consumerReaderOrder : String = ""
  val zookeeper : String =""
  val kerberosName : String =""
  val batchDuration :Int = 15
  val topics : Array[String] = Array("")

  /*schema de données */
  val schema_kafka = StructType(Array(
    StructField("Zipcode", IntegerType,true),
    StructField("ZipCodeType", StringType,true),
    StructField("City", StringType,true),
    StructField("Stata", StringType,true)
  ))

  private val trace_consommation = LogManager.getLogger("console pour twitter consommation")


  def main(args: Array[String]): Unit = {

    val ssc = getSparkStreamingContext(true, batchDuration)

    val kafkaStreams = getConsommateurKafka(bootstrapServers, consumerGroupId, consumerReaderOrder, zookeeper, kerberosName, topics, ssc)

   /*
    Premiere methode val donnees = kafkaStreams.map(record => record.value())
    ensuite faire ssc.start() et ssc.awaitTermination()
    */

    /*
    Deuxieme méthode: appliquer la méthode foreachDD sur les RDD du DStream
     */
    kafkaStreams.foreachRDD{
      rddKafka => {
        if (!rddKafka.isEmpty()) {
          //Récupération des offsets
          val offsets = rddKafka.asInstanceOf[HasOffsetRanges].offsetRanges
          trace_consommation.info("Récupération des offsets")
          // Récupération des données dans le consumerRecord
          val spark = SparkSession.builder().config(rddKafka.sparkContext.getConf).getOrCreate()
          import spark.implicits._
          val eventData = rddKafka.map(record => record.value())
          val streamDF = eventData.toDF("tweet_event")
          streamDF.createOrReplaceGlobalTempView("tweet_message")
          val kafkaDataFrame = spark.sql("select * from tweet_message")
          kafkaDataFrame.show()

          /*les RDD de kafka renvoient des fichiers json. Pour des structures json complexes il faut choisir un typage fort
          * comme ci-dessous, en utulisant un schema de données */
          val kafkaDataFrame2 = streamDF.withColumn("tweet_event", from_json(col("tweet_event"),schema_kafka))
            .select(col("tweet_event.*"))

          // Semantque de livraison et traitement exactement-une-fois. Persistance des offsets dans Kafka

          trace_consommation.info("Persistance des offsets dans Kafka en cours ...")
          rddKafka.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
          trace_consommation.info("Persistance des offsets terminée avec succès!")
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
