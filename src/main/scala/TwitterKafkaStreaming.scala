import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.httpclient.auth._
import com.twitter.hbc.core.{Client, Constants}
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.Collections
import scala.collection.JavaConverters._
import KafkaStreaming._
import org.apache.log4j.LogManager
import org.apache.log4j.LogManager._
import twitter4j.Twitter
import twitter4j.TwitterFactory
import twitter4j._
import twitter4j.conf.{Configuration, ConfigurationBuilder}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import SparkBigData._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.Minutes



class TwitterKafkaStreaming {

  private val trace_twitter = LogManager.getLogger("console pour twitter")

  def ProducerTwitterKafkaHbc(consumer_key : String, consumer_secret : String, access_token : String, access_token_secret : String, list_hashtags : String, KafkaBootstrapServers : String, topic_hbc : String) : Unit = {

    trace_twitter.info(" Constitution d'une file de messages d'au plus 10 mille tweets...")
    val queue : BlockingQueue[String] = new LinkedBlockingQueue[String](10000)
    val auth : Authentication = new OAuth1(consumer_key, consumer_secret, access_token, access_token_secret)

    // Choisir les hashtags qui déterminent les messages d'intérêt. Il faut créer un endpoint pour indiquer les termes à traquer
    // API Client twitter n'écoutera que les messages contenant les termes recherchés

    trace_twitter.info("Recherche des tweets contenant les termes de la liste : " + list_hashtags + "...")
    val endp : StatusesFilterEndpoint = new StatusesFilterEndpoint()
    endp.trackTerms(List(list_hashtags).asJava)

    // Créer un client twitter hbc
    val constructeur_hbc : ClientBuilder = new ClientBuilder().hosts(Constants.STREAM_HOST)
      .endpoint(endp)
      .authentication(auth)
      .gzipEnabled(true)
      .processor(new StringDelimitedProcessor(queue))

    val client_hbc : Client = constructeur_hbc.build()

    // Lecture des messages

    try {
      trace_twitter.info("Établir la connexion avec la source de données à écouter")
      client_hbc.connect()
    } catch {
      case excep: Exception =>
        trace_twitter.error("Impossible de se connecter à la source de données à écouter" + excep.printStackTrace())
    }

    try {

      while (!client_hbc.isDone) {

        trace_twitter.info("Le client HBC recupére les tweets stockés lors des 15 dernieres secondes dans la queue")
        val tweets = queue.poll(15, TimeUnit.SECONDS) // lecture dans la queue pour un fénêtrage de 15 s

        trace_twitter.info("intégration au Producer kafka, des tweets récupérés lors des 15 dernières secondes")
        getProducteurKafka(KafkaBootstrapServers = KafkaBootstrapServers, topic_name = topic_hbc, tweets)
        /* Le tweets écouté viennent d'être intégrés au producer Kafka */

        println(" message Twitter : " + tweets)
      }

    } catch {
      case ex:InterruptedException =>
        trace_twitter.error("Erreur dans la récupération des messages dans la queue par le client HBC: "  + ex.printStackTrace())
    } finally {
      client_hbc.stop()
    }



  }

  //fonction qui renvoie les paramètres d'authentification à Twitter par le client Twitter4j

  private def twitter0AuthConf (consumer_key : String, consumer_secret : String, access_token : String, access_token_secret : String) : ConfigurationBuilder = {

    // create a twitter object that will allow twitter4j to access Tweeter API on my behalf
    val twitterConfig : ConfigurationBuilder = new ConfigurationBuilder()

    twitterConfig.setOAuthConsumerKey(consumer_key)
      .setOAuthConsumerSecret(consumer_secret)
      .setOAuthAccessToken(access_token)
      .setOAuthAccessTokenSecret(access_token_secret)
      .setDebugEnabled(true)
      .setJSONStoreEnabled(true)

    return twitterConfig
  }

  /**
   *
   * @param consumer_key
   * @param consumer_secret
   * @param access_token
   * @param access_token_secret
   * @param list_requette list of terms to track inside each tweet
   * @param KafkaBootstrapServers list of Ip addresses and ports for kafka cluster agents
   * @param topic_tw4j kafka topic for the twitter4j client to receive tweets
   */

  def ProducerTwitter4jKafka (consumer_key : String, consumer_secret : String, access_token : String, access_token_secret : String,  list_requette : String, KafkaBootstrapServers : String, topic_tw4j : String) : Unit = {


    trace_twitter.info(" Constitution d'une file de messages d'au plus 10 mille tweets...")

    val queue : BlockingQueue[Status] = new LinkedBlockingQueue[Status](10000)

    // create a twitter object that will allow twitter4j to access Tweeter API on my behalf, using twitter0AuthConf function

    val twitterConfig = twitter0AuthConf(consumer_key, consumer_secret, access_token, access_token_secret)

    // create an object twitter4j client using the builder twitterConfig, and get an instance

    val twitterStream = new TwitterStreamFactory(twitterConfig.build()).getInstance()

    // create the listener

    val listener : StatusListener = new StatusListener {
      override def onStatus(status: Status): Unit = {

        trace_twitter.info("Evenement d'ajout du tweet detecté. Tweet complet: " + status)
        queue.put(status)

        // Methode 1
        getProducteurKafka(KafkaBootstrapServers = KafkaBootstrapServers, topic_name = topic_tw4j, message = status.getText)
      }

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
      override def onTrackLimitationNotice(i: Int): Unit = {}
      override def onScrubGeo(l: Long, l1: Long): Unit = {}
      override def onStallWarning(stallWarning: StallWarning): Unit = {}

      override def onException(e: Exception): Unit = {
        trace_twitter.error("Erreur générée par twitter : " + e.printStackTrace())
      }
    }

    twitterStream.addListener(listener) // Add the listener to the client

    /* load tweets in the client twiiterStream. Use an object FilterQuery instead of the sample method
    because sample method will collect all tweets
     */
   // twitterStream.sample()

val query = new FilterQuery().track(list_requette)
    twitterStream.filter(query)  // to load tweets from an hashtag list named list_requette

    // Methode 2: call the Producer outside the override Onstatus method after filling the queue in. Use while loop

    while(true){
      val tweet : Status = queue.poll(15, TimeUnit.SECONDS)
      getProducteurKafka(KafkaBootstrapServers, topic_tw4j, tweet.getText)
    }
    getProducteurKafka(KafkaBootstrapServers, topic_name ="", message = "").close()
    twitterStream.shutdown()

  }

  /* Cleint Spark Streaming Kafka. Ce client se connecte a twitter et publie les infos à kafka via un producer Kafka
   */

  def producerTwitterkafkaSpark (consumer_key : String, consumer_secret : String, access_token : String, access_token_secret : String,  list_filtre : Array[String], KafkaBootstrapServers : String, topic_spark : String) : Unit = {

    // create an authorization with a protocol to connect to twitter, using twitter4j client
    val twitterConfig = twitter0AuthConf(consumer_key, consumer_secret, access_token, access_token_secret)
    val auth0 = new OAuthAuthorization(twitterConfig.build())

    // create the twitter spark streaming client using TwitterUtils class
    val client_streaming_twitter = TwitterUtils.createStream(getSparkStreamingContext(true, 15), Some(auth0))

    /* Quelques exemples de récupération de données en spark(non pas des tweets mais des jeux de données en format RDD)
     Une occasion de placer les filtres*/
    val tweetsmsg = client_streaming_twitter.flatMap(status => status.getText())
    val tweetsComplets = client_streaming_twitter.flatMap(status => (status.getText() ++ status.getContributors() ++ status.getLang()))
    val tweetsFR = client_streaming_twitter.filter(status => status.getLang() =="FR")
    val hashtags = client_streaming_twitter.flatMap(status => status.getText().split(" ").filter(word => word.startsWith("#")))
    // compter les hashtags obtenus toutes les 3 minutes
    val hashtagsCount = hashtags.window(Minutes(3))

    // to persist data in the Kafka cluster

    tweetsmsg.foreachRDD{
      (tweetsRDD, temps) =>
        if (!tweetsRDD.isEmpty()) {
          tweetsRDD.foreachPartition {
            partitionOfTweets =>
              val producer_kafka = new KafkaProducer[String, String](getKafkaProducerParams(KafkaBootstrapServers))
              partitionOfTweets.foreach {
                tweetEvent =>
                  val record_publish = new ProducerRecord[String, String](topic_spark, tweetEvent.toString)
                  producer_kafka.send(record_publish)
              }
              producer_kafka.close()
          }
        }
    }

    // Another example to persist tweets using our in-build producer

    tweetsComplets.foreachRDD {
      tweetsRDD =>
        if (!tweetsRDD.isEmpty()) {
          tweetsRDD.foreachPartition {
            partitionOfTweets =>
              partitionOfTweets.foreach {
                tweet =>
                  getProducteurKafka(KafkaBootstrapServers, topic_spark, tweet.toString)
              }
          }
        }
        getProducteurKafka(KafkaBootstrapServers, topic_name = "", message = "").close()
    }



  }



  }
