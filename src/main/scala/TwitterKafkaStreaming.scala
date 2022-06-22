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

    // create a twitter object that will allow twitter4j to access Tweeter API on my behalf
    val twitterConfig : ConfigurationBuilder = new ConfigurationBuilder()
      .setOAuthConsumerKey(consumer_key)
      .setOAuthConsumerSecret(consumer_secret)
      .setOAuthAccessToken(access_token)
      .setOAuthAccessTokenSecret(access_token_secret)
      .setDebugEnabled(true)
      .setJSONStoreEnabled(true)

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

}
