import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkBigData {

  var ss: SparkSession = null

  /**
   * Function qui initialise et instancie une sesseion Spark
   * @param Env est une variable qui indique l'environnment sur lequel notre application sera déployée.
   * @return Si Env = true, l'appli est déployée en local, sinon elle se déploiera sur un cluster.
   */

  def session_spark(Env: Boolean=true): SparkSession={
    if (Env == true){
      System.setProperty("hadoop.home.dir", "C:/hadoop")
      ss = SparkSession.builder()
        .master("local[*]")
        .config("spark.sql.crossJoin.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    } else {
      ss = SparkSession.builder()
        .appName("Une application Simple")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.crossJoin.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()

    }
    ss
  }

  /** Deuxième étape: configurer le Streaming context */

  var spConf: SparkConf = null

  /**
   * function qui initialise le contexte Spark Streaming
   * @param env est l'environnement sur lequel est déployé notre application. Si true, elle deployée en local. Sinon, elle est déployée sur un cluster
   * @param duree_batch est le SParkStreamingBatchDuration ounla durée d'un micor-batch en secondes
   * @return la functoin renvoie en résultat une instance du contexte streaming
   */

  def getSparkStreamingContext(env: Boolean=true, duree_batch:Int): StreamingContext = {

    if (env){
      spConf = new SparkConf().setMaster("LocalHost[*]")
        .setAppName("Mon application streaming")
    } else {
      spConf = new SparkConf().setAppName("Mon application streaming")
    }

    val ssc = new StreamingContext(spConf, Seconds(duree_batch))
    return ssc
  }

}
