import org.apache.spark.sql.SparkSession

object StreamingMain extends App {

  val spark = SparkSession.builder()
    .appName("SocialMediaStreaming")
    .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val kafkaBroker = sys.env.getOrElse("KAFKA_BROKER", "localhost:9092")

  println("[STREAMING] Iniciando los 3 jobs en paralelo...")

  // Lanza cada job en su propio hilo (corren en paralelo)
  val trendThread = new Thread(() => TrendDetection.start(spark, kafkaBroker))
  val sentThread  = new Thread(() => SentimentAnalysis.start(spark, kafkaBroker))
  val alertThread = new Thread(() => AlertDetection.start(spark, kafkaBroker))

  trendThread.start()
  sentThread.start()
  alertThread.start()

  trendThread.join()
  sentThread.join()
  alertThread.join()
}
