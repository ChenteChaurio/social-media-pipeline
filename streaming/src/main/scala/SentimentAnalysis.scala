import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.kafka.clients.admin.AdminClient
import java.util.Properties

object SentimentAnalysis {

  // ── Espera a que Kafka esté listo ────────────────────────────
  def waitForKafka(brokers: String, maxRetries: Int = 10): Unit = {
    var attempts = 0
    while (attempts < maxRetries) {
      try {
        println(s"[SENTIMENT] Intentando conectar a Kafka (intento ${attempts + 1}/$maxRetries)...")
        val props = new java.util.Properties()
        props.put("bootstrap.servers", brokers)
        val admin = org.apache.kafka.clients.admin.AdminClient.create(props)
        val result = admin.listTopics()
        result.names().get(5, java.util.concurrent.TimeUnit.SECONDS)
        admin.close()
        println("[SENTIMENT] ✓ Kafka está listo")
        return
      } catch {
        case _: Exception =>
          attempts += 1
          if (attempts < maxRetries) {
            println(s"[SENTIMENT] Kafka no responde, reintentando en 2s...")
            Thread.sleep(2000)
          }
      }
    }
    println("[SENTIMENT] ✗ Timeout esperando Kafka (continuando de todos modos)")
  }

  // ── Función para escribir a PostgreSQL ──────────────────────────
  def writeToPostgres(batchDF: DataFrame, batchId: Long): Unit = {
    try {
      if (batchDF.count() > 0) {
        val props = new Properties()
        props.setProperty("driver", "org.postgresql.Driver")
        props.setProperty("user", "grafana")
        props.setProperty("password", "grafana")

        // Adaptar DataFrame a la tabla sentiment_analysis
        val dfClean = batchDF.select(
          col("text"),
          col("sentiment"),
          lit(0.8).as("confidence"),
          current_timestamp().as("timestamp")
        )

        dfClean
          .write
          .mode("append")
          .jdbc("jdbc:postgresql://postgres:5432/social_media", "sentiment_analysis", props)

        println(s"[SENTIMENT] ✓ Lote $batchId escrito a PostgreSQL (${batchDF.count()} registros)")
      }
    } catch {
      case e: Exception =>
        println(s"[SENTIMENT] ✗ Error escribiendo a PostgreSQL: ${e.getMessage}")
    }
  }

  // ── Listas de palabras para clasificar sentimiento ──────────────
  val positiveWords = Set(
    "encanta", "genial", "hermoso", "increible", "increíble",
    "gran", "mejor", "excelente", "maravilloso", "feliz",
    "bueno", "bonito", "perfecto", "fantastico", "fantástico"
  )

  val negativeWords = Set(
    "odio", "terrible", "horrible", "mal", "peor",
    "aburrido", "triste", "feo", "malo", "detesto",
    "pesimo", "pésimo", "molesto", "enojado", "furioso"
  )

  // ── Función pura: clasifica un texto ───────────────────────────
  def classify(text: String): (String, Double) = {
    val words = text.toLowerCase.split("\\s+").toSet
    val posCount = words.count(w => positiveWords.contains(w))
    val negCount = words.count(w => negativeWords.contains(w))

    if (posCount > negCount) ("positivo", 1.0)
    else if (negCount > posCount) ("negativo", -1.0)
    else ("neutro", 0.0)
  }

  def start(spark: SparkSession, kafkaBroker: String): Unit = {
    import spark.implicits._

    waitForKafka(kafkaBroker)

    // Registrar UDFs
    val sentimentUDF = udf((text: String) => classify(text)._1)
    val scoreUDF     = udf((text: String) => classify(text)._2)

    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", "raw-posts")
      .option("startingOffsets", "latest")
      .load()

    val posts = rawStream
      .select(from_json(col("value").cast("string"),
        schema = new org.apache.spark.sql.types.StructType()
          .add("user",      "string")
          .add("text",      "string")
          .add("hashtag",   "string")
          .add("likes",     "string")
          .add("timestamp", "string")
      ).as("data"))
      .select("data.*")

    // Aplica clasificación de sentimiento a cada post
    val withSentiment = posts
      .filter(col("text").isNotNull)
      .withColumn("sentiment", sentimentUDF(col("text")))
      .withColumn("score", scoreUDF(col("text")))

    // Escribe a PostgreSQL
    val postgresQuery = withSentiment
      .select("user", "text", "sentiment")
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .foreachBatch((df: DataFrame, batchId: Long) => writeToPostgres(df, batchId))
      .option("checkpointLocation", "/tmp/checkpoints/sentiment-postgres")
      .outputMode("append")
      .start()

    // Publica en sentiment-events (Kafka)
    val kafkaQuery = withSentiment
      .select(
        col("user").as("key"),
        to_json(struct(
          col("user"),
          col("hashtag"),
          col("sentiment"),
          col("score"),
          col("text")
        )).as("value")
      )
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", "sentiment-events")
      .option("checkpointLocation", "/tmp/checkpoints/sentiment")
      .outputMode("append")
      .start()

    println("[SENTIMENT] Job de análisis de sentimiento iniciado.")
    postgresQuery.awaitTermination()
  }
}
