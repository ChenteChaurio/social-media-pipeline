import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BatchAggregations extends App {

  val spark = SparkSession.builder()
    .appName("BatchAggregations")
    .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.executor.instances", "1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val outputPath = sys.env.getOrElse("OUTPUT_PATH", "/data/batch-output")
  val kafkaBroker = sys.env.getOrElse("KAFKA_BROKER", "localhost:9092")

  import spark.implicits._

  // ── Lee datos históricos de trending-hashtags ──────────────────
  val trendingDF = spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBroker)
    .option("subscribe", "trending-hashtags")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
    .select(from_json(col("value").cast("string"),
      schema = new org.apache.spark.sql.types.StructType()
        .add("hashtag",      "string")
        .add("post_count",   "long")
        .add("total_likes",  "long")
        .add("window_start", "string")
    ).as("data"))
    .select("data.*")

  // ── Lee datos históricos de sentiment-events ───────────────────
  val sentimentDF = spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBroker)
    .option("subscribe", "sentiment-events")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
    .select(from_json(col("value").cast("string"),
      schema = new org.apache.spark.sql.types.StructType()
        .add("user",      "string")
        .add("hashtag",   "string")
        .add("sentiment", "string")
        .add("score",     "double")
    ).as("data"))
    .select("data.*")

  // ── Agregación 1: Top hashtags de la última hora ───────────────
  val topHashtags = trendingDF
    .groupBy("hashtag")
    .agg(
      sum("post_count").as("total_posts"),
      sum("total_likes").as("total_likes"),
      count("*").as("windows_appeared")
    )
    .orderBy(col("total_posts").desc)
    .limit(10)

  // ── Agregación 2: Sentimiento por hashtag ─────────────────────
  val sentimentByHashtag = sentimentDF
    .groupBy("hashtag", "sentiment")
    .agg(count("*").as("count"))
    .groupBy("hashtag")
    .pivot("sentiment", Seq("positivo", "negativo", "neutro"))
    .agg(sum("count"))
    .na.fill(0)

  // ── Imprime resumen en consola ─────────────────────────────────
  println("\n===== TOP 10 HASHTAGS =====")
  topHashtags.show()

  println("\n===== SENTIMIENTO POR HASHTAG =====")
  sentimentByHashtag.show()

  println(s"\n[BATCH] Completado: ${java.time.LocalDateTime.now()}")

  spark.stop()
}