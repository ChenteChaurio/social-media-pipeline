import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object TrendDetection {

  def start(spark: SparkSession, kafkaBroker: String): Unit = {
    import spark.implicits._

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
      .withColumn("event_time",
        to_timestamp((col("timestamp").cast("long") / 1000).cast("long")
          .cast("timestamp")))

    // Ventana de 30 segundos: cuenta hashtags (programación funcional pura)
    val trends = posts
      .filter(col("hashtag").isNotNull)
      .groupBy(
        window(col("event_time"), "30 seconds", "10 seconds"),
        col("hashtag")
      )
      .agg(
        count("*").as("post_count"),
        sum(col("likes").cast("int")).as("total_likes")
      )
      .filter(col("post_count") > 5)  // solo hashtags con tracción
      .orderBy(col("post_count").desc)

    // Publica en trending-hashtags
    val query = trends
      .select(
        col("hashtag").as("key"),
        to_json(struct(
          col("hashtag"),
          col("post_count"),
          col("total_likes"),
          col("window.start").as("window_start"),
          col("window.end").as("window_end")
        )).as("value")
      )
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", "trending-hashtags")
      .option("checkpointLocation", "/tmp/checkpoints/trending")
      .outputMode("update")
      .start()

    query.awaitTermination()
  }
}
