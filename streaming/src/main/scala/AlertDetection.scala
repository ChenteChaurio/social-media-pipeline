import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import java.util.Properties
import javax.mail._
import javax.mail.internet._
import org.apache.kafka.clients.admin.AdminClient

object AlertDetection {

  // ── Espera a que Kafka esté listo ────────────────────────────
  def waitForKafka(brokers: String, maxRetries: Int = 10): Unit = {
    var attempts = 0
    while (attempts < maxRetries) {
      try {
        println(s"[ALERTA] Intentando conectar a Kafka (intento ${attempts + 1}/$maxRetries)...")
        val props = new java.util.Properties()
        props.put("bootstrap.servers", brokers)
        val admin = org.apache.kafka.clients.admin.AdminClient.create(props)
        val result = admin.listTopics()
        result.names().get(5, java.util.concurrent.TimeUnit.SECONDS)
        admin.close()
        println("[ALERTA] ✓ Kafka está listo")
        return
      } catch {
        case _: Exception =>
          attempts += 1
          if (attempts < maxRetries) {
            println(s"[ALERTA] Kafka no responde, reintentando en 2s...")
            Thread.sleep(2000)
          }
      }
    }
    println("[ALERTA] ✗ Timeout esperando Kafka (continuando de todos modos)")
  }

  // ── Función para escribir a PostgreSQL ──────────────────────────
  def writeToPostgres(batchDF: DataFrame, batchId: Long): Unit = {
    try {
      if (batchDF.count() > 0) {
        val props = new Properties()
        props.setProperty("driver", "org.postgresql.Driver")
        props.setProperty("user", "grafana")
        props.setProperty("password", "grafana")

        // Adaptar DataFrame a la tabla alert_events
        val dfClean = batchDF.select(
          col("user").as("username"),
          col("text"),
          lit("Contenido sensible detectado").as("alert_reason"),
          current_timestamp().as("timestamp")
        )

        dfClean
          .write
          .mode("append")
          .jdbc("jdbc:postgresql://postgres:5432/social_media", "alert_events", props)

        println(s"[ALERTA] ✓ Lote $batchId escrito a PostgreSQL (${batchDF.count()} registros)")
      }
    } catch {
      case e: Exception =>
        println(s"[ALERTA] ✗ Error escribiendo a PostgreSQL: ${e.getMessage}")
    }
  }

  // ── Palabras clave sensibles (suicidio, autolesión) ────────────
  val sensitiveKeywords = List(
    "suicidio", "quiero morir", "no quiero vivir",
    "hacerme daño", "depresion grave", "fin de todo",
    "me quiero morir", "no vale la pena vivir"
  )

  // ── Función pura: detecta contenido sensible ──────────────────
  def isSensitive(text: String, flagged: String): Boolean = {
    val lower = text.toLowerCase
    flagged == "true" || sensitiveKeywords.exists(kw => lower.contains(kw))
  }

  // ── Envío de correo vía Gmail SMTP ────────────────────────────
  def sendAlertEmail(user: String, text: String): Unit = {
    val gmailUser = sys.env.getOrElse("GMAIL_USER", "")
    val gmailPass = sys.env.getOrElse("GMAIL_PASSWORD", "")
    val alertTo   = sys.env.getOrElse("ALERT_EMAIL", "")

    if (gmailUser.isEmpty || gmailPass.isEmpty || alertTo.isEmpty) {
      println(s"[ALERTA] ⚠ Correo no configurado. Alerta solo en log:")
      println(s"[ALERTA] Usuario: $user | Texto: $text")
      return
    }

    try {
      val props = new Properties()
      props.put("mail.smtp.auth", "true")
      props.put("mail.smtp.starttls.enable", "true")
      props.put("mail.smtp.host", "smtp.gmail.com")
      props.put("mail.smtp.port", "587")

      val session = Session.getInstance(props, new Authenticator {
        override def getPasswordAuthentication: PasswordAuthentication =
          new PasswordAuthentication(gmailUser, gmailPass)
      })

      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress(gmailUser))
      message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(alertTo).asInstanceOf[Array[Address]])
      message.setSubject("[ALERTA] Contenido sensible detectado")
      message.setText(
        s"""Se detectó contenido potencialmente sensible en la plataforma.
           |
           |Usuario: $user
           |Contenido: $text
           |Hora: ${java.time.LocalDateTime.now()}
           |
           |Este es un mensaje automático del sistema de monitoreo.
           |Si conoces a esta persona, considera contactar a las líneas de ayuda.
           |""".stripMargin
      )

      Transport.send(message)
      println(s"[ALERTA] ✓ Correo enviado a $alertTo por post de $user")
    } catch {
      case e: Exception =>
        println(s"[ALERTA] ✗ Error enviando correo: ${e.getMessage}")
    }
  }

  def start(spark: SparkSession, kafkaBroker: String): Unit = {
    import spark.implicits._

    waitForKafka(kafkaBroker)

    val sensitiveUDF = udf((text: String, flagged: String) =>
      isSensitive(text, flagged))

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
          .add("sensitive", "string")
      ).as("data"))
      .select("data.*")

    // Filtra solo posts sensibles
    val alerts = posts
      .filter(col("text").isNotNull)
      .filter(sensitiveUDF(col("text"), coalesce(col("sensitive"), lit("false"))))

    // Escribe a PostgreSQL
    val postgresQuery = alerts
      .select("user", "text")
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .foreachBatch((df: DataFrame, batchId: Long) => writeToPostgres(df, batchId))
      .option("checkpointLocation", "/tmp/checkpoints/alerts-postgres")
      .outputMode("append")
      .start()

    // Publica alertas en Kafka
    val kafkaQuery = alerts
      .select(
        col("user").as("key"),
        to_json(struct(
          col("user"),
          col("text"),
          col("timestamp")
        )).as("value")
      )
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", "alert-events")
      .option("checkpointLocation", "/tmp/checkpoints/alerts")
      .outputMode("append")
      .start()

    // Job paralelo que consume las alertas para enviar correos
    val emailQuery = alerts
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
        val alertRows = batchDF.collect()
        alertRows.foreach { row =>
          val user = row.getAs[String]("user")
          val text = row.getAs[String]("text")
          sendAlertEmail(user, text)
        }
        if (alertRows.nonEmpty) {
          println(s"[ALERTA] Batch $batchId: ${alertRows.length} alertas procesadas.")
        }
      }
      .option("checkpointLocation", "/tmp/checkpoints/alerts-email")
      .start()

    println("[ALERTA] Job de detección de alertas iniciado.")
    kafkaQuery.awaitTermination()
  }
}
