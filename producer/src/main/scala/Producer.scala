import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json.Json
import java.util.Properties
import scala.util.Random

object Producer extends App {

  // ── Configuración ──────────────────────────────────────────────
  val broker  = sys.env.getOrElse("KAFKA_BROKER", "localhost:9092")
  val topic   = "raw-posts"
  val random  = new Random()

  val props = new Properties()
  props.put("bootstrap.servers", broker)
  props.put("key.serializer",   classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](props)

  // ── Datos simulados ────────────────────────────────────────────
  val users = List(
    "maria_co", "pedro99", "ana_bogota", "carlos_med", "lucia_cali",
    "juan_bar", "sofia_b", "diego_co", "valentina_r", "andres_m"
  )

  val normalHashtags = List(
    "#Colombia", "#Futbol", "#Musica", "#Tecnologia", "#Noticias",
    "#Clima", "#Comida", "#Viajes", "#Deportes", "#Arte"
  )

  val sensitiveKeywords = List(
    "suicidio", "quiero morir", "no quiero vivir",
    "hacerme daño", "depresion grave", "fin de todo"
  )

  val normalTexts = List(
    "Hermoso dia hoy en %s!",
    "No me puedo creer lo de %s",
    "Alguien más viendo %s hoy?",
    "Gran partido de %s esta noche",
    "Me encanta todo sobre %s"
  )

  // ── Funciones puras ────────────────────────────────────────────
  def randomFrom[A](list: List[A]): A =
    list(random.nextInt(list.length))

  def buildNormalPost(user: String): Map[String, Any] = {
    val hashtag = randomFrom(normalHashtags)
    val text    = randomFrom(normalTexts).format(hashtag)
    Map(
      "user"      -> user,
      "text"      -> text,
      "hashtag"   -> hashtag,
      "likes"     -> random.nextInt(500),
      "timestamp" -> System.currentTimeMillis(),
      "sensitive" -> false
    )
  }

  def buildSensitivePost(user: String): Map[String, Any] = {
    val keyword = randomFrom(sensitiveKeywords)
    Map(
      "user"      -> user,
      "text"      -> s"Me siento muy mal, $keyword",
      "hashtag"   -> "#ayuda",
      "likes"     -> 0,
      "timestamp" -> System.currentTimeMillis(),
      "sensitive" -> true
    )
  }

  def buildPost(user: String): Map[String, Any] =
    if (random.nextInt(20) == 0) buildSensitivePost(user)  // 5% sensibles
    else buildNormalPost(user)

  def postToJson(post: Map[String, Any]): String =
    Json.stringify(Json.toJson(post.map { case (k, v) =>
      k -> v.toString
    }))

  def sendPost(post: Map[String, Any]): Unit = {
    val json   = postToJson(post)
    val record = new ProducerRecord[String, String](topic, post("user").toString, json)
    producer.send(record)
    println(s"[PRODUCER] Enviado: ${post("user")} → ${post("text").toString.take(50)}")
  }

  // ── Loop principal (funcional con recursión) ───────────────────
  def run(count: Int): Unit = {
    val user = randomFrom(users)
    val post = buildPost(user)
    sendPost(post)
    Thread.sleep(200) // 5 posts por segundo
    run(count + 1)
  }

  println("[PRODUCER] Iniciando productor de posts...")
  run(0)
}
