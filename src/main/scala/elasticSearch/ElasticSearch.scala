package elasticSearch
import java.util.{Arrays, Properties}

import scala.collection.JavaConverters._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}

class ElasticSearch {

  private def getKafkaConsumer ( props : Properties ) : KafkaConsumer[String, String] = {
    println("inside getKafkaConsumer")
    new KafkaConsumer[String, String](props)
  }

  private def populateElasticSearchIndex(id : Long,
                                 createdAt : Long,
                                 screenName : String,
                                 text: String,
                                 client : ElasticClient,
                                 esIndex: String) = {

    println("inside populateElasticSearchIndex")
    import com.sksamuel.elastic4s.ElasticDsl._
    client.execute{
      indexInto(esIndex).fields("id" -> id,
        "createdAt" -> createdAt,
        "screenName" -> screenName,
        "text" -> text).
        refresh(RefreshPolicy.Immediate)
    }.await

    println("exiting populateElasticSearchIndex")
  }

  private def decomposeKafkaTopic(topic : String, consumer : KafkaConsumer[String, String],
                        client: ElasticClient,
                        esIndexName : String): Unit = {
    println("inside decomposeKafkaTopic")
    consumer.subscribe(Arrays.asList(topic))

    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator) {
        val id: Long = data.key().toLong

        val tweetString = data.value().split("~~")

        val createdAt: Long = tweetString(0).toLong
        val screenName: String = tweetString(1)
        val text: String = tweetString(2)

        println(id + ":" + createdAt + ":" + screenName + ":" + text)

        populateElasticSearchIndex(id,
          createdAt,
          screenName,
          text,
          client,
          esIndexName)
      }
    }
    println("exiting decomposeKafkaTopic")
  }

  private def createElasticSearchIndex(client : ElasticClient, indexName : String): Unit = {
    import com.sksamuel.elastic4s.ElasticDsl._
    println("inside createElasticSearchIndex")
    val mapDefinition = new MappingDefinition {
      properties(
        longField("id"),
        longField("createdAt"),
        textField("screenName"),
        textField("text"))
    }

    val resp = client.execute{
      createIndex(indexName).mapping(mapDefinition)
    }.await
    println("exiting createElasticSearchIndex")
  }

  def init (topic : String) : Unit = {
    println("inside init")
    val props = new Properties()
    val bootStrapServers: String = "localhost:9092"
    val consumerGroupId = "first_application_group"
    val indexName = "tweets"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
    val consumer = getKafkaConsumer(props)

    val client = ElasticClient(JavaClient(ElasticProperties(s"http://${sys.env.getOrElse("ES_HOST", "127.0.0.1")}:" +
      s"${sys.env.getOrElse("ES_PORT", "9200")}")))

    createElasticSearchIndex(client, indexName)
    decomposeKafkaTopic(topic, consumer, client, indexName)
    println("exiting init")
  }
}

object ElasticSearch extends App {
  if (args.length > 0) {
    val topic: String = args(0)
    val elasticSearch = new ElasticSearch()
    elasticSearch.init(topic)
  }
}

