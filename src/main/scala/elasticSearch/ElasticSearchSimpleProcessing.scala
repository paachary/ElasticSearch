package elasticSearch
import java.time.Duration
import java.util
import java.util.Properties

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * This class is responsible to read messages from a kafka topic and populate an elasticsearch index
 */
class ElasticSearchSimpleProcessing {

  /**
   * Function returns an instance of a KafkaConsumer for given set of configuration properties.
   * @param props - Properties containing configuration for kafka consumer
   * @return - an instance of KafkaConsumer
   */
  private def getKafkaConsumer ( props : Properties ) : KafkaConsumer[String, String] = {
    new KafkaConsumer[String, String](props)
  }

  /**
   * Function inserting data into the elasticsearch index
   * @param id         = unique identifier for a message (Long)
   * @param createdAt  = Unix time (Long)
   * @param screenName = Username (String)
   * @param text       = Actual Text msg (String)
   * @param client     = ElasticSearchSimpleProcessing client (ElasticClient)
   * @param esIndex    = ElasticSearchSimpleProcessing Index (String)
   */
  private def populateElasticSearchIndex(id : Long,
                                 createdAt : Long,
                                 screenName : String,
                                 text: String,
                                 client : ElasticClient,
                                 esIndex: String): Unit = {

    import com.sksamuel.elastic4s.ElasticDsl._
    client.execute{
      indexInto(esIndex).fields(
        "id" -> id,
        "createdAt" -> createdAt,
        "screenName" -> screenName,
        "text" -> text).id(id.toString).
        refresh(RefreshPolicy.Immediate)
    }.await
  }

  /**
   * Function to split the Kafka message and populate the elasticsearch index
   * @param topic       = Kafka topic name (String)
   * @param consumer    = Kafka consumer ( KafkaConsumer)
   * @param client      = ElasticSearchSimpleProcessing client (ElasticClient)
   * @param esIndexName = ElasticSearchSimpleProcessing index (String)
   */
  private def decomposeKafkaTopic(topic : String,
                                  consumer : KafkaConsumer[String, String],
                                  client: ElasticClient,
                                  esIndexName : String): Unit = {
    val logger = LoggerFactory.getLogger(ElasticSearchSimpleProcessing.getClass.getName+"_decomposeKafkaTopic")

    consumer.subscribe(util.Arrays.asList(topic))

    while (true) {
      val records: ConsumerRecords[String, String]
      = consumer.poll(Duration.ofMillis(100))
      val count = records.count()
      logger.info("Received " + count + " records")

      if (count != 0) {
        records.iterator().asScala.foreach {
          data: ConsumerRecord[String, String] =>
            val id: Long = data.key().toLong
            val tweetString = data.value().split("~~")

            val createdAt: Long = tweetString(0).toLong
            val screenName: String = tweetString(1)
            val text: String = tweetString(2)

            logger.info("Received id = " + id)

            populateElasticSearchIndex(id,
              createdAt,
              screenName,
              text,
              client,
              esIndexName)
        }
        logger.info("Committing the offsets")
        consumer.commitSync()
        logger.info("Offsets have been committed")

        try {
          Thread.sleep(1000)
        } catch {
          case ex: InterruptedException => logger.info("interrupted exception")
        }
      }
    }
  }

  /**
   * Function to create the elasticsearch index
   * @param client      = ElasticSearchSimpleProcessing client (ElasticClient)
   * @param indexName   = ElasticSearchSimpleProcessing index (String)
   */
  private def createElasticSearchIndex(client : ElasticClient, indexName : String): Unit = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val mapDefinition = new MappingDefinition {
      properties(
        longField("id"),
        longField("createdAt"),
        textField("screenName"),
        textField("text"))
    }
    client.execute{
      createIndex(indexName).mapping(mapDefinition)
    }.await
  }

  /**
   * Function to
   *   1. setup KafkaConsumer configuration properties
   *   2. initialize elasticsearch client
   *   3. subscribe to kafkatopic
   *   4. populate the elasticsearch index
   * @param topic  = Kafka topic (String)
   */
  def init (topic : String) : Unit = {
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
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10")

    val consumer = getKafkaConsumer(props)

    val client = ElasticClient(JavaClient(ElasticProperties(s"http://${sys.env.getOrElse("ES_HOST", "127.0.0.1")}:" +
      s"${sys.env.getOrElse("ES_PORT", "9200")}")))

    createElasticSearchIndex(client, indexName)
    decomposeKafkaTopic(topic, consumer, client, indexName)
  }
}

/**
 * ElasticSearchSimpleProcessing companion object to invoke the init method by
 * passing the reqiured kafkatopic as an argument
 */
object ElasticSearchSimpleProcessing extends App {
  if (args.length > 0) {
    val topic: String = args(0)
    val elasticSearchSimpleProcessing = new ElasticSearchSimpleProcessing()
    elasticSearchSimpleProcessing.init(topic)
  }
}
