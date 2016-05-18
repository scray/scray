package scray.example.job

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.StreamingContext
import org.apache.hadoop.mapreduce.InputFormat
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel
import kafka.serializer.Decoder
import kafka.serializer.StringDecoder
import scray.example.job.serializers.GenericKafkaKryoSerializer

object StreamingDStreams {

  val KAFKA_CONSUMER_GROUP = "SparkStreamMsgTypes"

  /**
   * initializes distributed stream source for a generic Kafka source, which needs serializers to be specified
   */
  def getKafkaStreamSource[K, V, U <: Decoder[_], T <: Decoder[_]](ssc: StreamingContext, kafkaDStreamURL: Option[String], 
      kafkaTopic: Option[Map[String, Int]], storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)(
      implicit k: ClassTag[K], v: ClassTag[V], u: ClassTag[U], t: ClassTag[T]): Option[DStream[(K, V)]] = {
    val kafkaParams = Map[String, String]("zookeeper.connect" -> kafkaDStreamURL.get)
    Some(KafkaUtils.createStream[K, V, U, T](ssc, kafkaParams, kafkaTopic.get, storageLevel))
  }

  /**
   * initializes distributed stream source for a (String, String) Kafka source
   */
  def getKafkaStringSource(ssc: StreamingContext, kafkaDStreamURL: Option[String], kafkaTopic: Option[Map[String, Int]], 
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2): Option[DStream[(String, String)]] = 
    getKafkaStreamSource[String, String, StringDecoder, StringDecoder](ssc, kafkaDStreamURL, kafkaTopic, storageLevel)

  /**
   * initializes distributed stream source for a Kafka source, which serializes its objects with Kryo.writeClassAndObject
   */
  def getKafkaKryoSource[K, V](ssc: StreamingContext, kafkaDStreamURL: Option[String], kafkaTopic: Option[Map[String, Int]], 
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)(
      implicit k: ClassTag[K], v: ClassTag[V]): Option[DStream[(K, V)]] = 
    getKafkaStreamSource[K, V, GenericKafkaKryoSerializer[K], GenericKafkaKryoSerializer[V]](
        ssc, kafkaDStreamURL, kafkaTopic, storageLevel)

  /**
   * initializes distributed stream source for hdfs text files
   */
  def getTextStreamSource(ssc: StreamingContext, 
      hdfsDStreamURL: Option[String]): Option[DStream[String]] = hdfsDStreamURL.map(url => ssc.textFileStream(url))

}