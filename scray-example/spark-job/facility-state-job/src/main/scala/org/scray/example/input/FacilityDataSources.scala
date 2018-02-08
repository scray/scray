package org.scray.example.input



import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.OffsetRange
import org.scray.example.data.Facility
import org.scray.example.data.JsonFacilityParser

import com.datastax.spark.connector.toSparkContextFunctions
import scray.jdbc.sync.OnlineBatchSyncJDBC
import scray.jdbc.sync.JDBCDbSession
import scray.jdbc.sync.OnlineBatchSyncJDBC
import scray.querying.sync.JobInfo
import scray.jdbc.sync.OnlineBatchSyncJDBC
import scray.jdbc.sync.OnlineBatchSyncJDBC
import scala.util.Failure
import scala.util.Success
import com.typesafe.scalalogging.LazyLogging


object FacilityDataSources extends LazyLogging {
  
  def getFacilityFromCassandraDb(sc: SparkContext, keyspace: String, table: String): Option[RDD[Facility[Long]]] = {
    val facilities = sc.cassandraTable(keyspace, table).map { row => {
        Facility(row.getString("type"), row.getString("state"), row.getLong("time"))
      }
    }
    
    Some(facilities)
  }
  
  @transient lazy val jsonParser = new JsonFacilityParser
  def getFacilityFromTextFile(sc: SparkContext, filePath: String): Option[RDD[Facility[Long]]] = {
    val file = sc.textFile(filePath)
      .map(jsonParser.parse)
      .flatMap(x => x)
      
     Some(file)
  }
  

    def getKafkaFacilitySource(
        sc: SparkContext,
        kafkaTopic: String,
        offsetRanges: Array[OffsetRange],
        kafkaBootstrapServers: String,
        syncApi: OnlineBatchSyncJDBC,
        jobInfo: JobInfo[java.sql.PreparedStatement,java.sql.PreparedStatement,java.sql.ResultSet]
  ): Option[RDD[Facility[Long]]] = {

      val onlineStartPoints = syncApi.getRunningOnlineJob(jobInfo)
      .map( slot => 
        syncApi.getOnlineStartPointAsString(jobInfo, slot.get)
        ) match {
        case Success(points) => Some(points)
        case Failure(ex) => logger.warn(s"Unable to query online start points from database. Exception ${ex} ${ex.printStackTrace()}"); None
      }
 
      if(onlineStartPoints.isDefined) {
        
        val startPointsMap = onlineStartPoints.get.foldLeft(Map[String, Long]()){(acc, onlineStartpoint) =>
          
          val endPossition = KafkaEndPossition.fromJson(onlineStartpoint.startPoint).get
          val key = endPossition.topic + endPossition.partition 
          acc.+((key, endPossition.offset))          
        }
        
        
        if(startPointsMap.keySet.size < 1) {
          logger.error(s"No KafkaEndPossitions in database found")
        }
      
        val offsetReader = new KafkaOffsetReader(kafkaBootstrapServers)
  
        val startPossitions = offsetReader.getCurrentKafkaLowestOffsets(kafkaTopic)
        .foldLeft(List[OffsetRange]())(
          (acc, startPos) => {
              startPointsMap.get(startPos.topic+startPos.partition) match {
                case Some(kafkaStartOffset) => OffsetRange(startPos.topic, startPos.partition, startPos.offset, kafkaStartOffset) :: acc
                case None  => {
                  logger.error(s"No matching kafka start value for topic ${startPos.topic} and partition ${startPos.partition} found. Awailable keys: ${startPointsMap.keySet.map(print)}")
                  startPointsMap.keySet.map(println)
                    acc
                }
            }  
          }
        )
  
         val kafkaParams: java.util.Map[java.lang.String, Object] = Map[String, Object](
            "bootstrap.servers" -> kafkaBootstrapServers,
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "group.id" -> ("FacilityStateJobKafkaBatch" + System.currentTimeMillis())
            ).asJava
      
          val facilitiesRddAsString: org.apache.spark.rdd.RDD[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]] = KafkaUtils.createRDD[String, String](sc, kafkaParams, startPossitions.toArray, LocationStrategies.PreferConsistent)
  
          Some(facilitiesRddAsString.map{facility => jsonParser.parse(facility.value())}.flatMap(x => x))
      } else {
        logger.warn("No online start points fund. Return None")
        None
      }
  }
}