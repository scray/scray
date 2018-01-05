package org.scray.example

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import org.apache.spark._
import scray.cassandra.sync.CassandraImplementation._
import scray.cassandra.sync.OnlineBatchSyncCassandra
import scray.querying.sync.JobInfo
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.streaming.State
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.scray.example.data.JsonFacilityParser

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert

import scray.example.input.db.fasta.model.Facility.StateEnum
import scray.querying.sync.JobInfo
import org.apache.spark.streaming.StateSpec
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.typesafe.scalalogging.LazyLogging
import scray.example.input.db.fasta.model.Facility.TypeEnum
import org.apache.spark.streaming.Seconds
import org.scray.example.conf.JobParameter
import org.scray.example.output.GraphiteWriter
import org.spark_project.jetty.server.handler.ContextHandler.Availability
import org.scray.example.data.Facility
import java.util.Calendar
import org.scray.example.output.GraphiteForeachWriter
import org.scray.example.data.FacilityStateCounter

/**
 * Class containing all the batch stuff
 */

case class AggregationKey(facilityType: String, state: String, timeStamp: Long)
class BatchJob(@transient val sc: SparkContext, conf: JobParameter) extends LazyLogging with Serializable {

  @transient lazy val jsonParser = new JsonFacilityParser
  @transient lazy val graphiteOutput = new GraphiteForeachWriter(conf.graphiteHost)
  graphiteOutput.initConnection

  def run = {
    sc.textFile(conf.batchFilePath)
      .map(jsonParser.parse)
      .flatMap(x => x)
      .map(facility => (createAggreationKey(facility, 20), 1))
      .reduceByKey(_ + _).map(x => FacilityStateCounter(x._1.facilityType, x._1.state, x._2, x._1.timeStamp))

      .foreachPartition { availableHostsPartition =>
        {
          availableHostsPartition.foreach(graphiteOutput.process)
        }
      }
  }

  val calendar = Calendar.getInstance

  /**
   * @param requestRate Seconds between two requests
   */
  def createAggreationKey(fac: Facility[Long], requestRate: Int): AggregationKey = {
    calendar.setTimeInMillis(fac.timestamp)
    calendar.set(Calendar.MILLISECOND, 0)
    val secondInMinute = calendar.get(Calendar.SECOND) % (60 / requestRate)
    calendar.set(Calendar.SECOND, secondInMinute)
    AggregationKey(fac.facilitytype, fac.state, calendar.getTimeInMillis / 1000)
  }
}