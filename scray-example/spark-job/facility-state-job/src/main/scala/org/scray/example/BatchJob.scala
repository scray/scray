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

import scray.querying.sync.JobInfo
import org.apache.spark.streaming.StateSpec
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.streaming.Seconds
import org.scray.example.conf.JobParameter
import org.spark_project.jetty.server.handler.ContextHandler.Availability
import org.scray.example.data.Facility
import java.util.Calendar
import org.scray.example.output.GraphiteForeachWriter
import org.scray.example.data.FacilityStateCounter
import org.scray.example.input.FacilityDataSources
import org.apache.spark.rdd.RDD


case class AggregationKey(facilityType: String, state: String, timeStamp: Long)
class BatchJob(val facilityData: RDD[Facility[Long]], conf: JobParameter) extends LazyLogging with Serializable {

  @transient lazy val graphiteOutput = new GraphiteForeachWriter(conf.graphiteHost)

  def run = {
      facilityData   
      .map(facility => (createAggreationKey(facility), 1))
      .reduceByKey(_ + _)
      .map{case (facilityKey, count) => FacilityStateCounter(facilityKey.facilityType, facilityKey.state, count, facilityKey.timeStamp)}
      .foreachPartition { availableHostsPartition =>
        {
          availableHostsPartition.foreach(graphiteOutput.process)
        }
      }
  }

  val calendar = Calendar.getInstance
  def createAggreationKey(fac: Facility[Long]): AggregationKey = {
    calendar.setTimeInMillis(fac.timestamp)
    AggregationKey(fac.facilitytype, fac.state, calendar.getTimeInMillis / 1000)
  }
  
}