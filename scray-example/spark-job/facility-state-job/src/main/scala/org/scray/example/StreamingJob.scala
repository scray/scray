package org.scray.example

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
import org.scray.example.output.GraphiteForeachWriter
import org.scray.example.data.Facility
import org.scray.example.data.FacilityStateCounter
import scala.collection.mutable.HashMap

case class FacilityInWindow(facilityType: String, state: String, windowStartTime: Long)

@SerialVersionUID(1000L)
class StreamingJob(@transient val ssc: StreamingContext, conf: JobParameter) extends LazyLogging with Serializable {

  lazy val jsonParser = new JsonFacilityParser
  lazy val graphiteWriter = new GraphiteForeachWriter(conf.graphiteHost)

  def runTuple[T <: org.apache.spark.streaming.dstream.DStream[ConsumerRecord[String, String]]](dstream: T) = {

    val facilities = dstream.flatMap(facilitiesAsJson => {
      jsonParser.parse(facilitiesAsJson.value())           // Parse json data and create Facility objects
        .map(fac => (fac.facilitytype + fac.state, fac))   // Create key value pairs
    })
      .mapWithState(StateSpec.function(createWindowKey _)) // Add window time stamp to key
      .map(facilityWindow => (facilityWindow, 1))

    facilities.reduceByKey(_ + _)                         // Count all elements in same window
      .mapWithState(StateSpec.function(updateWindows _))  // Add counter from previous batch
      .foreachRDD { rdd =>                                // Write counted data to graphite
        rdd.foreachPartition { availableHostsPartition =>
          {
            availableHostsPartition.foreach(graphiteWriter.process)
          }
        }
      }
  }

  /**
   * Group all facilies together where distance < maxDistance
   */
  def createWindowKey(
    batchTime: Time,
    facilityKey: String,
    facilityIn: Option[Facility[Long]],
    state: State[Long]): Option[FacilityInWindow] = {

    val facility = facilityIn.getOrElse(Facility("", "", 0L))
    val windowStartTime = state.getOption().getOrElse(0L)

    val distance = Math.abs(facility.timestamp - windowStartTime)

    if (distance > conf.maxDistInWindow) {
      state.update(facility.timestamp) // Start new window if distance is > maxDistInWindow
    }

    if (facility.timestamp == 0) {
      None
    } else {
      Some(FacilityInWindow(facility.facilitytype, facility.state, windowStartTime / 1000))
    }
  }

  def updateWindows(
    batchTime: Time,
    facilityGroup: FacilityInWindow,
    count: Option[Int],
    state: State[Int]): Option[FacilityStateCounter] = {

    val newCount = state.getOption().getOrElse(0) + count.getOrElse(0)
    state.update(newCount)

    Some(FacilityStateCounter(facilityGroup.facilityType, facilityGroup.state, newCount, facilityGroup.windowStartTime))
  }
}
