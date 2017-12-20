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

import scray.example.input.db.fasta.model.Facility
import scray.example.input.db.fasta.model.Facility.StateEnum
import scray.querying.sync.JobInfo
import org.apache.spark.streaming.StateSpec
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.typesafe.scalalogging.LazyLogging
import scray.example.input.db.fasta.model.Facility.TypeEnum
import scray.example.output.GraphiteWriter
import org.apache.spark.streaming.Seconds
import org.scray.example.conf.JobParameter


case class Availability(activeCounter: Integer, inactiveCounter: Integer, unknownCounter: Integer)

@SerialVersionUID(1000L)
class StreamingJob(@transient val ssc: StreamingContext, jobInfo: JobInfo[Statement, Insert, ResultSet], conf: JobParameter) extends LazyLogging with Serializable {

  lazy val jsonParser = new JsonFacilityParser
  lazy val outputOperation = new GraphiteWriter(conf.graphiteHost)

  def runTuple[T <: org.apache.spark.streaming.dstream.DStream[ConsumerRecord[String, String]]](dstream: T) = {
   
    // Parse input data and create K, V. K ::= Equipmentnumber, V ::= State counter
    val facilities = dstream.flatMap(facilitiesAsJson => {
      val parsedFacilities = jsonParser.jsonReader(facilitiesAsJson.value())
      parsedFacilities.map(facilities =>
        facilities.map(facility => {
          (facility.getType, mapStateToCount(facility))
        }))
    }).flatMap(x => x)

    // Count all states of this batch
    val availibilityBatch = facilities.reduceByKey((a: Availability, b: Availability) => {
      Availability(
          a.activeCounter + b.activeCounter,
          a.inactiveCounter + b.inactiveCounter,
          a.unknownCounter + b.unknownCounter)
    })
    
    availibilityBatch.foreachRDD { rdd =>
      rdd.foreachPartition { availableHostsPartition =>
        availableHostsPartition.foreach { availibility =>
        
          val (facilityType, count) = availibility
          
          if(facilityType.equals(TypeEnum.ELEVATOR)) {
            if(count.inactiveCounter > 10 && count.activeCounter > 10) { 
              outputOperation.sentElevator(count.inactiveCounter, count.activeCounter)
            }
          }
          if(facilityType.equals(TypeEnum.ESCALATOR)) {
            if(count.inactiveCounter > 10 && count.activeCounter > 10) { 
              outputOperation.sentEscalator(count.inactiveCounter, count.activeCounter)
            }
          }
        }
      }
    }
  }
    

  def mapStateToCount(fac: Facility): Availability = {
     if(fac.getState == StateEnum.ACTIVE) {
       Availability(1, 0, 0)
    } else if(fac.getState == StateEnum.INACTIVE) {
        Availability(0, 1, 0)
    } else {
        Availability(0, 0, 1)
    }
  }
  
  
  def addOldAvailibility(
     batchTime: Time, 
     facId: java.lang.Long,
     batchAvailibility: Option[Availability],
     state: State[Tuple3[Long, Long, Long]]
  ): Option[Tuple2[Long, Float]] = {
    
    val (activeCount, inactiveCount, unknownCount) = state.getOption().getOrElse((0L, 0L, 0L))
    
    batchAvailibility.map( lastBatchResult => {
    state.update(
        (
            activeCount   +  lastBatchResult.activeCounter,
            inactiveCount +  lastBatchResult.inactiveCounter, 
            unknownCount  +  lastBatchResult.unknownCounter
         )
       ) 
    })
    
    val (activeCountNew, inactiveCountNew, unknownCountNew) = state.get()
    val allValues = activeCountNew + inactiveCountNew
    val av = (activeCountNew * 1f)/ allValues
    

    Some((facId, av)) 
  }
}
