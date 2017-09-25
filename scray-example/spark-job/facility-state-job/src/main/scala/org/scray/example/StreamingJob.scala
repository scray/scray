//package org.scray.example
//
//import org.apache.spark.streaming.State
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.Time
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
//import org.scray.example.data.JsonFacilityParser
//
//import com.datastax.driver.core.ResultSet
//import com.datastax.driver.core.Statement
//import com.datastax.driver.core.querybuilder.Insert
//import com.typesafe.scalalogging.slf4j.LazyLogging
//
//import scray.example.input.db.fasta.model.Facility
//import scray.example.input.db.fasta.model.Facility.StateEnum
//import scray.querying.sync.JobInfo
//import org.apache.spark.streaming.StateSpec
//
//
//case class Availability(activeCounter: Integer, inactiveCounter: Integer, unknownCounter: Integer)
//
//class StreamingJob(@transient val ssc: StreamingContext, jobInfo: JobInfo[Statement, Insert, ResultSet]) extends LazyLogging with Serializable {
//
//  lazy val jsonParser = new JsonFacilityParser
//
//  def runTuple[T <: org.apache.spark.streaming.dstream.DStream[(String, String)]](dstream: T) = {
//   
//    // Parse input data and create K, V. K ::= Equipmentnumber, V ::= State counter
//    val facilities = dstream.flatMap(facilitiesAsJson => {
//      val parsedFacilities = jsonParser.jsonReader(facilitiesAsJson._2)
//      parsedFacilities.map(facilities =>
//        facilities.map(facility => {
//          (facility.getEquipmentnumber, mapStateToCount(facility))
//        }))
//    }).flatMap(x => x)
//
//    // Count all states of this batch
//    val availibilityBatch = facilities.reduceByKey((a, b) => {
//      Availability(
//          a.activeCounter + b.activeCounter,
//          a.inactiveCounter + b.inactiveCounter,
//          a.unknownCounter + b.unknownCounter)
//    })
//    
//    val availibilitySinceStart = availibilityBatch.mapWithState(StateSpec.function(addOldAvailibility _))
//    
//    availibilitySinceStart.print(200)
//
//  }
//    
//  def mapStateToCount(fac: Facility): Availability = {
//     if(fac.getState == StateEnum.ACTIVE) {
//       Availability(1, 0, 0)
//    } else if(fac.getState == StateEnum.INACTIVE) {
//        Availability(0, 1, 0)
//    } else {
//        Availability(0, 0, 1)
//    }
//  }
//  
//  
//  def addOldAvailibility(
//     batchTime: Time, 
//     facId: java.lang.Long,
//     batchAvailibility: Option[Availability],
//     state: State[Tuple3[Long, Long, Long]]
//  ): Option[Tuple2[Long, Float]] = {
//    
//    val (activeCount, inactiveCount, unknownCount) = state.getOption().getOrElse((0L, 0L, 0L))
//    
//    batchAvailibility.map( lastBatchResult => {
//    state.update(
//        (
//            activeCount + lastBatchResult.activeCounter,
//            inactiveCount + lastBatchResult.inactiveCounter, 
//            unknownCount + lastBatchResult.unknownCounter
//         )
//       ) 
//    })
//    
//    val (activeCountNew, inactiveCountNew, unknownCountNew) = state.get()
//    val allValues = activeCountNew + inactiveCountNew
//    val av = (activeCountNew * 1f)/ allValues
//    
//
//    Some((facId, av)) 
//  }
//}
