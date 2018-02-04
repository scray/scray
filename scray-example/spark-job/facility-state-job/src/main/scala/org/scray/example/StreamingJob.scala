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
import org.apache.spark.streaming.Minutes
import java.util.Date
import scala.collection.mutable.LinkedList
import scala.collection.mutable.ListBuffer
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import com.google.common.collect.EvictingQueue
import slick.jdbc.H2Profile.api._
import scray.querying.sync.Column
import scray.querying.sync.SyncTableBasicClasses
import scray.querying.sync.Table
import scray.querying.sync.SyncTable
import scray.jdbc.sync.tables.DriverComponent
import slick.jdbc.JdbcProfile
import slick.jdbc.OracleProfile
import scala.util.Success
import scala.util.Failure
import scala.collection.mutable.HashMap
import scray.jdbc.sync.JDBCJobInfo
import scray.jdbc.sync.OnlineBatchSyncJDBC
import scray.jdbc.sync.JDBCDbSession


case class FacilityInWindow(facilityType: String, state: String, windowStartTime: Long)

@SerialVersionUID(1000L)
class StreamingJob(@transient val ssc: StreamingContext, conf: JobParameter) extends LazyLogging with Serializable {

  lazy val jsonParser = new JsonFacilityParser
  lazy val graphiteWriter = new GraphiteForeachWriter(conf.graphiteHost)

  def runTuple[T <: org.apache.spark.streaming.dstream.DStream[ConsumerRecord[String, String]]](dstream: T) = {

    var isFirstElement = true
    val endTimes = new HashMap[String, Long]
    
    val facilities = dstream.flatMap(facilitiesAsJson => {    
      jsonParser.parse(facilitiesAsJson.value()) // Parse json data and create Facility objects
      .map(fac => (fac.facilitytype + fac.state, fac)) // Create key value pairs
    })
      .mapWithState(StateSpec.function(createWindowKey _)) // Add window time stamp to key
      .map(facilityWindow => (facilityWindow, 1))

    facilities.reduceByKey(_ + _) // Count all elements in same window
      .mapWithState(StateSpec.function(updateWindows _)) // Add counter from previous batch
      .foreachRDD { rdd => // Write counted data to graphite
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
    state: State[EvictingQueue[Long]]): Option[FacilityInWindow] =  {

    val windowState =  state.getOption().getOrElse({
        val windowHistory =   EvictingQueue.create[Long](15000)    // Store some old windows to handle late events (locally)
        windowHistory.add(0)
        windowHistory
      })
      
    facilityIn.map(facility => {
      val window = getWindow(facility, windowState)
      state.update(windowState)
      
      FacilityInWindow(facility.facilitytype, facility.state, window / 1000)
    })
  }

  /**
   * Return new or existing window depending on the distance between current facility and existing windows
   */
  def getWindow(facility: Facility[Long], state: EvictingQueue[Long]): Long = {

    val oldWindowsIter = state.iterator()
    var lastOldWindow: Option[Long] = None;

    while (oldWindowsIter.hasNext()) {
      val oldWindow = oldWindowsIter.next()
      val distance = Math.abs(facility.timestamp - oldWindow)
      
      if (distance < conf.maxDistInWindow) {
        lastOldWindow = Some(oldWindow)
      }
    }

    // If no window was found create a new one
    lastOldWindow.getOrElse(
        {
          state.add(facility.timestamp)
          facility.timestamp
        }
       )
  }

  def updateWindows(
    batchTime: Time,
    facilityGroup: FacilityInWindow,
    count: Option[Int],
    state: State[Int]): Option[FacilityStateCounter] = {

    val newCount = count.getOrElse(0) + state.getOption().getOrElse(0)
    state.update(newCount)

    Some(FacilityStateCounter(facilityGroup.facilityType, facilityGroup.state, newCount, facilityGroup.windowStartTime))
  }
  
  @transient
  lazy val syncDbConnection = JDBCDbSession.getNewJDBCDbSession("jdbc:mariadb://127.0.0.1:3306/scray", "scray", "scray")

  def setFirstElementTime(id: String, time: Long) = {
    
//    val jobInfo = new JDBCJobInfo("FacilityStreamingJob")
//          syncDbConnection    
//          //JDBCDbSession.getNewJDBCDbSession("jdbc:h2:mem:test", "scray", "scray")
//            .map(new OnlineBatchSyncJDBC(_))
//            .map { syncTable => {
//                syncTable.initJobIfNotExists(jobInfo)
//                syncTable.startNextOnlineJob(jobInfo)
//                syncTable.setOnlineStartPoint(jobInfo, time, id) 
//                //syncTable.completeOnlineJob(jobInfo)
//              } 
//            }
//          
//          match {
//              case Success(lines) => 
//              case Failure(ex)    => println(s"Problem rendering URL content: ${ex.printStackTrace()}")
//            }
        }
  @transient
  lazy val connection =  JDBCDbSession.getNewJDBCDbSession("jdbc:mariadb://127.0.0.1:3306/scray", "scray", "scray")
  
  def getLastBatchElement(endTimes: HashMap[String, Long]) = {
    
        val jobInfo = new JDBCJobInfo("FacilityStreamingJob")
      
        connection
        //JDBCDbSession.getNewJDBCDbSession("jdbc:h2:mem:test", "scray", "scray")
        .map(new OnlineBatchSyncJDBC(_))
        .map { syncTable =>
          {
//              val onlineSlot = syncTable.getRunningOnlineJob(jobInfo).get.get
//              val data = syncTable.getOnlineStartPoint(jobInfo, onlineSlot)
//              
//              val iter = data.iterator
    
              
//              while(iter.hasNext) {
//                val nextStartPoint = iter.next()
//                val existingStartpint = endTimes.get(nextStartPoint.jobname)
//                
//                if(existingStartpint.isDefined) {
//                  if(existingStartpint.get > nextStartPoint.firstElementTime) {
//                    endTimes.put(nextStartPoint.jobname, nextStartPoint.firstElementTime)
//                  }
//                } else {
//                  endTimes.put(nextStartPoint.jobname, nextStartPoint.firstElementTime)
//                }
//                
//              }               
            
            
            
          }
          
    }
    endTimes
    }
}
