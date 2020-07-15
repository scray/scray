package com.seeburger.research.cloud.ai

import java.util.concurrent.locks.ReentrantLock

import scala.collection.convert.decorateAsScala.asScalaIteratorConverter
import scala.collection.convert.decorateAsScala.asScalaBufferConverter
import scala.collection.mutable.HashMap
import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import com.seeburger.research.cloud.ai.data.AggregationKey
import com.typesafe.scalalogging.LazyLogging


/**
 * job that transforms data :)
 */
class StreamingJob(@transient val ssc: StreamingContext) extends LazyLogging with Serializable {
  
  /**
   * Print received strings and count them
   */
   def runTuple[T <:  DStream[String]](dstream: T, host: Option[String], keyspace: Option[String], master: String) = {  
     dstream.map(data => {println(data); data}).count().print()    
   }
}

object StreamingJob {
  
  /**
   * setup a key for the aggregation function using the key class defined in data
   */
  def buildAggregationKey(row: (String, String, String, String)): Option[AggregationKey] = {
    Some(AggregationKey(row._1, row._2, row._3, row._4))
  }

  /**
   * TODO: transform data into common tuple format for storage
   */
  def saveDataMap(data: (AggregationKey, Long)): (String, String, String, String, Long) =
    (data._1.access, data._1.typ, data._1.category, data._1.direction, data._2)
}
