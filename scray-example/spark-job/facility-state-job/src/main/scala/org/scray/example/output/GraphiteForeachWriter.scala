package org.scray.example.output

import java.io.PrintWriter
import java.net.Socket

import org.apache.spark.sql.ForeachWriter

import com.typesafe.scalalogging.LazyLogging
import org.scray.example.data.FacilityStateCounter

/**
 * Write FacilityStateCounter to Graphite
 */
class GraphiteForeachWriter(graphiteHostname: String, port: Int, numConnectionRetries: Int = 20) extends ForeachWriter[FacilityStateCounter] with LazyLogging {

  var graphiteStream: PrintWriter = null
  var retriedNum = 0  // Number of executed retries

  def open(partitionId: Long, version: Long): Boolean = {
    initConnection
  }

  def process(record: FacilityStateCounter) = {
    
    val dataIn = s"bahn.equipment.type.${record.facilityType}.all.state.${record.state}.count ${record.count} ${System.currentTimeMillis() / 1000}\n"

    logger.debug(s"Write to graphite ${dataIn}")
    
    try {
      graphiteStream.printf(dataIn)
      graphiteStream.flush()

    } catch {
      case e: Exception => {
        logger.error(s"Error while writing to graphite: ${e.getMessage} ")
        
        if(numConnectionRetries >= retriedNum) {
          val sleepTime = 10 // Time in seconds
          logger.info(s"Try to connect to graphite ${graphiteHostname}:${port} in ${sleepTime}s. Retry ${retriedNum}/${numConnectionRetries}")
          Thread.sleep(sleepTime *1000)
          retriedNum += 1
          initConnection
        } else {
          logger.error(s"Maximum number of retries reached ${numConnectionRetries}")
          throw new RuntimeException(s"Unable to connect to graphite ${e.getMessage}")
        }
      }
    }
  }

  def close(errorOrNull: Throwable): Unit = {
    graphiteStream.close()
  }
  
  def initConnection = {
     try {
      val connection = new Socket(graphiteHostname, port).getOutputStream
      graphiteStream = new PrintWriter(connection, true)
      true
    } catch {
      case e: Exception => {
        logger.error(s"Unable to create connection to graphite ${e.getMessage}")
        e.printStackTrace();
        false
      }
    }
  }

}