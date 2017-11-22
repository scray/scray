package org.scray.example.output

import org.apache.spark.sql.ForeachWriter
import java.io.DataOutputStream;
import java.net.Socket;
import java.io.PrintWriter
import org.apache.log4j.Logger
import com.typesafe.scalalogging.LazyLogging
import org.scray.example.FacilityStateCounter

/**
 * Write FacilityStateCounter to Graphite
 */
class GraphiteForeachWriter(graphiteIP: String, port: Int) extends ForeachWriter[FacilityStateCounter] with LazyLogging {

  var graphiteStream: PrintWriter = null

  def open(partitionId: Long, version: Long): Boolean = {

    try {
      val connection = new Socket(graphiteIP, port).getOutputStream
      graphiteStream = new PrintWriter(connection, true);
      true
    } catch {
      case e: Exception => {
        logger.error(s"Unable to create connection to graphite ${e.getMessage}")
        e.printStackTrace();
        false
      }
    }
  }

  def process(record: FacilityStateCounter) = {
    
    val dataIn = s"bahn.equipment.type.${record.facilityType}.all.state.${record.state}.count ${record.count} ${System.currentTimeMillis() / 1000}\n"

    logger.debug(s"Write to graphite ${dataIn}")

    graphiteStream.printf(dataIn)
    graphiteStream.flush()
  }

  def close(errorOrNull: Throwable): Unit = {
    graphiteStream.close()
  }

}