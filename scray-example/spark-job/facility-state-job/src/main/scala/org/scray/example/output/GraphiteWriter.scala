package org.scray.example.output

import java.io.DataOutputStream;
import java.net.Socket;
import java.io.PrintWriter
import org.apache.log4j.Logger

class GraphiteWriter(graphiteIP: String) {

  
  @transient lazy val conn = new Socket(graphiteIP, 2003);
  @transient lazy val dos = conn.getOutputStream
  @transient lazy val out = new PrintWriter(dos, true);

  @transient
  lazy val logger = Logger.getLogger(getClass.getName)

  def sentElevator(countIn: Integer, countAc: Integer) = {
    val dataIn = s"bahn.equipment.type.ELEVATOR.all.state.INACTIVE.count ${countIn} ${System.currentTimeMillis() / 1000}\n"
    val dataAc = s"bahn.equipment.type.ELEVATOR.all.state.ACTIVE.count ${countAc} ${System.currentTimeMillis() / 1000}\n"

    logger.debug(s"Write to graphite ${dataAc}")
    logger.debug(s"Write to graphite ${dataIn}")
    
    out.printf(dataIn)
    out.printf(dataAc)
    out.flush()
  }

  def sentEscalator(countIn: Integer, countAc: Integer) = {
    val dataIn = s"bahn.equipment.type.ESCALATOR.all.state.INACTIVE.count ${countIn} ${System.currentTimeMillis() / 1000}\n"
    val dataAc = s"bahn.equipment.type.ESCALATOR.all.state.ACTIVE.count ${countAc} ${System.currentTimeMillis() / 1000}\n"

    logger.info(s"Write to graphite ${dataAc}")
    logger.info(s"Write to graphite ${dataIn}")   
 
    out.printf(dataIn)
    out.printf(dataAc)

    out.flush()
  }
  


  def close = {
    conn.close();
  }
}
